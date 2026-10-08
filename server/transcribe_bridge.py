"""Transcribe bridge (CQ-4): one Cobalt Transcribe stream per player per question.

Life of a stream (``Stream``, one background task each):

- ``question_started`` opens one per player in the room: connect, send the
  config, then wait for audio. Connecting takes 1-4 s, so doing it here keeps
  it out of the answer latency. ``hold_start`` opens one for a player who has
  none (joined mid-round, or their stream died before they pressed hold).
- Audio chunks go into the stream's queue. Chunks that arrive before the
  stream is connected and configured wait there and are sent in order.
- ``hold_end`` queues the empty end-of-audio message and starts a
  ``FINAL_TIMEOUT`` clock.
- Partial results go to ``room.on_partial`` (finals so far + current partial).
  Transcribe may send several finals per stream ("um", "Paris."); they are
  joined in order and reported once, with ``room.on_final``, at the first of:
  - quiet gap: a non-empty final has arrived after the end of audio was sent,
    and then no result (partial or final) for ``quiet_gap`` seconds
    (``FINAL_QUIET_GAP``, 0.5 s). The demo server only closes the stream
    ~2 s after its last result, so this saves the player those 2 s;
  - close: the server closes the stream normally (see below);
  - timeout: the clock runs out. The finals received so far are reported; if
    there are none, the player gets an empty final with ``transcribe_timeout``.
  After a quiet-gap report the stream is read until it closes (or the clock
  runs out); finals arriving then are not reported again but counted in
  ``late_finals``, to show whether the gap is too short.
- A close is normal only after the end of audio was sent and with code 1000
  or 1006 (the demo server always drops the connection: 1006). A close before
  the end of audio, or with any other code, is a ``transcribe_error``.
- Errors (connect failure, demo-server cap, ``{"error": ...}``, an abnormal
  close, a failure sending audio) are logged and recorded in the metrics, and
  the player gets a final with an error code (text: whatever was recognised,
  usually ""), which the room does not score. Every answer gets an outcome
  and the room carries on.
- ``round_ended`` closes streams nobody pressed hold on. Streams that got
  hold_end finish by themselves, at most ``FINAL_TIMEOUT`` later. The next
  ``question_started`` and ``player_left`` close whatever is left.

Demo-server guard: when ``TRANSCRIBE_URL`` points at demo.cobaltspeech.com, at
most ``DEMO_MAX_STREAMS`` streams hold a slot (connecting or open) in this
process. A stream that finds the cap full waits up to ``CONNECT_TIMEOUT`` for
a slot (e.g. while last round's streams finish); if none frees it is
refused: logged, recorded as ``refused``, and the player's answer gets
``transcribe_unavailable`` (hold_start retries). A stream closed by the bridge
gives its slot back at once, while its socket is still closing (at most
``CLOSE_TIMEOUT``). ``ALLOW_DEMO_LOAD=1`` lifts the cap.

``bridge.metrics`` holds one ``StreamMetrics`` per stream, for CQ-10.
"""
import asyncio
import base64
import json
import logging
import ssl
import time
from dataclasses import dataclass, field
from urllib.parse import urlparse

import certifi
import websockets

from . import config
from .bridge import Bridge

log = logging.getLogger("quiz.transcribe")

FINAL_TIMEOUT = 10.0    # seconds after hold_end to wait for the stream to finish
CONNECT_TIMEOUT = 10.0  # also the longest wait for a free slot under the demo cap
CLOSE_TIMEOUT = 1.0     # the demo server drops connections; don't wait long for a close frame
DEMO_HOST = "demo.cobaltspeech.com"
DEMO_MAX_STREAMS = 4
NORMAL_CLOSE_CODES = (1000, 1006)  # 1006: the demo server never sends a close frame

# Error codes sent to the player in "final" (see docs/protocol.md).
ERR_UNAVAILABLE = "transcribe_unavailable"  # no stream: connect failed or demo cap reached
ERR_FAILED = "transcribe_error"             # Transcribe sent an error or the stream failed
ERR_TIMEOUT = "transcribe_timeout"          # nothing recognised within FINAL_TIMEOUT of hold_end

END_OF_AUDIO = json.dumps({"audio": {"data": ""}})


def stream_config(model: str) -> dict:
    return {"config": {"model_id": model, "audio_format_raw": {
        "encoding": "AUDIO_ENCODING_SIGNED", "bit_depth": 16,
        "byte_order": "BYTE_ORDER_LITTLE_ENDIAN", "sample_rate": config.SAMPLE_RATE, "channels": 1}}}


class StreamLimit:
    """Cap on streams holding a slot at once; acquire waits for a free one."""

    def __init__(self, max_streams: int):
        self.max = max_streams
        self.open = 0
        self._waiters: list[asyncio.Event] = []

    async def acquire(self, timeout: float) -> bool:
        """Take a slot, waiting up to ``timeout`` seconds. False if none freed."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while self.open >= self.max:
            remaining = deadline - loop.time()
            if remaining <= 0:
                return False
            freed = asyncio.Event()
            self._waiters.append(freed)
            try:
                await asyncio.wait_for(freed.wait(), remaining)
            except TimeoutError:
                return False
            finally:
                self._waiters.remove(freed)
        self.open += 1
        return True

    def release(self):
        self.open -= 1
        for freed in self._waiters:
            freed.set()  # each waiter re-checks; whoever runs first gets the slot


DEMO_LIMIT = StreamLimit(DEMO_MAX_STREAMS)  # process-wide, shared by every bridge


def default_limit(url: str) -> StreamLimit | None:
    # Matches the host name only: the demo server's IP address, or another
    # name for it, would bypass the cap. No DNS lookup; that's good enough.
    host = (urlparse(url).hostname or "").lower().rstrip(".")
    if host == DEMO_HOST and not config.ALLOW_DEMO_LOAD:
        return DEMO_LIMIT
    return None


class StreamError(Exception):
    def __init__(self, code: str, detail: str, outcome: str = "error"):
        super().__init__(detail)
        self.code = code
        self.detail = detail
        self.outcome = outcome


@dataclass
class StreamMetrics:
    """Timeline of one stream (one player in one round), for CQ-10.

    Times are ``time.monotonic()`` seconds, None if it didn't happen.
    ``wall_start`` is ``time.time()`` when the stream was requested.

    The player's wait splits into three parts:
    ``answer_latency_s = send_backlog_s + end_to_final_s + quiet_wait_s``."""
    room: str
    round: int
    player_id: str
    player: str
    wall_start: float
    open_requested: float
    slot_acquired: float | None = None     # got a slot under the demo cap (or no cap)
    connected: float | None = None         # WebSocket to Transcribe open
    config_sent: float | None = None       # stream ready for audio
    hold_start: float | None = None        # player pressed hold
    ready_at_hold_start: bool | None = None  # stream was ready when the player pressed hold
    first_audio: float | None = None       # first chunk from the player reached the bridge
    first_audio_sent: float | None = None  # ... and was sent to Transcribe (later if buffered)
    hold_end: float | None = None          # player released, or the round cut them off
    backlog_at_release_s: float | None = None  # audio not yet sent to Transcribe at hold_end
    end_sent: float | None = None          # empty audio message sent to Transcribe
    first_partial: float | None = None
    last_final: float | None = None        # last non-empty final that went into the answer
    reported: float | None = None          # room.on_final called
    closed: float | None = None            # stream finished (closed, failed or cancelled)
    close_code: int | None = None          # WebSocket close code from Transcribe
    close_reason: str | None = None
    audio_bytes: int = 0
    buffered_chunks: int = 0               # chunks that arrived before the config was sent
    partials: int = 0
    finals: int = 0                        # final results received (empty ones included)
    transcript: str | None = None          # what was reported to the room
    report_trigger: str | None = None      # quiet_gap | close | timeout | error
    late_finals: int = 0                   # non-empty finals after a quiet-gap report
    late_final_texts: list[str] = field(default_factory=list)
    outcome: str = "open"  # final | timeout | error | refused | unused | cancelled | replaced
    error: str | None = None               # error code (ERR_*) reported to the player
    error_detail: str | None = None
    post_report_error: str | None = None   # trouble after the answer was already reported

    @staticmethod
    def _span(start, end):
        return end - start if start is not None and end is not None else None

    @property
    def connect_s(self):
        """Stream requested -> WebSocket open (includes any wait for a slot)."""
        return self._span(self.open_requested, self.connected)

    @property
    def answer_latency_s(self):
        """Player released -> final reported: what the player waits for."""
        return self._span(self.hold_end, self.reported)

    @property
    def send_backlog_s(self):
        """Player released -> end of audio sent: buffered audio still going out
        (large when the stream connected after the player started speaking)."""
        return self._span(self.hold_end, self.end_sent)

    @property
    def end_to_final_s(self):
        """End of audio sent -> last final. Transcribe's own answer latency
        only for a live stream (``ready_at_hold_start`` and no backlog);
        otherwise it includes catching up on audio that was sent in a burst."""
        return self._span(self.end_sent, self.last_final)

    @property
    def quiet_wait_s(self):
        """Last final -> reported: the quiet gap (or the wait for the close)."""
        return self._span(self.last_final, self.reported)

    @property
    def close_delay_s(self):
        """Answer reported -> stream closed (what the quiet gap saves)."""
        return self._span(self.reported, self.closed)

    @property
    def first_partial_s(self):
        """First audio sent -> first partial."""
        return self._span(self.first_audio_sent, self.first_partial)


class Stream:
    """One Transcribe stream for one player in one round."""

    def __init__(self, bridge: "TranscribeBridge", room, rnd, player):
        self.bridge, self.room, self.rnd, self.player = bridge, room, rnd, player
        self.m = StreamMetrics(room.code, rnd.number, player.id, player.name,
                               time.time(), time.monotonic())
        self.queue: asyncio.Queue = asyncio.Queue()  # PCM chunks; None = end of audio
        self.unsent_bytes = 0  # queued, not yet sent to Transcribe
        self.ready = False     # connected and config sent
        self.held = False      # hold_start seen
        self.ended = False     # hold_end seen
        self.finals: list[str] = []
        self.result: tuple[str, str | None] | None = None  # (text, error) once known
        self.reported = False  # on_final called, or nothing to report any more
        self.trigger: str | None = None     # what _finish would report as
        self.slot = False      # holds a slot of bridge.limit
        self._quiet: asyncio.TimerHandle | None = None  # quiet-gap timer
        self.deadline: float | None = None  # loop time; set by hold_end
        self._timeout: asyncio.Timeout | None = None
        self.task = asyncio.create_task(self.run(), name=f"transcribe {room.code} r{rnd.number} {player.id}")

    # --- called by the bridge hooks ------------------------------------------

    def hold_start(self):
        self.held = True
        self.m.hold_start = time.monotonic()
        self.m.ready_at_hold_start = self.ready

    def audio(self, chunk: bytes):
        if self.ended:
            return
        if self.m.first_audio is None:
            self.m.first_audio = time.monotonic()
        self.m.audio_bytes += len(chunk)
        if not self.ready:
            self.m.buffered_chunks += 1
        self.unsent_bytes += len(chunk)
        self.queue.put_nowait(chunk)

    def end(self):
        if self.ended:
            return
        self.ended = True
        self.m.hold_end = time.monotonic()
        self.m.backlog_at_release_s = self.unsent_bytes / config.BYTES_PER_SEC
        self.queue.put_nowait(None)
        self.deadline = asyncio.get_running_loop().time() + self.bridge.final_timeout
        if self._timeout is not None and not self.task.done():
            self._timeout.reschedule(self.deadline)  # run() is inside the timeout block
        self._report()

    def close(self, outcome: str):
        """Stop the stream without reporting a result (round over, player gone)."""
        if self.task.done():
            return
        if not self.reported:
            self.m.outcome = outcome
        self.reported = True  # nothing (more) to report
        self._release_slot()  # at once: the next question's streams may be waiting
        self.task.cancel()

    def _release_slot(self):
        if self.slot:
            self.slot = False
            self.bridge.limit.release()

    # --- the task ------------------------------------------------------------

    async def run(self):
        timeout = asyncio.timeout_at(self.deadline)  # None until hold_end sets it
        self._timeout = timeout
        try:
            async with timeout:
                limit = self.bridge.limit
                if limit is not None:
                    if not await limit.acquire(self.bridge.slot_wait):
                        raise StreamError(ERR_UNAVAILABLE, f"demo server cap of {limit.max} streams "
                                          "still full (set ALLOW_DEMO_LOAD=1 to lift it)", "refused")
                    self.slot = True
                self.m.slot_acquired = time.monotonic()
                await self._stream()
            self._finish("final")
        except TimeoutError as e:
            if timeout.expired():
                self._finish("timeout", None if self.finals else ERR_TIMEOUT,
                             f"no end of stream {self.bridge.final_timeout:.0f} s after hold_end")
            else:
                self._finish("error", ERR_FAILED, repr(e))
        except StreamError as e:
            self._finish(e.outcome, e.code, e.detail)
        except asyncio.CancelledError:
            raise  # closed by the bridge; close() recorded why
        except Exception as e:
            log.exception("stream %s/%s round %d failed", self.room.code, self.player.name, self.rnd.number)
            self._finish("error", ERR_FAILED, repr(e))
        finally:
            if self._quiet is not None:
                self._quiet.cancel()
            self._release_slot()
            self.m.closed = time.monotonic()
            self._log()

    async def _stream(self):
        try:
            ws = await websockets.connect(self.bridge.url, ssl=self.bridge.ssl, open_timeout=CONNECT_TIMEOUT,
                                          close_timeout=CLOSE_TIMEOUT, max_size=None)
        except (OSError, TimeoutError, websockets.InvalidHandshake, websockets.InvalidURI) as e:
            raise StreamError(ERR_UNAVAILABLE, f"connect failed: {e!r}")
        async with ws:
            self.m.connected = time.monotonic()
            await ws.send(json.dumps(stream_config(self.bridge.model)))
            self.m.config_sent = time.monotonic()
            self.ready = True
            writer = asyncio.create_task(self._write(ws))
            reader = asyncio.create_task(self._read(ws))
            try:
                while not reader.done():
                    await asyncio.wait({reader} if writer.done() else {reader, writer},
                                       return_when=asyncio.FIRST_COMPLETED)
                    if writer.done() and not writer.cancelled() and writer.exception() is not None:
                        raise StreamError(ERR_FAILED, f"sending audio failed: {writer.exception()!r}")
                reader.result()  # raises the StreamError for an {"error": ...} message
            finally:
                writer.cancel()
                reader.cancel()
                await asyncio.gather(writer, reader, return_exceptions=True)
        self.m.close_code, self.m.close_reason = ws.close_code, ws.close_reason or None
        if self.m.end_sent is None:
            raise StreamError(ERR_FAILED, f"stream closed before the end of the audio (code {ws.close_code})")
        if ws.close_code not in NORMAL_CLOSE_CODES:
            raise StreamError(ERR_FAILED, f"stream closed with code {ws.close_code} {ws.close_reason!r}")

    async def _write(self, ws):
        try:
            while True:
                chunk = await self.queue.get()
                if chunk is None:
                    await ws.send(END_OF_AUDIO)
                    self.m.end_sent = time.monotonic()
                    return
                await ws.send(json.dumps({"audio": {"data": base64.b64encode(chunk).decode()}}))
                self.unsent_bytes -= len(chunk)
                if self.m.first_audio_sent is None:
                    self.m.first_audio_sent = time.monotonic()
        except websockets.ConnectionClosed:
            pass  # the reader sees it too

    async def _read(self, ws):
        try:
            async for message in ws:
                msg = json.loads(message)
                err = msg.get("error")
                if err:
                    raise StreamError(ERR_FAILED, str(err.get("message", err) if isinstance(err, dict) else err))
                result = (msg.get("result") or {}).get("result")
                if not result or not result.get("alternatives"):
                    continue
                text = result["alternatives"][0].get("transcript_formatted", "").strip()
                partial = bool(result.get("is_partial"))
                if self.reported:  # answer already given at the quiet gap
                    if not partial and text:
                        self.m.late_finals += 1
                        self.m.late_final_texts.append(text)
                        log.info("stream %s/%s round %d: late final %r after the quiet gap",
                                 self.room.code, self.player.name, self.rnd.number, text)
                    continue
                now = time.monotonic()
                if partial:
                    self.m.partials += 1
                    if self.m.first_partial is None:
                        self.m.first_partial = now
                    self._safe(self.room.on_partial, self.rnd, self.player, " ".join(self.finals + [text]).strip())
                else:
                    self.m.finals += 1
                    if text:
                        self.m.last_final = now
                        self.finals.append(text)
                # The quiet gap starts at the first non-empty final after the end
                # of audio; every later result (more speech) restarts it.
                if self.m.end_sent is not None and (self._quiet is not None or (not partial and text)):
                    self._arm_quiet()
        except websockets.ConnectionClosed:
            pass  # _stream checks the close code

    def _arm_quiet(self):
        if self._quiet is not None:
            self._quiet.cancel()
        self._quiet = asyncio.get_running_loop().call_later(self.bridge.quiet_gap, self._on_quiet)

    def _on_quiet(self):
        """No result for quiet_gap seconds after a final: the answer is complete."""
        self._quiet = None
        if not self.reported and self.finals:
            self.m.transcript = " ".join(self.finals)
            self.m.outcome = "final"
            self.result = (self.m.transcript, None)
            self._report("quiet_gap")

    # --- results -------------------------------------------------------------

    def _finish(self, outcome: str, error: str | None = None, detail: str | None = None):
        """The stream is over (closed, failed or timed out)."""
        if self._quiet is not None:
            self._quiet.cancel()
            self._quiet = None
        if self.reported:  # answered at the quiet gap: keep that outcome
            if error or outcome != "final":
                self.m.post_report_error = f"{outcome}: {error or ''} {detail or ''}".strip()
                log.info("stream %s/%s round %d after its answer: %s", self.room.code,
                         self.player.name, self.rnd.number, self.m.post_report_error)
            return
        self.m.outcome, self.m.error, self.m.error_detail = outcome, error, detail
        if error:
            log.warning("stream %s/%s round %d: %s (%s)", self.room.code, self.player.name,
                        self.rnd.number, error, detail)
        self.m.transcript = " ".join(self.finals)
        self.result = (self.m.transcript, error)
        self.trigger = "timeout" if outcome == "timeout" else "error" if error else "close"
        self._report(self.trigger)

    def _report(self, trigger: str | None = None):
        """Give the room the result once there is one and the answer has ended."""
        if self.result is None or not self.ended or self.reported:
            return
        self.reported = True
        self.m.reported = time.monotonic()
        self.m.report_trigger = trigger or self.trigger
        text, error = self.result
        self._safe(self.room.on_final, self.rnd, self.player, text, error)

    def _safe(self, fn, *args):
        try:
            fn(*args)
        except Exception:
            log.exception("room %s: %s failed", self.room.code, fn.__name__)

    def _log(self):
        m = self.m

        def s(v):
            return f"{v:.2f}s" if v is not None else "-"
        log.info("stream %s/%s round %d %s (reported on %s): connect %s, answer %s = backlog %s + "
                 "end-to-final %s + quiet %s, close delay %s, code %s, %d B, %r, %d late finals",
                 m.room, m.player, m.round, m.outcome, m.report_trigger, s(m.connect_s),
                 s(m.answer_latency_s), s(m.send_backlog_s), s(m.end_to_final_s), s(m.quiet_wait_s),
                 s(m.close_delay_s), m.close_code, m.audio_bytes, m.transcript, m.late_finals)


class TranscribeBridge(Bridge):
    """Streams each player's answer to Cobalt Transcribe. See the module docstring."""

    def __init__(self, url: str | None = None, model: str | None = None,
                 limit: StreamLimit | None | str = "default", final_timeout: float = FINAL_TIMEOUT,
                 quiet_gap: float | None = None, slot_wait: float = CONNECT_TIMEOUT):
        self.url = url or config.TRANSCRIBE_URL
        self.model = model or config.TRANSCRIBE_MODEL
        self.limit = default_limit(self.url) if limit == "default" else limit
        self.final_timeout = final_timeout
        self.quiet_gap = config.FINAL_QUIET_GAP if quiet_gap is None else quiet_gap
        self.slot_wait = slot_wait
        self.ssl = ssl.create_default_context(cafile=certifi.where()) if self.url.startswith("wss:") else None
        self.streams: dict[tuple[str, str], Stream] = {}  # (room code, player id) -> current stream
        self.tasks: set[asyncio.Task] = set()             # every stream task still running
        self.metrics: list[StreamMetrics] = []

    def open_count(self) -> int:
        return len(self.tasks)

    def _open(self, room, rnd, player) -> Stream:
        key = (room.code, player.id)
        old = self.streams.get(key)
        if old is not None:
            old.close("replaced")
        stream = Stream(self, room, rnd, player)
        self.streams[key] = stream
        self.metrics.append(stream.m)
        self.tasks.add(stream.task)
        stream.task.add_done_callback(lambda task: self._task_done(key, stream))
        return stream

    def _task_done(self, key, stream: Stream):
        self.tasks.discard(stream.task)
        if stream.m.closed is None:  # cancelled before it started
            stream.m.closed = time.monotonic()
        if stream.m.outcome == "open":
            stream.m.outcome = "cancelled"
        self._forget(key, stream)

    def _forget(self, key, stream: Stream):
        """Drop a finished stream unless hold_end still needs it to report."""
        if self.streams.get(key) is stream and stream.task.done() and (stream.reported or not stream.held):
            del self.streams[key]

    def _current(self, room, rnd, player) -> Stream | None:
        stream = self.streams.get((room.code, player.id))
        return stream if stream is not None and stream.rnd is rnd else None

    # --- hooks ---------------------------------------------------------------

    async def question_started(self, room, rnd):
        # Close last round's streams first: that frees their slots for the new ones.
        for key, stream in list(self.streams.items()):
            if key[0] == room.code:
                stream.close("cancelled")
                del self.streams[key]
        for player in list(room.players.values()):
            self._open(room, rnd, player)

    async def hold_start(self, room, rnd, player):
        stream = self._current(room, rnd, player)
        if stream is None or stream.task.done():
            stream = self._open(room, rnd, player)
        stream.hold_start()

    async def audio(self, room, rnd, player, chunk: bytes):
        stream = self._current(room, rnd, player)
        if stream is not None:
            stream.audio(chunk)

    async def hold_end(self, room, rnd, player):
        stream = self._current(room, rnd, player)
        if stream is None:
            log.warning("room %s: hold_end for %s without a stream", room.code, player.name)
            room.on_final(rnd, player, "", ERR_UNAVAILABLE)
            return
        stream.end()
        self._forget((room.code, player.id), stream)

    async def round_ended(self, room, rnd):
        for key, stream in list(self.streams.items()):
            if key[0] == room.code and stream.rnd is rnd and not stream.held:
                stream.close("unused")
                del self.streams[key]

    async def player_left(self, room, player):
        stream = self.streams.pop((room.code, player.id), None)
        if stream is not None:
            stream.close("cancelled")

    async def aclose(self):
        """Close every stream and wait for them (server shutdown, tests)."""
        for stream in list(self.streams.values()):
            stream.close("cancelled")
        self.streams.clear()
        tasks = list(self.tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
