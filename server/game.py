"""Rooms, players and rounds, held in memory on the server's event loop.

Concurrency: everything runs on one asyncio loop, so code between two awaits
never interleaves with another player's. Round transitions (start, end) take
the room lock, so the round timer and the host's "end round" can't both end
a round. Sending never blocks game logic: each connection has an outbox queue
drained by its own writer task (see ``Client``).

Bridge hooks are called through ``Room._hook``, which logs and swallows their
exceptions: a failing Transcribe bridge must not stall a round or close a room.
"""
import asyncio
import logging
import random
from dataclasses import dataclass, field

from . import scoring
from .bridge import Bridge
from .config import MAX_ANSWER_BYTES, MAX_CHUNK_BYTES
from .questions import Question

log = logging.getLogger("quiz.game")

ROOM_CODE_LETTERS = "ABCDEFGHJKLMNPQRSTUVWXYZ"  # no I or O: easy to read off a projector
MAX_NAME_LEN = 24
OUTBOX_LIMIT = 500  # messages queued for one client before we decide it's too slow
CLOSE_ROOM_CLOSED = 4000
CLOSE_TOO_SLOW = 4001
CLOSE_TIMEOUT = 5  # seconds to wait for a close frame to go out


class GameError(Exception):
    """A client did something not allowed right now. Sent back as an error message."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


class Client:
    """Outgoing side of one WebSocket: ``send`` queues, a writer task delivers in order.

    A client that falls ``OUTBOX_LIMIT`` messages behind is disconnected
    (close code 4001) rather than silently missing messages.
    """

    CLOSE = object()

    def __init__(self, ws):
        self.ws = ws
        self.outbox: asyncio.Queue = asyncio.Queue(maxsize=OUTBOX_LIMIT)
        self.closed = False  # no more messages will be sent
        self.writer: asyncio.Task | None = None

    def start(self):
        self.writer = asyncio.create_task(self._run_writer())

    def stop(self):
        if self.writer:
            self.writer.cancel()

    def send(self, msg: dict):
        if self.closed:
            return
        try:
            self.outbox.put_nowait(msg)
        except asyncio.QueueFull:
            log.warning("client %d messages behind, disconnecting", OUTBOX_LIMIT)
            self.abort(CLOSE_TOO_SLOW)

    def close(self, code: int = 1000):
        """Close the socket after everything already queued has been sent."""
        if self.closed:
            return
        self.closed = True
        try:
            self.outbox.put_nowait((self.CLOSE, code))
        except asyncio.QueueFull:
            self.abort(code)

    def abort(self, code: int):
        """Close now, dropping whatever is still queued."""
        self.closed = True
        self.stop()
        self.writer = asyncio.create_task(self._close(code))

    async def _close(self, code: int):
        try:
            await asyncio.wait_for(self.ws.close(code=code), CLOSE_TIMEOUT)
        except Exception as e:
            log.debug("close failed: %r", e)

    async def _run_writer(self):
        try:
            while True:
                msg = await self.outbox.get()
                if isinstance(msg, tuple) and msg[0] is self.CLOSE:
                    await self.ws.close(code=msg[1])
                    return
                await self.ws.send_json(msg)
        except Exception as e:  # socket gone; the read loop sees the disconnect and cleans up
            log.debug("writer stopped: %r", e)


@dataclass
class Player:
    id: str
    name: str
    client: Client
    score: int = 0


@dataclass
class Answer:
    """One player's answer in one round. Times are seconds since the question started."""
    hold_start: float
    hold_end: float | None = None
    cut_off: bool = False  # still holding when the round ended; the server ended it
    audio_bytes: int = 0
    chunks: int = 0
    transcript: str | None = None
    final_at: float | None = None
    correct: bool | None = None
    points: int = 0

    @property
    def holding(self) -> bool:
        return self.hold_end is None


@dataclass
class Round:
    number: int
    index: int  # position in the question list
    question: Question
    time_limit: float
    started_at: float  # loop.time()
    answers: dict[str, Answer] = field(default_factory=dict)  # by player id
    open: bool = True
    timer: asyncio.Task | None = None

    def elapsed(self) -> float:
        return asyncio.get_running_loop().time() - self.started_at


class Room:
    def __init__(self, code: str, host: Client, questions: list[Question], bridge: Bridge,
                 round_seconds: float):
        self.code = code
        self.host = host
        self.questions = questions
        self.bridge = bridge
        self.round_seconds = round_seconds
        self.players: dict[str, Player] = {}
        self.round: Round | None = None
        self.next_index = 0
        self.lock = asyncio.Lock()
        self._next_player = 0

    async def _hook(self, name: str, *args):
        try:
            await getattr(self.bridge, name)(self, *args)
        except Exception:
            log.exception("room %s: bridge.%s failed", self.code, name)

    # --- sending -----------------------------------------------------------

    def to_players(self, msg: dict):
        for p in self.players.values():
            p.client.send(msg)

    def to_everyone(self, msg: dict):
        self.host.send(msg)
        self.to_players(msg)

    def leaderboard_msg(self) -> dict:
        """Ranking by score (ties share a rank, listed by name), with each
        player's points in the current (or last) round."""
        ranked = sorted(self.players.values(), key=lambda p: (-p.score, p.name.lower()))
        answers = self.round.answers if self.round else {}
        rows, rank = [], 0
        for i, p in enumerate(ranked):
            if i == 0 or p.score != ranked[i - 1].score:
                rank = i + 1
            a = answers.get(p.id)
            rows.append({"player_id": p.id, "name": p.name, "score": p.score, "rank": rank,
                         "round_points": a.points if a else 0})
        return {"type": "leaderboard", "round": self.round.number if self.round else None,
                "players": rows}

    def question_msg(self, rnd: Round) -> dict:
        return {"type": "question", "round": rnd.number, "question_id": rnd.question.id,
                "index": rnd.index, "total": len(self.questions), "text": rnd.question.text,
                "hint": rnd.question.hint, "time_limit": rnd.time_limit,
                "remaining": round(max(0.0, rnd.time_limit - rnd.elapsed()), 2)}

    # --- players -----------------------------------------------------------

    def add_player(self, name: str, client: Client) -> Player:
        name = " ".join(str(name).split())
        if not name or len(name) > MAX_NAME_LEN:
            raise GameError("bad_name", f"name must be 1-{MAX_NAME_LEN} characters")
        if any(p.name.lower() == name.lower() for p in self.players.values()):
            raise GameError("name_taken", f"someone in this room is already called {name!r}")
        self._next_player += 1
        player = Player(f"p{self._next_player}", name, client)
        self.players[player.id] = player
        client.send({"type": "joined", "room": self.code, "player_id": player.id, "name": name})
        self.host.send({"type": "player_joined", "player_id": player.id, "name": name,
                        "count": len(self.players)})
        if self.round and self.round.open:
            client.send(self.question_msg(self.round))
        log.info("room %s: %s joined (%d players)", self.code, name, len(self.players))
        return player

    async def remove_player(self, player: Player):
        if self.players.pop(player.id, None) is None:
            return
        self.host.send({"type": "player_left", "player_id": player.id, "name": player.name,
                        "count": len(self.players)})
        log.info("room %s: %s left (%d players)", self.code, player.name, len(self.players))
        self.to_everyone(self.leaderboard_msg())  # so boards drop them
        await self._hook("player_left", player)

    # --- rounds ------------------------------------------------------------

    async def start_question(self, index: int | None = None, time_limit: float | None = None):
        async with self.lock:
            if self.round and self.round.open:
                raise GameError("round_in_progress", "end the current round first")
            if index is None:
                index = self.next_index
            index %= len(self.questions)  # wraps, so long load-test runs never run out
            time_limit = float(time_limit or self.round_seconds)
            number = self.round.number + 1 if self.round else 1
            loop = asyncio.get_running_loop()
            rnd = Round(number, index, self.questions[index], time_limit, loop.time())
            self.round = rnd
            self.next_index = index + 1
            self.to_everyone(self.question_msg(rnd))
            rnd.timer = asyncio.create_task(self._round_timer(rnd))
            await self._hook("question_started", rnd)
            log.info("room %s round %d: %s", self.code, number, rnd.question.text)

    async def _round_timer(self, rnd: Round):
        await asyncio.sleep(rnd.time_limit)
        await self.end_round("timeout", rnd)

    async def end_round(self, reason: str, rnd: Round | None = None):
        """End the open round. ``rnd`` pins which round, so a late timer can't end the next one.

        Answers still being held are ended here (bridge.hold_end), so every
        Transcribe stream gets its end-of-audio message."""
        async with self.lock:
            rnd = rnd or self.round
            if rnd is None or not rnd.open or rnd is not self.round:
                if reason == "host":
                    raise GameError("no_round", "no round is running")
                return
            rnd.open = False
            if rnd.timer and rnd.timer is not asyncio.current_task():
                rnd.timer.cancel()
            cut_off = []
            for pid, answer in rnd.answers.items():
                if answer.holding:
                    answer.hold_end = rnd.elapsed()
                    answer.cut_off = True
                    if pid in self.players:
                        cut_off.append(self.players[pid])
            # Tell clients first, so a slow or failing bridge can't hold up round_end.
            self.to_everyone(self.round_end_msg(rnd, reason))
            self.to_everyone(self.leaderboard_msg())
            for player in cut_off:
                await self._hook("hold_end", rnd, player)
            await self._hook("round_ended", rnd)

    def round_end_msg(self, rnd: Round, reason: str) -> dict:
        results = [{"player_id": pid, "name": self.players[pid].name, "transcript": a.transcript,
                    "correct": a.correct, "points": a.points}
                   for pid, a in rnd.answers.items() if pid in self.players]
        return {"type": "round_end", "round": rnd.number, "question_id": rnd.question.id,
                "reason": reason, "answer": rnd.question.answers[0],
                "answered": len(results), "players": len(self.players), "results": results}

    # --- answers (player -> server) ----------------------------------------

    async def hold_start(self, player: Player):
        rnd = self.round
        if rnd is None or not rnd.open:
            raise GameError("no_round", "no question is open")
        if player.id in rnd.answers:
            raise GameError("already_answered", "one answer per question")
        rnd.answers[player.id] = Answer(hold_start=rnd.elapsed())
        await self._hook("hold_start", rnd, player)

    async def audio(self, player: Player, chunk: bytes):
        if not chunk:
            # An empty chunk would read as end-of-audio to Transcribe; hold_end does that.
            raise GameError("bad_message", "empty audio chunk")
        rnd = self.round
        if rnd is None or not rnd.open:
            raise GameError("no_round", "the round has ended")
        answer = rnd.answers.get(player.id)
        if answer is None or not answer.holding:
            raise GameError("not_holding", "send hold_start first")
        if len(chunk) > MAX_CHUNK_BYTES:
            raise GameError("audio_too_large", f"audio chunks must be <= {MAX_CHUNK_BYTES} bytes")
        if answer.audio_bytes + len(chunk) > MAX_ANSWER_BYTES:
            raise GameError("audio_too_large", "answer is too long")
        answer.audio_bytes += len(chunk)
        answer.chunks += 1
        await self._hook("audio", rnd, player, chunk)

    async def hold_end(self, player: Player):
        """End the player's answer. Ignored if there is nothing to end (e.g. the
        round already ended it), so phones can always send it on release."""
        rnd = self.round
        answer = rnd.answers.get(player.id) if rnd else None
        if answer is None or not answer.holding:
            return
        answer.hold_end = rnd.elapsed()
        await self._hook("hold_end", rnd, player)

    # --- results (bridge -> server) ----------------------------------------

    def on_partial(self, rnd: Round, player: Player, text: str):
        if rnd is self.round and player.id in self.players:
            player.client.send({"type": "partial", "round": rnd.number, "text": text})

    def on_final(self, rnd: Round, player: Player, text: str, error: str | None = None):
        """Score a final transcript for ``rnd``. Accepted until the next question
        starts, so answers given just before the timer still count when
        Transcribe is slow; finals for older rounds are dropped.

        ``error`` (e.g. "transcribe_timeout") means recognition failed; ``text``
        is then whatever was recognised, usually "". Such an answer is not
        scored (0 points, correct false), since part of it may be missing. The
        error is passed on to the clients so a phone can say "couldn't hear
        you" rather than "wrong"."""
        if rnd is not self.round:
            log.info("room %s: dropping final for old round %d from %s", self.code, rnd.number, player.name)
            return
        answer = rnd.answers.get(player.id)
        if answer is None or answer.transcript is not None or player.id not in self.players:
            return
        answer.transcript = text
        answer.final_at = rnd.elapsed()
        if error:
            answer.correct, answer.points = False, 0
        else:
            answer.correct, answer.points = scoring.score(rnd.question, text, answer.final_at, rnd.time_limit)
        player.score += answer.points
        final = {"type": "final", "round": rnd.number, "player_id": player.id, "name": player.name,
                 "text": text, "correct": answer.correct, "points": answer.points,
                 "score": player.score}
        if error:
            final["error"] = error
        player.client.send(final)
        self.host.send(final)
        self.to_everyone(self.leaderboard_msg())

    # --- teardown ----------------------------------------------------------

    async def close(self):
        """Host left: end the round and disconnect everyone."""
        if self.round and self.round.open:
            await self.end_round("host_left")
        for p in list(self.players.values()):
            p.client.send({"type": "room_closed", "room": self.code})
            p.client.close(CLOSE_ROOM_CLOSED)


class Lobby:
    """All rooms on this server, by code."""

    def __init__(self, questions: list[Question], bridge: Bridge, round_seconds: float):
        self.questions = questions
        self.bridge = bridge
        self.round_seconds = round_seconds
        self.rooms: dict[str, Room] = {}

    def create_room(self, host: Client) -> Room:
        while True:
            code = "".join(random.choices(ROOM_CODE_LETTERS, k=4))
            if code not in self.rooms:
                break
        room = Room(code, host, self.questions, self.bridge, self.round_seconds)
        self.rooms[code] = room
        log.info("room %s created", code)
        return room

    def get(self, code: str) -> Room:
        room = self.rooms.get(str(code).strip().upper())
        if room is None:
            raise GameError("room_not_found", f"no room {code!r}")
        return room

    async def close_room(self, room: Room):
        self.rooms.pop(room.code, None)
        await room.close()
        log.info("room %s closed", room.code)
