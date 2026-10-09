"""Phone page end to end (CQ-6): headless Chromium with a fake mic against a
real game server (uvicorn in a thread), and the resampler run in the browser.

    .venv/bin/pip install playwright && .venv/bin/python -m playwright install chromium
    sudo .venv/bin/playwright install-deps chromium   # system libs (libnss3 etc.), once per machine
    .venv/bin/python -m pytest -q tests/test_phone_e2e.py

Skips when playwright or Chromium isn't installed (it is a dev-only tool, not
in requirements.txt). The fake mic is a WAV file Chromium plays in a loop
(--use-file-for-fake-audio-capture). A recording bridge stands in for
Transcribe, so nothing leaves the machine, except:

    # ONE real stream to Transcribe (demo server): the page answers "Paris"
    LIVE_TRANSCRIBE=1 flock /tmp/cobalt_demo.lock .venv/bin/python -m pytest -q tests/test_phone_e2e.py -k live
"""
import array
import json
import math
import os
import socket
import threading
import time
import wave
from pathlib import Path

import pytest

sync_api = pytest.importorskip("playwright.sync_api")
import uvicorn  # noqa: E402
from websockets.sync.client import connect as ws_connect  # noqa: E402

from server.app import create_app  # noqa: E402
from server.bridge import Bridge  # noqa: E402
from server.config import QUESTIONS_FILE, ROOT  # noqa: E402
from server.questions import Question, load_questions  # noqa: E402

QUESTIONS = [
    Question("france-capital", "What is the capital of France?", ["paris"]),
    Question("red-planet", "Which planet is known as the Red Planet?", ["mars"]),
]
TONE_HZ = 440
LIVE_CLIP = ROOT / "clips" / "france-capital" / "00_cobalt_steve.wav"


# --- helpers -------------------------------------------------------------------

def write_wav(path: Path, rate: int, samples):
    with wave.open(str(path), "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(rate)
        w.writeframes(array.array("h", samples).tobytes())


def tone(rate=44100, seconds=10.0, freq=TONE_HZ, amp=0.5):
    return [int(amp * 32767 * math.sin(2 * math.pi * freq * i / rate)) for i in range(int(rate * seconds))]


def tone_power(samples, rate, freq):
    """Goertzel: power of ``freq`` in ``samples``."""
    c = 2 * math.cos(2 * math.pi * freq / rate)
    s1 = s2 = 0.0
    for x in samples:
        s1, s2 = x + c * s1 - s2, s1
    return s1 * s1 + s2 * s2 - c * s1 * s2


class ServerThread:
    """uvicorn on a free localhost port, in a background thread."""

    def __init__(self, app):
        self.sock = socket.socket()
        self.sock.bind(("127.0.0.1", 0))
        self.port = self.sock.getsockname()[1]
        self.server = uvicorn.Server(uvicorn.Config(app, log_level="warning"))
        self.thread = threading.Thread(target=self.server.run, kwargs={"sockets": [self.sock]}, daemon=True)

    def __enter__(self):
        self.thread.start()
        deadline = time.monotonic() + 10
        while not self.server.started:
            assert time.monotonic() < deadline, "server didn't start"
            time.sleep(0.05)
        return self

    def __exit__(self, *exc):
        self.server.should_exit = True
        self.thread.join(10)

    @property
    def http(self):
        return f"http://127.0.0.1:{self.port}"

    @property
    def ws(self):
        return f"ws://127.0.0.1:{self.port}"


class Host:
    def __init__(self, server: ServerThread):
        self.ws = ws_connect(server.ws + "/ws/host")
        self.room = self.expect("room_created")["room"]

    def send(self, msg):
        self.ws.send(json.dumps(msg))

    def expect(self, kind, timeout=10):
        deadline = time.monotonic() + timeout
        while True:
            msg = json.loads(self.ws.recv(timeout=max(0.01, deadline - time.monotonic())))
            if msg["type"] == kind:
                return msg

    def close(self):
        self.ws.close()


class RecordingBridge(Bridge):
    """Keeps every answer's audio; sends a partial once 0.5 s has arrived (so
    the page shows it while still holding) and the final "Paris." on hold_end."""

    def __init__(self):
        self.answers = {}  # (round, name) -> {"chunks": [...], "start": t, "end": t}

    async def hold_start(self, room, rnd, player):
        self.answers[(rnd.number, player.name)] = {"chunks": [], "start": time.monotonic(), "end": None,
                                                   "partial": False}

    async def audio(self, room, rnd, player, chunk):
        a = self.answers[(rnd.number, player.name)]
        a["chunks"].append(bytes(chunk))
        if not a["partial"] and sum(map(len, a["chunks"])) >= 16000:
            a["partial"] = True
            room.on_partial(rnd, player, "par")

    async def hold_end(self, room, rnd, player):
        a = self.answers[(rnd.number, player.name)]
        a["end"] = time.monotonic()
        if not rnd.answers[player.id].cut_off:
            room.on_final(rnd, player, "Paris.")


@pytest.fixture(scope="module")
def playwright():
    with sync_api.sync_playwright() as p:
        yield p


def launch(playwright, wav: Path):
    try:
        return playwright.chromium.launch(args=[
            "--use-fake-ui-for-media-stream", "--use-fake-device-for-media-stream",
            f"--use-file-for-fake-audio-capture={wav}", "--autoplay-policy=no-user-gesture-required"])
    except Exception as e:  # browser not installed / missing system libraries
        pytest.skip(f"Chromium not available: {str(e).splitlines()[0]}")


@pytest.fixture(scope="module")
def browser(playwright, tmp_path_factory):
    wav = tmp_path_factory.mktemp("mic") / "tone.wav"
    write_wav(wav, 44100, tone())
    b = launch(playwright, wav)
    yield b
    b.close()


@pytest.fixture
def game():
    bridge = RecordingBridge()
    with ServerThread(create_app(QUESTIONS, bridge, 30)) as server:
        server.bridge = bridge
        yield server


def new_page(browser, server, **kw):
    ctx = browser.new_context(base_url=server.http, permissions=["microphone"], **kw)
    page = ctx.new_page()
    page.on("console", lambda m: print("console:", m.type, m.text))
    page.on("pageerror", lambda e: print("pageerror:", e))
    return page


def join(page, room, name):
    page.goto(f"/play?room={room}")
    page.fill("#name", name)
    page.click("#join-btn")


def expect_text(page, selector, text, timeout=10000):
    sync_api.expect(page.locator(selector)).to_contain_text(text, timeout=timeout)


# --- tests ---------------------------------------------------------------------

def test_resampler_in_browser(browser, game):
    """The worklet's Resampler/PcmChunker, run in Chromium: length, passband
    gain, frequency, anti-aliasing, block-size independence, LE int16 chunks."""
    page = new_page(browser, game)
    page.goto("/play")
    res = page.evaluate("""() => {
      const out = {};
      const sine = (rate, f, secs, amp) => Float32Array.from({length: Math.round(rate * secs)},
                                                            (_, i) => amp * Math.sin(2 * Math.PI * f * i / rate));
      const run = (r, x, block) => { const parts = []; for (let i = 0; i < x.length; i += block)
                                       parts.push(Array.from(r.process(x.subarray(i, i + block)))); return parts.flat(); };
      const rms = (a) => Math.sqrt(a.reduce((s, v) => s + v * v, 0) / a.length);
      const crossings = (a) => { let n = 0; for (let i = 1; i < a.length; i++) if ((a[i - 1] < 0) !== (a[i] < 0)) n++; return n; };
      for (const rate of [8000, 16000, 22050, 44100, 48000, 96000]) {
        const r = new Resampler(rate, 16000);
        const y = run(r, sine(rate, 1000, 1, 0.5), 128);
        const mid = y.slice(1000, 15000);
        const alias = rate > 16000 ? rms(run(new Resampler(rate, 16000), sine(rate, Math.min(rate / 2 - 1000, 12000), 1, 0.5), 128).slice(1000, 15000)) : null;
        out[rate] = {len: y.length, half: r.half, rms: rms(mid), crossings: crossings(mid), alias};
      }
      // Same output whatever the block sizes.
      const x = sine(44100, 700, 0.5, 0.3);
      const a = run(new Resampler(44100), x, 128), b = run(new Resampler(44100), x, 10000);
      const c = run(new Resampler(44100), x, 37);
      out.sameBlocks = a.length === b.length && a.length === c.length && a.every((v, i) => Math.abs(v - b[i]) < 1e-6 && Math.abs(v - c[i]) < 1e-6);
      // Chunks: 1 s at 48 kHz -> 32000 bytes as 3200-byte chunks, little-endian.
      const chunks = [];
      const ch = new PcmChunker(48000, (buf) => chunks.push(buf));
      const dc = new Float32Array(48000).fill(0.5);
      for (let i = 0; i < dc.length; i += 128) ch.push([dc.subarray(i, i + 128)]);
      const rest = ch.flush();
      out.chunkSizes = chunks.map((c) => c.byteLength);
      out.rest = rest.byteLength;
      out.lastBytes = Array.from(new Uint8Array(chunks[chunks.length - 1]).slice(-2));
      // Stereo is mixed down.
      const st = new PcmChunker(16000, () => {});
      st.push([new Float32Array(1000).fill(0.5), new Float32Array(1000).fill(-0.5)]);
      out.stereoMax = Math.max(...new Int16Array(st.flush()).map(Math.abs));
      return out;
    }""")
    for rate in ("8000", "16000", "22050", "44100", "48000", "96000"):
        r = res[rate]
        latency = r["half"] * 16000 / int(rate)
        assert 16000 - latency - 3 <= r["len"] <= 16000, (rate, r)
        assert abs(r["rms"] - 0.5 / math.sqrt(2)) < 0.01, (rate, r)  # passband gain ~1
        assert abs(r["crossings"] - 2 * 1000 * 14000 / 16000) <= 4, (rate, r)  # still 1 kHz
        if r["alias"] is not None:
            assert r["alias"] < 0.005, (rate, r)  # > 8 kHz tone filtered out (< -40 dB), not aliased
    assert res["sameBlocks"]
    assert res["chunkSizes"] == [3200] * 9 and 0 < res["rest"] <= 3200, res
    assert res["rest"] % 2 == 0
    # 0.5 -> ~16383.5, rounded either way; read big-endian it would be ~0x3F/0x40 << 8 garbage
    assert abs(int.from_bytes(bytes(res["lastBytes"]), "little", signed=True) - 16383) <= 2, res
    assert res["stereoMax"] < 50


def test_flow_join_question_hold_result_leaderboard(browser, game):
    host = Host(game)
    page = new_page(browser, game, has_touch=True)
    page.goto(f"/play?room={host.room.lower()}")
    assert page.input_value("#room") == host.room
    page.fill("#name", "Alice")
    page.click("#join-btn")
    expect_text(page, "#screen-wait", "You're in, Alice")
    assert host.expect("player_joined")["name"] == "Alice"
    assert page.get_attribute("#hold", "aria-disabled") == "true"
    print("AudioContext rate:", page.evaluate("quizDebug.sampleRate()"),
          "worklet:", page.evaluate("quizDebug.usesWorklet()"))
    assert page.evaluate("quizDebug.usesWorklet()")

    # Round 1: mouse hold, right answer.
    host.send({"type": "start_question", "index": 0, "time_limit": 30})
    expect_text(page, "#q-text", "capital of France")
    assert page.inner_text("#q-hint").strip()
    assert page.inner_text("#q-timer") in ("30 s", "29 s")
    assert page.get_attribute("#hold", "aria-disabled") == "false"
    time.sleep(1.0)  # pre-roll fills
    page.hover("#hold")
    page.mouse.down()
    t0 = time.monotonic()
    expect_text(page, "#q-transcript", "par", timeout=5000)  # live partial while still holding
    assert page.get_attribute("#hold", "class") == "holding"
    time.sleep(max(0, 2.0 - (time.monotonic() - t0)))
    page.mouse.up()
    held = time.monotonic() - t0
    expect_text(page, "#r-verdict", "Correct!")
    expect_text(page, "#r-transcript", "Paris.")
    expect_text(page, "#r-points", "points")
    expect_text(page, "#board-me", "#1 of 1")
    assert int(page.inner_text("#bar-score").split()[0]) > 0

    a = game.bridge.answers[(1, "Alice")]
    sizes = [len(c) for c in a["chunks"]]
    assert all(s == 3200 for s in sizes[:-1]) and 0 < sizes[-1] <= 3200 and sizes[-1] % 2 == 0, sizes
    audio_s = sum(sizes) / 32000
    print(f"held {held:.2f} s, server got {audio_s:.2f} s of audio in {len(sizes)} chunks")
    # held + 0.2 s tail + 0.2-0.3 s pre-roll, give or take the click latency
    assert held + 0.15 <= audio_s <= held + 0.9, (held, audio_s)
    pcm = array.array("h", b"".join(a["chunks"]))
    window = pcm[8000:16000]  # 0.5 s from the middle
    assert math.sqrt(sum(x * x for x in window) / len(window)) > 3000  # the tone, not silence
    peak = max(range(300, 605, 5), key=lambda f: tone_power(window, 16000, f))
    assert abs(peak - TONE_HZ) <= 5, peak  # really 16 kHz: a wrong rate would move the tone
    p440 = tone_power(window, 16000, TONE_HZ)
    for wrong in (TONE_HZ * 16000 / 48000, TONE_HZ * 16000 / 44100, TONE_HZ * 3):
        assert tone_power(window, 16000, wrong) < p440 / 100

    host.send({"type": "end_round"})
    expect_text(page, "#r-answer", "The answer: paris")
    assert page.get_attribute("#hold", "aria-disabled") == "true"

    # Round 2: touch hold (pointerType touch), wrong answer.
    host.send({"type": "start_question", "index": 1, "time_limit": 30})
    expect_text(page, "#q-text", "Red Planet")
    box = page.locator("#hold").bounding_box()
    point = {"x": box["x"] + box["width"] / 2, "y": box["y"] + box["height"] / 2}
    cdp = page.context.new_cdp_session(page)
    cdp.send("Input.dispatchTouchEvent", {"type": "touchStart", "touchPoints": [point]})
    time.sleep(1.2)
    assert page.get_attribute("#hold", "class") == "holding"
    cdp.send("Input.dispatchTouchEvent", {"type": "touchEnd", "touchPoints": []})
    expect_text(page, "#r-verdict", "Not quite")
    expect_text(page, "#r-points", "0 points")
    assert page.evaluate("window.getSelection().toString()") == ""
    assert sum(map(len, game.bridge.answers[(2, "Alice")]["chunks"])) >= 1.2 * 32000
    host.send({"type": "end_round"})
    expect_text(page, "#r-answer", "mars")

    # Round 3: no answer.
    host.send({"type": "start_question", "index": 0, "time_limit": 30})
    expect_text(page, "#q-text", "capital of France")
    host.send({"type": "end_round"})
    expect_text(page, "#r-verdict", "Time's up")
    assert (3, "Alice") not in game.bridge.answers

    # Host leaves: room closed.
    host.close()
    expect_text(page, "#gone-title", "The game has ended")
    assert page.is_hidden("#rejoin-btn")
    page.click("#newroom-btn")
    assert page.is_visible("#screen-join")


def test_join_errors_and_disconnect(browser, game):
    host = Host(game)
    alice = new_page(browser, game)
    join(alice, host.room, "Alice")
    expect_text(alice, "#screen-wait", "You're in")

    bob = new_page(browser, game)
    join(bob, host.room, "alice")
    expect_text(bob, "#join-error", "already has that name")
    join(bob, "ZZZZ", "Bob")
    expect_text(bob, "#join-error", "No room with that code")
    bob.goto("/play")
    bob.fill("#room", "AB")
    bob.fill("#name", "Bob")
    bob.click("#join-btn")
    expect_text(bob, "#join-error", "4 letters")

    # Server goes away: disconnected screen with a rejoin button.
    game.server.should_exit = True
    # The hidden screen's static HTML already says "Disconnected": wait for it to show.
    sync_api.expect(alice.locator("#screen-gone")).to_be_visible(timeout=15000)
    expect_text(alice, "#gone-title", "Disconnected")
    sync_api.expect(alice.locator("#rejoin-btn")).to_be_visible()


def test_insecure_context_message(browser, game):
    page = new_page(browser, game)
    page.add_init_script("Object.defineProperty(window, 'isSecureContext', {value: false})")
    page.goto("/play?room=ABCD")
    assert page.is_visible("#insecure")
    expect_text(page, "#insecure", "https://")
    page.fill("#name", "Carol")
    page.click("#join-btn")
    expect_text(page, "#join-error", "secure link")


@pytest.mark.skipif(os.environ.get("LIVE_TRANSCRIBE") != "1", reason="set LIVE_TRANSCRIBE=1 (1 demo-server stream)")
def test_live_transcribe_paris(playwright, tmp_path):
    """One player, one real Transcribe stream: the page's audio must be heard as Paris.
    Chromium starts the fake-mic file when the page opens the mic (on join), so
    the clip plays 3 s after joining; hold from 2.5 s to just after it ends."""
    from server.transcribe_bridge import TranscribeBridge

    with wave.open(str(LIVE_CLIP), "rb") as w:
        assert (w.getframerate(), w.getnchannels(), w.getsampwidth()) == (16000, 1, 2)
        clip = array.array("h", w.readframes(w.getnframes()))
    clip_s = len(clip) / 16000
    wav = tmp_path / "live.wav"
    write_wav(wav, 16000, [0] * 48000 + list(clip) + [0] * 112000)
    questions = load_questions(QUESTIONS_FILE)
    index = next(i for i, q in enumerate(questions) if q.id == "france-capital")

    class Recording(TranscribeBridge):
        chunks = []

        async def audio(self, room, rnd, player, chunk):
            self.chunks.append(bytes(chunk))
            await super().audio(room, rnd, player, chunk)

    bridge = Recording()
    browser = launch(playwright, wav)
    try:
        with ServerThread(create_app(questions, bridge, 30)) as server:
            host = Host(server)
            page = new_page(browser, server)
            page.goto(f"/play?room={host.room}")
            page.fill("#name", "Live")
            t0 = time.monotonic()
            page.click("#join-btn")
            expect_text(page, "#screen-wait", "You're in")
            host.send({"type": "start_question", "index": index, "time_limit": 30})
            expect_text(page, "#q-text", "France")
            time.sleep(max(0, t0 + 2.5 - time.monotonic()))
            page.hover("#hold")
            page.mouse.down()
            time.sleep(max(0, t0 + 3.0 + clip_s + 0.5 - time.monotonic()))
            page.mouse.up()
            try:
                expect_text(page, "#r-verdict", "", timeout=15000)
                sync_api.expect(page.locator("#r-transcript")).not_to_have_text("…", timeout=15000)
                verdict, heard = page.inner_text("#r-verdict"), page.inner_text("#r-transcript")
            finally:
                pcm = array.array("h", b"".join(bridge.chunks))
                env = [int(math.sqrt(sum(x * x for x in pcm[i:i + 1600]) / 1600)) for i in range(0, len(pcm) - 1599, 1600)]
                print(f"sent {len(pcm) / 16000:.2f} s; RMS per 0.1 s: {env}")
            print(f"live: verdict {verdict!r}, heard {heard!r}")
            assert verdict == "Correct!" and "paris" in heard.lower(), (verdict, heard)
            host.close()
    finally:
        browser.close()
