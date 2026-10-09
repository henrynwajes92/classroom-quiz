"""Simulator (CQ-8): N fake players answer questions over the game-server protocol.

    # join a room someone else hosts (host screen, another script), until it closes
    python scripts/simulate.py --players 4 --room KXQB --server ws://localhost:8000
    # or host the room too: create it, run 2 questions, end each once all have answered
    python scripts/simulate.py --players 4 --host --rounds 2 --out results.json

Each player connects to /ws/play like a phone (docs/protocol.md), so a run
tests the whole system: game server, Transcribe bridge and Transcribe. On each
question a player picks a clip for that question_id from clips/manifest.json
(right with probability --correct-rate, else wrong; in its own voice when
there is one), waits a random --min-delay..--max-delay seconds, then holds,
streams the clip as raw PCM in 0.1 s chunks paced in real time, and releases.
No clip for the question: the player stays silent. --seed makes the choices
repeatable.

At the end it prints each answer (clip, transcript, correct, points, latency =
hold_end sent -> final received) and per player and overall: answers sent,
finals, correct, finals with an error (not scored, not in latency), errors,
and latency (count, p50/p95/max) split into "ready" (spoke >= 3 s after the
question, stream most likely connected: the API's latency) and "early" (also
includes catching up on audio buffered while the stream connected). audio_s
of a cut-off answer is an upper bound (chunks in flight at round_end are
rejected). --out writes the same as JSON.

Exit code: 0 if every player joined and nothing went wrong; 1 if the host
failed, no player joined, or there was any error (protocol error, disconnect,
error final, round ended before a player spoke); 2 for refused/bad arguments.

Demo server: every player is one Transcribe stream. The simulator asks the
game server's /health which Transcribe host it uses (and its stream cap); if
the server can't say, it falls back to TRANSCRIBE_URL (this shell's, or the
server default; --transcribe-url overrides) and treats an unknown URL as the
demo server. There it refuses more than 4 players unless --i-asked-ops. Even
then the server's own cap (4 streams per process, lifted
by ALLOW_DEMO_LOAD=1 on the server) still applies: players beyond it wait up
to 10 s for a slot, then get a final with error transcribe_unavailable, shown
in the errors here. That cap is process-wide, so other rooms on the same
server (another simulator, play_clip.py) share it.

Streams open when a question starts and take 1-4 s to connect; a player who
speaks sooner has its audio buffered meanwhile, so its latency includes
catching up (use --min-delay 3 to measure the API latency alone).
"""
import argparse
import asyncio
import json
import math
import random
import signal
import sys
import time
import urllib.request
import wave
from pathlib import Path
from urllib.parse import urlparse

import websockets

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from server import config  # noqa: E402
from server.transcribe_bridge import DEMO_HOST, DEMO_MAX_STREAMS, FINAL_TIMEOUT  # noqa: E402

CHUNK = 3200  # 0.1 s of 16 kHz 16-bit mono, like a phone
BYTES_PER_SEC = config.BYTES_PER_SEC
FINAL_GRACE = FINAL_TIMEOUT + 2  # after round_end, how long to wait for outstanding finals
MAX_NAME_LEN = 24
MAX_PREFIX_LEN = 16  # "<prefix>-NNN" must fit in a name
# Streams take 1-4 s to connect after the question; an answer started this late
# most likely found its stream ready, so its latency is the API's alone. Earlier
# answers' latency includes catching up on audio buffered while connecting.
READY_DELAY = 3.0


class HostError(Exception):
    """The server refused something the host sent."""


def is_demo(url: str | None) -> bool:
    """Same host check as the server's bridge; an unknown URL counts as the demo server."""
    host = (urlparse(url or "").hostname or "").lower().rstrip(".")
    return host in ("", DEMO_HOST)


def server_status(server: str) -> dict | None:
    """The game server's Transcribe host and stream cap from /health, or None
    if it can't say (unreachable, or an older server)."""
    url = "http" + server.rstrip("/")[2:] + "/health"  # ws:// -> http://, wss:// -> https://
    try:
        with urllib.request.urlopen(url, timeout=3) as r:
            status = json.loads(r.read()).get("transcribe")
    except (OSError, ValueError):
        return None
    return status if isinstance(status, dict) and "host" in status else None


def cap_error(players: int, transcribe_url: str | None, override: bool,
              status: dict | None = None) -> str | None:
    """Why this run must not go ahead, or None. ``status`` (from /health), when
    known, wins over ``transcribe_url``."""
    if status is not None:
        demo = status["host"] == DEMO_HOST
        transcribe_url = f"{status['host']} (reported by the game server)" if status["host"] else None
        if not status["host"]:
            return None  # no Transcribe behind this server (BRIDGE=logging)
    else:
        demo = is_demo(transcribe_url)
    if players > DEMO_MAX_STREAMS and demo and not override:
        return (f"Refusing {players} players: Transcribe is {transcribe_url or 'unknown'}, treated as "
                f"the shared demo server, which keeps up with ~{DEMO_MAX_STREAMS} streams (1 per player). "
                f"Use --players {DEMO_MAX_STREAMS} or fewer, point TRANSCRIBE_URL (or --transcribe-url) at "
                f"the server's dedicated instance, or pass --i-asked-ops once ops has agreed.")
    return None


def load_manifest(path) -> dict[str, list[dict]]:
    """Clips by question_id. Relative clip paths are relative to the repo root."""
    clips: dict[str, list[dict]] = {}
    for c in json.loads(Path(path).read_text()):
        clips.setdefault(c["question_id"], []).append(c)
    return clips


def read_pcm(path) -> bytes:
    with wave.open(str(path if Path(path).is_absolute() else ROOT / path), "rb") as w:
        if (w.getframerate(), w.getnchannels(), w.getsampwidth()) != (config.SAMPLE_RATE, 1, 2):
            raise ValueError(f"{path}: need 16 kHz mono 16-bit")
        return w.readframes(w.getnframes())


def percentile(values, p):
    """Nearest-rank percentile; None for no values."""
    if not values:
        return None
    s = sorted(values)
    return s[max(0, math.ceil(p / 100 * len(s)) - 1)]


def stats(values) -> dict:
    return {"count": len(values), "p50": percentile(values, 50), "p95": percentile(values, 95),
            "max": max(values) if values else None}


class SimPlayer:
    def __init__(self, sim, index: int, voice: str | None, rng: random.Random):
        self.sim, self.voice, self.rng = sim, voice, rng
        base = f"{sim.args.prefix}-{index + 1:02d}"  # fits: parse_args limits --prefix
        self.name = base + (f" ({voice})" if voice else "")[:MAX_NAME_LEN - len(base)]
        self.player_id = None
        self.answers: dict[int, dict] = {}  # by round number
        self.errors: list[dict] = []
        self.joined = asyncio.Event()
        self.gone = False  # disconnected or failed: nothing more to wait for
        self.round_open = False
        self.rounds_ended = 0
        self.ws = None
        self.answering: asyncio.Task | None = None

    def error(self, rnd, what):
        self.errors.append({"round": rnd, "error": what})

    def round_done(self, rnd: int) -> bool:
        a = self.answers.get(rnd)
        return self.gone or (a is not None and a["_done"])

    def pick(self, question_id: str) -> dict | None:
        clips = self.sim.clips.get(question_id, [])
        if not clips:
            return None
        want = self.rng.random() < self.sim.args.correct_rate
        pool = [c for c in clips if c["correct"] == want] or clips  # fall back to whatever exists
        mine = [c for c in pool if c["voice"] == self.voice]
        return self.rng.choice(mine or pool)

    async def run(self):
        try:
            async with websockets.connect(f"{self.sim.server}/ws/play", open_timeout=10) as ws:
                self.ws = ws
                await ws.send(json.dumps({"type": "join", "room": self.sim.room, "name": self.name}))
                await self.read_loop(ws)
        except websockets.ConnectionClosed as e:
            self.error(self.current_round(), f"disconnected (code {e.rcvd.code if e.rcvd else 1006})")
        except Exception as e:  # one player failing must not end the run
            self.error(self.current_round(), f"{type(e).__name__}: {e}")
        finally:
            self.gone = True
            self.joined.set()  # unblock the host if we never joined
            if self.answering:
                self.answering.cancel()

    def current_round(self):
        return max(self.answers, default=None)

    async def read_loop(self, ws):
        max_rounds = None if self.sim.args.host else self.sim.args.rounds
        last_end = None
        while True:
            if max_rounds is not None and self.rounds_ended >= max_rounds:  # --room --rounds K: leave after K
                rnd = self.current_round()
                if rnd is None or self.round_done(rnd) or time.monotonic() - last_end > FINAL_GRACE:
                    return
            try:
                raw = await asyncio.wait_for(ws.recv(), 0.5)
            except TimeoutError:
                continue
            msg = json.loads(raw)
            kind = msg.get("type")
            if kind == "joined":
                self.player_id = msg["player_id"]
                self.joined.set()
            elif kind == "error":
                if self.player_id is None:  # join refused (room_not_found, name_taken)
                    self.error(None, f"join failed: {msg.get('code')}")
                    return
                if msg.get("code") == "no_round" and not self.round_open:
                    continue  # last audio chunk after round_end; expected
                self.error(self.current_round(), f"{msg.get('code')}: {msg.get('message')}")
            elif kind == "question":
                if self.answering:
                    self.answering.cancel()
                self.round_open = True
                a = self.answers[msg["round"]] = {
                    "round": msg["round"], "question_id": msg["question_id"], "clip": None,
                    "clip_text": None, "expected_correct": None, "delay_s": None, "audio_s": None,
                    "stream_likely_ready": None, "cut_off": False, "partials": 0, "text": None,
                    "correct": None, "points": None, "score": None, "error": None, "latency_s": None,
                    "_sent": 0, "_end": None, "_done": False}
                self.answering = asyncio.create_task(self.answer(ws, a))
            elif kind == "partial":
                if msg.get("round") in self.answers:
                    self.answers[msg["round"]]["partials"] += 1
            elif kind == "final" and msg.get("player_id") == self.player_id:
                a = self.answers.get(msg.get("round"))
                if a is None:
                    continue
                if a["_end"] is not None:
                    a["latency_s"] = round(time.monotonic() - a["_end"], 3)
                a.update(text=msg.get("text"), correct=msg.get("correct"), points=msg.get("points"),
                         score=msg.get("score"), error=msg.get("error"), _done=True)
            elif kind == "round_end":
                self.round_open = False
                self.rounds_ended += 1
                last_end = time.monotonic()
                a = self.answers.get(msg.get("round"))
                if a is not None:
                    if a["clip"] is None or a["delay_s"] is None:
                        a["_done"] = True  # silent, or the round ended before we spoke
                    elif a["_end"] is None:  # the server ended our answer
                        a["cut_off"], a["_end"] = True, last_end
                        # Sent before we saw round_end: an upper bound, chunks in flight
                        # may have been rejected with no_round.
                        a["audio_s"] = round(a["_sent"] / BYTES_PER_SEC, 2)
            elif kind == "room_closed":
                return

    async def answer(self, ws, a):
        try:
            clip = self.pick(a["question_id"])
            if clip is None:
                a["_done"] = True  # no clips for this question: stay silent
                return
            pcm = self.sim.pcm(clip["path"])
            delay = self.rng.uniform(self.sim.args.min_delay, self.sim.args.max_delay)
            a.update(clip=clip["path"], clip_text=clip["text"], expected_correct=clip["correct"])
            await asyncio.sleep(delay)
            if not self.round_open:
                a["error"], a["_done"] = "round ended before speaking", True
                return
            a["delay_s"] = round(delay, 3)
            a["stream_likely_ready"] = delay >= READY_DELAY
            await ws.send(json.dumps({"type": "hold_start"}))
            start = time.monotonic()
            for i in range(0, len(pcm), CHUNK):
                if not self.round_open:
                    break  # round over: the server already ended the answer
                a["_sent"] = min(len(pcm), i + CHUNK)  # counted once handed over, so audio_s is an upper bound
                await ws.send(pcm[i:i + CHUNK])
                # Pace against the clock so the audio arrives in real time.
                await asyncio.sleep(max(0, start + a["_sent"] / BYTES_PER_SEC - time.monotonic()))
            if a["_end"] is None:  # before the send, so a final read meanwhile gets a latency
                a["_end"] = time.monotonic()
                a["audio_s"] = round(a["_sent"] / BYTES_PER_SEC, 2)
            await ws.send(json.dumps({"type": "hold_end"}))
        except websockets.ConnectionClosed:
            pass  # run() records the disconnect
        except Exception as e:
            a["error"], a["_done"] = f"{type(e).__name__}: {e}", True


class Simulation:
    def __init__(self, args):
        self.args = args
        self.server = args.server.rstrip("/")
        self.room = args.room
        self.clips = load_manifest(args.manifest)
        self._pcm: dict[str, bytes] = {}
        voices = sorted({c["voice"] for cs in self.clips.values() for c in cs})
        random.Random(args.seed).shuffle(voices)
        self.players = [SimPlayer(self, i, voices[i % len(voices)] if voices else None,
                                  random.Random(f"{args.seed}-{i}") if args.seed is not None else random.Random())
                        for i in range(args.players)]
        self.rounds: list[dict] = []  # host mode: what the host saw
        self.host_error = None

    def pcm(self, path) -> bytes:
        if path not in self._pcm:
            self._pcm[path] = read_pcm(path)
        return self._pcm[path]

    async def run(self):
        tasks = []
        try:
            if self.args.host:
                try:
                    await self.host(tasks)
                except (OSError, TimeoutError, HostError, websockets.WebSocketException) as e:
                    self.host_error = f"host: {type(e).__name__}: {e}"
                    print(self.host_error, file=sys.stderr)
            else:
                tasks += [asyncio.create_task(p.run()) for p in self.players]
                await asyncio.gather(*tasks)
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def host(self, tasks):
        """Create a room, start --rounds questions, end each once every player is done."""
        async with websockets.connect(f"{self.server}/ws/host", open_timeout=10) as host:
            inbox: asyncio.Queue = asyncio.Queue()

            async def read():
                async for raw in host:
                    inbox.put_nowait(json.loads(raw))

            async def next_msg(kind, timeout):
                async with asyncio.timeout(timeout):
                    while True:
                        msg = await inbox.get()
                        if msg["type"] == kind:
                            return msg
                        if msg["type"] == "error" and msg.get("code") != "no_round":  # no_round: end_round raced the timer
                            raise HostError(f"{msg.get('code')}: {msg.get('message')}")

            reader = asyncio.create_task(read())
            tasks.append(reader)
            self.room = (await next_msg("room_created", 10))["room"]
            print(f"room {self.room}")
            tasks += [asyncio.create_task(p.run()) for p in self.players]
            for p in self.players:
                await p.joined.wait()
            for _ in range(self.args.rounds):
                start = {"type": "start_question"}
                if self.args.time_limit:
                    start["time_limit"] = self.args.time_limit
                await host.send(json.dumps(start))
                q = await next_msg("question", 10)
                print(f"round {q['round']}: {q['text']} ({q['question_id']})")
                ended = asyncio.create_task(next_msg("round_end", q["time_limit"] + 5))
                while not ended.done() and not all(p.round_done(q["round"]) for p in self.players):
                    await asyncio.sleep(0.05)
                if not ended.done():
                    await host.send(json.dumps({"type": "end_round"}))
                end = await ended
                self.rounds.append({k: end.get(k) for k in ("round", "question_id", "reason", "answered",
                                                            "players")})
                deadline = time.monotonic() + FINAL_GRACE  # late finals (released near the end)
                while time.monotonic() < deadline and not all(p.round_done(q["round"]) for p in self.players):
                    await asyncio.sleep(0.05)
                await asyncio.sleep(self.args.gap)
        # Leaving closes the room; players get room_closed and finish.
        await asyncio.wait(tasks[1:], timeout=10)

    # --- results -----------------------------------------------------------

    def results(self) -> dict:
        """Latency only counts finals without an error, split by whether the
        stream was likely connected when the player spoke (see READY_DELAY)."""
        def latency(answers, ready):
            return stats([a["latency_s"] for a in answers if a["text"] is not None and not a["error"]
                          and a["latency_s"] is not None and a["stream_likely_ready"] == ready])

        players, everything = [], []
        for p in self.players:
            answers = [{k: v for k, v in a.items() if not k.startswith("_")} for a in p.answers.values()]
            everything += answers
            finals = [a for a in answers if a["text"] is not None]
            players.append({"name": p.name, "player_id": p.player_id, "voice": p.voice,
                            "joined": p.player_id is not None,
                            "answers_sent": sum(a["delay_s"] is not None for a in answers),
                            "results": len(finals), "correct": sum(bool(a["correct"]) for a in finals),
                            "error_finals": sum(bool(a["error"]) for a in finals),
                            "errors": len(p.errors) + sum(bool(a["error"]) for a in answers),
                            "latency_ready": latency(answers, True), "latency_early": latency(answers, False),
                            "answers": answers, "error_log": p.errors})
        total = lambda k: sum(p[k] for p in players)  # noqa: E731
        return {"server": self.server, "room": self.room, "seed": self.args.seed,
                "correct_rate": self.args.correct_rate, "ready_delay_s": READY_DELAY, "rounds": self.rounds,
                "host_error": self.host_error, "players": players,
                "aggregate": {"players": len(players), "joined": total("joined"),
                              "answers_sent": total("answers_sent"), "results": total("results"),
                              "correct": total("correct"),
                              "expected_correct": sum(bool(a["expected_correct"]) for a in everything
                                                      if a["delay_s"] is not None),
                              "error_finals": total("error_finals"), "errors": total("errors"),
                              "latency_ready": latency(everything, True),
                              "latency_early": latency(everything, False)}}


def print_summary(res: dict):
    def s(v):
        return f"{v:.2f}s" if v is not None else "-"
    print(f"\nroom {res['room']}: per answer (latency = hold_end sent -> final received)")
    for p in res["players"]:
        for a in p["answers"]:
            said = f"{a['clip_text']!r} ({'right' if a['expected_correct'] else 'wrong'})" if a["clip"] else "(silent)"
            heard = f"-> {a['text']!r} correct={a['correct']} pts={a['points']}" if a["text"] is not None else "-> no final"
            extra = (" cut off" if a["cut_off"] else "") + (f" error={a['error']}" if a["error"] else "")
            print(f"  {p['name']:24} r{a['round']} {a['question_id']:15} {said:34} {heard} "
                  f"{s(a['latency_s'])}{extra}")
        for e in p["error_log"]:
            print(f"  {p['name']:24} r{e['round']} error: {e['error']}")
    def lat(st):
        return f"{st['count']:>2} {s(st['p50']):>6} {s(st['p95']):>6} {s(st['max']):>6}"
    ready = f"ready (delay >= {res['ready_delay_s']:g} s): n p50 p95 max"
    print(f"\n{'player':24} {'sent':>4} {'res':>4} {'ok':>3} {'efin':>4} {'err':>3}  {ready:32} early: n p50 p95 max")
    for p in res["players"] + [{"name": "ALL", **res["aggregate"]}]:
        print(f"{p['name']:24} {p['answers_sent']:>4} {p['results']:>4} {p['correct']:>3} {p['error_finals']:>4} "
              f"{p['errors']:>3}  {lat(p['latency_ready']):32} {lat(p['latency_early'])}")
    agg = res["aggregate"]
    print(f"latency: hold_end sent -> final received, finals without error only. 'early' answers started "
          f"before the stream was likely connected, so they include catching up on buffered audio.\n"
          f"efin: finals with an error (not scored); err: all errors. Clips meant to be right: "
          f"{agg['expected_correct']}/{agg['answers_sent']}, scored right: {agg['correct']}/{agg['results']}")


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--players", type=int, default=4)
    room = p.add_mutually_exclusive_group(required=True)
    room.add_argument("--room", help="room code to join")
    room.add_argument("--host", action="store_true", help="create the room and start the questions too")
    p.add_argument("--server", default="ws://localhost:8000", help="game server base URL (ws:// or wss://)")
    p.add_argument("--rounds", type=int,
                   help="--host: questions to run (default 1); --room: leave after this many rounds "
                        "(default: stay until the room closes)")
    p.add_argument("--time-limit", type=float, help="--host: seconds per question (default: the server's)")
    p.add_argument("--gap", type=float, default=1.0, help="--host: seconds between questions")
    p.add_argument("--min-delay", type=float, default=0.0, help="seconds after the question before speaking")
    p.add_argument("--max-delay", type=float, default=2.0)
    p.add_argument("--correct-rate", type=float, default=0.75, help="chance each answer uses a right clip")
    p.add_argument("--seed", type=int)
    p.add_argument("--prefix", default="Sim", help="player names: <prefix>-01 (<voice>)")
    p.add_argument("--manifest", default=str(ROOT / "clips" / "manifest.json"))
    p.add_argument("--transcribe-url", default=config.TRANSCRIBE_URL,
                   help="the game server's TRANSCRIBE_URL (default: this shell's, or the server default)")
    p.add_argument("--i-asked-ops", action="store_true",
                   help=f"allow more than {DEMO_MAX_STREAMS} players against the demo server")
    p.add_argument("--out", help="also write the results as JSON here")
    args = p.parse_args(argv)
    if args.players < 1 or args.min_delay < 0 or args.max_delay < args.min_delay:
        p.error("need --players >= 1 and 0 <= --min-delay <= --max-delay")
    if not 0 <= args.correct_rate <= 1:
        p.error("--correct-rate must be 0-1")
    if not 1 <= len(args.prefix) <= MAX_PREFIX_LEN:
        p.error(f"--prefix must be 1-{MAX_PREFIX_LEN} characters")
    if args.time_limit is not None and not config.MIN_TIME_LIMIT <= args.time_limit <= config.MAX_TIME_LIMIT:
        p.error(f"--time-limit must be {config.MIN_TIME_LIMIT}-{config.MAX_TIME_LIMIT} seconds")
    if args.rounds is not None and args.rounds < 1:
        p.error("--rounds must be >= 1")
    if args.host and args.rounds is None:
        args.rounds = 1
    return args


async def amain(args) -> dict:
    sim = Simulation(args)
    task = asyncio.current_task()
    try:  # Ctrl-C: stop the players and still print what we have
        asyncio.get_running_loop().add_signal_handler(signal.SIGINT, task.cancel)
    except (NotImplementedError, RuntimeError):
        pass
    try:
        await sim.run()
    except asyncio.CancelledError:
        print("\ninterrupted")
    finally:
        try:
            asyncio.get_running_loop().remove_signal_handler(signal.SIGINT)
        except (NotImplementedError, RuntimeError):
            pass
    return sim.results()


def main(argv=None) -> int:
    args = parse_args(argv)
    status = server_status(args.server)
    refused = cap_error(args.players, args.transcribe_url, args.i_asked_ops, status)
    if refused:
        print(refused, file=sys.stderr)
        return 2
    if status is not None:
        print(f"{args.players} players -> {args.server}; Transcribe {status['host'] or 'none (no recognition)'}"
              f", stream cap {status['stream_cap'] or 'none'} (from /health)")
        cap = status["stream_cap"]
    else:
        print(f"{args.players} players -> {args.server}; Transcribe {args.transcribe_url} (server didn't say)")
        cap = DEMO_MAX_STREAMS if is_demo(args.transcribe_url) else None
    if cap and args.players > cap:
        print(f"the server caps Transcribe at {cap} streams unless it runs with ALLOW_DEMO_LOAD=1; "
              f"beyond that answers get transcribe_unavailable.")
    res = asyncio.run(amain(args))
    print_summary(res)
    if args.out:
        Path(args.out).write_text(json.dumps(res, indent=1))
        print(f"wrote {args.out}")
    agg = res["aggregate"]
    return 1 if res["host_error"] or not agg["joined"] or agg["errors"] else 0


if __name__ == "__main__":
    sys.exit(main())
