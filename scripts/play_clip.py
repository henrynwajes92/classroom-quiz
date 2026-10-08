"""Live check of the Transcribe bridge: players answer one question with WAV clips.

    python scripts/play_clip.py --players 1,3 clips/france-capital/00_cobalt_steve.wav [more.wav ...]

Starts the game server in this process (with the real TranscribeBridge, so
TRANSCRIBE_URL / TRANSCRIBE_MODEL apply), then for each player count runs one
round in a fresh room: N players join over /ws/play, the host starts the
question, and each player waits until its Transcribe stream is ready (as a
player who thinks for a few seconds would), then holds, streams its clip (raw
PCM after the 44-byte WAV header, paced in real time) and releases. Clips are
handed out in turn. Prints each player's final and score next to the
bridge's stream metrics, and checks no Transcribe stream is left open.

--early speaks right after the question instead, so audio is buffered while
the stream connects (answer latency then includes catching up on it).

Mind the demo server: each player is one Transcribe stream, and the bridge
allows at most 4 at once there.
"""
import argparse
import asyncio
import json
import os
import sys
import time

import uvicorn
import websockets

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from server.app import create_app  # noqa: E402
from server.transcribe_bridge import CONNECT_TIMEOUT, FINAL_TIMEOUT, TranscribeBridge  # noqa: E402

WAV_HEADER = 44
BYTES_PER_SEC = 32000
CHUNK = 3200  # 0.1 s, like a phone


async def recv(ws, kind, timeout=30):
    """Next message of type ``kind`` (others skipped)."""
    async with asyncio.timeout(timeout):
        while True:
            msg = json.loads(await ws.recv())
            if msg["type"] == kind:
                return msg


async def stream_ready(bridge, room, player_id):
    """Wait until the bridge's stream for this player is connected and configured."""
    try:
        async with asyncio.timeout(2 * CONNECT_TIMEOUT):
            while True:
                stream = bridge.streams.get((room, player_id))
                if stream is not None and (stream.ready or stream.task.done()):
                    return
                await asyncio.sleep(0.05)
    except TimeoutError:
        pass  # speak anyway; the metrics will show ready_at_hold_start=False


async def player(url, bridge, room, name, clip, args, out):
    pcm = open(clip, "rb").read()[WAV_HEADER:]
    async with websockets.connect(f"{url}/ws/play") as ws:
        await ws.send(json.dumps({"type": "join", "room": room, "name": name}))
        player_id = (await recv(ws, "joined"))["player_id"]
        out["ready"].set()
        await recv(ws, "question")
        if not args.early:
            await stream_ready(bridge, room, player_id)
        await asyncio.sleep(args.delay)  # "thinking"
        await ws.send(json.dumps({"type": "hold_start"}))
        start = time.monotonic()
        for i in range(0, len(pcm), CHUNK):
            await ws.send(pcm[i:i + CHUNK])
            # Pace against the clock so the audio arrives in real time.
            await asyncio.sleep(max(0, start + (i + CHUNK) / BYTES_PER_SEC - time.monotonic()))
        await ws.send(json.dumps({"type": "hold_end"}))
        try:
            out["final"] = await recv(ws, "final", FINAL_TIMEOUT + 10)
        finally:
            out["done"].set()
        await recv(ws, "round_end")  # stay, so the stream ends on its own (not on player_left)


async def play_round(url, bridge, n, args):
    clips = args.clips
    async with websockets.connect(f"{url}/ws/host") as host:
        room = (await recv(host, "room_created"))["room"]
        names = [f"Bot{i + 1}" for i in range(n)]
        outs = [{"ready": asyncio.Event(), "done": asyncio.Event()} for _ in names]
        tasks = [asyncio.create_task(player(url, bridge, room, name, clips[i % len(clips)], args, outs[i]))
                 for i, name in enumerate(names)]
        for out in outs:
            await out["ready"].wait()
        await host.send(json.dumps({"type": "start_question", "index": args.question, "time_limit": 30}))
        q = await recv(host, "question")
        for out, task in zip(outs, tasks):  # each bot has its final (or failed)
            waiter = asyncio.create_task(out["done"].wait())
            await asyncio.wait([waiter, task], return_when=asyncio.FIRST_COMPLETED)
            waiter.cancel()
        # Let Transcribe close the answered streams itself, to see the close delay.
        async with asyncio.timeout(FINAL_TIMEOUT + 2):
            while bridge.open_count():
                await asyncio.sleep(0.1)
        await host.send(json.dumps({"type": "end_round"}))
        await recv(host, "round_end")
        results = await asyncio.gather(*tasks, return_exceptions=True)
    await asyncio.sleep(0.5)

    print(f"\n{n} player(s), room {room}: {q['text']}")
    print(f"{'player':6} {'clip':30} {'transcript':26} {'ok':5} {'pts':>4} {'score':>5} {'connect':>7} "
          f"{'ready':5} {'unsent':>6} {'answer':>6} {'=backlog':>8} {'+e2f':>6} {'+quiet':>6} "
          f"{'close':>6} {'trigger':9} {'fin':>3} {'late':>4} {'code':>4}")
    print("connect: stream requested -> open; ready: stream ready when the player pressed hold; "
          "unsent: audio not yet sent at release; answer: release -> final scored = backlog "
          "(release -> end of audio sent) + e2f (end sent -> last final; the API latency when "
          "ready and no backlog) + quiet (last final -> scored); close: scored -> stream closed; "
          "fin: finals; late: finals after the quiet gap; code: close code")
    metrics = {m.player: m for m in bridge.metrics if m.room == room}

    def s(v):
        return f"{v:.2f}s" if v is not None else "-"
    for i, name in enumerate(names):
        f, m = outs[i].get("final", {}), metrics.get(name)
        if isinstance(results[i], Exception):
            print(f"{name}: player failed: {results[i]!r}")
        if m is None:
            print(f"{name}: no stream metrics")
            continue
        print(f"{name:6} {clips[i % len(clips)][-30:]:30} {f.get('text', '-')!r:26} "
              f"{str(f.get('correct')):5} {f.get('points', 0):>4} {f.get('score', 0):>5} "
              f"{s(m.connect_s):>7} {'yes' if m.ready_at_hold_start else 'no':5} "
              f"{s(m.backlog_at_release_s):>6} {s(m.answer_latency_s):>6} {s(m.send_backlog_s):>8} "
              f"{s(m.end_to_final_s):>6} {s(m.quiet_wait_s):>6} {s(m.close_delay_s):>6} "
              f"{m.report_trigger or '-':9} {m.finals:>3} {m.late_finals:>4} {str(m.close_code):>4}"
              + (f" {m.late_final_texts}" if m.late_finals else "")
              + (f" error={f['error']}" if f.get("error") else "")
              + (f" post={m.post_report_error}" if m.post_report_error else ""))
    print(f"streams still open after the round: {bridge.open_count()}")


async def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("clips", nargs="+")
    p.add_argument("--players", default="1", help="player count per round, e.g. 1,3")
    p.add_argument("--question", type=int, default=0, help="question index (0 = capital of France)")
    p.add_argument("--delay", type=float, default=0.5,
                   help="seconds each player waits (after its stream is ready) before speaking")
    p.add_argument("--early", action="store_true", help="don't wait for the stream to be ready")
    p.add_argument("--port", type=int, default=8765)
    args = p.parse_args()

    bridge = TranscribeBridge()
    print(f"Transcribe: {bridge.url} model {bridge.model}, cap "
          f"{bridge.limit.max if bridge.limit else 'none'}")
    server = uvicorn.Server(uvicorn.Config(create_app(bridge=bridge), port=args.port, log_level="warning"))
    serving = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.05)
    try:
        for n in map(int, args.players.split(",")):
            await play_round(f"ws://127.0.0.1:{args.port}", bridge, n, args)
    finally:
        server.should_exit = True
        await serving


if __name__ == "__main__":
    asyncio.run(main())
