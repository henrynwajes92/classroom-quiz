"""Stream a WAV file to Cobalt Transcribe over WebSocket and report timing.

    python scripts/transcribe_file.py clips/paris.wav [model_id] [--realtime]

--realtime paces the audio like a live microphone instead of sending it all at
once, so the latency numbers match what a phone player would see.
"""
import asyncio
import base64
import json
import ssl
import sys
import time

import certifi
import websockets

URL = "wss://demo.cobaltspeech.com/transcribe/api/transcribe/v5/streaming-recognize"
CHUNK = 8192  # bytes; 16 kHz * 16-bit mono = 32000 bytes per second
BYTES_PER_SEC = 32000
TLS = ssl.create_default_context(cafile=certifi.where())


async def transcribe(path, model_id, realtime=False):
    with open(path, "rb") as f:
        audio = f.read()
    audio_seconds = (len(audio) - 44) / BYTES_PER_SEC

    t_connect = time.monotonic()
    first_partial = None
    final_at = None
    finals = []
    async with websockets.connect(URL, ssl=TLS, max_size=None) as ws:
        t0 = time.monotonic()  # timings below start once the connection is open
        connect_s = t0 - t_connect
        await ws.send(json.dumps({"config": {
            "model_id": model_id,
            "audio_format_headered": "AUDIO_FORMAT_HEADERED_WAV",
        }}))

        async def send_audio():
            for i in range(0, len(audio), CHUNK):
                chunk = audio[i:i + CHUNK]
                await ws.send(json.dumps({"audio": {"data": base64.b64encode(chunk).decode()}}))
                if realtime:
                    await asyncio.sleep(len(chunk) / BYTES_PER_SEC)
            # An empty audio message marks the end of the audio.
            await ws.send(json.dumps({"audio": {"data": ""}}))
            return time.monotonic() - t0

        sender = asyncio.create_task(send_audio())
        try:
            async for message in ws:
                msg = json.loads(message)
                if "error" in msg:
                    raise SystemExit("server error: " + msg["error"]["message"])
                result = msg.get("result", {}).get("result")
                if not result or not result.get("alternatives"):
                    continue
                text = result["alternatives"][0]["transcript_formatted"]
                now = time.monotonic() - t0
                if result.get("is_partial"):
                    if first_partial is None:
                        first_partial = now
                    print(f"  {now:5.2f}s partial: {text}")
                else:
                    finals.append(text)
                    final_at = now
                    print(f"  {now:5.2f}s FINAL:   {text}")
        except websockets.ConnectionClosedError:
            # The demo server closes without a close frame (1006); normal once finals arrived.
            pass
        sent_at = await sender

    def fmt(t):
        return f"{t:.2f} s" if t is not None else "(none)"

    print()
    print(f"transcript:     {' '.join(finals) or '(none)'}")
    print(f"audio length:   {audio_seconds:.2f} s")
    print(f"connect:        {connect_s:.2f} s")
    print(f"audio sent by:  {fmt(sent_at)}")
    print(f"first partial:  {fmt(first_partial)}")
    print(f"final result:   {fmt(final_at)}")
    wait = final_at - sent_at if final_at is not None else None
    print(f"answer latency: {fmt(wait)}  (final result after the player stops talking)")


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    asyncio.run(transcribe(args[0], args[1] if len(args) > 1 else "en_us-gen2", "--realtime" in sys.argv))
