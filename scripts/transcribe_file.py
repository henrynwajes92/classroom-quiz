"""Stream a WAV file to Cobalt Transcribe over WebSocket and report timing.

    python scripts/transcribe_file.py clips/paris.wav [model_id] [--realtime] [--raw | --streamhdr]

--realtime paces the audio like a live microphone instead of sending it all at
once, so the latency numbers match what a phone player would see.

--raw strips the 44-byte WAV header and sends headerless 16 kHz mono 16-bit
signed little-endian PCM using `audio_format_raw` (what a phone produces).

--streamhdr sends a WAV header whose RIFF/data sizes are 0xFFFFFFFF
("unknown length", as when streaming), followed by the PCM. Fallback in case
raw is not accepted.
"""
import asyncio
import base64
import json
import ssl
import struct
import sys
import time

import certifi
import websockets

URL = "wss://demo.cobaltspeech.com/transcribe/api/transcribe/v5/streaming-recognize"
CHUNK = 8192  # bytes; 16 kHz * 16-bit mono = 32000 bytes per second
BYTES_PER_SEC = 32000
WAV_HEADER = 44
TLS = ssl.create_default_context(cafile=certifi.where())

WAV_CONFIG = {"audio_format_headered": "AUDIO_FORMAT_HEADERED_WAV"}
RAW_CONFIG = {"audio_format_raw": {
    "encoding": "AUDIO_ENCODING_SIGNED",
    "bit_depth": 16,
    "byte_order": "BYTE_ORDER_LITTLE_ENDIAN",
    "sample_rate": 16000,
    "channels": 1,
}}


def streaming_wav_header(sample_rate=16000, channels=1, bits=16):
    """A 44-byte WAV header with unknown (0xFFFFFFFF) RIFF and data sizes."""
    block_align = channels * bits // 8
    return (b"RIFF" + struct.pack("<I", 0xFFFFFFFF) + b"WAVE"
            + b"fmt " + struct.pack("<IHHIIHH", 16, 1, channels, sample_rate,
                                    sample_rate * block_align, block_align, bits)
            + b"data" + struct.pack("<I", 0xFFFFFFFF))


async def transcribe(path, model_id, realtime=False, mode="wav"):
    with open(path, "rb") as f:
        audio = f.read()
    pcm = audio[WAV_HEADER:]
    audio_seconds = len(pcm) / BYTES_PER_SEC
    if mode == "raw":
        audio, fmt_config = pcm, RAW_CONFIG
    elif mode == "streamhdr":
        audio, fmt_config = streaming_wav_header() + pcm, WAV_CONFIG
    else:
        fmt_config = WAV_CONFIG

    t_connect = time.monotonic()
    first_partial = None
    final_at = None
    finals = []
    async with websockets.connect(URL, ssl=TLS, max_size=None) as ws:
        t0 = time.monotonic()  # timings below start once the connection is open
        connect_s = t0 - t_connect
        await ws.send(json.dumps({"config": {"model_id": model_id, **fmt_config}}))

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
                    sender.cancel()
                    raise SystemExit("server error: " + json.dumps(msg["error"]))
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
    print(f"format:         {mode}  model: {model_id}")
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
    mode = "raw" if "--raw" in sys.argv else "streamhdr" if "--streamhdr" in sys.argv else "wav"
    asyncio.run(transcribe(args[0], args[1] if len(args) > 1 else "en_us-gen2",
                           "--realtime" in sys.argv, mode))
