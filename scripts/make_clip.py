"""Generate a spoken answer clip with Cobalt VoiceGen, as 16 kHz mono 16-bit WAV.

    python scripts/make_clip.py "The answer is Paris" clips/paris.wav [speaker_id]
"""
import os
import struct
import sys

import requests

BASE = "https://demo.cobaltspeech.com/voicegen/api/voicegen/v1"


def fix_wav_header(path):
    # Streamed WAV headers carry placeholder sizes; Transcribe rejects them.
    size = os.path.getsize(path)
    with open(path, "r+b") as f:
        f.seek(4)
        f.write(struct.pack("<I", size - 8))
        f.seek(40)
        f.write(struct.pack("<I", size - 44))


def synthesize(text, out_path, speaker_id="cobalt_steve"):
    params = {
        "text.text": text,
        "config.model_id": "en_US",
        "config.speaker_id": speaker_id,
        "config.audio_format.codec": "AUDIO_CODEC_WAV",
        "config.audio_format.sample_rate": 16000,
        "config.audio_format.channels": 1,
        "config.audio_format.bit_depth": 16,
        "config.audio_format.encoding": "AUDIO_ENCODING_SIGNED",
        "config.audio_format.byte_order": "BYTE_ORDER_LITTLE_ENDIAN",
    }
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with requests.get(f"{BASE}/streaming-synthesize", params=params, stream=True, timeout=60) as resp:
        if resp.status_code != 200:
            raise SystemExit(f"HTTP {resp.status_code}: {resp.text}")
        with open(out_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
    fix_wav_header(out_path)
    seconds = (os.path.getsize(out_path) - 44) / 32000
    print(f"wrote {out_path} ({seconds:.1f} s of audio)")


if __name__ == "__main__":
    synthesize(sys.argv[1], sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else "cobalt_steve")
