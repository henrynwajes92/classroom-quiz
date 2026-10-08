"""Settings from environment variables, with defaults for local development."""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

TRANSCRIBE_URL = os.environ.get(
    "TRANSCRIBE_URL",
    "wss://demo.cobaltspeech.com/transcribe/api/transcribe/v5/streaming-recognize",
)
TRANSCRIBE_MODEL = os.environ.get("TRANSCRIBE_MODEL", "en_us-gen1-16khz")
# "transcribe" (default) or "logging" (no recognition; see server/bridge.py).
BRIDGE = os.environ.get("BRIDGE", "transcribe")
# The demo server keeps up with ~4 streams, so the bridge opens at most 4 at
# once there. ALLOW_DEMO_LOAD=1 lifts that cap; only with ops' go-ahead.
ALLOW_DEMO_LOAD = os.environ.get("ALLOW_DEMO_LOAD") == "1"
# Seconds without a new result, after a final for the end of the answer, before
# the answer counts as complete (Transcribe itself only closes the stream ~2 s later).
FINAL_QUIET_GAP = float(os.environ.get("FINAL_QUIET_GAP", "0.5"))

QUESTIONS_FILE = Path(os.environ.get("QUESTIONS_FILE", ROOT / "questions" / "general.json"))
ROUND_SECONDS = float(os.environ.get("ROUND_SECONDS", "20"))
MIN_TIME_LIMIT, MAX_TIME_LIMIT = 3, 120  # allowed per-question override from the host

# Audio from phones: 16 kHz mono 16-bit PCM, i.e. 32000 bytes per second.
SAMPLE_RATE = 16000
BYTES_PER_SEC = 32000
MAX_CHUNK_BYTES = 64 * 1024     # one audio message; phones send ~0.1-0.25 s chunks
MAX_ANSWER_BYTES = 15 * BYTES_PER_SEC  # 15 s of audio per answer is plenty
