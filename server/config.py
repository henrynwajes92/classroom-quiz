"""Settings from environment variables, with defaults for local development."""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

TRANSCRIBE_URL = os.environ.get(
    "TRANSCRIBE_URL",
    "wss://demo.cobaltspeech.com/transcribe/api/transcribe/v5/streaming-recognize",
)
TRANSCRIBE_MODEL = os.environ.get("TRANSCRIBE_MODEL", "en_us-gen1-16khz")

QUESTIONS_FILE = Path(os.environ.get("QUESTIONS_FILE", ROOT / "questions" / "general.json"))
ROUND_SECONDS = float(os.environ.get("ROUND_SECONDS", "20"))
MIN_TIME_LIMIT, MAX_TIME_LIMIT = 3, 120  # allowed per-question override from the host

# Audio from phones: 16 kHz mono 16-bit PCM, i.e. 32000 bytes per second.
SAMPLE_RATE = 16000
BYTES_PER_SEC = 32000
MAX_CHUNK_BYTES = 64 * 1024     # one audio message; phones send ~0.1-0.25 s chunks
MAX_ANSWER_BYTES = 15 * BYTES_PER_SEC  # 15 s of audio per answer is plenty
