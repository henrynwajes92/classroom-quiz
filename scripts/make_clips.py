"""Generate the simulated-player clip library with Cobalt VoiceGen (CQ-5).

For every question in the question file this writes 8 correct clips (one per
VoiceGen voice, mixed phrasings such as "Paris", "The answer is Paris",
"I think it's Paris") and 4 wrong clips (plausible wrong answers, different
voices), each with a slightly varied speech rate (0.9-1.15). Spoken forms
and wrong answers live in `questions/general_clips.json`: per question id,
"say" (correct forms, each an accepted answer), optional "in_sentence" (forms
used after "It's ..." etc., default "say") and "wrong".

Regenerate (from the repo root, in WSL):

    .venv/bin/python scripts/make_clips.py                  # whole library, skips up-to-date clips
    .venv/bin/python scripts/make_clips.py --questions france-capital,red-planet
    .venv/bin/python scripts/make_clips.py --force          # re-synthesize everything
    .venv/bin/python scripts/make_clips.py --check          # scorer check only, no VoiceGen calls

Options: --questions-file (default questions/general.json), --answers-file
(default questions/general_clips.json), --out (default clips).

The demo VoiceGen server allows only a few concurrent requests, so this uses
2 workers and retries 503s with backoff. The full library (144 clips) takes
about 2 minutes. A clip is re-synthesized when its file is missing or its
text/voice/rate differs from the manifest entry.

Output: `clips/<question_id>/<nn>_<voice>.wav`, 16 kHz mono 16-bit WAV with
correct header sizes. `clips/` is gitignored, so neither clips nor manifest
are committed; regenerate them with this script.

Manifest `clips/manifest.json` is a JSON list, one object per clip:

    {"path": "clips/france-capital/01_LTTS_8797.wav",  # relative to repo root
     "question_id": "france-capital",
     "text": "The answer is Paris",   # exactly what was spoken
     "answer": "Paris",               # the answer inside the text
     "correct": true,                 # whether the answer is right
     "voice": "LTTS_8797",
     "speech_rate": 1.04,
     "duration": 1.31}                # seconds

Each run also checks the spoken text against `server.scoring`: wrong clips
must score wrong; correct clips' bare answers must score correct. Correct
clips with a phrasing ("The answer is ...") are listed if the scorer does not
accept the full text (since CQ-7's filler stripping, all are accepted).
"""
import argparse
import json
import os
import random
import struct
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scripts.make_clip import BASE, fix_wav_header  # noqa: E402
from server.questions import load_questions  # noqa: E402
from server.scoring import score  # noqa: E402

FALLBACK_VOICES = ["cobalt_steve", "LTTS_8797", "LTTS_2300", "LTTS_251",
                   "LTTS_8123", "LTTS_5789", "LTTS_4137", "LTTS_8555"]
# One correct clip per template (and per voice); None means the bare answer.
CORRECT_TEMPLATES = [None, "The answer is {}", None, "I think it's {}",
                     None, "It's {}", None, "{}, I think"]
WRONG_TEMPLATES = [None, "I think it's {}", None, "The answer is {}"]
WORKERS = 2  # the demo server fails above ~6 concurrent requests
RETRIES = 6
MIN_SECONDS = 0.3


def list_voices():
    try:
        resp = requests.get(f"{BASE}/list-models", timeout=10)
        resp.raise_for_status()
        model = next(m for m in resp.json()["models"] if m["id"] == "en_US")
        return [s["id"] for s in model["attributes"]["speakers"]]
    except Exception as e:
        print(f"list-models failed ({e}); using built-in voice list")
        return FALLBACK_VOICES


def say(template, answer):
    text = template.format(answer) if template else answer
    return text[0].upper() + text[1:]


def plan_clips(questions, answers, voices, out_dir):
    """The full list of clips to have, as manifest entries without durations.
    Voices rotate by the question's position in the answers file, so a subset
    run plans the same clips as a full run."""
    order = [k for k in answers if not k.startswith("_")]
    clips = []
    for q in questions:
        qi = order.index(q.id)
        forms, wrong = answers[q.id]["say"], answers[q.id]["wrong"]
        in_sentence = answers[q.id].get("in_sentence", forms)
        specs = []
        bare = phrased = 0
        for i, template in enumerate(CORRECT_TEMPLATES):
            if template:
                answer, phrased = in_sentence[phrased % len(in_sentence)], phrased + 1
            else:
                answer, bare = forms[bare % len(forms)], bare + 1
            specs.append((template, answer, True, voices[(qi + i) % len(voices)]))
        for j, (template, answer) in enumerate(zip(WRONG_TEMPLATES, wrong)):
            specs.append((template, answer, False, voices[(qi + 2 * j + 1) % len(voices)]))
        for n, (template, answer, correct, voice) in enumerate(specs):
            rate = round(random.Random(f"{q.id}/{n}").uniform(0.9, 1.15), 2)
            path = out_dir / q.id / f"{n:02d}_{voice}.wav"
            clips.append({"path": path.relative_to(ROOT).as_posix(), "question_id": q.id,
                          "text": say(template, answer), "answer": answer, "correct": correct,
                          "voice": voice, "speech_rate": rate, "duration": None})
    return clips


def synthesize(text, out_path, voice, rate):
    """Like make_clip.synthesize, plus speech rate, retries and an atomic write.
    Returns the number of retries needed."""
    params = {
        "text.text": text,
        "config.model_id": "en_US",
        "config.speaker_id": voice,
        "config.speech_rate": rate,
        "config.audio_format.codec": "AUDIO_CODEC_WAV",
        "config.audio_format.sample_rate": 16000,
        "config.audio_format.channels": 1,
        "config.audio_format.bit_depth": 16,
        "config.audio_format.encoding": "AUDIO_ENCODING_SIGNED",
        "config.audio_format.byte_order": "BYTE_ORDER_LITTLE_ENDIAN",
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(".part")
    for attempt in range(RETRIES):
        try:
            with requests.get(f"{BASE}/streaming-synthesize", params=params,
                              stream=True, timeout=60) as resp:
                if resp.status_code == 200:
                    with open(tmp, "wb") as f:
                        for chunk in resp.iter_content(chunk_size=8192):
                            f.write(chunk)
                    fix_wav_header(tmp)
                    os.replace(tmp, out_path)
                    return attempt
                if resp.status_code < 500:
                    raise RuntimeError(f"HTTP {resp.status_code}: {resp.text}")
                problem = f"HTTP {resp.status_code}"
        except requests.RequestException as e:
            problem = type(e).__name__
        wait = min(2 ** (attempt + 1), 30)
        print(f"  retry {out_path.name}: {problem}, waiting {wait} s")
        time.sleep(wait)
    raise RuntimeError(f"{out_path}: gave up after {RETRIES} attempts")


def validate(path):
    """Check header sizes and format; return the duration in seconds."""
    size = path.stat().st_size
    with open(path, "rb") as f:
        header = f.read(44)
        pcm = f.read()
    riff, riff_size, wave, fmt, _, codec, channels, rate, _, _, bits, data, data_size = \
        struct.unpack("<4sI4s4sIHHIIHH4sI", header)
    problems = []
    if (riff, wave, fmt, data) != (b"RIFF", b"WAVE", b"fmt ", b"data"):
        problems.append("not a plain 44-byte-header WAV")
    if riff_size != size - 8 or data_size != size - 44:
        problems.append("header sizes not fixed")
    if (codec, channels, rate, bits) != (1, 1, 16000, 16):
        problems.append(f"format {codec}/{channels}ch/{rate}Hz/{bits}bit")
    seconds = data_size / 32000
    if seconds < MIN_SECONDS:
        problems.append(f"too short ({seconds:.2f} s)")
    samples = struct.unpack(f"<{len(pcm) // 2}h", pcm[:len(pcm) // 2 * 2])
    if max(map(abs, samples), default=0) < 1000:
        problems.append("silent")
    if problems:
        raise ValueError(f"{path}: {', '.join(problems)}")
    return round(seconds, 2)


def check_scoring(clips, questions):
    """Run spoken text through server.scoring. Returns (errors, unaccepted phrasings)."""
    by_id = {q.id: q for q in questions}
    errors, phrased = [], []
    for c in clips:
        q = by_id[c["question_id"]]
        text_ok = score(q, c["text"], 1.0, 20.0)[0]
        answer_ok = score(q, c["answer"], 1.0, 20.0)[0]
        if not c["correct"] and (text_ok or answer_ok):
            errors.append(f"wrong clip scores correct: {c['question_id']}: {c['text']!r}")
        elif c["correct"] and not answer_ok:
            errors.append(f"correct answer not accepted: {c['question_id']}: {c['answer']!r}")
        elif c["correct"] and not text_ok:
            phrased.append(c)
    return errors, phrased


def main():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--questions", help="comma-separated question ids (default: all)")
    p.add_argument("--questions-file", default="questions/general.json")
    p.add_argument("--answers-file", default="questions/general_clips.json")
    p.add_argument("--out", default="clips")
    p.add_argument("--force", action="store_true", help="re-synthesize existing clips")
    p.add_argument("--check", action="store_true", help="scorer check only, no VoiceGen calls")
    args = p.parse_args()

    questions = load_questions(ROOT / args.questions_file)
    with open(ROOT / args.answers_file, encoding="utf-8") as f:
        answers = json.load(f)
    missing = [q.id for q in questions if q.id not in answers]
    if missing:
        raise SystemExit(f"no spoken answers for: {', '.join(missing)} (add them to {args.answers_file})")
    if args.questions:
        wanted = args.questions.split(",")
        unknown = set(wanted) - {q.id for q in questions}
        if unknown:
            raise SystemExit(f"unknown question ids: {', '.join(sorted(unknown))}")
        questions = [q for q in questions if q.id in wanted]

    out_dir = (ROOT / args.out).resolve()
    voices = FALLBACK_VOICES if args.check else list_voices()
    clips = plan_clips(questions, answers, voices, out_dir)

    errors, phrased = check_scoring(clips, questions)
    print(f"scorer check: {len(clips)} clips, {len(errors)} errors, "
          f"{len(phrased)} correct clips whose full phrasing the scorer does not accept yet")
    for e in errors:
        print("  ERROR", e)
    if phrased:
        forms = sorted({c["text"].lower().replace(c["answer"].lower(), "...") for c in phrased})
        print("  not accepted yet:", ", ".join(forms))
    if errors:
        raise SystemExit("fix questions/general_clips.json")
    if args.check:
        return

    manifest_path = out_dir / "manifest.json"
    previous = []
    if manifest_path.exists():
        with open(manifest_path, encoding="utf-8") as f:
            previous = json.load(f)
    # A clip is up to date if its file exists and was made with the same text/voice/rate.
    made = {c["path"]: (c["text"], c["voice"], c["speech_rate"]) for c in previous}

    def up_to_date(c):
        return ((ROOT / c["path"]).exists()
                and made.get(c["path"], (c["text"], c["voice"], c["speech_rate"]))
                == (c["text"], c["voice"], c["speech_rate"]))

    todo = [c for c in clips if args.force or not up_to_date(c)]
    print(f"{len(clips)} clips for {len(questions)} questions, {len(todo)} to synthesize "
          f"({len(clips) - len(todo)} up to date), {len(voices)} voices")
    started = time.monotonic()

    def make(c):
        retries = synthesize(c["text"], ROOT / c["path"], c["voice"], c["speech_rate"])
        print(f"  {c['path']}  {c['text']!r}")
        return retries

    with ThreadPoolExecutor(WORKERS) as pool:
        retries = sum(pool.map(make, todo))
    elapsed = time.monotonic() - started

    for c in clips:
        c["duration"] = validate(ROOT / c["path"])

    # Keep manifest entries of questions not generated this run.
    done_ids = {q.id for q in questions}
    old = [c for c in previous if c["question_id"] not in done_ids]
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(sorted(old + clips, key=lambda c: c["path"]), f, indent=1)

    print()
    print(f"{'question':16} correct wrong  audio s")
    for q in questions:
        qc = [c for c in clips if c["question_id"] == q.id]
        print(f"{q.id:16} {sum(c['correct'] for c in qc):7} {sum(not c['correct'] for c in qc):5}"
              f"  {sum(c['duration'] for c in qc):7.1f}")
    total = sum(c["duration"] for c in clips)
    print(f"total: {len(clips)} clips, {total:.1f} s of audio, all valid; "
          f"synthesized {len(todo)} in {elapsed:.1f} s with {retries} retries")
    print(f"manifest: {manifest_path.relative_to(ROOT)} ({len(old) + len(clips)} clips)")


if __name__ == "__main__":
    main()
