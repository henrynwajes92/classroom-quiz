# CLAUDE.md

Guidance for Claude Code when working in this repository.

## Project

Classroom quiz: a multiplayer voice quiz that doubles as a load test for the
Cobalt transcribe API. ~30 players answer aloud on their phones at once, and a
live leaderboard updates from the transcriptions. See `PROBLEM.md` for the full
problem statement, demo plan and open questions.

## Goal for the hackathon

By Wednesday: 30 simulated players + 5 real players in one live round, plus a
report of API latency/errors/accuracy under that load.

## Rules

- **Never run a load test (many concurrent streams) against the Cobalt API
  without confirming ops has been notified.** Ask the user first.
- Keep it rough and demoable; prefer simple over complete.
- Track work as GitHub issues; reference the issue number in commits.
- Don't commit API keys or credentials. Use environment variables / `.env`
  (gitignored).

## Transcribe API

See "What the API guide tells us" in `PROBLEM.md`. Key points: WebSocket
streaming with JSON and base64 audio, 16 kHz mono 16-bit WAV, close code 1006
is the normal end of a stream. The shared demo server only keeps up with ~1
`en_us-gen2` / ~4 `en_us-gen1-16khz` real-time streams, so use it for
development at low concurrency only.

## Plan

`PLAN.md` holds the agreed stack (Python FastAPI server + plain web pages),
architecture, milestones and the issue list. Follow it; update it when a
decision changes.

## Architecture (planned, update as it firms up)

- **Game server**: rooms, questions, scoring, leaderboard; pushes updates to clients.
- **Phone client**: mobile web page that joins a room and streams mic audio.
- **Transcription bridge**: forwards each player's audio stream to the transcribe API and returns text.
- **Simulator**: script that spawns N fake players streaming pre-recorded answer audio.
- **Metrics**: per-stream latency, errors and accuracy, shown live and saved as a report.

## Commands

Run from the repo root (in WSL), using the venv:

```sh
# install
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt

# run the game server (host: ws://<ip>:8000/ws/host, players: /ws/play)
.venv/bin/uvicorn server.app:app --host 0.0.0.0 --port 8000

# tests (no Cobalt calls; Transcribe is faked by a local WebSocket server)
.venv/bin/python -m pytest -q

# live check through the game server against Transcribe: one round with 1
# player, then one with 3 (each player = 1 stream; keep it <= 4 on the demo server).
.venv/bin/python scripts/play_clip.py --players 1,3 clips/france-capital/00_cobalt_steve.wav

# scorer check of the clip texts (no network): correct clips must score right, wrong ones wrong
.venv/bin/python scripts/make_clips.py --check

# one clip with/without recognition context (gen1 only; 1 stream)
.venv/bin/python scripts/transcribe_file.py clips/largest-ocean/04_LTTS_251.wav en_us-gen1-16khz --raw --context=pacific,the\ pacific

# simulator: N fake players over /ws/play, streaming clips/manifest.json clips
# (random 0-2 s delay, voices spread, --correct-rate 0.75). Against a running
# server: --room KXQB joins an existing room; --host creates one and runs
# --rounds questions. Refuses > 4 players when the server's /health says it
# uses the demo server (or, if it can't say, while TRANSCRIBE_URL is or may be
# the demo server) unless --i-asked-ops; the server's own cap of 4 streams
# still applies unless it runs with ALLOW_DEMO_LOAD=1. Wrap live runs in
# `flock /tmp/cobalt_demo.lock` so agents don't overlap on the demo server.
.venv/bin/python scripts/simulate.py --players 4 --host --rounds 2 --server ws://localhost:8000 --out results.json
```

Server settings (env vars): `TRANSCRIBE_URL`, `TRANSCRIBE_MODEL`,
`QUESTIONS_FILE` (default `questions/general.json`), `ROUND_SECONDS`
(default 20), `BRIDGE` (`transcribe`, the default, or `logging` for no
recognition), `ALLOW_DEMO_LOAD` (set to `1` to lift the bridge's cap of 4
concurrent streams when `TRANSCRIBE_URL` is the demo server; beyond the cap a
stream waits up to 10 s for a free slot, then the answer gets a
`transcribe_unavailable` final. Only with ops' go-ahead),
`FINAL_QUIET_GAP` (default 0.5: seconds of no new result after a final
before the answer is scored, instead of waiting ~2 s for Transcribe to close
the stream), `RECOGNITION_CONTEXT` (set to `1` to compile each question's
accepted answers with Transcribe's CompileContext and send them as recognition
context; gen1 models only; off by default). The
client/server message contract is `docs/protocol.md`. Scoring rules and the
points formula are in the `server/scoring.py` docstring.
