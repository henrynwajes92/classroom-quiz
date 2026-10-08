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

# tests (no Cobalt calls)
.venv/bin/python -m pytest -q
```

Server settings (env vars): `TRANSCRIBE_URL`, `TRANSCRIBE_MODEL`,
`QUESTIONS_FILE` (default `questions/general.json`), `ROUND_SECONDS`
(default 20). The client/server message contract is `docs/protocol.md`.
