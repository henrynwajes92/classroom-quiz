# Plan: Classroom quiz, thirty at once

Status: agreed, 2026-10-08. Goal and constraints are in `PROBLEM.md`.

## Stack

- **Game server**: Python 3, FastAPI + WebSockets (asyncio). One process.
- **Phone client**: one plain HTML/JS page served by the game server. No
  framework, no install.
- **Host screen**: a second page (laptop/projector) showing the question, the
  live leaderboard and live API metrics.
- **Simulator**: Python asyncio script, reusing `scripts/make_clip.py` and
  the streaming code from `scripts/transcribe_file.py`.
- **Config**: `TRANSCRIBE_URL` and `TRANSCRIBE_MODEL` environment variables,
  so moving from the demo server to a dedicated instance is a config change.

## Architecture

```
 phone (x5 real)  ──┐                                    ┌──> Cobalt Transcribe
 simulator (x30)  ──┼─ WebSocket ─> game server ─ WS x N ┤    (1 stream per player
                    │   (audio up,   - rooms/questions    │     per question)
 host screen  <─────┘   results      - scoring            └──<  partial/final results
                         down)       - leaderboard
                                     - metrics recorder
```

**Audio goes through the game server**, not straight from phone to Cobalt:
- every stream is measured in one place, for real and simulated players alike
- the server can open the Transcribe connection early (see below)
- the Transcribe URL never has to reach the phones

The simulator connects to the **game server** like a phone does, so a
load run tests the whole system, not just the API.

## How one question works

1. Host starts a question. Server broadcasts it to all players.
2. **Server opens one Transcribe stream per player right away and sends the
   config.** Our test showed connecting takes 1–2 s; doing it here keeps that
   delay out of the answer latency.
3. Player taps "hold to answer" and speaks. The phone sends 16 kHz mono 16-bit
   PCM chunks; the server forwards them to that player's stream.
4. On release, the phone sends "done"; the server sends the empty audio message.
5. Partial results are shown on the player's phone as they arrive. The final
   result is scored.
6. Leaderboard update is broadcast to everyone.
7. After a time limit, unused streams are closed and the round ends.

## Scoring

- Each question has a list of accepted answers (`"paris"`, `"paris france"`).
- Normalize the transcript (lowercase, strip punctuation and filler like "the
  answer is"), then exact match, then fuzzy match (edit distance) as fallback.
- Points: correct answers score; faster final results score more.
- Questions use short, distinct answers so transcription errors are rare and
  the game stays fair.

## Metrics (per stream)

Recorded by the server for every player and question:

- connect time
- time from end of speech to first partial and to final result (**answer latency**)
- errors, timeouts, dropped connections
- transcript vs expected answer (correct/incorrect; WER for simulated players,
  whose exact words we know)

Shown live on the host screen. Saved after each round as JSON + CSV, with a
summary: p50/p95 latency, error rate and accuracy **by number of concurrent
streams**.

## Load test design

Ramp up instead of jumping to 35: **1 → 5 → 10 → 20 → 35** concurrent players,
a few rounds each. That shows where latency starts to climb, which is the most
useful thing to show customers.

- Demo server: development and tests at **≤ 4 streams only**, per the guide's limits.
- Dedicated instance (to be agreed with ops): the full ramp and the demo.
- Simulated players get random start delays (0–2 s) and 8 different VoiceGen
  voices, so they don't all speak in perfect sync.

## Risks to check early

| Risk | Why it matters | What we do |
|---|---|---|
| No dedicated instance from ops | Demo server keeps up with ~4 streams | Ask today; build on demo server meanwhile |
| Phone mic needs HTTPS | Browsers block the microphone on plain `http://` LAN addresses | Serve over HTTPS via a tunnel (e.g. cloudflared) or a local certificate; test on a phone in Day 1 |
| Raw PCM format untested | The guide only tested WAV files; phones produce raw PCM | Spike `audio_format_raw` first; fall back to a WAV header if it fails |
| iPhone Safari audio quirks | Sample rate and permission behaviour differ | Resample to 16 kHz in the page; test on iOS and Android |
| Venue Wi-Fi | 35 streams over one network | Simulator runs from a laptop on wired/stable network; phones only add 5 |

## Milestones

| Day | Goal |
|---|---|
| Thu 10/8 | ✅ Problem defined, streaming test working. Plan reviewed, issues created, ops contacted |
| Fri 10/9 | Raw-PCM spike, game server skeleton, one simulated player playing one round end to end |
| Mon 10/12 | Phone page working over HTTPS on a real phone; scoring; live leaderboard; simulator for N players |
| Tue 10/13 | Metrics + host dashboard + report; ramp test; dress rehearsal (5 real + 30 simulated) |
| Wed 10/14 | Record and post the 5-minute demo before the meeting |

Weekend work is optional; the plan doesn't depend on it.

## Issues (in dependency order)

Items in the same group can be worked in parallel by separate agents.

**Group 0 (now)**
1. Contact Cobalt ops: dedicated Transcribe instance for a 35-stream test
2. Spike: stream raw 16 kHz PCM (`audio_format_raw`) to Transcribe

**Group 1**
3. Game server skeleton: rooms, join, question broadcast, WebSocket protocol doc
4. Transcribe bridge: per-player stream, opened at question start, forwards audio, returns partial/final results
5. Generate clip library: accepted answers × 8 voices with VoiceGen

**Group 2**
6. Phone page: join, hold-to-answer, mic capture → 16 kHz PCM → server, show partials
7. Scoring + leaderboard broadcast
8. Simulator: N fake players over the game-server protocol, random delays and voices
9. HTTPS for phones (tunnel or certificate) + test on iOS and Android

**Group 3**
10. Metrics recorder + JSON/CSV report with p50/p95 by concurrency
11. Host screen: question, live leaderboard, live metrics
12. Ramp test runner (1 → 5 → 10 → 20 → 35)

**Group 4**
13. Dress rehearsal (5 real + 30 simulated), fix issues
14. Record the 5-minute demo video

## Decisions (2026-10-08)

- **Stack**: Python game server + plain web page for phones and host screen.
- **Answer input**: "hold to answer" button. Pressing marks the start of
  speech, releasing marks the end, so no voice-activity detection is needed.
- **Question set**: general knowledge, with short and distinct spoken answers.
