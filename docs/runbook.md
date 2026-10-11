# Runbook: full ramp, dress rehearsal, demo recording

Everything below is built and tested. The steps that are left need people
or ops: a dedicated Transcribe endpoint (CQ-1), real phones (CQ-6/CQ-9,
CQ-13) and the recording (CQ-14). Run from the repo root in WSL.

## 0. Before anything at scale: CQ-1

The demo server keeps up with ~4 streams, and every guard here (server
cap, simulator, ramp) refuses more against it. For 35 streams you need from
ops: the dedicated instance's streaming URL, the model id, and the agreed
time window. Then for every step below:

```sh
export TRANSCRIBE_URL=wss://<dedicated-host>/.../streaming-recognize
export TRANSCRIBE_MODEL=en_us-gen1-16khz   # or what ops says
```

`/health` then reports that host, so the simulator and ramp let more than 4
players through without `--i-asked-ops`. Never point them at
`demo.cobaltspeech.com` with `--i-asked-ops` or `ALLOW_DEMO_LOAD=1`.

## 1. Real phones (CQ-6/CQ-9, ~15 min, today if possible)

```sh
scripts/tunnel.sh            # starts the server too if none runs; prints the https URL + QR
```

Open `http://localhost:8000/host` on the laptop: it shows the tunnel's https
link and QR. Join from one iPhone (Safari) and one Android (Chrome), answer
two questions each. Check: mic permission prompt, partials while holding,
the result, and the leaderboard showing them with the PHONE tag. Note what
breaks on CQ-6 / CQ-9, then mark them done.

## 2. Full ramp (CQ-12), inside ops' window

```sh
.venv/bin/uvicorn server.app:app --host 0.0.0.0 --port 8000      # with TRANSCRIBE_URL set
.venv/bin/python scripts/ramp.py --levels 1,5,10,20,35 --rounds 3 --pause 10 \
    --server ws://localhost:8000 --out reports/ramp-$(date +%Y%m%d)
```

About 15 rounds × ~25 s + pauses, so ~10 minutes. Output: `ramp.json`,
`ramp.csv`, `ramp.svg` (latency and errors vs players) and each level's
stream rows. Commit the folder (`reports/ramp-*/` is tracked). The dry run
at 1 and 4 players on the demo server is in `reports/ramp-dry-run-20261011/`.

## 3. Dress rehearsal (CQ-13): 5 phones + 30 simulated

1. Server with `TRANSCRIBE_URL` set, plus `scripts/tunnel.sh`.
2. Host screen on the projector/laptop: `http://localhost:8000/host`.
   Note the room code.
3. Five people join from their phones using the QR.
4. Simulated players join the same room:
   ```sh
   .venv/bin/python scripts/simulate.py --players 30 --room CODE --server ws://localhost:8000 --rounds 5
   ```
5. Run 5 questions from the host screen (N = next, E = end round).
6. Afterwards: `reports/<time>-<CODE>/summary.csv` has the numbers;
   check errors, cut-offs, phone players' transcripts. Fix and repeat.

Watch for: venue Wi-Fi (simulator on a wired/stable laptop), phones that
lock their screen (mic stops), the tunnel URL changing on every
`tunnel.sh` restart (re-show the QR).

## 4. Demo recording (CQ-14), 5 minutes

Record the host screen at 1920×1080 (browser full screen, F11).

1. **0:00 Pitch (30 s).** Thirty players answer aloud at once; the
   leaderboard comes from live transcription. A game on top, a load test
   underneath.
2. **0:30 Join (45 s).** Host screen with QR; phones join, then 30
   simulated players (run the simulate command off camera).
3. **1:15 Two or three live rounds (2 min).** Answers feed and leaderboard
   update live, PHONE tags on real players; the metrics panel shows
   streams open, answer p50/p95, API latency and errors.
4. **3:15 The evidence (1 min 15 s).** Open `ramp.svg` from step 2:
   latency per level from 1 to 35 players, error rate. Say what "answer"
   (release → scored, includes the 0.5 s quiet gap) and "API" (Transcribe's
   end-of-audio → final) mean.
5. **4:30 Wrap-up (30 s).** What it shows customers, where the report lives,
   what we'd test next.

Post the video and `reports/ramp-<date>/` before Wednesday's meeting.
