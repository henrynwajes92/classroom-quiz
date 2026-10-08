# Problem: Classroom quiz, thirty at once

## Pitch

Thirty players answer a quiz question aloud at the same time, each on their own
phone. Answers are transcribed live and the leaderboard updates in real time.
It's a game on the surface and a load test underneath.

## Why it matters (what it sells)

Evidence that the Cobalt transcribe API holds up at classroom scale: about 30
concurrent audio streams with acceptable latency and accuracy.

## What we can do by Wednesday that we can't do today

Run a live quiz round with **30 simulated players + 5 real players** streaming
audio concurrently to the transcribe API, and show:

- a live leaderboard driven by transcribed answers
- what the API did under that load: latency, errors, dropped streams, accuracy

## Demo (5-minute video)

1. Five real people join on their phones; thirty simulated players join from a script.
2. A question is shown; everyone answers aloud at once.
3. The leaderboard updates live.
4. A metrics view/report shows API behaviour under the 35-stream load.

## Constraints

- **Tell Cobalt ops before pointing 30+ streams at the API.** No load runs
  until ops has acknowledged them.
- Scope is "rough": a working, demoable prototype, not production code.

## What the API guide tells us

Source: "Cobalt Demo Speech APIs" developer guide (tested 2026-10-07/08).

- **Server**: `demo.cobaltspeech.com`, TLS, no API key.
- **Transport**: `StreamingRecognize` over gRPC (`:443`) or WebSocket
  (`wss://demo.cobaltspeech.com/transcribe/api/transcribe/v5/streaming-recognize`).
  WebSocket is plain JSON with base64 audio, so phones can stream straight from
  the browser.
- **Protocol**: config message first, then audio chunks, then an empty audio
  message. Partial results stream back; `is_partial: false` is final.
- **Audio**: 16 kHz, mono, 16-bit WAV (`AUDIO_FORMAT_HEADERED_WAV`).
- **Models**: `en_us-gen2` (most accurate, 3.2% WER), `en_us-gen1-16khz`
  (faster, 8.8% WER, supports recognition context, useful for a fixed answer
  vocabulary), plus Nigerian English models.
- **Quirk**: the WebSocket closes with code 1006 even on success. Treat it as
  the normal end once a final result has arrived.
- **Simulated players**: VoiceGen (8 voices) can generate the answer audio.
  Generate it once, ahead of time, and fix the WAV header sizes before sending
  it to Transcribe.

### Capacity: the main risk

The demo server is a single small shared instance. Measured limits:

| Model | Real-time streams it keeps up with | Beyond that |
|---|---|---|
| `en_us-gen2` | about 1 | 4 streams: results take ~3x the audio length |
| `en_us-gen1-16khz` | about 4 | 8 streams: ~2x the audio length |

Requests queue rather than fail. **35 concurrent streams on the demo server
will not be real time**, and the guide says not to load-test it without
agreeing with the Cobalt team first, because others use it for live demos.

Options to discuss with ops:
1. A dedicated Transcribe deployment for the test (the only way to show
   "holds up at classroom scale").
2. Use the demo server for development only (1–5 streams), and run the
   35-stream test against the dedicated instance.
3. If there's no dedicated instance, the report shows how the demo server
   degrades, which is honest data but the opposite of the pitch.

## Open questions

- Can ops provide a dedicated Transcribe instance for the load test, and when?
- How are answers scored: exact match, fuzzy match, or keyword? Could gen1
  recognition context bias toward the answer options?
- Phone client: mobile web page (no install) is the assumed default.
