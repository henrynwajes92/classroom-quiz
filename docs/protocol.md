# Game server WebSocket protocol

The contract between the game server (`server/`) and its clients: the phone
page, the host screen and the simulator. The simulator speaks exactly the
player protocol, same as a phone.

Status markers:
- **CQ-7**: scoring is a placeholder (exact match, flat 100 points) until
  CQ-7. Message shapes stay the same; only the numbers change.

## Connections

| Endpoint | Who | Notes |
|---|---|---|
| `ws://<server>/ws/host` | host screen | Connecting **creates a room**. One host per room. When the host disconnects, the room closes. |
| `ws://<server>/ws/play` | phones, simulator | First message must be `join`. One player per connection. |
| `GET /health` | anyone | `{"ok": true, "rooms": 1, "players": 35}` |

Use `wss://` when the server is behind HTTPS (phones need it for the mic).

## Message format

- Every text frame is one JSON object with a `"type"` field.
- Players may also send **binary frames**: each one is a chunk of answer audio
  (see `audio`). The server never sends binary frames.
- Unknown fields are ignored. Times are in **seconds** (floats).
- A bad message gets an `error` reply; the connection stays open.

## Game flow

```
host                      server                        player(s)
 |-- connect /ws/host ------>|                              |
 |<----------- room_created -|                              |
 |                           |<----------- join ------------|
 |<----------- player_joined-|------------ joined --------->|
 |-- start_question -------->|                              |
 |<---------------- question-|------------ question ------->|   timer starts
 |                           |<----------- hold_start ------|
 |                           |<----------- audio (xN) ------|
 |                           |<----------- hold_end --------|
 |                           |------------ partial -------->|   while speaking
 |<---------------- final ---|------------ final ---------->|   ~1 s after hold_end
 |<------------- leaderboard-|------------ leaderboard ---->|
 |-- end_round (optional) -->|                              |   or time runs out
 |<--------------- round_end-|------------ round_end ------>|
 |<------------- leaderboard-|------------ leaderboard ---->|
 |-- start_question -------->|  ... next question ...       |
```

Rules:
- One round open at a time. The host can only start a question when no round
  is open.
- A round ends when its `time_limit` runs out or the host sends `end_round`.
  A player still holding at that moment has their answer ended by the server
  (as if they had sent `hold_end`); the audio already sent still counts.
- Each player answers **once per question**: one `hold_start` … `hold_end`.
- A player who joins while a round is open gets that `question` right after
  `joined`, with `remaining` set to the time left.
- A `final` can still arrive **after** `round_end` (for an answer released
  before the end, while Transcribe is slow). It is scored and followed by a
  `leaderboard`. Finals stop being accepted when the next question starts.
- Messages to one client always arrive in the order the server sent them,
  with none skipped. A client that falls 500 messages behind (not reading
  its socket) is disconnected with close code **4001** instead of silently
  missing messages; it can `join` again as a new player.

Close codes sent by the server: **4000** room closed (host left),
**4001** client too slow.

---

## Player → server

### `join`
First message on `/ws/play`. Room codes are 4 letters, case-insensitive.
Names are 1–24 characters and unique in the room (case-insensitive).

```json
{"type": "join", "room": "KXQB", "name": "Alice"}
```
Reply: `joined`, or `error` with `room_not_found`, `bad_name`, `name_taken`,
`already_joined`.

### `hold_start`
Player pressed "hold to answer". Only while a round is open, once per round.

```json
{"type": "hold_start"}
```
Errors: `no_round`, `already_answered`.

### `audio`
A chunk of the answer: **16 kHz, mono, 16-bit little-endian PCM, no header**.
Only between `hold_start` and `hold_end`. Send chunks as they are recorded
(about 0.1–0.25 s each, 3200–8000 bytes). Either form works:

- binary frame: the raw PCM bytes (preferred; no base64 overhead)
- text frame:
  ```json
  {"type": "audio", "data": "AAABAAIAAwA..."}
  ```
  `data` is base64 of the raw PCM bytes.

Limits: 64 KiB per chunk, 15 s of audio (480000 bytes) per answer.
Empty chunks (a zero-length binary frame, or `data` missing or `""`) are
rejected with `bad_message`: to Transcribe an empty chunk means "end of
audio", which is what `hold_end` is for.

Audio is accepted only while the round is open. After `round_end`, chunks
get `no_round` and are dropped (phones may still be flushing their last
chunk; that error can be ignored).

Errors: `no_round`, `not_holding`, `audio_too_large`, `bad_message`
(empty chunk or invalid base64).

The server forwards each chunk to the player's Transcribe stream, which it
opens when the question starts. Connecting takes 1–3 s, up to ~4 s with
several streams on the demo server. Chunks that arrive before that stream
is connected are held and sent in order once it is, so a player can start
speaking straight away (their `final` then comes later; see `final`).

### `hold_end`
Player released the button: the answer is complete.

```json
{"type": "hold_end"}
```
Never an error: phones should always send it on release. If there is no
answer in progress it is ignored. That covers:
- the round ended while the player was holding: the server already ended the
  answer at `round_end`, so this `hold_end` changes nothing (the answer can
  still get a `final`);
- the next question has started and the player hasn't pressed hold in it;
- `hold_end` without `hold_start`.

---

## Host → server

### `start_question`
Start the next question (in file order, wrapping round at the end), or a
specific one by `index` (0-based integer, wraps; `true`/`false` are
rejected). `time_limit` overrides the server default (`ROUND_SECONDS`, 20 s)
and must be a finite number from 3 to 120 seconds. Both fields are optional.

```json
{"type": "start_question"}
{"type": "start_question", "index": 3, "time_limit": 15}
```
Errors: `round_in_progress`, `bad_message`.

### `end_round`
End the open round now.

```json
{"type": "end_round"}
```
Errors: `no_round`.

---

## Server → clients

### `room_created` (→ host)
Sent right after the host connects.

```json
{"type": "room_created", "room": "KXQB", "questions": 12, "round_seconds": 20.0}
```

### `joined` (→ that player)
```json
{"type": "joined", "room": "KXQB", "player_id": "p7", "name": "Alice"}
```
`player_id` is unique within the room and appears in `final`,
`leaderboard` and `round_end`.

### `player_joined` / `player_left` (→ host)
```json
{"type": "player_joined", "player_id": "p7", "name": "Alice", "count": 12}
{"type": "player_left", "player_id": "p7", "name": "Alice", "count": 11}
```
A player who disconnects is removed (score lost). There is no reconnect yet;
joining again creates a new player.

### `question` (→ host and all players)
Sent when a round starts, and to a player joining mid-round.
The accepted answers are **not** sent.

```json
{"type": "question", "round": 1, "question_id": "france-capital",
 "index": 0, "total": 12, "text": "What is the capital of France?",
 "time_limit": 20.0, "remaining": 20.0}
```
`round` counts up from 1 per room. Clients should count down from
`remaining` using their own clock.

### `partial` (→ that player)
Interim transcript of the player's own answer, to show on the phone while
they speak. May arrive many times; each replaces the previous one (it
already includes any earlier finished phrases of the same answer).

```json
{"type": "partial", "round": 1, "text": "par"}
```

### `final` (→ that player and host) — scoring CQ-7
Final transcript of one answer, with its score. Exactly one per answer
(per `hold_start`), unless the player leaves or the next question starts
first.

```json
{"type": "final", "round": 1, "player_id": "p7", "name": "Alice",
 "text": "Paris.", "correct": true, "points": 100, "score": 300}
```
`points` is for this answer; `score` is the player's new total and already
includes `points`. `text` is the whole answer: if the player paused
("Um." … "Paris."), the phrases are joined ("Um. Paris.").

Timing: sent once Transcribe has a final result for the end of the answer
and nothing more has come for 0.5 s (`FINAL_QUIET_GAP`), or when Transcribe
closes the stream if that is sooner, and at most 10 s after `hold_end`. A
phrase recognised after that is not added. How soon depends on whether the
player's stream was connected when they started speaking:
- connected (the player waited a few seconds into the question): about
  0.75–1 s after `hold_end` on the demo server (≈0.25 s recognition + the
  0.5 s quiet gap);
- still connecting (the player spoke within the first 1–4 s): the audio
  held meanwhile is sent in a burst when the stream connects, so the
  `final` can take a second or two longer.

Recognition worked, `"error"` absent: `text` is what was heard and the
answer is scored. That includes hearing nothing (`"text": ""`), and a
timeout after some phrases were recognised (`text` has those phrases, no
`error`, scored as usual).

Recognition failed, `"error"` present: the answer is **not scored**
(`correct: false`, `points: 0`), because part of it may be missing. `text`
is whatever was recognised before the failure, usually `""` (e.g. the stream
dropped after "Um." gives `"text": "Um."` with `transcribe_error`). The phone
can show "couldn't hear you" rather than "wrong":

```json
{"type": "final", "round": 1, "player_id": "p7", "name": "Alice",
 "text": "", "correct": false, "points": 0, "score": 200,
 "error": "transcribe_unavailable"}
```

| `error` | Meaning |
|---|---|
| `transcribe_unavailable` | No Transcribe stream: connecting failed, or the demo server's cap of 4 streams stayed full for 10 s |
| `transcribe_error` | Transcribe returned an error, closed the stream before the end of the answer's audio, closed it with an error code (anything but 1000/1006), or sending audio failed |
| `transcribe_timeout` | Nothing recognised within 10 s of `hold_end` |

### `leaderboard` (→ host and all players)
Full ranking, highest score first (ties by name). Sent after each `final`
and after each `round_end`.

```json
{"type": "leaderboard", "players": [
  {"player_id": "p7", "name": "Alice", "score": 300},
  {"player_id": "p2", "name": "Bob", "score": 200}
]}
```

### `round_end` (→ host and all players)
`reason`: `"timeout"`, `"host"` (host sent `end_round`) or `"host_left"`.
`answer` is the main accepted answer, to reveal. `results` has one entry per
player still in the room who pressed hold this round, and `answered` is its
length; `players` is the number of players in the room. `transcript`/`correct`
are `null` while no final has arrived (usual for answers released in the
last ~1 s); a late final is sent separately as `final` + `leaderboard`.

```json
{"type": "round_end", "round": 1, "question_id": "france-capital",
 "reason": "timeout", "answer": "paris", "answered": 2, "players": 3,
 "results": [
   {"player_id": "p7", "name": "Alice", "transcript": "Paris.", "correct": true, "points": 100},
   {"player_id": "p2", "name": "Bob", "transcript": null, "correct": null, "points": 0}
 ]}
```

### `room_closed` (→ all players)
The host disconnected. The server closes the player sockets right after
(close code 4000).

```json
{"type": "room_closed", "room": "KXQB"}
```

### `error` (→ the sender of a bad message)
```json
{"type": "error", "code": "not_holding", "message": "send hold_start first"}
```

| `code` | Meaning |
|---|---|
| `bad_message` | Not JSON, no `type`, or a bad field value |
| `unknown_type` | `type` not valid on this endpoint |
| `not_joined` | Player sent something before `join` |
| `already_joined` | Second `join` on one connection |
| `room_not_found` | No room with that code |
| `bad_name` | Name empty or over 24 characters |
| `name_taken` | Name already used in the room |
| `no_round` | No round open (`hold_start`, `audio`, `end_round`) |
| `already_answered` | Second `hold_start` in one round |
| `not_holding` | `audio` without `hold_start`, or after `hold_end` |
| `audio_too_large` | Chunk over 64 KiB or answer over 15 s |
| `round_in_progress` | `start_question` while a round is open |
| `internal_error` | Server bug while handling the message; logged on the server |

## Server side: where Transcribe plugs in

`server/bridge.py` defines the hooks the room calls: `question_started`,
`hold_start`, `audio`, `hold_end`, `round_ended`, `player_left`. Each
answer hook gets the round it belongs to (`rnd`), and `hold_end` is called
exactly once per `hold_start` (by the player's release or by the round
ending). The bridge reports results with `room.on_partial(rnd, player, text)`
and `room.on_final(rnd, player, text, error=None)`, which produce the
`partial`, `final` and `leaderboard` messages above; results for a round
that is no longer the current one are dropped. Hook exceptions are logged
and never reach clients.

`server/transcribe_bridge.py` (`TranscribeBridge`, the default) opens one
Transcribe stream per player per question; `BRIDGE=logging` swaps in
`LoggingBridge`, which never sends `partial` or `final`. Clients don't see
the bridge otherwise.
