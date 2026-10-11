# Demo video narration (Plan B, 4 streams)

For `classroom-quiz-demo.mp4` (2:41, 1920×1080, no sound). It was recorded
on 2026-10-11: a live game against the shared Cobalt demo Transcribe
server, 4 simulated players, driven from the host screen. Read over it at a
relaxed pace. The times are when each part starts on screen.

---

**0:00 · Title**

> This is Classroom Quiz: a multiplayer voice quiz that doubles as a load
> test for Cobalt Transcribe. Everyone answers out loud, on their own phone,
> at the same time. Every answer is a live Transcribe stream, and the
> leaderboard is built from what Transcribe hears.

**0:14 · Host screen**

> This is the host screen, on a laptop or projector. Opening it creates a
> room. Players scan the QR code: it's a secure link, because phones only
> allow the microphone over https.

**0:23 · Players join**

> Four players have joined. These are simulated players: each one is a
> script that joins like a phone and plays a recorded voice. Each one still
> sends real audio to Transcribe on its own stream, so to Transcribe they
> look the same as real phones. We've also tested with a real Android phone.

**0:31 · Round 1**

> I start a question. Each player holds the answer button, speaks and
> releases. As each final result comes in, it's scored, and the leaderboard
> and the answers list update straight away.

*(around 0:41, all answers in)*

> On the right is Transcribe under load, measured on the server for every
> answer. "Answer" is the player's wait, from letting go of the button to
> the answer being scored: about eight tenths of a second. "API" is
> Transcribe's own part, from the end of the audio to the final result:
> about three tenths of a second. No errors.

*(around 0:47, round ends)*

> One player said "Madrid", and it's scored wrong. That's the scorer
> working correctly, not a recognition error. Transcribe heard every word
> the simulated players said, so word accuracy is a hundred percent.

**0:59 · Round 2**

> Round two. When a question starts, the server opens one Transcribe
> stream per player, so the connection is ready before anyone speaks.
> That's the "streams open" number, and it stays within the demo server's
> limit of four.

**1:26 · Round 3**

> Round three. After each round the server writes a report: every stream's
> timings, errors and transcript, as JSON and CSV. The table under the
> numbers groups them by how many players were in the round.

**1:53 · The evidence**

> This is the ramp test. It runs the same game at one, two, three and four
> players, three rounds each, and charts the latency. It stays flat: about
> 0.8 seconds for the player and 0.3 seconds for Transcribe, with zero
> errors at every level. Four is where this run stops, because the shared
> demo server is sized for about four streams.

**2:23 · Next**

> Everything for the full test is built: the phone page, host screen,
> simulator, metrics report and ramp runner. Running thirty-five players
> is one command against a dedicated Transcribe instance. That's the next
> step.

---

**If you record yourself instead:** follow `docs/runbook.md`, "Plan B".
Use 2 real phones + 2 simulated players, and use the same narration,
changing "these are simulated players" to "two real phones and two
simulated players".
