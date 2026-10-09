// Phone page (CQ-6): join a room, hold to answer, stream the mic as 16 kHz
// mono 16-bit PCM over /ws/play, show partials, result and leaderboard.
// Message contract: docs/protocol.md. Capture/resampling: capture-worklet.js.
"use strict";

const CHUNK_BYTES = 3200;         // 0.1 s of 16 kHz 16-bit mono, like the simulator
const BYTES_PER_SEC = 32000;
const MAX_ANSWER_BYTES = 15 * BYTES_PER_SEC - 2 * CHUNK_BYTES;  // stop just under the server's 15 s
const PREROLL_CHUNKS = 2;         // 0.2-0.3 s from before the press (with the chunk in progress)
const PREROLL_MAX_AGE_MS = 600;   // older chunks are stale (context was suspended)
const TAIL_MS = 200;              // keep sending this long after release (last syllable)
const FLUSH_TIMEOUT_MS = 300;
const BOARD_TOP = 5;
// Mic processing: echo cancellation on (harmless, nothing plays), noise
// suppression off (it smears consonants and tends to hurt ASR more than the
// noise does; Transcribe is trained on noisy audio), auto gain on (phones are
// held at all distances and the levels vary a lot otherwise).
const MIC_CONSTRAINTS = {
  channelCount: 1,
  echoCancellation: true,
  noiseSuppression: false,
  autoGainControl: true,
};

const $ = (id) => document.getElementById(id);
const el = {
  bar: $("bar"), barRoom: $("bar-room"), barName: $("bar-name"), barScore: $("bar-score"),
  insecure: $("insecure"),
  join: $("screen-join"), joinForm: $("join-form"), room: $("room"), name: $("name"),
  joinError: $("join-error"), joinBtn: $("join-btn"),
  wait: $("screen-wait"), waitTitle: $("wait-title"), waitText: $("wait-text"),
  question: $("screen-question"), qNum: $("q-num"), qTimer: $("q-timer"), qTimebar: $("q-timebar"),
  qText: $("q-text"), qHint: $("q-hint"), qStatus: $("q-status"), qTranscript: $("q-transcript"),
  result: $("screen-result"), rVerdict: $("r-verdict"), rPoints: $("r-points"),
  rTranscript: $("r-transcript"), rAnswer: $("r-answer"),
  gone: $("screen-gone"), goneTitle: $("gone-title"), goneText: $("gone-text"),
  rejoinBtn: $("rejoin-btn"), newroomBtn: $("newroom-btn"),
  board: $("board"), boardMe: $("board-me"), boardList: $("board-list"),
  toast: $("toast"), holdArea: $("hold-area"), hold: $("hold"), holdLabel: $("hold-label"),
};

// --- state -----------------------------------------------------------------

const S = {
  ws: null,
  room: "", name: "", playerId: null,
  joined: false, roomClosed: false,
  round: null, roundOpen: false, timeLimit: 0, deadline: 0, timer: null,
  answered: false,      // pressed hold in this round
  holding: false,       // button down: audio goes to the server
  releasing: false,     // released, still sending the tail
  sentBytes: 0,
  pointerId: null,
  myFinal: null, revealed: null,
  score: 0,
};

const A = {
  ctx: null, stream: null, source: null, node: null, sink: null, chunker: null,
  preroll: [],          // [{buf, t}] newest last, while not holding
  flushWaiter: null,
};

// --- screens ---------------------------------------------------------------

const SCREENS = ["join", "wait", "question", "result", "gone"];

function show(name) {
  for (const s of SCREENS) el[s].hidden = s !== name;
  el.board.hidden = !(name === "wait" || name === "result") || !el.boardList.children.length;
  el.holdArea.hidden = !(S.joined && (name === "wait" || name === "question" || name === "result"));
  el.bar.hidden = !S.joined;
}

let toastTimer = null;
function toast(text) {
  el.toast.textContent = text;
  el.toast.hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { el.toast.hidden = true; }, 3000);
}

function setHold(enabled, label) {
  el.hold.classList.toggle("off", !enabled);
  el.hold.setAttribute("aria-disabled", enabled ? "false" : "true");
  el.hold.classList.toggle("holding", S.holding);
  el.holdLabel.textContent = label;
}

function joinError(text) {
  el.joinError.textContent = text;
  el.joinError.hidden = !text;
  el.joinBtn.disabled = false;
  el.joinBtn.textContent = "Join";
  show("join");
}

// --- microphone --------------------------------------------------------------

function micAvailable() {
  return window.isSecureContext && navigator.mediaDevices && navigator.mediaDevices.getUserMedia;
}

function micLive() {
  return A.stream && A.stream.getAudioTracks().some((t) => t.readyState === "live");
}

function onChunk(buf) {
  if (S.holding || S.releasing) {
    sendAudio(buf);
  } else {
    A.preroll.push({ buf, t: performance.now() });
    if (A.preroll.length > PREROLL_CHUNKS) A.preroll.shift();
  }
}

function onWorkletMessage(e) {
  const m = e.data;
  if (m.type === "chunk") onChunk(m.buf);
  else if (m.type === "flush" && A.flushWaiter) A.flushWaiter(m.buf);
}

// Call from a user gesture (join/rejoin/press): creates and resumes the
// AudioContext there, as iOS requires. Keeps the stream open for the game.
async function setupMic() {
  if (!micAvailable()) throw new Error("insecure");
  const Ctx = window.AudioContext || window.webkitAudioContext;
  if (!A.ctx) A.ctx = new Ctx();
  const resumed = A.ctx.resume().catch(() => {});
  if (!micLive()) {
    const stream = await navigator.mediaDevices.getUserMedia({ audio: MIC_CONSTRAINTS });
    if (A.source) A.source.disconnect();
    if (A.stream) A.stream.getTracks().forEach((t) => t.stop());
    A.stream = stream;
    A.source = null;
  }
  await resumed;
  if (!A.node) {
    A.sink = A.ctx.createGain();
    A.sink.gain.value = 0;   // the graph must reach the destination to run; play nothing
    A.sink.connect(A.ctx.destination);
    if (A.ctx.audioWorklet && window.AudioWorkletNode) {
      await A.ctx.audioWorklet.addModule("/static/capture-worklet.js");
      A.node = new AudioWorkletNode(A.ctx, "pcm-capture", {
        numberOfInputs: 1, numberOfOutputs: 1, outputChannelCount: [1],
        channelCount: 1, channelCountMode: "explicit", channelInterpretation: "speakers",
        processorOptions: { chunkBytes: CHUNK_BYTES },
      });
      A.node.port.onmessage = onWorkletMessage;
    } else {
      // Old browsers without AudioWorklet: same resampler on the main thread.
      A.chunker = new PcmChunker(A.ctx.sampleRate, onChunk, CHUNK_BYTES);
      A.node = A.ctx.createScriptProcessor(4096, 1, 1);
      A.node.onaudioprocess = (e) => A.chunker.push([e.inputBuffer.getChannelData(0)]);
    }
    A.node.connect(A.sink);
  }
  if (!A.source) {
    A.source = A.ctx.createMediaStreamSource(A.stream);
    A.source.connect(A.node);
  }
  if (A.ctx.state !== "running") A.ctx.resume().catch(() => {});
}

// The audio captured since the last full chunk (resolves to null on timeout).
function flushCapture() {
  if (A.chunker) return Promise.resolve(A.chunker.flush());
  if (!A.node || !A.node.port) return Promise.resolve(null);
  return new Promise((resolve) => {
    const timer = setTimeout(() => { A.flushWaiter = null; resolve(null); }, FLUSH_TIMEOUT_MS);
    A.flushWaiter = (buf) => { clearTimeout(timer); A.flushWaiter = null; resolve(buf); };
    A.node.port.postMessage("flush");
  });
}

// --- socket ------------------------------------------------------------------

function sendJson(msg) {
  if (S.ws && S.ws.readyState === WebSocket.OPEN) S.ws.send(JSON.stringify(msg));
}

function sendAudio(buf) {
  if (!buf || !buf.byteLength || !S.ws || S.ws.readyState !== WebSocket.OPEN) return;
  if (S.sentBytes + buf.byteLength > MAX_ANSWER_BYTES) {
    if (S.holding) { release(); toast("That's the 15 s limit: answer sent"); }
    return;
  }
  S.sentBytes += buf.byteLength;
  S.ws.send(buf);
}

function connect() {
  if (S.ws) { S.ws.onclose = null; try { S.ws.close(); } catch (e) { /* ignore */ } }
  S.joined = false; S.roomClosed = false; S.playerId = null;
  const url = (location.protocol === "https:" ? "wss://" : "ws://") + location.host + "/ws/play";
  const ws = new WebSocket(url);
  ws.binaryType = "arraybuffer";
  S.ws = ws;
  const openTimer = setTimeout(() => { if (ws.readyState !== WebSocket.OPEN) ws.close(); }, 10000);
  ws.onopen = () => { clearTimeout(openTimer); sendJson({ type: "join", room: S.room, name: S.name }); };
  ws.onmessage = (e) => {
    if (typeof e.data !== "string") return;
    let msg;
    try { msg = JSON.parse(e.data); } catch (err) { return; }
    handle(msg);
  };
  ws.onclose = (e) => { clearTimeout(openTimer); if (S.ws === ws) onClosed(e.code); };
}

function onClosed(code) {
  const wasJoined = S.joined;
  S.ws = null;
  stopRound();
  S.joined = false;
  if (!wasJoined) {
    if (el.joinError.hidden) joinError("Couldn't reach the game server. Check your connection and try again.");
    return;
  }
  if (S.roomClosed || code === 4000) {
    el.goneTitle.textContent = "The game has ended";
    el.goneText.textContent = "The host closed the room. Thanks for playing!";
    el.rejoinBtn.hidden = true;
  } else {
    el.goneTitle.textContent = "Disconnected";
    el.goneText.textContent = (code === 4001 ? "Your connection was too slow and the game dropped it. "
                                             : "The connection to the game was lost. ")
      + "Rejoin to keep playing (your score starts again from 0).";
    el.rejoinBtn.hidden = false;
  }
  show("gone");
}

// --- messages ----------------------------------------------------------------

const JOIN_ERRORS = {
  room_not_found: "No room with that code. Check the code on the screen.",
  name_taken: "Someone in this room already has that name. Pick another one.",
  bad_name: "Your name must be 1-24 characters.",
};

function handle(msg) {
  switch (msg.type) {
    case "joined": onJoined(msg); break;
    case "question": onQuestion(msg); break;
    case "partial":
      if (msg.round === S.round && !S.myFinal) {
        el.qTranscript.textContent = msg.text || "";
        el.qTranscript.classList.add("partial");
      }
      break;
    case "final":
      if (msg.player_id === S.playerId && msg.round === S.round) onFinal(msg);
      break;
    case "round_end": if (msg.round === S.round) onRoundEnd(msg); break;
    case "leaderboard": onLeaderboard(msg); break;
    case "room_closed": S.roomClosed = true; break;
    case "error": onError(msg); break;
  }
}

function onJoined(msg) {
  S.joined = true;
  S.playerId = msg.player_id;
  S.room = msg.room;
  S.name = msg.name;
  S.score = 0;
  S.round = null; S.roundOpen = false; S.myFinal = null;
  el.boardList.textContent = "";
  el.boardMe.textContent = "";
  el.barRoom.textContent = "Room " + msg.room;
  el.barName.textContent = msg.name;
  el.barScore.textContent = "0 pts";
  el.joinBtn.disabled = false;
  el.joinBtn.textContent = "Join";
  el.joinError.hidden = true;
  try { localStorage.setItem("quiz-name", msg.name); } catch (e) { /* private mode */ }
  el.waitTitle.textContent = "You're in, " + msg.name + "!";
  el.waitText.textContent = "Waiting for the next question…";
  setHold(false, "Wait for the question");
  show("wait");
  keepAwake();
}

function onQuestion(msg) {
  S.round = msg.round;
  S.roundOpen = true;
  S.answered = false;
  S.holding = false;
  S.releasing = false;
  S.myFinal = null;
  S.revealed = null;
  S.timeLimit = msg.time_limit || 20;
  S.deadline = performance.now() + (msg.remaining != null ? msg.remaining : S.timeLimit) * 1000;
  el.qNum.textContent = "Question " + (msg.index + 1) + " of " + msg.total;
  el.qText.textContent = msg.text;
  el.qHint.textContent = msg.hint || "";
  el.qStatus.textContent = "Hold the button and say your answer.";
  el.qTranscript.textContent = "";
  el.qTranscript.classList.remove("partial");
  el.rAnswer.hidden = true;
  setHold(true, "Hold to answer");
  show("question");
  clearInterval(S.timer);
  S.timer = setInterval(tick, 200);
  tick();
}

function tick() {
  const left = Math.max(0, S.deadline - performance.now()) / 1000;
  el.qTimer.textContent = Math.ceil(left) + " s";
  el.qTimebar.style.transform = "scaleX(" + (S.timeLimit ? left / S.timeLimit : 0) + ")";
  if (left <= 0) clearInterval(S.timer);
}

function stopRound() {
  clearInterval(S.timer);
  S.roundOpen = false;
  if (S.holding || S.releasing) {
    S.holding = false;
    S.releasing = false;
    sendJson({ type: "hold_end" });  // ignored by the server if it already ended the answer
  }
}

function onFinal(msg) {
  S.myFinal = msg;
  S.score = msg.score;
  el.barScore.textContent = msg.score + " pts";
  renderResult();
}

function renderResult() {
  const f = S.myFinal;
  el.rVerdict.className = "verdict";
  if (f) {
    if (f.error) {
      el.rVerdict.textContent = "Couldn't hear you";
      el.rVerdict.classList.add("meh");
      el.rPoints.textContent = "No points: the speech service had a problem with your answer.";
    } else if (f.correct) {
      el.rVerdict.textContent = "Correct!";
      el.rVerdict.classList.add("good");
      el.rPoints.textContent = "+" + f.points + " points";
    } else {
      el.rVerdict.textContent = f.text ? "Not quite" : "We didn't hear anything";
      el.rVerdict.classList.add(f.text ? "bad" : "meh");
      el.rPoints.textContent = "0 points";
    }
    el.rTranscript.textContent = f.text ? f.text : "(nothing)";
  } else if (S.answered) {
    el.rVerdict.textContent = "Checking your answer…";
    el.rVerdict.classList.add("meh");
    el.rPoints.textContent = "";
    el.rTranscript.textContent = el.qTranscript.textContent || "…";
  } else {
    el.rVerdict.textContent = "Time's up";
    el.rVerdict.classList.add("meh");
    el.rPoints.textContent = "You didn't answer this one.";
    el.rTranscript.textContent = "(nothing)";
  }
  if (S.revealed) {
    el.rAnswer.textContent = "The answer: " + S.revealed;
    el.rAnswer.hidden = false;
  }
  show("result");
}

function onRoundEnd(msg) {
  stopRound();
  S.revealed = msg.answer || null;
  setHold(false, "Wait for the next question");
  renderResult();
}

function onLeaderboard(msg) {
  const rows = msg.players || [];
  el.boardList.textContent = "";
  const me = rows.find((r) => r.player_id === S.playerId);
  rows.forEach((r, i) => {
    if (i >= BOARD_TOP && r !== me) return;
    const li = document.createElement("li");
    if (r === me) li.className = "mine";
    const name = document.createElement("span");
    name.textContent = r.rank + ". " + r.name;
    const score = document.createElement("span");
    score.textContent = r.score + (r.round_points ? " (+" + r.round_points + ")" : "");
    li.append(name, score);
    el.boardList.append(li);
  });
  if (me) {
    S.score = me.score;
    el.barScore.textContent = me.score + " pts";
    el.boardMe.textContent = "You: " + me.score + " points, #" + me.rank + " of " + rows.length;
  }
  const screen = SCREENS.find((s) => !el[s].hidden);
  if (screen) show(screen);  // refresh whether the board is visible
}

function onError(msg) {
  if (!S.joined) {
    joinError(JOIN_ERRORS[msg.code] || ("Couldn't join: " + (msg.message || msg.code)));
    if (S.ws) { const ws = S.ws; S.ws = null; ws.onclose = null; ws.close(); }
    return;
  }
  switch (msg.code) {
    case "no_round":
      if (S.roundOpen && S.holding) toast("Too late: the round is over");
      break;  // otherwise a last chunk after round_end: expected
    case "not_holding":
      break;  // a chunk after the server ended the answer
    case "already_answered":
      toast("You already answered this question");
      break;
    case "audio_too_large":
      toast("Your answer was too long and got cut off");
      break;
    default:
      toast("Error: " + (msg.message || msg.code));
  }
}

// --- hold to answer ----------------------------------------------------------

function press() {
  if (!S.roundOpen || S.answered || S.holding || !S.ws) return;
  S.answered = true;
  S.holding = true;
  S.sentBytes = 0;
  if (!micLive() || !A.ctx || A.ctx.state !== "running") {
    // iOS suspends the context (and may end the track) in the background;
    // this press is a gesture, so we can resume/reacquire it here.
    setupMic().catch(() => toast("Microphone unavailable: check the browser's mic permission"));
  }
  sendJson({ type: "hold_start" });
  const now = performance.now();
  for (const p of A.preroll) if (now - p.t < PREROLL_MAX_AGE_MS) sendAudio(p.buf);
  A.preroll = [];
  el.qStatus.textContent = "Listening… release when you're done.";
  el.qTranscript.textContent = "";
  setHold(true, "Release to send");
  if (navigator.vibrate) try { navigator.vibrate(20); } catch (e) { /* ignore */ }
}

function release() {
  if (!S.holding) return;
  S.holding = false;
  S.releasing = true;
  const round = S.round;
  el.qStatus.textContent = "Checking your answer…";
  setHold(false, "Answer sent");
  setTimeout(async () => {
    const rest = await flushCapture();
    if (!S.releasing || S.round !== round) return;  // round ended meanwhile (stopRound sent hold_end)
    sendAudio(rest);
    S.releasing = false;
    sendJson({ type: "hold_end" });
  }, TAIL_MS);
}

el.hold.addEventListener("pointerdown", (e) => {
  if (e.pointerType === "mouse" && e.button !== 0) return;
  e.preventDefault();
  if (S.pointerId !== null) return;  // a second finger
  S.pointerId = e.pointerId;
  try { el.hold.setPointerCapture(e.pointerId); } catch (err) { /* ignore */ }
  press();
});
function pointerDone(e) {
  if (e.pointerId !== S.pointerId) return;
  S.pointerId = null;
  release();
}
for (const type of ["pointerup", "pointercancel", "lostpointercapture"]) {
  el.hold.addEventListener(type, pointerDone);
}
window.addEventListener("pointerup", pointerDone);
// iOS: no callout/selection loupe/double-tap zoom on the button.
for (const type of ["touchstart", "touchend", "touchmove"]) {
  el.hold.addEventListener(type, (e) => e.preventDefault(), { passive: false });
}
el.hold.addEventListener("contextmenu", (e) => e.preventDefault());

// Keyboard (laptop testing): hold space.
document.addEventListener("keydown", (e) => {
  if (e.code !== "Space" || e.repeat || document.activeElement instanceof HTMLInputElement) return;
  if (!S.joined) return;
  e.preventDefault();
  press();
});
document.addEventListener("keyup", (e) => {
  if (e.code === "Space" && S.pointerId === null) release();
});

// Leaving the page or switching apps ends the answer.
function leaving() { S.pointerId = null; release(); }
window.addEventListener("pagehide", leaving);
window.addEventListener("blur", leaving);
document.addEventListener("visibilitychange", () => {
  if (document.hidden) leaving();
  else keepAwake();
});

// --- screen wake lock (best effort) ------------------------------------------

let wakeLock = null;
async function keepAwake() {
  if (!S.joined || !navigator.wakeLock || (wakeLock && !wakeLock.released)) return;
  try { wakeLock = await navigator.wakeLock.request("screen"); } catch (e) { /* not allowed */ }
}

// --- join --------------------------------------------------------------------

async function join(room, name) {
  el.joinError.hidden = true;
  el.joinBtn.disabled = true;
  el.joinBtn.textContent = "Connecting…";
  S.room = room;
  S.name = name;
  try {
    await setupMic();
  } catch (e) {
    const why = e && e.message === "insecure"
      ? "The microphone needs a secure link: open the https:// address of this game."
      : e && (e.name === "NotAllowedError" || e.name === "SecurityError")
        ? "Microphone blocked. Allow the microphone for this site in your browser settings, then join again."
        : "Couldn't start the microphone (" + ((e && (e.name || e.message)) || "unknown error") + ").";
    joinError(why);
    return;
  }
  connect();
}

el.joinForm.addEventListener("submit", (e) => {
  e.preventDefault();
  const room = el.room.value.replace(/[^a-z]/gi, "").toUpperCase();
  const name = el.name.value.trim().replace(/\s+/g, " ");
  if (room.length !== 4) { joinError("The room code is 4 letters."); return; }
  if (!name) { joinError("Enter your name."); return; }
  join(room, name);
});

el.rejoinBtn.addEventListener("click", () => join(S.room, S.name));
el.newroomBtn.addEventListener("click", () => {
  el.room.value = "";
  el.joinError.hidden = true;
  el.joinBtn.disabled = false;
  el.joinBtn.textContent = "Join";
  show("join");
  el.room.focus();
});

// --- start -------------------------------------------------------------------

(function init() {
  // The server fills in ?room=; this also covers the page opened some other way.
  if (el.room.value === "__ROOM__") el.room.value = "";
  const fromUrl = (new URLSearchParams(location.search).get("room") || "").replace(/[^a-z]/gi, "").toUpperCase();
  if (!el.room.value && fromUrl.length === 4) el.room.value = fromUrl;
  try { el.name.value = localStorage.getItem("quiz-name") || ""; } catch (e) { /* private mode */ }
  if (!micAvailable()) el.insecure.hidden = false;
  setHold(false, "Hold to answer");
  show("join");
})();

// For the browser tests (tests/test_phone_e2e.py).
window.quizDebug = {
  state: S,
  sampleRate: () => (A.ctx ? A.ctx.sampleRate : null),
  usesWorklet: () => !!(A.node && A.node.port),
};
