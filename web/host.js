// Host screen (CQ-11): create a room over /ws/host, run questions, show the
// live leaderboard, the answers as they come in and live Transcribe metrics.
// Message contract: docs/protocol.md (stream_metrics / metrics_summary: CQ-10).
// Player names are user input: everything is rendered with textContent.
"use strict";

const HEALTH_EVERY_MS = 1000;   // "streams open" and the Transcribe host come from /health
const FEED_MAX = 12;
const BOARD_MAX = 36;           // two columns of 18 fit at 1080p
const BOARD_ONE_COLUMN = 12;    // up to this many rows: one column, read top to bottom
const BOARD_DENSE = 20;         // more rows than this: smaller type so 2 x 18 fit

const $ = (id) => document.getElementById(id);
const el = {
  roomCode: $("room-code"), players: $("players"), transcribe: $("transcribe"), conn: $("conn"),
  qNum: $("q-num"), qAnswers: $("q-answers"), qTimer: $("q-timer"), qTimebar: $("q-timebar"),
  qText: $("q-text"), qReveal: $("q-reveal"),
  next: $("btn-next"), end: $("btn-end"), timeLimit: $("time-limit"),
  board: $("board"), boardEmpty: $("board-empty"),
  join: $("join"), qr: $("qr"), joinUrl: $("join-url"), joinWarn: $("join-warn"),
  mStreams: $("m-streams"), mP50: $("m-p50"), mP95: $("m-p95"), mApi: $("m-api"),
  mErrors: $("m-errors"), mWer: $("m-wer"), mScope: $("m-scope"),
  byStreams: $("by-streams"), feed: $("feed"),
};

const state = {
  ws: null,
  room: null,
  players: 0,
  round: null,          // {number, index, total, text, timeLimit, endsAt, open}
  answered: new Set(),  // player ids with a final this round
  live: [],             // stream_metrics of this round
  lastBoard: new Map(), // player id -> score, to flash changes
  feedLat: new Map(),   // "round:player id" -> latency cell of that feed row
  timer: null,
};

// --- helpers -----------------------------------------------------------------

function make(tag, cls, text) {
  const node = document.createElement(tag);
  if (cls) node.className = cls;
  if (text !== undefined && text !== null) node.textContent = text;
  return node;
}

function secs(v) {
  return v === null || v === undefined ? "–" : `${v.toFixed(2)}s`;
}

function percentile(values, p) {
  if (!values.length) return null;
  const s = [...values].sort((a, b) => a - b);
  return s[Math.max(0, Math.ceil((p / 100) * s.length) - 1)];
}

function send(msg) {
  if (state.ws && state.ws.readyState === WebSocket.OPEN) state.ws.send(JSON.stringify(msg));
}

// --- connection ----------------------------------------------------------------

function connect() {
  const url = (location.protocol === "https:" ? "wss://" : "ws://") + location.host + "/ws/host";
  const ws = new WebSocket(url);
  state.ws = ws;
  ws.onmessage = (ev) => onMessage(JSON.parse(ev.data));
  ws.onclose = () => {
    // The room lives as long as this connection: a reload makes a new room.
    el.conn.hidden = false;
    el.conn.textContent = "Disconnected: reload for a new room";
    el.next.disabled = el.end.disabled = true;
    stopTimer();
  };
}

function onMessage(msg) {
  switch (msg.type) {
    case "room_created": onRoomCreated(msg); break;
    case "player_joined":
    case "player_left": setPlayers(msg.count); break;
    case "question": onQuestion(msg); break;
    case "final": onFinal(msg); break;
    case "leaderboard": renderBoard(msg.players); break;
    case "round_end": onRoundEnd(msg); break;
    case "stream_metrics": onStreamMetrics(msg); break;
    case "metrics_summary": onSummary(msg); break;
    case "error": showError(msg); break;
  }
}

function onRoomCreated(msg) {
  state.room = msg.room;
  el.roomCode.textContent = msg.room;
  el.timeLimit.value = Math.round(msg.round_seconds);
  el.next.disabled = false;
  document.title = `Quiz ${msg.room}`;
  loadJoin();
}

async function loadJoin() {
  const q = `room=${encodeURIComponent(state.room)}`;
  el.qr.src = `/join-qr.svg?${q}`;
  try {
    const info = await (await fetch(`/join-info?${q}`)).json();
    el.joinUrl.textContent = info.url.replace(/^https?:\/\//, "");
    el.joinWarn.hidden = info.https;
  } catch (e) {
    el.joinUrl.textContent = `${location.host}/play?room=${state.room}`;
  }
}

function setPlayers(n) {
  state.players = n;
  el.players.textContent = `${n} player${n === 1 ? "" : "s"}`;
  renderAnswered();
}

function showError(msg) {
  if (msg.code === "no_round") return;  // end_round raced the timer
  el.qReveal.hidden = false;
  el.qReveal.textContent = `Server: ${msg.message}`;
}

// --- rounds ----------------------------------------------------------------------

function onQuestion(msg) {
  state.round = {
    number: msg.round, index: msg.index, total: msg.total, text: msg.text,
    timeLimit: msg.time_limit, endsAt: performance.now() + msg.remaining * 1000, open: true,
  };
  state.answered.clear();
  state.live = [];
  state.feedLat.clear();
  el.qNum.textContent = `Question ${msg.index + 1} of ${msg.total} · round ${msg.round}`;
  el.qText.textContent = msg.text;
  el.qReveal.hidden = true;
  el.next.textContent = "Next question";
  el.next.disabled = true;
  el.end.disabled = false;
  el.join.classList.add("small");
  el.mScope.textContent = "this round";
  renderAnswered();
  renderLiveMetrics();
  startTimer();
}

function onFinal(msg) {
  if (!state.round || msg.round !== state.round.number) return;
  state.answered.add(msg.player_id);
  renderAnswered();
  addFeed(msg);
}

function onRoundEnd(msg) {
  if (state.round) state.round.open = false;
  stopTimer();
  el.qTimer.textContent = "";
  el.qTimebar.style.width = "0";
  el.qReveal.hidden = false;
  el.qReveal.replaceChildren("Answer: ", make("b", null, msg.answer),
    ` · ${msg.answered} of ${msg.players} answered`);
  el.next.disabled = false;
  el.end.disabled = true;
}

function renderAnswered() {
  el.qAnswers.textContent = state.round ? `${state.answered.size} / ${state.players} answers in` : "";
}

function startTimer() {
  stopTimer();
  const tick = () => {
    const r = state.round;
    if (!r || !r.open) return;
    const left = Math.max(0, (r.endsAt - performance.now()) / 1000);
    el.qTimer.textContent = Math.ceil(left);
    el.qTimer.classList.toggle("low", left <= 5);
    el.qTimebar.style.width = `${(left / r.timeLimit) * 100}%`;
  };
  tick();
  state.timer = setInterval(tick, 200);
}

function stopTimer() {
  if (state.timer) clearInterval(state.timer);
  state.timer = null;
}

function startQuestion() {
  if (el.next.disabled) return;
  const msg = { type: "start_question" };
  const limit = Number(el.timeLimit.value);
  if (limit >= 3 && limit <= 120) msg.time_limit = limit;
  el.next.disabled = true;
  send(msg);
}

function endRound() {
  if (el.end.disabled) return;
  el.end.disabled = true;
  send({ type: "end_round" });
}

// --- leaderboard and feed ------------------------------------------------------

function renderBoard(rows) {
  el.boardEmpty.hidden = rows.length > 0;
  const items = rows.slice(0, BOARD_MAX).map((p) => {
    const li = make("li", p.rank <= 3 ? "top" : "");
    const name = make("span", "name", p.name);
    if (!p.simulated) name.append(make("span", "tag real", "phone"));
    li.append(make("span", "rank", p.rank), name, make("span", "delta", p.round_points ? `+${p.round_points}` : ""),
      make("span", "score", p.score));
    const before = state.lastBoard.get(p.player_id);
    if (before !== undefined && before !== p.score) li.classList.add("flash");
    return li;
  });
  state.lastBoard = new Map(rows.map((p) => [p.player_id, p.score]));
  el.board.classList.toggle("two", items.length > BOARD_ONE_COLUMN);
  el.board.classList.toggle("dense", items.length > BOARD_DENSE);
  el.board.replaceChildren(...items);
}

function addFeed(msg) {
  const live = state.live.find((m) => m.player_id === msg.player_id && m.round === msg.round);
  const li = make("li");
  li.append(make("span", "who", msg.name));
  if (msg.error) {
    li.append(make("span", "said err", msg.error.replace("transcribe_", "transcribe ")));
  } else {
    li.append(make("span", `said ${msg.correct ? "good" : "bad"}`, msg.text || "(nothing heard)"));
  }
  // The final comes just before its stream_metrics: the latency usually lands a moment later.
  const lat = make("span", "lat", live ? secs(live.answer_latency_s) : "");
  li.append(lat);
  state.feedLat.set(`${msg.round}:${msg.player_id}`, lat);
  el.feed.prepend(li);
  while (el.feed.children.length > FEED_MAX) el.feed.lastChild.remove();
  if (state.feedLat.size > 4 * FEED_MAX) state.feedLat.delete(state.feedLat.keys().next().value);
}

function onStreamMetrics(msg) {
  state.live.push(msg);
  const lat = state.feedLat.get(`${msg.round}:${msg.player_id}`);
  if (lat) lat.textContent = secs(msg.answer_latency_s);
  renderLiveMetrics();
}

// --- metrics -------------------------------------------------------------------

function renderLiveMetrics() {
  // Live, from this round's stream_metrics; replaced by the server's summary
  // (same definitions, server/metrics.py) once the round's streams are done.
  const ok = state.live.filter((m) => !m.error);
  const answers = ok.map((m) => m.answer_latency_s).filter((v) => v !== null);
  const api = ok.map((m) => m.api_latency_s).filter((v) => v !== null);
  el.mP50.textContent = secs(percentile(answers, 50));
  el.mP95.textContent = secs(percentile(answers, 95));
  el.mApi.textContent = secs(percentile(api, 50));
  const errors = state.live.length - ok.length;
  el.mErrors.textContent = state.live.length ? `${errors}/${state.live.length}` : "–";
  el.mErrors.className = `v ${errors ? "bad" : state.live.length ? "good" : ""}`;
}

function onSummary(msg) {
  if (state.round && msg.round === state.round.number) {
    const s = msg.this_round;
    el.mScope.textContent = `round ${msg.round}, final`;
    el.mP50.textContent = secs(s.answer_latency_s.p50);
    el.mP95.textContent = secs(s.answer_latency_s.p95);
    el.mApi.textContent = secs(s.api_latency_s.p50);
    el.mErrors.textContent = s.answers ? `${s.errors}/${s.answers}` : "–";
    el.mErrors.className = `v ${s.errors ? "bad" : s.answers ? "good" : ""}`;
    el.mWer.textContent = s.mean_wer === null ? "–" : `${Math.round((1 - s.mean_wer) * 100)}%`;
  }
  const body = el.byStreams.querySelector("tbody");
  body.replaceChildren(...msg.by_streams.map((g) => {
    const tr = make("tr", g.round_streams === "all" ? "all" : "");
    const errs = g.answers ? `${g.errors}/${g.answers}` : "–";
    for (const v of [g.round_streams, g.rounds, g.answers, secs(g.answer_latency_s.p50),
      secs(g.answer_latency_s.p95), secs(g.api_latency_s.p50), errs]) tr.append(make("td", null, v));
    return tr;
  }));
  el.byStreams.hidden = msg.by_streams.length === 0;
}

async function pollHealth() {
  try {
    const h = await (await fetch("/health", { cache: "no-store" })).json();
    const t = h.transcribe || {};
    el.transcribe.textContent = t.host ? `Transcribe: ${t.host}${t.stream_cap ? ` (cap ${t.stream_cap})` : ""}`
      : "Transcribe: off";
    if (typeof t.open_streams === "number") {
      el.mStreams.textContent = t.open_streams;
    }
  } catch (e) {
    // server gone: the WebSocket close says so
  }
}

// --- wiring --------------------------------------------------------------------

el.next.addEventListener("click", startQuestion);
el.end.addEventListener("click", endRound);
document.addEventListener("keydown", (ev) => {
  if (ev.target instanceof HTMLInputElement) return;
  if (ev.key === "n" || ev.key === "N") startQuestion();
  else if (ev.key === "e" || ev.key === "E") endRound();
});
window.addEventListener("beforeunload", (ev) => {
  if (state.players > 0) ev.preventDefault();  // leaving closes the room for everyone
});

connect();
pollHealth();
setInterval(pollHealth, HEALTH_EVERY_MS);
