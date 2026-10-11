"""Metrics recorder and report (CQ-10): what Transcribe did under load.

``TranscribeBridge`` keeps a ``StreamMetrics`` timeline per stream. Once a
round has ended and all its streams have finished, the bridge hands them to
``MetricsRecorder.round_done``, which:

- joins each stream with the game's answer (expected answer, correct, points,
  real or simulated player, what a simulated player said);
- keeps one row per player per round: a player can have several streams in a
  round (refused under the demo cap, then retried at hold_start; or replaced).
  The row is the stream that reported the answer, else the last one, and
  ``attempts`` counts them all;
- writes ``reports/<start time>-<room>/``:
  - ``round-NN.json`` / ``round-NN.csv``: that round's rows and summary;
  - ``streams.csv``: every row of the run so far;
  - ``summary.json`` / ``summary.csv``: the run so far, one group per number
    of streams in the round (``round_streams``), plus "all";
- sends the host a ``metrics_summary`` message (live feed for CQ-11).

Each reported answer also goes to the host at once as ``stream_metrics``.

Columns (seconds unless noted; empty when it didn't happen):

- ``round_streams``: streams opened for the round, about the number of
  players. This is the concurrency the summary groups by: every player's
  stream is open from question start until their answer is in.
- ``open_streams_at_release``: Transcribe stream tasks (waiting for a slot,
  connecting or open) in this server process, all rooms, at release.
- ``connect_s``: stream requested -> WebSocket open, including any wait for a
  slot under the demo cap (``slot_wait_s``). Not part of answer latency.
- ``answer_latency_s`` = ``send_backlog_s`` + ``end_to_final_s`` +
  ``quiet_wait_s``: player released -> answer scored.
- ``api_latency_s``: ``end_to_final_s`` only for streams that were ready when
  the player pressed hold and had no audio backlog at release, i.e.
  Transcribe's own end-of-audio -> final time. Empty otherwise, and empty
  when the final came before the end of audio (``end_to_final_s`` is then 0).
- ``first_partial_s``: first audio sent -> first partial.
- ``wer``: word error rate of the transcript against what a simulated player
  said (the clip text); lowercase words, punctuation ignored, so "6" vs "six"
  counts as an error.
- ``close_code`` / ``close_reason``: how Transcribe closed the stream (1006
  is normal). Empty code with reason "closed by the bridge (...)": we closed
  it first, e.g. the next question started after the answer was in.
- ``error``: the error code the player got (``transcribe_unavailable``,
  ``transcribe_error``, ``transcribe_timeout``); such answers are not scored
  and are left out of latency and accuracy. ``no_result:<outcome>``: the
  player pressed hold but no result was ever reported (stream cancelled by
  the next question or the player leaving, or replaced): counted as an error.

Summary per group: rows, answers (players who pressed hold), errors and
error rate (of answers, by code), cut-offs, connect / answer / API latency
(count, p50, p95, max), ``correct_rate`` of scored answers (the game's
right-answer rate: with the simulator it mostly reflects its --correct-rate
mix of right and wrong clips, not Transcribe), and for simulated players the
ASR accuracy: ``mean_wer`` and ``scorer_agrees`` (how often the scorer agreed
with the clip's right/wrong label). ``dropped_after_answer``: streams that
failed after their answer was already reported (``post_report_error``).
"""
import csv
import datetime
import json
import logging
import math
import re
from dataclasses import dataclass, field
from pathlib import Path

from .config import BYTES_PER_SEC

log = logging.getLogger("quiz.metrics")

BACKLOG_TOLERANCE = 0.05  # seconds of unsent audio at release still counted as "no backlog"

COLUMNS = [
    "room", "round", "question_id", "player_id", "player", "simulated", "attempts", "outcome", "error",
    "error_detail", "report_trigger", "close_code", "close_reason", "context", "round_streams", "open_streams_at_release",
    "ready_at_hold_start", "cut_off", "connect_s", "slot_wait_s", "answer_latency_s", "send_backlog_s",
    "end_to_final_s", "quiet_wait_s", "api_latency_s", "first_partial_s", "close_delay_s",
    "backlog_at_release_s", "audio_s", "partials", "finals", "late_finals", "transcript", "said",
    "expected_answer", "correct", "expect_correct", "points", "wer", "post_report_error", "started_at",
]


def percentile(values, p):
    """Nearest-rank percentile; None for no values."""
    if not values:
        return None
    s = sorted(values)
    return s[max(0, math.ceil(p / 100 * len(s)) - 1)]


def stats(values) -> dict:
    values = [v for v in values if v is not None]
    r = lambda v: round(v, 3) if v is not None else None  # noqa: E731
    return {"count": len(values), "p50": r(percentile(values, 50)), "p95": r(percentile(values, 95)),
            "max": r(max(values, default=None))}


def words(text: str | None) -> list[str]:
    return re.findall(r"[a-z0-9']+", (text or "").lower().replace("’", "'"))


def wer(said: str | None, heard: str | None) -> float | None:
    """Word error rate of ``heard`` against ``said``; None without a reference."""
    ref, hyp = words(said), words(heard)
    if not ref:
        return None
    prev = list(range(len(hyp) + 1))
    for i in range(1, len(ref) + 1):
        cur = [i] + [0] * len(hyp)
        for j in range(1, len(hyp) + 1):
            cur[j] = min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (ref[i - 1] != hyp[j - 1]))
        prev = cur
    return round(prev[-1] / len(ref), 3)


def _r(v):
    return round(v, 3) if isinstance(v, float) else v


def pick_streams(metrics) -> list[tuple[object, int]]:
    """One (stream, attempts) per player: the stream that reported, else the last."""
    by_player: dict[str, list] = {}
    for m in metrics:
        by_player.setdefault(m.player_id, []).append(m)
    picked = []
    for ms in by_player.values():
        reported = [m for m in ms if m.reported is not None]
        picked.append((reported[-1] if reported else ms[-1], len(ms)))
    return picked


def make_row(room, rnd, m, attempts: int, round_streams: int) -> dict:
    answer = rnd.answers.get(m.player_id)
    api = None
    error, detail = m.error, m.error_detail
    if m.ready_at_hold_start is not None and m.reported is None and not error:
        # Pressed hold but never got a result: the stream was cancelled (next
        # question, player left) or replaced before Transcribe answered.
        error, detail = f"no_result:{m.outcome}", "answer never reported"
    if (m.ready_at_hold_start and m.backlog_at_release_s is not None
            and m.backlog_at_release_s <= BACKLOG_TOLERANCE and not error and not m.final_before_end):
        api = m.end_to_final_s
    said = answer.said if answer else None
    scored = answer is not None and answer.transcript is not None and not error
    player = room.players.get(m.player_id)
    simulated = answer.simulated if answer else player.simulated if player else None
    return {k: _r(v) for k, v in {
        "room": room.code, "round": rnd.number, "question_id": rnd.question.id,
        "player_id": m.player_id, "player": m.player,
        "simulated": simulated, "attempts": attempts,
        "outcome": m.outcome, "error": error, "error_detail": detail,
        "report_trigger": m.report_trigger, "close_code": m.close_code, "close_reason": m.close_reason,
        "context": m.context,
        "round_streams": round_streams, "open_streams_at_release": m.open_streams_at_hold_end,
        "ready_at_hold_start": m.ready_at_hold_start, "cut_off": answer.cut_off if answer else None,
        "connect_s": m.connect_s, "slot_wait_s": m._span(m.open_requested, m.slot_acquired),
        "answer_latency_s": m.answer_latency_s, "send_backlog_s": m.send_backlog_s,
        "end_to_final_s": m.end_to_final_s, "quiet_wait_s": m.quiet_wait_s, "api_latency_s": api,
        "first_partial_s": m.first_partial_s, "close_delay_s": m.close_delay_s,
        "backlog_at_release_s": m.backlog_at_release_s, "audio_s": m.audio_bytes / BYTES_PER_SEC,
        "partials": m.partials, "finals": m.finals, "late_finals": m.late_finals,
        "transcript": m.transcript, "said": said, "expected_answer": rnd.question.answers[0],
        "correct": answer.correct if scored else None,
        "expect_correct": answer.expect_correct if answer else None,
        "points": answer.points if answer else None,
        "wer": wer(said, m.transcript) if said is not None and m.transcript is not None and not error else None,
        "post_report_error": m.post_report_error,
        "started_at": datetime.datetime.fromtimestamp(m.wall_start).isoformat(timespec="milliseconds"),
    }.items()}


def summarize(rows: list[dict]) -> dict:
    answers = [r for r in rows if r["ready_at_hold_start"] is not None]  # pressed hold
    errors = [r for r in answers if r["error"]]
    ok = [r for r in answers if not r["error"]]
    scored = [r for r in ok if r["correct"] is not None]
    sim = [r for r in scored if r["simulated"]]
    wers = [r["wer"] for r in sim if r["wer"] is not None]
    labelled = [r for r in sim if r["expect_correct"] is not None]
    codes: dict[str, int] = {}
    for r in errors:
        codes[r["error"]] = codes.get(r["error"], 0) + 1
    return {
        "rows": len(rows), "answers": len(answers), "errors": len(errors),
        "error_rate": round(len(errors) / len(answers), 3) if answers else None, "error_codes": codes,
        "cut_off": sum(bool(r["cut_off"]) for r in answers),
        "retried": sum(r["attempts"] > 1 for r in rows),
        "connect_s": stats(r["connect_s"] for r in rows),
        "answer_latency_s": stats(r["answer_latency_s"] for r in ok),
        "api_latency_s": stats(r["api_latency_s"] for r in ok),
        "correct": sum(bool(r["correct"]) for r in scored), "scored": len(scored),
        "correct_rate": round(sum(bool(r["correct"]) for r in scored) / len(scored), 3) if scored else None,
        "dropped_after_answer": sum(bool(r["post_report_error"]) for r in rows),
        "sim_scored": len(sim), "mean_wer": round(sum(wers) / len(wers), 3) if wers else None,
        "scorer_agrees": (round(sum(r["correct"] == r["expect_correct"] for r in labelled) / len(labelled), 3)
                          if labelled else None),
    }


def grouped(rows: list[dict]) -> list[dict]:
    """Summary per number of streams in the round, then "all"."""
    groups = sorted({r["round_streams"] for r in rows})
    out = [{"round_streams": n, "rounds": len({r["round"] for r in rows if r["round_streams"] == n}),
            **summarize([r for r in rows if r["round_streams"] == n])} for n in groups]
    out.append({"round_streams": "all", "rounds": len({r["round"] for r in rows}), **summarize(rows)})
    return out


SUMMARY_COLUMNS = [
    "round_streams", "rounds", "rows", "answers", "errors", "error_rate", "cut_off", "retried",
    "connect_p50", "connect_p95", "connect_max", "answer_n", "answer_p50", "answer_p95", "answer_max",
    "api_n", "api_p50", "api_p95", "api_max", "correct", "scored", "correct_rate", "sim_scored", "mean_wer",
    "scorer_agrees", "dropped_after_answer", "error_codes",
]


def flat_summary(g: dict) -> dict:
    flat = {k: g[k] for k in SUMMARY_COLUMNS if k in g}
    for name, key in (("connect", "connect_s"), ("answer", "answer_latency_s"), ("api", "api_latency_s")):
        for stat in ("p50", "p95", "max"):
            flat[f"{name}_{stat}"] = g[key][stat]
        if name != "connect":
            flat[f"{name}_n"] = g[key]["count"]
    flat["error_codes"] = " ".join(f"{k}={v}" for k, v in sorted(g["error_codes"].items()))
    return flat


def write_csv(path: Path, columns: list[str], rows: list[dict]):
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, columns, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


@dataclass
class Run:
    """One room's report: every row so far."""
    room: object
    dir: Path | None
    rows: list[dict] = field(default_factory=list)


class MetricsRecorder:
    def __init__(self, out_dir: Path | str | None):
        """``out_dir`` None: keep and send summaries, write no files."""
        self.out_dir = Path(out_dir) if out_dir is not None else None
        self.runs: dict[str, Run] = {}  # by room code; replaced when a new room reuses the code

    def _run(self, room) -> Run:
        run = self.runs.get(room.code)
        if run is None or run.room is not room:
            path = None
            if self.out_dir is not None:
                stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
                path = self.out_dir / f"{stamp}-{room.code}"
            run = self.runs[room.code] = Run(room, path)
        return run

    def stream_reported(self, room, rnd, m):
        """An answer was just reported: live row for the host screen."""
        api = (m.end_to_final_s if m.ready_at_hold_start and not m.error and not m.final_before_end
               and (m.backlog_at_release_s or 0) <= BACKLOG_TOLERANCE else None)
        room.host.send({"type": "stream_metrics", "round": rnd.number, "player_id": m.player_id,
                        "name": m.player, "connect_s": _r(m.connect_s),
                        "answer_latency_s": _r(m.answer_latency_s), "api_latency_s": _r(api),
                        "error": m.error, "open_streams": m.open_streams_at_hold_end})

    def round_done(self, room, rnd, metrics) -> list[dict]:
        """All of the round's streams have finished: add its rows, write the report."""
        picked = pick_streams(metrics)
        rows = sorted((make_row(room, rnd, m, n, len(picked)) for m, n in picked),
                      key=lambda r: int(r["player_id"].lstrip("p") or 0))
        run = self._run(room)
        run.rows = [r for r in run.rows if r["round"] != rnd.number] + rows
        round_summary = summarize(rows)
        groups = grouped(run.rows)
        if run.dir is not None:
            try:
                self._write(run, rnd.number, rows, round_summary, groups)
            except OSError:
                log.exception("room %s: writing the report to %s failed", room.code, run.dir)
        s = round_summary
        log.info("room %s round %d metrics: %d streams, %d answers, %d errors, answer p50 %s p95 %s, "
                 "api p50 %s, connect p95 %s, correct %d/%d%s", room.code, rnd.number, s["rows"],
                 s["answers"], s["errors"], s["answer_latency_s"]["p50"], s["answer_latency_s"]["p95"],
                 s["api_latency_s"]["p50"], s["connect_s"]["p95"], s["correct"], s["scored"],
                 f", report in {run.dir}" if run.dir else "")
        room.host.send({"type": "metrics_summary", "round": rnd.number,
                        "report": str(run.dir) if run.dir else None,
                        "this_round": round_summary, "by_streams": groups})
        return rows

    def _write(self, run: Run, number: int, rows, round_summary, groups):
        run.dir.mkdir(parents=True, exist_ok=True)
        (run.dir / f"round-{number:02d}.json").write_text(
            json.dumps({"summary": round_summary, "streams": rows}, indent=1), encoding="utf-8")
        write_csv(run.dir / f"round-{number:02d}.csv", COLUMNS, rows)
        write_csv(run.dir / "streams.csv", COLUMNS, run.rows)
        (run.dir / "summary.json").write_text(json.dumps({"by_streams": groups}, indent=1), encoding="utf-8")
        write_csv(run.dir / "summary.csv", SUMMARY_COLUMNS, [flat_summary(g) for g in groups])
