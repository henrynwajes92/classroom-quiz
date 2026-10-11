"""Ramp test (CQ-12): where latency starts to climb as players are added.

    # dry run on the demo server (never more than 4 streams there)
    flock /tmp/cobalt_demo.lock python scripts/ramp.py --levels 1,4 --rounds 2 --server ws://localhost:8000
    # full ramp, only against the agreed dedicated instance, inside the agreed window
    python scripts/ramp.py --levels 1,5,10,20,35 --rounds 3 --pause 10 --server ws://localhost:8000

For each level it runs the simulator (scripts/simulate.py, CQ-8) with that
many players in a room of its own, hosting --rounds questions, then pauses
--pause seconds so the server and Transcribe settle before the next level.
Every level starts at the first question, so levels hear the same clips.

One room per level, so the server's CQ-10 report for that room (the
metrics_summary the host gets, and reports/<time>-<room>/ on the server) is
that level's. The level's numbers come from the last metrics_summary (its
"all" group: every round of the room): connect / answer / API latency
p50/p95, errors and codes, correct_rate, mean_wer, scorer_agrees. It comes up
to ~10 s after the last round_end, so each level waits up to --metrics-wait
for it. A server without Transcribe (BRIDGE=logging) or that sends none: the
level falls back to the simulator's own numbers (answer latency = hold_end
sent -> final received, which also includes the network to the game server;
no connect or API latency), marked source=client. The client-side answer
latency is always kept as a cross-check (client_answer_*).

Players speak --min-delay..--max-delay s after the question (default 3-5 s:
streams take 1-4 s to connect, so most answers measure Transcribe, not the
connect backlog).

Writes to --out (default reports/ramp-<time>/): ramp.json (settings, server
status, one entry per level), ramp.csv (one row per level), ramp.svg (answer
and API latency p50/p95 vs players, and error rate), level-NN-sim.json (the
simulator's full results), and, when the server's report folder is on this
machine, a copy of it as level-NN-room/. Also prints a table.

Demo server: refuses the whole ramp if any level is above 4 players while the
game server's /health says it uses the demo server (or, if it can't say, while
TRANSCRIBE_URL is or may be the demo server), unless --i-asked-ops; same check
as simulate.py. The server's own cap of 4 streams still applies.

Exit code: 0 if every level ran (host fine, every player joined; errors under
load are results, they are in the report); 1 if a level failed to run or the
ramp was interrupted; 2 for refused/bad arguments.
"""
import argparse
import asyncio
import csv
import datetime
import json
import math
import shutil
import sys
import time
from pathlib import Path
from xml.sax.saxutils import escape

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from scripts import simulate  # noqa: E402
from server import metrics  # noqa: E402
from server.transcribe_bridge import DEMO_MAX_STREAMS, FINAL_TIMEOUT, REPORT_SLACK  # noqa: E402

METRICS_WAIT = FINAL_TIMEOUT + REPORT_SLACK + 5  # the server's backstop for a round report, plus slack

COLUMNS = [
    "players", "source", "room", "rounds", "joined", "answers", "errors", "error_rate", "error_codes", "cut_off",
    "connect_p50", "connect_p95", "answer_n", "answer_p50", "answer_p95", "answer_max", "api_n", "api_p50",
    "api_p95", "api_max", "client_answer_n", "client_answer_p50", "client_answer_p95", "correct", "scored",
    "correct_rate", "mean_wer", "scorer_agrees", "host_error", "report",
]


def client_latencies(res: dict) -> list[float]:
    """hold_end sent -> final received, finals without an error."""
    return [a["latency_s"] for p in res["players"] for a in p["answers"]
            if a["text"] is not None and not a["error"] and a["latency_s"] is not None]


def client_summary(res: dict) -> dict:
    """The simulator's own view, in the shape of metrics.summarize (no connect/API latency)."""
    answers = [a for p in res["players"] for a in p["answers"] if a["delay_s"] is not None]
    codes: dict[str, int] = {}
    for p in res["players"]:
        for what in [a["error"] for a in p["answers"] if a["error"]] + [e["error"] for e in p["error_log"]]:
            code = what.split(":")[0]
            codes[code] = codes.get(code, 0) + 1
    errors = sum(codes.values())
    scored = [a for a in answers if a["text"] is not None and not a["error"] and a["correct"] is not None]
    wers = [w for w in (metrics.wer(a["clip_text"], a["text"]) for a in scored) if w is not None]
    labelled = [a for a in scored if a["expected_correct"] is not None]
    none = metrics.stats([])
    return {
        "rounds": len(res["rounds"]), "answers": len(answers), "errors": errors,
        "error_rate": round(errors / len(answers), 3) if answers else None, "error_codes": codes,
        "cut_off": sum(bool(a["cut_off"]) for a in answers),
        "connect_s": none, "answer_latency_s": metrics.stats(client_latencies(res)), "api_latency_s": none,
        "correct": sum(bool(a["correct"]) for a in scored), "scored": len(scored),
        "correct_rate": round(sum(bool(a["correct"]) for a in scored) / len(scored), 3) if scored else None,
        "mean_wer": round(sum(wers) / len(wers), 3) if wers else None,
        "scorer_agrees": (round(sum(a["correct"] == a["expected_correct"] for a in labelled) / len(labelled), 3)
                          if labelled else None),
    }


def level_row(players: int, res: dict) -> dict:
    """One level's line of the report: the server's numbers when it sent them, else the client's."""
    summary = res.get("metrics_summary")
    group = next((g for g in (summary or {}).get("by_streams", []) if g.get("round_streams") == "all"), None)
    source = "server" if group else "client"
    g = group or client_summary(res)
    flat = {
        "rounds": g.get("rounds"), "answers": g["answers"], "errors": g["errors"], "error_rate": g["error_rate"],
        "error_codes": " ".join(f"{k}={v}" for k, v in sorted(g["error_codes"].items())),
        "cut_off": g["cut_off"], "correct": g["correct"], "scored": g["scored"],
        "correct_rate": g["correct_rate"], "mean_wer": g["mean_wer"], "scorer_agrees": g["scorer_agrees"],
    }
    for name, key in (("connect", "connect_s"), ("answer", "answer_latency_s"), ("api", "api_latency_s")):
        for stat in ("p50", "p95", "max"):
            flat[f"{name}_{stat}"] = g[key][stat]
        flat[f"{name}_n"] = g[key]["count"]
    client = metrics.stats(client_latencies(res))
    return {"players": players, "source": source, "room": res["room"],
            "joined": res["aggregate"]["joined"], **flat,
            "client_answer_n": client["count"], "client_answer_p50": client["p50"],
            "client_answer_p95": client["p95"], "host_error": res["host_error"],
            "report": (summary or {}).get("report")}


# --- chart -------------------------------------------------------------------

def nice_max(v: float) -> tuple[float, float]:
    """Axis top and tick step for values up to ``v``."""
    v = max(v, 1e-9)
    mag = 10 ** (len(str(int(v))) - 1) if v >= 1 else 10 ** -len(str(int(1 / v)))
    for m in (0.1, 0.2, 0.25, 0.5, 1, 2, 2.5, 5, 10):
        step = m * mag
        ticks = math.ceil(v / step - 1e-9)  # tolerance: 0.1 / 0.02 is 5.000000000000001
        if ticks <= 5:
            return round(ticks * step, 9), step
    return v, v / 5


SERIES = [  # key, label, color slot, dashed
    ("answer_p50", "answer p50", 1, False), ("answer_p95", "answer p95", 1, True),
    ("api_p50", "API p50", 2, False), ("api_p95", "API p95", 2, True),
]


def chart_svg(rows: list[dict], title: str = "Latency and errors vs players") -> str:
    """Self-contained SVG: latency lines (top) and error-rate bars (bottom),
    one x axis (players). Light and dark via prefers-color-scheme."""
    W, H, L, R = 720, 470, 64, 24
    top0, top1 = 70, 290       # latency panel y range
    bot0, bot1 = 340, 420      # error panel y range
    xs = [r["players"] for r in rows]
    xmax = max(xs + [1])
    x = lambda n: L + (W - L - R) * (n / xmax if xmax else 0)  # noqa: E731
    vals = [r[k] for r in rows for k, *_ in SERIES if r.get(k) is not None]
    ytop, ystep = nice_max(max(vals, default=1.0))
    y = lambda v: top1 - (top1 - top0) * v / ytop  # noqa: E731
    emax = max([r["error_rate"] or 0 for r in rows] + [0.1])
    etop, _ = nice_max(emax)
    ey = lambda v: bot1 - (bot1 - bot0) * v / etop  # noqa: E731
    out = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" '
           f'font-family="system-ui, sans-serif" font-size="12" role="img" aria-label="{escape(title)}">',
           "<style>"
           ":root{--bg:#fcfcfb;--ink:#0b0b0b;--muted:#52514e;--grid:#e4e3df;--s1:#2a78d6;--s2:#eb6834;--err:#e34948}"
           "@media (prefers-color-scheme: dark){:root{--bg:#1a1a19;--ink:#f0efec;--muted:#c3c2b7;"
           "--grid:#3a3936;--s1:#3987e5;--s2:#d95926;--err:#e66767}}"
           "text{fill:var(--muted)} .t{fill:var(--ink);font-size:15px;font-weight:600} .grid{stroke:var(--grid)}"
           ".s1{stroke:var(--s1)} .s2{stroke:var(--s2)} .f1{fill:var(--s1)} .f2{fill:var(--s2)}"
           "</style>",
           f'<rect width="{W}" height="{H}" fill="var(--bg)"/>',
           f'<text class="t" x="{L}" y="24">{escape(title)}</text>']
    # legend
    lx = L
    for key, label, slot, dashed in SERIES:
        dash = ' stroke-dasharray="6 4"' if dashed else ""
        out.append(f'<line class="s{slot}" x1="{lx}" y1="46" x2="{lx + 22}" y2="46" stroke-width="2"{dash}/>'
                   f'<text x="{lx + 28}" y="50">{label}</text>')
        lx += 120
    out.append(f'<text x="{lx}" y="50">seconds, scored answers</text>')
    # latency grid
    for i in range(round(ytop / ystep) + 1):
        t = round(i * ystep, 9)
        out.append(f'<line class="grid" x1="{L}" y1="{y(t):.1f}" x2="{W - R}" y2="{y(t):.1f}"/>'
                   f'<text x="{L - 8}" y="{y(t) + 4:.1f}" text-anchor="end">{t:g} s</text>')
    for key, label, slot, dashed in SERIES:
        pts = [(x(r["players"]), y(r[key]), r) for r in rows if r.get(key) is not None]
        dash = ' stroke-dasharray="6 4"' if dashed else ""
        if len(pts) > 1:
            path = " ".join(f"{px:.1f},{py:.1f}" for px, py, _ in pts)
            out.append(f'<polyline class="s{slot}" points="{path}" fill="none" stroke-width="2"{dash}/>')
        for px, py, r in pts:
            out.append(f'<circle class="f{slot}" cx="{px:.1f}" cy="{py:.1f}" r="4" stroke="var(--bg)" '
                       f'stroke-width="2"><title>{r["players"]} players: {label} {r[key]:.3f} s</title></circle>')
    # error panel
    out.append(f'<text x="{L}" y="{bot0 - 14}">error rate (of answers)</text>')
    for t in (0, etop / 2, etop):  # a short panel: three gridlines are enough
        out.append(f'<line class="grid" x1="{L}" y1="{ey(t):.1f}" x2="{W - R}" y2="{ey(t):.1f}"/>'
                   f'<text x="{L - 8}" y="{ey(t) + 4:.1f}" text-anchor="end">{t * 100:g}%</text>')
    bw = 18
    for r in rows:
        e = r["error_rate"]
        cx = x(r["players"])
        label = "n/a" if e is None else f"{e * 100:.0f}%"
        if e:
            out.append(f'<rect x="{cx - bw / 2:.1f}" y="{ey(e):.1f}" width="{bw}" height="{bot1 - ey(e):.1f}" '
                       f'rx="2" fill="var(--err)"><title>{r["players"]} players: {r["errors"]} errors '
                       f'of {r["answers"]} answers {r["error_codes"]}</title></rect>')
        out.append(f'<text x="{cx:.1f}" y="{(ey(e) if e else bot1) - 5:.1f}" text-anchor="middle">{label}</text>')
    # x axis
    for n in xs:
        out.append(f'<text x="{x(n):.1f}" y="{bot1 + 18}" text-anchor="middle">{n}</text>')
    out.append(f'<text x="{(L + W - R) / 2}" y="{H - 12}" text-anchor="middle">players (concurrent streams)</text>')
    out.append("</svg>")
    return "\n".join(out) + "\n"


# --- report ------------------------------------------------------------------

def write_report(out: Path, meta: dict, rows: list[dict]):
    out.mkdir(parents=True, exist_ok=True)
    (out / "ramp.json").write_text(json.dumps({**meta, "levels": rows}, indent=1), encoding="utf-8")
    with (out / "ramp.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, COLUMNS, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    (out / "ramp.svg").write_text(chart_svg(rows), encoding="utf-8")


def print_table(rows: list[dict]):
    def s(v):
        return f"{v:.2f}" if isinstance(v, (int, float)) else "-"

    def pct(v):
        return f"{v * 100:.0f}%" if isinstance(v, (int, float)) else "-"
    head = (f"{'players':>7} {'src':6} {'answers':>7} {'errors':>6} {'err%':>5} {'answer p50':>10} {'p95':>5} "
            f"{'api p50':>7} {'p95':>5} {'connect p50':>11} {'p95':>5} {'correct':>7} {'wer':>5} {'agree':>5}")
    print("\n" + head)
    for r in rows:
        print(f"{r['players']:>7} {r['source']:6} {r['answers']:>7} {r['errors']:>6} {pct(r['error_rate']):>5} "
              f"{s(r['answer_p50']):>10} {s(r['answer_p95']):>5} {s(r['api_p50']):>7} {s(r['api_p95']):>5} "
              f"{s(r['connect_p50']):>11} {s(r['connect_p95']):>5} {pct(r['correct_rate']):>7} "
              f"{s(r['mean_wer']):>5} {pct(r['scorer_agrees']):>5}"
              + (f"  errors: {r['error_codes']}" if r["error_codes"] else ""))
    print("seconds. answer = player released -> answer scored; api = Transcribe end of audio -> final "
          "(streams ready at hold); src=client: the simulator's own timing, no api/connect.")


# --- run ---------------------------------------------------------------------

def sim_args(args, players: int, expect_summary: bool):
    argv = ["--host", "--players", str(players), "--rounds", str(args.rounds), "--server", args.server,
            "--min-delay", str(args.min_delay), "--max-delay", str(args.max_delay),
            "--correct-rate", str(args.correct_rate), "--manifest", args.manifest,
            "--prefix", f"L{players}", "--transcribe-url", args.transcribe_url or "",
            "--metrics-wait", str(args.metrics_wait if expect_summary else 0)]
    if args.seed is not None:
        argv += ["--seed", str(args.seed)]
    if args.time_limit:
        argv += ["--time-limit", str(args.time_limit)]
    if args.i_asked_ops:
        argv.append("--i-asked-ops")
    return simulate.parse_args(argv)


def parse_levels(text: str) -> list[int]:
    levels = [int(v) for v in text.split(",") if v.strip()]
    if not levels or min(levels) < 1:
        raise ValueError
    return levels


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--levels", default="1,5,10,20,35", help="players per level, comma separated")
    p.add_argument("--rounds", type=int, default=3, help="questions per level")
    p.add_argument("--pause", type=float, default=10.0, help="seconds between levels, so the server settles")
    p.add_argument("--server", default="ws://localhost:8000", help="game server base URL (ws:// or wss://)")
    p.add_argument("--out", help="report folder (default reports/ramp-<time>/)")
    p.add_argument("--min-delay", type=float, default=3.0,
                   help="seconds after the question before speaking (>= 3: streams are usually ready)")
    p.add_argument("--max-delay", type=float, default=5.0)
    p.add_argument("--correct-rate", type=float, default=0.75)
    p.add_argument("--time-limit", type=float, help="seconds per question (default: the server's)")
    p.add_argument("--seed", type=int)
    p.add_argument("--metrics-wait", type=float, default=METRICS_WAIT,
                   help="per level, longest wait for the server's last metrics_summary")
    p.add_argument("--manifest", default=str(ROOT / "clips" / "manifest.json"))
    p.add_argument("--transcribe-url", default=simulate.config.TRANSCRIBE_URL,
                   help="the game server's TRANSCRIBE_URL, if /health can't say")
    p.add_argument("--i-asked-ops", action="store_true",
                   help=f"allow levels above {DEMO_MAX_STREAMS} players against the demo server")
    args = p.parse_args(argv)
    try:
        args.levels = parse_levels(args.levels)
    except ValueError:
        p.error("--levels must be positive whole numbers, e.g. 1,5,10")
    if args.rounds < 1 or args.pause < 0 or args.metrics_wait < 0:
        p.error("need --rounds >= 1, --pause >= 0, --metrics-wait >= 0")
    if args.min_delay < 0 or args.max_delay < args.min_delay:
        p.error("need 0 <= --min-delay <= --max-delay")
    return args


def main(argv=None) -> int:
    args = parse_args(argv)
    status = simulate.server_status(args.server)
    refused = simulate.cap_error(max(args.levels), args.transcribe_url, args.i_asked_ops, status)
    if refused:
        print(refused.replace("--players", "levels of"), file=sys.stderr)
        return 2
    # BRIDGE=logging says it has no Transcribe: no metrics_summary will come, don't wait for one.
    expect_summary = not (status is not None and status["host"] is None)
    started = datetime.datetime.now()
    out = Path(args.out) if args.out else ROOT / "reports" / f"ramp-{started:%Y%m%d-%H%M%S}"
    meta = {"started": started.isoformat(timespec="seconds"), "server": args.server, "transcribe": status,
            "levels_planned": args.levels, "rounds": args.rounds, "pause_s": args.pause,
            "min_delay_s": args.min_delay, "max_delay_s": args.max_delay, "correct_rate": args.correct_rate,
            "seed": args.seed}
    print(f"ramp {args.levels} x {args.rounds} rounds -> {args.server}; Transcribe "
          f"{(status or {}).get('host') or ('none' if status else 'unknown (server did not say)')}; report {out}")
    rows, failed = [], False
    for i, n in enumerate(args.levels):
        if i:
            print(f"\npausing {args.pause:g} s")
            try:
                time.sleep(args.pause)
            except KeyboardInterrupt:
                print("interrupted")
                failed = True
                break
        print(f"\n=== level {i + 1}/{len(args.levels)}: {n} players ===")
        res = asyncio.run(simulate.amain(sim_args(args, n, expect_summary)))
        out.mkdir(parents=True, exist_ok=True)
        (out / f"level-{n:02d}-sim.json").write_text(json.dumps(res, indent=1), encoding="utf-8")
        row = level_row(n, res)
        rows.append(row)
        if row["report"] and Path(row["report"]).is_dir():  # server on this machine: keep its stream rows
            shutil.copytree(row["report"], out / f"level-{n:02d}-room", dirs_exist_ok=True)
        print(f"level {n}: {row['answers']} answers, {row['errors']} errors, answer p50 {row['answer_p50']} "
              f"p95 {row['answer_p95']} (source: {row['source']})")
        if res["host_error"] or res["aggregate"]["joined"] < n or res.get("interrupted"):
            failed = True
            if res.get("interrupted"):
                break
    meta["finished"] = datetime.datetime.now().isoformat(timespec="seconds")
    write_report(out, meta, rows)
    print_table(rows)
    print(f"wrote {out}/ramp.json, ramp.csv, ramp.svg")
    return 1 if failed or not rows else 0


if __name__ == "__main__":
    sys.exit(main())
