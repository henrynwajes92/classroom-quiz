"""Ramp test (CQ-12) tests: scripts/ramp.py against the real app on a local port (no Cobalt calls).

A fake bridge answers every clip with "Paris" and, like the Transcribe
bridge, sends the host a metrics_summary shortly after each round ends,
built with server.metrics from made-up rows whose latency grows with the
number of players.
"""
import asyncio
import csv
import json
import threading
import time
import xml.etree.ElementTree as ET

import pytest
import uvicorn

from scripts import ramp
from server import metrics
from server.app import create_app
from server.bridge import Bridge
from tests.test_simulate import DEMO, QUESTIONS, RIGHT, write_manifest


def fake_row(rnd, n, i):
    return {"round": rnd, "round_streams": n, "ready_at_hold_start": True, "error": None, "correct": True,
            "simulated": True, "wer": 0.0, "expect_correct": True, "cut_off": False, "attempts": 1,
            "connect_s": 0.5, "answer_latency_s": 0.5 + 0.1 * n + 0.01 * i, "api_latency_s": 0.2 + 0.05 * n,
            "post_report_error": None}


class FakeBridge(Bridge):
    def __init__(self, host="transcribe.example.com", summaries=True):
        self.host, self.summaries = host, summaries
        self.rows: dict[str, list] = {}

    def status(self):
        return {"host": self.host, "stream_cap": None}

    async def hold_end(self, room, rnd, player):
        room.on_final(rnd, player, "Paris.")

    async def round_ended(self, room, rnd):
        if not self.summaries:
            return
        n = len(room.players)
        rows = self.rows.setdefault(room.code, [])
        rows += [fake_row(rnd.number, n, i) for i in range(n)]

        async def later():  # the real one comes once the round's streams have finished
            await asyncio.sleep(0.3)
            room.host.send({"type": "metrics_summary", "round": rnd.number, "report": None,
                            "this_round": metrics.summarize(rows), "by_streams": metrics.grouped(rows)})
        asyncio.get_running_loop().create_task(later())


def serve(bridge):
    srv = uvicorn.Server(uvicorn.Config(create_app(QUESTIONS[:1], bridge, 20), port=0, log_level="warning"))
    thread = threading.Thread(target=srv.run)
    thread.start()
    while not srv.started:
        time.sleep(0.02)
    return srv, thread, f"ws://127.0.0.1:{srv.servers[0].sockets[0].getsockname()[1]}"


@pytest.fixture
def server(request):
    srv, thread, url = serve(FakeBridge(*getattr(request, "param", ())))
    yield url
    srv.should_exit = True
    thread.join()


@pytest.fixture
def manifest(tmp_path):
    return write_manifest(tmp_path, [(f"right_{v}", RIGHT, v, True, "Paris") for v in "ABCD"])


def run(url, manifest, out, *args):
    return ramp.main(["--server", url, "--manifest", manifest, "--out", str(out), "--pause", "0",
                      "--min-delay", "0", "--max-delay", "0.1", "--seed", "1", "--metrics-wait", "5", *args])


def test_combined_report_from_metrics_summary(server, manifest, tmp_path, capsys):
    out = tmp_path / "ramp"
    assert run(server, manifest, out, "--levels", "1,3", "--rounds", "2") == 0
    levels = json.loads((out / "ramp.json").read_text())["levels"]
    assert [lv["players"] for lv in levels] == [1, 3]
    assert {lv["source"] for lv in levels} == {"server"}
    one, three = levels
    assert (one["answers"], one["rounds"], three["answers"], three["rounds"]) == (2, 2, 6, 2)
    assert one["answer_p50"] == 0.6 and three["answer_p50"] == 0.81 and three["answer_p95"] == 0.82
    assert three["api_p50"] == 0.35 and three["connect_p95"] == 0.5
    assert three["errors"] == 0 and three["error_rate"] == 0 and three["correct_rate"] == 1
    assert three["mean_wer"] == 0 and three["scorer_agrees"] == 1
    assert three["client_answer_n"] == 6  # the simulator's own timing kept as a cross-check
    assert one["room"] != three["room"]  # one room per level
    with (out / "ramp.csv").open() as f:
        rows = list(csv.DictReader(f))
    assert [r["players"] for r in rows] == ["1", "3"] and rows[1]["answer_p50"] == "0.81"
    assert (out / "level-03-sim.json").exists()
    svg = ET.parse(out / "ramp.svg").getroot()
    assert svg.tag.endswith("svg") and "3 players: answer p50 0.810 s" in (out / "ramp.svg").read_text()
    printed = capsys.readouterr().out
    assert "answer p50" in printed and "level 3:" in printed


@pytest.mark.parametrize("server", [(None, False)], indirect=True)
def test_falls_back_to_client_numbers_without_a_summary(server, manifest, tmp_path):
    out = tmp_path / "ramp"
    start = time.monotonic()
    assert run(server, manifest, out, "--levels", "2", "--rounds", "1") == 0
    assert time.monotonic() - start < 5  # no Transcribe (host None): doesn't wait for a summary
    (lv,) = json.loads((out / "ramp.json").read_text())["levels"]
    assert lv["source"] == "client" and lv["answers"] == 2 and lv["answer_n"] == 2
    assert lv["answer_p50"] is not None and lv["api_p50"] is None and lv["connect_p50"] is None
    assert lv["correct_rate"] == 1 and lv["scorer_agrees"] == 1
    assert (out / "ramp.svg").exists()


@pytest.mark.parametrize("server", [("demo.cobaltspeech.com",)], indirect=True)
def test_refuses_levels_above_4_on_the_demo_server(server, manifest, tmp_path, capsys):
    out = tmp_path / "ramp"
    assert run(server, manifest, out, "--levels", "1,5") == 2
    assert "Refusing 5 players" in capsys.readouterr().err and not out.exists()  # nothing ran
    assert ramp.main(["--levels", "1,4,5", "--server", "ws://127.0.0.1:9", "--transcribe-url", DEMO,
                      "--out", str(out)]) == 2  # server can't say: TRANSCRIBE_URL decides


def test_chart_handles_missing_values():
    row = {"players": 4, "answer_p50": None, "answer_p95": None, "api_p50": None, "api_p95": None,
           "error_rate": None, "errors": 0, "answers": 0, "error_codes": ""}
    ET.fromstring(ramp.chart_svg([row]))
    ET.fromstring(ramp.chart_svg([]))


@pytest.mark.parametrize("bad", [["--levels", "0,5"], ["--levels", "a"], ["--rounds", "0"],
                                 ["--min-delay", "4", "--max-delay", "3"]])
def test_bad_arguments(bad):
    with pytest.raises(SystemExit):
        ramp.parse_args(bad)
