"""Metrics recorder and report (CQ-10), against the fake Transcribe server."""
import asyncio
import csv
import json
from types import SimpleNamespace

from fastapi.testclient import TestClient

from server.app import create_app
from server.bridge import LoggingBridge
from server.game import Client
from server.metrics import COLUMNS, MetricsRecorder, percentile, pick_streams, wer
from server.transcribe_bridge import ERR_UNAVAILABLE, TranscribeBridge
from tests.test_transcribe_bridge import CHUNK, QUESTIONS, fake_server, make_room, messages, run, until


def host_messages(room, kind):
    log = room.host.__dict__.setdefault("log", [])
    while not room.host.outbox.empty():
        log.append(room.host.outbox.get_nowait())
    return [m for m in log if m["type"] == kind]


def test_wer_and_percentile():
    assert wer("The answer is yellow", "The answer is yellow.") == 0
    assert wer("The answer is yellow", "answer is mellow") == 0.5  # one deletion, one substitution
    assert wer("Six legs", "6 legs.") == 0.5
    assert wer("", "anything") is None
    assert percentile([], 50) is None
    assert percentile([3, 1, 2, 4], 50) == 2 and percentile([3, 1, 2, 4], 95) == 4


def test_one_row_per_player_prefers_the_stream_that_reported():
    m = lambda pid, reported: SimpleNamespace(player_id=pid, reported=reported)  # noqa: E731
    refused, retry, other = m("p1", None), m("p1", 5.0), m("p2", None)
    assert pick_streams([refused, retry, other]) == [(retry, 2), (other, 1)]
    assert pick_streams([m("p1", None), last := m("p1", None)]) == [(last, 2)]


def test_round_report_rows_summary_files_and_host_feed(tmp_path):
    async def main():
        async with fake_server(close_after=0.2) as (fake, url):
            recorder = MetricsRecorder(tmp_path)
            bridge = TranscribeBridge(url, "m", limit=None, quiet_gap=0.1, recorder=recorder)
            room, _ = make_room(bridge)
            alice = room.add_player("Alice", Client(None), simulated=True)
            bob = room.add_player("Bob", Client(None))
            room.add_player("Cara", Client(None))  # never answers: unused stream
            await room.start_question()
            await until(lambda: fake.open == 3)
            await room.hold_start(alice, said="Paris", expect_correct=True)
            for _ in range(3):  # paced, so nothing is left unsent at release
                await room.audio(alice, CHUNK)
                await asyncio.sleep(0.05)
            await room.hold_end(alice)
            await until(lambda: messages(alice, "final"))
            [live] = host_messages(room, "stream_metrics")
            assert live["player_id"] == alice.id and live["error"] is None
            assert live["answer_latency_s"] > 0 and live["api_latency_s"] is not None
            await room.hold_start(bob)
            await room.audio(bob, CHUNK)
            await room.end_round("host")  # Bob is cut off
            await until(lambda: host_messages(room, "metrics_summary"))
            [summary] = host_messages(room, "metrics_summary")

        [run_dir] = tmp_path.iterdir()
        assert summary["report"] == str(run_dir)
        assert sorted(p.name for p in run_dir.iterdir()) == [
            "round-01.csv", "round-01.json", "streams.csv", "summary.csv", "summary.json"]
        rows = {r["player"]: r for r in json.loads((run_dir / "round-01.json").read_text())["streams"]}
        a, b, c = rows["Alice"], rows["Bob"], rows["Cara"]
        assert a["simulated"] is True and b["simulated"] is False and a["round_streams"] == 3
        assert a["outcome"] == "final" and a["correct"] is True and a["wer"] == 0 and a["said"] == "Paris"
        assert a["report_trigger"] == "quiet_gap" and a["close_code"] == 1006 and a["attempts"] == 1
        assert a["ready_at_hold_start"] is True and a["api_latency_s"] == a["end_to_final_s"]
        assert abs(a["answer_latency_s"] - (a["send_backlog_s"] + a["end_to_final_s"] + a["quiet_wait_s"])) < 0.005
        assert a["connect_s"] > 0 and a["open_streams_at_release"] >= 2 and a["expected_answer"] == "paris"
        assert b["cut_off"] is True and b["wer"] is None and b["outcome"] == "final"
        assert c["close_code"] is None and c["close_reason"] == "closed by the bridge (unused)"
        assert c["outcome"] == "unused" and c["answer_latency_s"] is None and c["ready_at_hold_start"] is None
        s = summary["this_round"]
        assert (s["rows"], s["answers"], s["errors"], s["error_rate"], s["cut_off"]) == (3, 2, 0, 0, 1)
        assert s["answer_latency_s"]["count"] == 2 and s["sim_scored"] == 1 and s["scorer_agrees"] == 1
        assert [g["round_streams"] for g in summary["by_streams"]] == [3, "all"]
        with (run_dir / "round-01.csv").open() as f:
            reader = csv.DictReader(f)
            assert reader.fieldnames == COLUMNS and len(list(reader)) == 3
        with (run_dir / "summary.csv").open() as f:
            [g3, total] = list(csv.DictReader(f))
            assert g3["round_streams"] == "3" and total["round_streams"] == "all" and g3["answer_n"] == "2"
    run(main())


def test_refused_then_retried_is_one_row_and_runs_accumulate(tmp_path):
    async def main():
        recorder = MetricsRecorder(tmp_path)
        bridge = TranscribeBridge("ws://127.0.0.1:9/", "m", limit=None, recorder=recorder)  # nothing listens
        room, [alice] = make_room(bridge, "Alice")
        for _ in range(2):
            await room.start_question()
            await until(lambda: bridge.open_count() == 0)
            await room.hold_start(alice)  # retries the failed stream; fails again
            await room.audio(alice, CHUNK)
            await room.hold_end(alice)
            await until(lambda: bridge.open_count() == 0)
            await room.end_round("host")
            await until(lambda: not bridge.reports)
        [run_dir] = tmp_path.iterdir()
        with (run_dir / "streams.csv").open() as f:
            rows = list(csv.DictReader(f))
        assert [(r["round"], r["attempts"], r["error"]) for r in rows] == [
            ("1", "2", ERR_UNAVAILABLE), ("2", "2", ERR_UNAVAILABLE)]
        [group, total] = json.loads((run_dir / "summary.json").read_text())["by_streams"]
        assert group["rounds"] == 2 and group["error_rate"] == 1 and group["error_codes"] == {ERR_UNAVAILABLE: 2}
        assert group["answer_latency_s"]["count"] == 0 and group["retried"] == 2
    run(main())


def test_bridge_metrics_list_is_capped(monkeypatch):
    from server import transcribe_bridge
    monkeypatch.setattr(transcribe_bridge, "METRICS_KEEP", 3)

    async def main():
        bridge = TranscribeBridge("ws://127.0.0.1:9/", "m", limit=None)
        room, _ = make_room(bridge, "A", "B")
        for _ in range(3):
            await room.start_question()
            await room.end_round("host")
        assert len(bridge.metrics) == 3 and bridge.metrics[-1].round == 3
        await bridge.aclose()
    run(main())


def test_join_simulated_and_hold_start_said_are_validated():
    app = create_app(QUESTIONS, LoggingBridge(), round_seconds=30)
    with TestClient(app) as tc, tc.websocket_connect("/ws/host") as host:
        code = host.receive_json()["room"]
        with tc.websocket_connect("/ws/play") as ws:
            ws.send_json({"type": "join", "room": code, "name": "Sim-01", "simulated": "yes"})
            assert ws.receive_json()["code"] == "bad_message"
            ws.send_json({"type": "join", "room": code, "name": "Sim-01", "simulated": True})
            assert ws.receive_json()["type"] == "joined"
            room = app.state.lobby.get(code)
            [player] = room.players.values()
            assert player.simulated is True
            host.send_json({"type": "start_question"})
            ws.receive_json()  # question
            ws.send_json({"type": "hold_start", "said": "x" * 201})
            assert ws.receive_json()["code"] == "bad_message"
            ws.send_json({"type": "hold_start", "said": "Paris", "expect_correct": 1})
            assert ws.receive_json()["code"] == "bad_message"
            ws.send_json({"type": "hold_start", "said": "Paris", "expect_correct": True})
            ws.send_json({"type": "hold_end"})
            host.send_json({"type": "end_round"})
            assert ws.receive_json()["type"] == "round_end"
            answer = room.round.answers[player.id]
            assert (answer.simulated, answer.said, answer.expect_correct) == (True, "Paris", True)


def test_final_before_end_of_audio_gives_no_negative_latency_and_uses_the_quiet_gap(tmp_path):
    """Transcribe endpoints while the player is still holding: the final arrives
    before the end of audio. The quiet gap must still apply (not the ~2 s close)."""
    import base64
    import json as _json

    import websockets

    from tests.test_transcribe_bridge import result

    async def handler(ws):
        await ws.recv()  # config
        chunks = 0
        async for message in ws:
            if not _json.loads(message)["audio"]["data"]:
                break
            chunks += 1
            if chunks == 2:
                await ws.send(result("Paris.", False))
        await asyncio.sleep(2)  # Cobalt closes ~2 s after its last result
        ws.transport.abort()

    async def main():
        async with websockets.serve(handler, "127.0.0.1", 0) as srv:
            url = f"ws://127.0.0.1:{srv.sockets[0].getsockname()[1]}/"
            recorder = MetricsRecorder(tmp_path)
            bridge = TranscribeBridge(url, "m", limit=None, quiet_gap=0.1, recorder=recorder)
            room, [alice] = make_room(bridge, "Alice")
            await room.start_question()
            await until(lambda: bridge.metrics[0].config_sent)
            await room.hold_start(alice)
            for _ in range(3):
                await room.audio(alice, CHUNK)
                await asyncio.sleep(0.05)
            await asyncio.sleep(0.3)  # still holding after the final
            await room.hold_end(alice)
            await until(lambda: messages(alice, "final"), timeout=1.0)  # quiet gap, not the 2 s close
            await room.end_round("host")
            await until(lambda: host_messages(room, "metrics_summary"), timeout=5)
        [run_dir] = tmp_path.iterdir()
        [row] = json.loads((run_dir / "round-01.json").read_text())["streams"]
        assert row["report_trigger"] == "quiet_gap" and row["answer_latency_s"] < 0.5
        assert row["end_to_final_s"] == 0 and row["api_latency_s"] is None and row["quiet_wait_s"] >= 0
        assert abs(row["answer_latency_s"] - (row["send_backlog_s"] + row["end_to_final_s"]
                                              + row["quiet_wait_s"])) < 0.005
    run(main())


def test_answer_cancelled_before_its_result_counts_as_an_error(tmp_path):
    async def main():
        async with fake_server(script=[("sleep", 1.0), ("final", "Paris.")]) as (fake, url):
            recorder = MetricsRecorder(tmp_path)
            bridge = TranscribeBridge(url, "m", limit=None, recorder=recorder)
            room, _ = make_room(bridge)
            alice = room.add_player("Alice", Client(None))
            sim = room.add_player("Sim-01", Client(None), simulated=True)  # never presses hold
            await room.start_question()
            await until(lambda: fake.open == 2)
            await room.hold_start(alice)
            await room.audio(alice, CHUNK)
            await room.end_round("host")   # Alice cut off; her final is 1 s away
            await room.start_question()    # ... and the next question cancels her stream
            await until(lambda: host_messages(room, "metrics_summary"))
            await bridge.aclose()
        [summary] = host_messages(room, "metrics_summary")
        s = summary["this_round"]
        assert (s["answers"], s["errors"], s["error_rate"]) == (1, 1, 1.0)
        assert s["error_codes"] == {"no_result:cancelled": 1}
        [run_dir] = tmp_path.iterdir()
        rows = {r["player"]: r for r in json.loads((run_dir / "round-01.json").read_text())["streams"]}
        assert rows["Sim-01"]["simulated"] is True and rows["Sim-01"]["error"] is None
        assert rows["Alice"]["error"] == "no_result:cancelled"
    run(main())
