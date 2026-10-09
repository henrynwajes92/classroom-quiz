"""Simulator tests: scripts/simulate.py against the real app on a local port (no Cobalt calls).

A fake bridge stands in for Transcribe: it "hears" Paris for the right clip
and London for anything else, told apart by their length.
"""
import json
import threading
import time
import wave

import pytest
import uvicorn
from websockets.sync.client import connect

from scripts import simulate
from server.app import create_app
from server.bridge import Bridge
from server.questions import Question

QUESTIONS = [
    Question("france-capital", "What is the capital of France?", ["paris"]),
    Question("red-planet", "Which planet is known as the Red Planet?", ["mars"]),  # no clips: silent
]
RIGHT, WRONG = 9600, 6400  # bytes of PCM in the test clips (0.3 s, 0.2 s)
DEMO = "wss://demo.cobaltspeech.com/transcribe/api/transcribe/v5/streaming-recognize"
LOCAL = "ws://localhost:9/"  # not the demo server: no cap


class FakeBridge(Bridge):
    def __init__(self):
        self.audio_bytes = {}
        self.first_chunk, self.ended = {}, {}  # monotonic times by (round, name)
        self.fail = set()  # names whose answers get an error final

    async def audio(self, room, rnd, player, chunk):
        key = (rnd.number, player.name)
        self.first_chunk.setdefault(key, time.monotonic())
        self.audio_bytes[key] = self.audio_bytes.get(key, 0) + len(chunk)

    async def hold_end(self, room, rnd, player):
        key = (rnd.number, player.name)
        self.ended[key] = time.monotonic()
        if player.name in self.fail:
            room.on_final(rnd, player, "", error="transcribe_unavailable")
            return
        room.on_partial(rnd, player, "pa")
        room.on_final(rnd, player, "Paris." if self.audio_bytes.get(key) == RIGHT else "London.")


@pytest.fixture
def server():
    """The game server in a thread, on a free port: yields (ws url, bridge)."""
    bridge = FakeBridge()
    srv = uvicorn.Server(uvicorn.Config(create_app(QUESTIONS, bridge, 20), port=0, log_level="warning"))
    thread = threading.Thread(target=srv.run)
    thread.start()
    while not srv.started:
        time.sleep(0.02)
    port = srv.servers[0].sockets[0].getsockname()[1]
    yield f"ws://127.0.0.1:{port}", bridge
    srv.should_exit = True
    thread.join()


def write_manifest(tmp_path, specs):
    """specs: (name, bytes of PCM, voice, correct, text) per france-capital clip."""
    clips = []
    for name, nbytes, voice, correct, text in specs:
        path = tmp_path / f"{name}.wav"
        with wave.open(str(path), "wb") as w:
            w.setnchannels(1), w.setsampwidth(2), w.setframerate(16000)
            w.writeframes(b"\x01\x00" * (nbytes // 2))
        clips.append({"path": str(path), "question_id": "france-capital", "text": text, "answer": text,
                      "correct": correct, "voice": voice, "duration": nbytes / 32000})
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(clips))
    return str(path)


@pytest.fixture
def manifest(tmp_path):
    return write_manifest(tmp_path, [(f"right_{v}", RIGHT, v, True, "Paris") for v in "ABCD"]
                          + [("wrong_A", WRONG, "A", False, "London")])


def run(server, manifest, tmp_path, *args, code=0):
    out = tmp_path / "results.json"
    assert simulate.main(["--server", server, "--manifest", manifest, "--max-delay", "0.2", "--seed", "1",
                          "--transcribe-url", LOCAL, "--out", str(out), *args]) == code
    return json.loads(out.read_text())


def test_four_players_complete_a_round(server, manifest, tmp_path, capsys):
    url, bridge = server
    res = run(url, manifest, tmp_path, "--players", "4", "--host", "--correct-rate", "1")
    agg = res["aggregate"]
    assert (agg["joined"], agg["answers_sent"], agg["results"], agg["correct"], agg["errors"]) == (4, 4, 4, 4, 0)
    # Spoke within 0.2 s, before a real stream would be connected: all "early".
    assert agg["latency_ready"]["count"] == 0
    assert agg["latency_early"]["count"] == 4 and agg["latency_early"]["max"] < 2
    names = [p["name"] for p in res["players"]]
    assert len(set(names)) == 4 and all(n.startswith("Sim-0") for n in names)
    assert sorted(p["voice"] for p in res["players"]) == ["A", "B", "C", "D"]  # one voice each
    for p in res["players"]:
        a = p["answers"][0]
        assert (a["text"], a["correct"], a["clip_text"], a["partials"]) == ("Paris.", True, "Paris", 1)
        assert a["points"] > 0 and 0 <= a["delay_s"] <= 0.2 and a["audio_s"] == 0.3
        assert a["stream_likely_ready"] is False and not a["cut_off"]
    assert sorted(bridge.audio_bytes.values()) == [RIGHT] * 4
    for key, t in bridge.ended.items():  # paced in real time: 0.3 s clip
        assert 0.25 <= t - bridge.first_chunk[key] <= 0.5, key
    assert res["rounds"][0]["reason"] == "host"  # ended as soon as all had answered
    assert "ALL" in capsys.readouterr().out


def test_wrong_answers_and_question_without_clips(server, manifest, tmp_path):
    url, _ = server
    res = run(url, manifest, tmp_path, "--players", "2", "--host", "--rounds", "2", "--correct-rate", "0")
    assert [r["answered"] for r in res["rounds"]] == [2, 0]  # red-planet has no clips: silent
    for p in res["players"]:
        r1, r2 = p["answers"]
        assert (r1["clip_text"], r1["expected_correct"], r1["text"], r1["correct"]) == \
            ("London", False, "London.", False)
        assert r2["question_id"] == "red-planet" and r2["clip"] is None and r2["delay_s"] is None
        assert p["errors"] == 0


def test_error_final_is_counted_not_timed(server, manifest, tmp_path):
    url, bridge = server
    bridge.fail = {"Sim-01 (A)", "Sim-01 (B)", "Sim-01 (C)", "Sim-01 (D)"}  # whichever voice it got
    res = run(url, manifest, tmp_path, "--players", "2", "--host", "--correct-rate", "1", code=1)
    agg = res["aggregate"]
    assert (agg["results"], agg["error_finals"], agg["errors"], agg["correct"]) == (2, 1, 1, 1)
    assert agg["latency_early"]["count"] == 1  # only the good final
    failed = res["players"][0]["answers"][0]
    assert failed["error"] == "transcribe_unavailable" and failed["latency_s"] is not None


def test_round_timeout_cuts_off_a_long_answer(server, tmp_path):
    url, bridge = server
    long_clip = write_manifest(tmp_path, [("long", 5 * 32000, "A", True, "Paris")])  # 5 s > 3 s round
    res = run(url, long_clip, tmp_path, "--players", "1", "--host", "--time-limit", "3", "--max-delay", "0")
    assert res["rounds"][0]["reason"] == "timeout"
    a = res["players"][0]["answers"][0]
    # Paced in real time, so at most ~3 s of the 5 s went out (exact amount depends on load);
    # audio_s is an upper bound on what the server accepted.
    (accepted,) = bridge.audio_bytes.values()
    assert a["cut_off"] and 0 < accepted / 32000 <= a["audio_s"] <= 3.3
    assert a["text"] == "London." and a["latency_s"] is not None  # the server ended it; still a final
    assert res["aggregate"]["errors"] == 0  # no_round for the last chunk in flight is expected


def join_room(url, manifest, room, out, *args):
    """simulate.py --room in a thread; returns (thread, result holder)."""
    result = {}
    argv = ["--server", url, "--manifest", manifest, "--room", room, "--players", "2", "--correct-rate", "1",
            "--transcribe-url", LOCAL, "--out", str(out), *args]
    thread = threading.Thread(target=lambda: result.update(code=simulate.main(argv)))
    thread.start()
    return thread, result


def wait_for(host, kind, n):
    """Read host messages until n of type ``kind`` have arrived."""
    seen = 0
    while seen < n:
        seen += json.loads(host.recv(timeout=15))["type"] == kind


def test_join_existing_room_until_it_closes(server, manifest, tmp_path):
    url, _ = server
    out = tmp_path / "results.json"
    with connect(f"{url}/ws/host") as host:
        room = json.loads(host.recv())["room"]
        sim, result = join_room(url, manifest, room, out, "--min-delay", "3", "--max-delay", "3")
        wait_for(host, "player_joined", 2)
        host.send(json.dumps({"type": "start_question"}))
        wait_for(host, "final", 2)
    sim.join(10)  # host left: room_closed ends the run
    assert not sim.is_alive() and result["code"] == 0
    agg = json.loads(out.read_text())["aggregate"]
    assert (agg["results"], agg["correct"], agg["errors"]) == (2, 2, 0)
    assert agg["latency_ready"]["count"] == 2  # spoke 3 s in: stream likely ready


def test_room_rounds_leaves_after_k(server, manifest, tmp_path):
    url, _ = server
    out = tmp_path / "results.json"
    with connect(f"{url}/ws/host") as host:
        room = json.loads(host.recv())["room"]
        sim, result = join_room(url, manifest, room, out, "--rounds", "1", "--max-delay", "0")
        wait_for(host, "player_joined", 2)
        host.send(json.dumps({"type": "start_question"}))
        wait_for(host, "final", 2)
        host.send(json.dumps({"type": "end_round"}))
        wait_for(host, "player_left", 2)  # both leave after round 1, room still open
        sim.join(10)
    assert not sim.is_alive() and result["code"] == 0
    assert [len(p["answers"]) for p in json.loads(out.read_text())["players"]] == [1, 1]


def test_join_failure_exits_non_zero(server, manifest, tmp_path):
    url, _ = server
    res = run(url, manifest, tmp_path, "--players", "2", "--room", "ZZZZ", code=1)
    assert res["aggregate"]["joined"] == 0
    assert res["players"][0]["error_log"][0]["error"] == "join failed: room_not_found"


def test_cap_against_demo_server(capsys):
    assert simulate.cap_error(5, DEMO, False)
    assert simulate.cap_error(5, "", False)  # unknown: treated as the demo server
    assert simulate.cap_error(4, DEMO, False) is None
    assert simulate.cap_error(5, DEMO, True) is None
    assert simulate.cap_error(30, "wss://transcribe.example.com/v5", False) is None
    assert simulate.main(["--players", "5", "--host", "--transcribe-url", DEMO]) == 2
    assert "Refusing 5 players" in capsys.readouterr().err


def test_cap_uses_what_the_server_reports():
    demo = {"host": "demo.cobaltspeech.com", "stream_cap": 4}
    # This shell points elsewhere, but the server says it uses the demo server.
    assert simulate.cap_error(5, "wss://transcribe.example.com/v5", False, demo)
    assert simulate.cap_error(5, "", False, {"host": "demo.cobaltspeech.com", "stream_cap": None})
    assert simulate.cap_error(4, "", False, demo) is None
    assert simulate.cap_error(5, "", True, demo) is None
    assert simulate.cap_error(30, DEMO, False, {"host": None, "stream_cap": None}) is None  # BRIDGE=logging
    assert simulate.cap_error(30, DEMO, False, {"host": "transcribe.example.com", "stream_cap": None}) is None


def test_health_reports_transcribe():
    from server.bridge import LoggingBridge
    from server.transcribe_bridge import TranscribeBridge
    assert LoggingBridge().status() == {"host": None, "stream_cap": None}
    assert TranscribeBridge(url=DEMO).status()["host"] == "demo.cobaltspeech.com"
    assert TranscribeBridge(url="ws://127.0.0.1:9/x").status() == {"host": "127.0.0.1", "stream_cap": None}


def test_server_status_from_health(server):
    url, _ = server
    assert simulate.server_status(url) is None  # the fake bridge can't say
    assert simulate.server_status("ws://127.0.0.1:9") is None  # unreachable


def test_override_allows_more_players(server, manifest, tmp_path, capsys):
    url, _ = server
    out = tmp_path / "results.json"
    assert simulate.main(["--server", url, "--manifest", manifest, "--players", "5", "--host",
                          "--max-delay", "0.2", "--transcribe-url", DEMO, "--i-asked-ops", "--out", str(out)]) == 0
    assert "ALLOW_DEMO_LOAD" in capsys.readouterr().out
    assert json.loads(out.read_text())["aggregate"]["results"] == 5


@pytest.mark.parametrize("bad", [["--correct-rate", "1.5"], ["--time-limit", "1"], ["--time-limit", "500"],
                                 ["--rounds", "0"], ["--prefix", "x" * 17], ["--max-delay", "-1"]])
def test_bad_arguments(bad):
    with pytest.raises(SystemExit):
        simulate.parse_args(["--host", *bad])


def test_names_fit_and_percentiles(manifest):
    sim = simulate.Simulation(simulate.parse_args(["--host", "--prefix", "x" * 16, "--players", "12",
                                                   "--manifest", manifest]))
    names = [p.name for p in sim.players]
    assert len(set(names)) == 12 and all(len(n) <= 24 and n.startswith("x" * 16 + "-") for n in names)
    assert simulate.stats([0.4, 0.1, 0.3, 0.2]) == {"count": 4, "p50": 0.2, "p95": 0.4, "max": 0.4}
    assert simulate.stats([]) == {"count": 0, "p50": None, "p95": None, "max": None}
