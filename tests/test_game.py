"""Game server tests: host + players over the real WebSocket endpoints (in-process).

    .venv/bin/python -m pytest -q

Test bridges stand in for Transcribe, so no Cobalt service is called.
"""
import asyncio
import base64
import json
from contextlib import ExitStack, contextmanager

import pytest
from fastapi.testclient import TestClient

from server.app import create_app
from server.bridge import Bridge
from server.config import QUESTIONS_FILE
from server.game import CLOSE_TOO_SLOW, OUTBOX_LIMIT, Client
from server.questions import Question, load_questions
from server.scoring import normalize

QUESTIONS = [
    Question("france-capital", "What is the capital of France?", ["paris", "paris france"]),
    Question("red-planet", "Which planet is known as the Red Planet?", ["mars"]),
]


class FakeBridge(Bridge):
    """Answers "Paris." for every player as soon as the answer ends."""

    def __init__(self):
        self.audio_bytes = {}
        self.started = []

    async def question_started(self, room, rnd):
        self.started.append((rnd.number, sorted(p.name for p in room.players.values())))

    async def audio(self, room, rnd, player, chunk):
        self.audio_bytes[player.name] = self.audio_bytes.get(player.name, 0) + len(chunk)

    async def hold_end(self, room, rnd, player):
        room.on_partial(rnd, player, "par")
        room.on_final(rnd, player, "Paris.")


class ManualBridge(Bridge):
    """Records hold_end calls; the test delivers results itself."""

    def __init__(self):
        self.ended = []  # (rnd, player, cut_off)

    async def hold_end(self, room, rnd, player):
        self.ended.append((rnd, player, rnd.answers[player.id].cut_off))


class BrokenBridge(Bridge):
    """Every hook raises."""

    async def question_started(self, room, rnd): raise RuntimeError("boom")
    async def hold_start(self, room, rnd, player): raise RuntimeError("boom")
    async def audio(self, room, rnd, player, chunk): raise RuntimeError("boom")
    async def hold_end(self, room, rnd, player): raise RuntimeError("boom")
    async def round_ended(self, room, rnd): raise RuntimeError("boom")
    async def player_left(self, room, player): raise RuntimeError("boom")


def expect(ws, kind):
    """Read messages until one of type ``kind`` arrives (others are skipped)."""
    for _ in range(50):
        msg = ws.receive_json()
        if msg["type"] == kind:
            return msg
    raise AssertionError(f"no {kind!r} message")


def sync(ws):
    """Wait until the server has handled everything ws sent so far: an unknown
    message gets an error reply, and messages are handled in order."""
    ws.send_json({"type": "sync"})
    msg = ws.receive_json()
    assert msg == {"type": "error", "code": "unknown_type", "message": "unknown message type 'sync'"}, msg


@contextmanager
def make_client(bridge, round_seconds=30):
    with TestClient(create_app(QUESTIONS, bridge, round_seconds)) as c, ExitStack() as stack:
        c.stack = stack  # sockets opened by join()/host() close at the end of the test
        yield c


@pytest.fixture
def bridge():
    return FakeBridge()


@pytest.fixture
def client(bridge):
    with make_client(bridge) as c:
        yield c


def host(client):
    ws = client.stack.enter_context(client.websocket_connect("/ws/host"))
    return ws, expect(ws, "room_created")["room"]


def join(client, room, name):
    ws = client.stack.enter_context(client.websocket_connect("/ws/play"))
    ws.send_json({"type": "join", "room": room, "name": name})
    return ws, expect(ws, "joined")


def test_two_players_get_question_and_host_ends_round(client, bridge):
    h, room = host(client)
    alice, joined_a = join(client, room, "Alice")
    bob, joined_b = join(client, room, "Bob")
    assert joined_a["room"] == room and joined_a["player_id"] != joined_b["player_id"]
    assert expect(h, "player_joined")["name"] == "Alice"
    assert expect(h, "player_joined")["count"] == 2

    h.send_json({"type": "start_question"})
    for ws in (h, alice, bob):
        q = expect(ws, "question")
        assert q["text"] == "What is the capital of France?"
        assert q["round"] == 1 and q["time_limit"] == 30
        assert "answers" not in q  # players never see the answers
    assert bridge.started == [(1, ["Alice", "Bob"])]

    h.send_json({"type": "end_round"})
    for ws in (h, alice, bob):
        end = expect(ws, "round_end")
        assert end["reason"] == "host" and end["answer"] == "paris"
        assert [p["score"] for p in expect(ws, "leaderboard")["players"]] == [0, 0]

    h.send_json({"type": "start_question"})
    assert expect(alice, "question")["question_id"] == "red-planet"


def test_round_ends_on_timer():
    with make_client(FakeBridge(), round_seconds=0.2) as client:
        h, room = host(client)
        alice, _ = join(client, room, "Alice")
        h.send_json({"type": "start_question"})
        expect(alice, "question")
        assert expect(alice, "round_end")["reason"] == "timeout"
        assert expect(h, "round_end")["reason"] == "timeout"


def test_answer_audio_and_scoring_via_bridge(client, bridge):
    h, room = host(client)
    alice, joined = join(client, room, "Alice")
    h.send_json({"type": "start_question"})
    expect(alice, "question")

    alice.send_json({"type": "hold_start"})
    alice.send_bytes(b"\x00\x01" * 1600)  # binary frame: raw PCM
    alice.send_json({"type": "audio", "data": base64.b64encode(b"\x00\x01" * 800).decode()})
    alice.send_json({"type": "hold_end"})

    assert expect(alice, "partial")["text"] == "par"
    final = expect(alice, "final")
    assert final["correct"] is True and final["points"] == 100 and final["score"] == 100
    assert expect(h, "final")["player_id"] == joined["player_id"]
    assert expect(alice, "leaderboard")["players"][0]["score"] == 100
    assert bridge.audio_bytes == {"Alice": 4800}

    alice.send_json({"type": "hold_start"})
    assert expect(alice, "error")["code"] == "already_answered"

    h.send_json({"type": "end_round"})
    end = expect(h, "round_end")
    assert end["results"][0]["transcript"] == "Paris." and end["results"][0]["correct"]


def test_late_final_from_previous_round_is_dropped():
    bridge = ManualBridge()
    with make_client(bridge) as client:
        h, room = host(client)
        alice, _ = join(client, room, "Alice")
        game = client.app.state.lobby.rooms[room]

        h.send_json({"type": "start_question"})  # round 1: France
        expect(alice, "question")
        alice.send_json({"type": "hold_start"})
        alice.send_json({"type": "hold_end"})
        sync(alice)
        h.send_json({"type": "end_round"})
        expect(alice, "round_end")
        h.send_json({"type": "start_question"})  # round 2: Mars
        assert expect(alice, "question")["round"] == 2
        alice.send_json({"type": "hold_start"})
        sync(alice)

        # Round 1's final arrives late, after round 2 started and Alice pressed hold.
        rnd1, player, _ = bridge.ended[0]
        client.portal.call(game.on_final, rnd1, player, "Paris.")
        alice.send_json({"type": "hold_end"})
        sync(alice)
        rnd2 = bridge.ended[1][0]
        assert rnd2.number == 2
        client.portal.call(game.on_final, rnd2, player, "Mars.")

        final = expect(alice, "final")  # the first final Alice sees is round 2's
        assert (final["round"], final["text"], final["correct"], final["score"]) == (2, "Mars.", True, 100)


def test_empty_audio_is_rejected(client, bridge):
    h, room = host(client)
    alice, _ = join(client, room, "Alice")
    h.send_json({"type": "start_question"})
    expect(alice, "question")
    alice.send_json({"type": "hold_start"})
    alice.send_bytes(b"")
    assert expect(alice, "error")["code"] == "bad_message"
    alice.send_json({"type": "audio", "data": ""})
    assert expect(alice, "error")["code"] == "bad_message"
    alice.send_json({"type": "audio"})
    assert expect(alice, "error")["code"] == "bad_message"
    sync(alice)
    assert bridge.audio_bytes == {}


def test_round_end_cuts_off_holding_players_and_rejects_audio():
    bridge = ManualBridge()
    with make_client(bridge) as client:
        h, room = host(client)
        alice, _ = join(client, room, "Alice")
        h.send_json({"type": "start_question"})
        expect(alice, "question")
        alice.send_json({"type": "hold_start"})
        alice.send_bytes(b"\x00" * 3200)
        sync(alice)

        h.send_json({"type": "end_round"})
        assert expect(alice, "round_end")["answered"] == 1
        alice.send_bytes(b"\x00" * 3200)
        assert expect(alice, "error")["code"] == "no_round"
        alice.send_json({"type": "hold_end"})  # the release after the round ended: ignored
        sync(alice)
        assert [(r.number, cut) for r, _, cut in bridge.ended] == [(1, True)]


def test_round_end_counts_only_present_players(client):
    h, room = host(client)
    alice, _ = join(client, room, "Alice")
    with client.websocket_connect("/ws/play") as bob:
        bob.send_json({"type": "join", "room": room, "name": "Bob"})
        expect(bob, "joined")
        h.send_json({"type": "start_question"})
        expect(alice, "question")
        expect(bob, "question")
        for ws in (alice, bob):
            ws.send_json({"type": "hold_start"})
            sync(ws)
    expect(h, "player_left")
    h.send_json({"type": "end_round"})
    end = expect(h, "round_end")
    assert end["answered"] == 1 == len(end["results"]) and end["players"] == 1


def test_bridge_errors_do_not_stall_rounds():
    with make_client(BrokenBridge(), round_seconds=1) as client:
        h, room = host(client)
        alice, _ = join(client, room, "Alice")
        h.send_json({"type": "start_question"})
        expect(alice, "question")
        alice.send_json({"type": "hold_start"})
        alice.send_bytes(b"\x00" * 3200)
        sync(alice)  # no error reply for the failed hooks
        assert expect(alice, "round_end")["reason"] == "timeout"  # cut-off hold_end + round_ended raise
        expect(alice, "leaderboard")
        assert expect(h, "round_end")["reason"] == "timeout"

        h.send_json({"type": "start_question"})
        expect(alice, "question")
        h.send_json({"type": "end_round"})
        assert expect(h, "round_end")["reason"] == "host"
        expect(h, "leaderboard")
        sync(h)  # host connection, and so the room, still alive


def test_slow_client_is_disconnected():
    class StuckSocket:
        closed_with = None

        async def send_json(self, msg):
            await asyncio.Event().wait()  # never drains

        async def close(self, code):
            self.closed_with = code

    async def run():
        ws = StuckSocket()
        c = Client(ws)
        c.start()
        for _ in range(OUTBOX_LIMIT + 2):
            c.send({"type": "leaderboard"})
        await asyncio.sleep(0.05)
        return ws.closed_with, c.closed

    assert asyncio.run(run()) == (CLOSE_TOO_SLOW, True)


def test_start_question_validation(client):
    h, room = host(client)
    for bad in ({"index": True}, {"index": "1"}, {"time_limit": float("inf")},
                {"time_limit": float("nan")}, {"time_limit": 1}, {"time_limit": 500},
                {"time_limit": False}):
        h.send_text(json.dumps({"type": "start_question", **bad}))
        assert expect(h, "error")["code"] == "bad_message", bad
    h.send_json({"type": "start_question", "index": 1, "time_limit": 3})
    q = expect(h, "question")
    assert (q["question_id"], q["time_limit"]) == ("red-planet", 3)


def test_errors(client):
    h, room = host(client)
    with client.websocket_connect("/ws/play") as ws:
        ws.send_json({"type": "hold_start"})
        assert expect(ws, "error")["code"] == "not_joined"
        ws.send_json({"type": "join", "room": "ZZZZ", "name": "Alice"})
        assert expect(ws, "error")["code"] == "room_not_found"
        ws.send_text("not json")
        assert expect(ws, "error")["code"] == "bad_message"

    alice, _ = join(client, room, "Alice")
    with client.websocket_connect("/ws/play") as ws:
        ws.send_json({"type": "join", "room": room.lower(), "name": "alice"})
        assert expect(ws, "error")["code"] == "name_taken"

    alice.send_json({"type": "hold_start"})
    assert expect(alice, "error")["code"] == "no_round"
    h.send_json({"type": "end_round"})
    assert expect(h, "error")["code"] == "no_round"

    h.send_json({"type": "start_question"})
    expect(alice, "question")
    h.send_json({"type": "start_question"})
    assert expect(h, "error")["code"] == "round_in_progress"
    alice.send_bytes(b"\x00\x00")
    assert expect(alice, "error")["code"] == "not_holding"


def test_late_joiner_gets_open_question(client):
    h, room = host(client)
    h.send_json({"type": "start_question"})
    expect(h, "question")
    late, _ = join(client, room, "Late")
    q = expect(late, "question")
    assert 0 < q["remaining"] <= 30


def test_host_leaving_closes_room(client):
    h, room = host(client)
    alice, _ = join(client, room, "Alice")
    h.close()
    assert expect(alice, "room_closed")["room"] == room


def test_question_file_answers_are_normalised(tmp_path):
    path = tmp_path / "q.json"
    path.write_text('[{"id": "x", "text": "?", "answers": ["Paris, France!", "PARIS"]}]')
    assert load_questions(path)[0].answers == ["paris france", "paris"]

    questions = load_questions(QUESTIONS_FILE)
    assert len({q.id for q in questions}) == len(questions)
    assert all(a == normalize(a) and a for q in questions for a in q.answers)
