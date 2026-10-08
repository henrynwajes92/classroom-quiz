"""TranscribeBridge tests against a local fake Transcribe server (no Cobalt calls).

The fake speaks the Transcribe WebSocket protocol: config, audio chunks, the
empty end-of-audio message, then results, and it drops the connection without
a close frame (1006) like the real server.
"""
import asyncio
import base64
import json
from contextlib import asynccontextmanager

import pytest
import websockets

from server import config
from server.app import default_bridge
from server.bridge import LoggingBridge
from server.game import Client, Room
from server.questions import Question
from server.transcribe_bridge import (DEMO_LIMIT, ERR_FAILED, ERR_TIMEOUT, ERR_UNAVAILABLE,
                                      StreamLimit, TranscribeBridge, default_limit)

QUESTIONS = [
    Question("france-capital", "What is the capital of France?", ["paris", "paris france"]),
    Question("red-planet", "Which planet is known as the Red Planet?", ["mars"]),
]
CHUNK = b"\x01\x00" * 1600  # 0.1 s of PCM


def result(text, partial):
    return json.dumps({"result": {"result": {"alternatives": [{"transcript_formatted": text}],
                                             "is_partial": partial}}})


class FakeTranscribe:
    """Records what each stream sent; replies with a partial per chunk, then after
    the end of audio runs ``script`` (default: one final per ``finals``): steps
    ("partial", text), ("final", text) or ("sleep", seconds). Then it drops the
    connection ``close_after`` seconds later (1006, like Cobalt), or closes it
    with ``close_code``, or never if ``hang``. With ``drop_after`` it runs the
    script and drops the connection after that many audio chunks instead."""

    def __init__(self, finals=("Paris.",), connect_delay=0.0, error=None, hang=False,
                 script=None, close_after=0.0, close_code=None, drop_after=None):
        self.script = script if script is not None else [("final", t) for t in finals]
        self.connect_delay = connect_delay
        self.error = error
        self.hang = hang  # never close after the end of audio
        self.close_after = close_after
        self.close_code = close_code
        self.drop_after = drop_after
        self.dropped = False
        self.configs, self.audio = [], []
        self.open = self.max_open = 0

    async def run_script(self, ws):
        for step, arg in self.script:
            if step == "sleep":
                await asyncio.sleep(arg)
            else:
                await ws.send(result(arg, step == "partial"))

    async def process_request(self, connection, request):
        await asyncio.sleep(self.connect_delay)

    async def handler(self, ws):
        self.open += 1
        self.max_open = max(self.max_open, self.open)
        try:
            self.configs.append(json.loads(await ws.recv()))
            audio, chunks = b"", 0
            async for message in ws:
                data = json.loads(message)["audio"]["data"]
                if not data:
                    break
                audio += base64.b64decode(data)
                chunks += 1
                await ws.send(result("par", True))
                if chunks == self.drop_after:  # mid-answer
                    await self.run_script(ws)
                    ws.transport.abort()
                    self.dropped = True
                    return
            else:
                return  # the bridge closed the stream before the end of audio
            self.audio.append(audio)
            if self.error:
                await ws.send(json.dumps({"error": {"message": self.error}}))
            await self.run_script(ws)
            if self.hang:
                await ws.wait_closed()  # until the bridge gives up
            await asyncio.sleep(self.close_after)  # Cobalt: ~2 s
            if self.close_code:
                await ws.close(self.close_code, "fake close")
            else:
                ws.transport.abort()  # like Cobalt: no close frame, the client sees 1006
        finally:
            self.open -= 1


@asynccontextmanager
async def fake_server(**kwargs):
    fake = FakeTranscribe(**kwargs)
    async with websockets.serve(fake.handler, "127.0.0.1", 0, process_request=fake.process_request) as srv:
        port = srv.sockets[0].getsockname()[1]
        yield fake, f"ws://127.0.0.1:{port}/"


def make_room(bridge, *names):
    room = Room("TEST", Client(None), QUESTIONS, bridge, 30)
    return room, [room.add_player(n, Client(None)) for n in names]


def messages(player, kind=None):
    """Everything sent to the player so far (drains the outbox into a log)."""
    log = player.client.__dict__.setdefault("log", [])
    while not player.client.outbox.empty():
        log.append(player.client.outbox.get_nowait())
    return [m for m in log if kind is None or m["type"] == kind]


async def until(cond, timeout=5.0):
    for _ in range(int(timeout / 0.01)):
        if cond():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not met in time")


async def answer(room, player, chunks=3):
    await room.hold_start(player)
    for _ in range(chunks):
        await room.audio(player, CHUNK)
    await room.hold_end(player)


def run(coro):
    asyncio.run(coro)


def test_audio_is_buffered_until_connected_and_final_is_scored():
    async def main():
        async with fake_server(connect_delay=0.3) as (fake, url):
            bridge = TranscribeBridge(url, "test-model", limit=None)
            room, [alice] = make_room(bridge, "Alice")
            await room.start_question()
            await answer(room, alice, chunks=3)  # all before the stream is connected
            await until(lambda: messages(alice, "final"))
            [final] = messages(alice, "final")
            assert final["text"] == "Paris." and final["correct"] is True and "error" not in final
            assert fake.audio == [CHUNK * 3]
            assert fake.configs[0]["config"]["model_id"] == "test-model"
            assert fake.configs[0]["config"]["audio_format_raw"]["sample_rate"] == 16000
            [m] = bridge.metrics
            assert m.outcome == "final" and m.report_trigger == "close" and m.close_code == 1006
            assert m.buffered_chunks == 3 and m.audio_bytes == len(CHUNK) * 3
            assert m.open_requested <= m.first_audio < m.connected <= m.config_sent <= m.first_audio_sent
            assert m.hold_end < m.end_sent <= m.last_final <= m.reported <= m.closed
            assert m.connect_s >= 0.3 and m.ready_at_hold_start is False
            assert m.backlog_at_release_s == 0.3  # all three chunks were still waiting
            parts = m.send_backlog_s + m.end_to_final_s + m.quiet_wait_s
            assert abs(m.answer_latency_s - parts) < 1e-6 and m.send_backlog_s >= 0.2
            await until(lambda: bridge.open_count() == 0)
    run(main())


def test_partials_and_multiple_finals_joined_once():
    async def main():
        async with fake_server(finals=("Um.", "", "Paris.")) as (fake, url):
            bridge = TranscribeBridge(url, "m", limit=None)
            room, [alice] = make_room(bridge, "Alice")
            await room.start_question()
            await until(lambda: bridge.metrics[0].config_sent)
            await answer(room, alice, chunks=2)
            await until(lambda: bridge.open_count() == 0)
            assert [m["text"] for m in messages(alice, "partial")] == ["par", "par"]
            finals = messages(alice, "final")
            assert len(finals) == 1 and finals[0]["text"] == "Um. Paris."
            assert bridge.metrics[0].finals == 3 and bridge.metrics[0].transcript == "Um. Paris."
    run(main())


def test_error_message_gives_empty_final_with_error():
    async def main():
        async with fake_server(error="model not found", finals=()) as (fake, url):
            bridge = TranscribeBridge(url, "m", limit=None)
            room, [alice] = make_room(bridge, "Alice")
            await room.start_question()
            await answer(room, alice)
            await until(lambda: messages(alice, "final"))
            [final] = messages(alice, "final")
            assert final["text"] == "" and final["error"] == ERR_FAILED and final["points"] == 0
            m = bridge.metrics[0]
            assert m.outcome == "error" and m.report_trigger == "error" and m.error_detail == "model not found"
            # The room carries on.
            await room.end_round("host")
            await room.start_question()
            assert room.round.number == 2
            await bridge.aclose()
    run(main())


def test_error_after_a_final_keeps_the_text_but_is_not_scored():
    async def main():
        async with fake_server(script=[("final", "Paris."), ("sleep", 0.05)], close_code=1011,
                               ) as (fake, url):
            bridge = TranscribeBridge(url, "m", limit=None, quiet_gap=5)
            room, [alice] = make_room(bridge, "Alice")
            await room.start_question()
            await answer(room, alice)
            await until(lambda: messages(alice, "final"))
            [final] = messages(alice, "final")
            assert final["text"] == "Paris." and final["error"] == ERR_FAILED
            assert final["correct"] is False and final["points"] == 0
            m = bridge.metrics[0]
            assert m.close_code == 1011 and m.close_reason == "fake close" and m.outcome == "error"
    run(main())


def test_close_1011_after_end_is_an_error():
    async def main():
        async with fake_server(finals=(), close_code=1011) as (fake, url):
            bridge = TranscribeBridge(url, "m", limit=None)
            room, [alice] = make_room(bridge, "Alice")
            await room.start_question()
            await answer(room, alice)
            await until(lambda: messages(alice, "final"))
            [final] = messages(alice, "final")
            assert final["text"] == "" and final["error"] == ERR_FAILED
            assert bridge.metrics[0].report_trigger == "error"
    run(main())


def test_close_1000_after_end_is_normal():
    async def main():
        async with fake_server(close_code=1000) as (fake, url):
            bridge = TranscribeBridge(url, "m", limit=None)
            room, [alice] = make_room(bridge, "Alice")
            await room.start_question()
            await answer(room, alice)
            await until(lambda: messages(alice, "final"))
            [final] = messages(alice, "final")
            assert final["text"] == "Paris." and "error" not in final
            assert bridge.metrics[0].close_code == 1000 and bridge.metrics[0].outcome == "final"
    run(main())


def test_drop_before_end_of_audio_is_an_error_even_with_a_final():
    async def main():
        async with fake_server(drop_after=1, script=[("final", "Um.")]) as (fake, url):
            bridge = TranscribeBridge(url, "m", limit=None)
            room, [alice] = make_room(bridge, "Alice")
            await room.start_question()
            await until(lambda: bridge.metrics[0].config_sent)
            await room.hold_start(alice)
            await room.audio(alice, CHUNK)
            await until(lambda: fake.dropped and bridge.open_count() == 0)
            await room.audio(alice, CHUNK)  # dropped: the stream is gone
            await room.hold_end(alice)
            [final] = messages(alice, "final")
            assert final["text"] == "Um." and final["error"] == ERR_FAILED and final["points"] == 0
            m = bridge.metrics[0]
            assert m.end_sent is None and m.close_code == 1006 and m.outcome == "error"
            assert m.ready_at_hold_start is True
    run(main())


def test_failure_sending_audio_ends_the_stream_at_once():
    async def main():
        async with fake_server(hang=True) as (fake, url):
            bridge = TranscribeBridge(url, "m", limit=None, final_timeout=5)
            room, [alice] = make_room(bridge, "Alice")
            await room.start_question()
            await room.hold_start(alice)
            await room.audio(alice, "not bytes")  # base64 fails in the writer task
            await room.hold_end(alice)
            await until(lambda: messages(alice, "final"), timeout=2)
            assert messages(alice, "final")[0]["error"] == ERR_FAILED
            assert "sending audio failed" in bridge.metrics[0].error_detail
            await until(lambda: bridge.open_count() == 0)
    run(main())


def test_timeout_without_finals_reports_error():
    async def main():
        async with fake_server(finals=(), hang=True) as (fake, url):
            bridge = TranscribeBridge(url, "m", limit=None, final_timeout=0.3)
            room, [alice] = make_room(bridge, "Alice")
            await room.start_question()
            await answer(room, alice)
            await until(lambda: messages(alice, "final"))
            assert messages(alice, "final")[0]["error"] == ERR_TIMEOUT
            assert bridge.metrics[0].outcome == "timeout" and bridge.metrics[0].report_trigger == "timeout"
            assert 0.3 <= bridge.metrics[0].answer_latency_s < 2
            await until(lambda: bridge.open_count() == 0 and fake.open == 0)
    run(main())


def test_timeout_uses_finals_received_so_far():
    async def main():
        async with fake_server(finals=("Paris.",), hang=True) as (fake, url):
            bridge = TranscribeBridge(url, "m", limit=None, final_timeout=0.3, quiet_gap=5)
            room, [alice] = make_room(bridge, "Alice")
            await room.start_question()
            await answer(room, alice)
            await until(lambda: messages(alice, "final"))
            [final] = messages(alice, "final")
            assert final["text"] == "Paris." and final["correct"] and "error" not in final
            assert bridge.metrics[0].outcome == "timeout" and bridge.metrics[0].report_trigger == "timeout"
    run(main())


async def quiet_gap_round(script, quiet_gap, close_after=1.0):
    """One answer against ``script``; returns (player's finals, metrics) once the stream closed."""
    async with fake_server(script=script, close_after=close_after) as (fake, url):
        bridge = TranscribeBridge(url, "m", limit=None, quiet_gap=quiet_gap)
        room, [alice] = make_room(bridge, "Alice")
        await room.start_question()
        await answer(room, alice)
        await until(lambda: bridge.open_count() == 0 and fake.open == 0)
        return messages(alice, "final"), bridge.metrics[0]


def test_quiet_gap_reports_before_the_stream_closes():
    async def main():
        [final], m = await quiet_gap_round([("final", "Paris.")], quiet_gap=0.2, close_after=1.0)
        assert final["text"] == "Paris." and final["correct"]
        assert m.report_trigger == "quiet_gap" and m.outcome == "final" and m.late_finals == 0
        assert 0.2 <= m.reported - m.last_final < 0.6
        assert m.close_delay_s > 0.5  # the server closed well after the answer was scored
    run(main())


def test_partial_resets_the_quiet_gap():
    async def main():
        script = [("final", "Um."), ("sleep", 0.15), ("partial", "Par"), ("sleep", 0.15),
                  ("partial", "Paris"), ("sleep", 0.15), ("final", "Paris.")]
        [final], m = await quiet_gap_round(script, quiet_gap=0.25)
        assert final["text"] == "Um. Paris." and m.report_trigger == "quiet_gap"
        assert m.late_finals == 0 and m.reported > m.last_final
    run(main())


def test_two_finals_within_the_gap_are_joined():
    async def main():
        [final], m = await quiet_gap_round([("final", "Um."), ("sleep", 0.1), ("final", "Paris.")],
                                           quiet_gap=0.3)
        assert final["text"] == "Um. Paris." and m.report_trigger == "quiet_gap" and m.finals == 2
    run(main())


def test_final_after_the_gap_is_late_and_not_reported_again():
    async def main():
        finals, m = await quiet_gap_round([("final", "Um."), ("sleep", 0.5), ("final", "Paris.")],
                                          quiet_gap=0.2)
        assert [f["text"] for f in finals] == ["Um."]  # on_final exactly once
        assert m.report_trigger == "quiet_gap" and m.transcript == "Um."
        assert m.late_finals == 1 and m.late_final_texts == ["Paris."]
    run(main())


def test_close_before_the_gap_still_reports():
    async def main():
        [final], m = await quiet_gap_round([("final", "Paris.")], quiet_gap=5, close_after=0.0)
        assert final["text"] == "Paris." and m.report_trigger == "close"
        assert m.reported - m.last_final < 1
    run(main())


def test_connect_failure_gives_unavailable():
    async def main():
        bridge = TranscribeBridge("ws://127.0.0.1:9/", "m", limit=None)  # nothing listens there
        room, [alice] = make_room(bridge, "Alice")
        await room.start_question()
        await until(lambda: bridge.open_count() == 0)
        await answer(room, alice)  # hold_start retries the failed stream; it fails again
        await until(lambda: messages(alice, "final"))
        assert messages(alice, "final")[0]["error"] == ERR_UNAVAILABLE
        assert [m.outcome for m in bridge.metrics] == ["error", "error"]
    run(main())


def test_round_end_closes_unused_streams_and_answered_ones_finish():
    async def main():
        async with fake_server() as (fake, url):
            bridge = TranscribeBridge(url, "m", limit=None)
            room, [alice, bob, cara] = make_room(bridge, "Alice", "Bob", "Cara")
            await room.start_question()
            await until(lambda: fake.open == 3)
            await answer(room, alice)
            await room.hold_start(bob)
            await room.audio(bob, CHUNK)  # still holding at round end: cut off
            await room.end_round("host")
            await until(lambda: bridge.open_count() == 0 and fake.open == 0)
            assert messages(alice, "final")[0]["text"] == "Paris."
            assert messages(bob, "final")[0]["text"] == "Paris."  # cut-off answers still count
            assert not messages(cara, "final")
            assert {m.player: m.outcome for m in bridge.metrics} == {
                "Alice": "final", "Bob": "final", "Cara": "unused"}
            assert all(m.closed for m in bridge.metrics) and not bridge.streams
    run(main())


def test_late_joiner_and_player_left():
    async def main():
        async with fake_server() as (fake, url):
            bridge = TranscribeBridge(url, "m", limit=None)
            room, [alice] = make_room(bridge, "Alice")
            await room.start_question()
            dave = room.add_player("Dave", Client(None))  # joins mid-round: no stream yet
            await answer(room, dave)
            await until(lambda: messages(dave, "final"))
            assert messages(dave, "final")[0]["text"] == "Paris."
            await room.remove_player(alice)
            await until(lambda: bridge.open_count() == 0 and fake.open == 0)
            assert {m.player: m.outcome for m in bridge.metrics} == {"Alice": "cancelled", "Dave": "final"}
    run(main())


def test_next_question_closes_leftover_streams():
    async def main():
        async with fake_server(finals=(), hang=True) as (fake, url):
            bridge = TranscribeBridge(url, "m", limit=None)
            room, [alice] = make_room(bridge, "Alice")
            await room.start_question()
            await answer(room, alice)  # no result: stream waits for the server to close
            await room.end_round("host")
            await room.start_question()
            await until(lambda: len(bridge.metrics) == 2 and bridge.metrics[0].closed)
            assert bridge.metrics[0].outcome == "cancelled" and bridge.open_count() == 1
            await bridge.aclose()
            assert bridge.open_count() == 0
    run(main())


def test_stream_cap_refuses_extra_streams():
    async def main():
        async with fake_server() as (fake, url):
            limit = StreamLimit(2)
            bridge = TranscribeBridge(url, "m", limit=limit, slot_wait=0.3)
            room, players = make_room(bridge, "A", "B", "C")
            await room.start_question()
            await until(lambda: fake.open == 2 and bridge.metrics[2].outcome != "open")
            assert [m.outcome for m in bridge.metrics] == ["open", "open", "refused"]
            await answer(room, players[2])  # retried on hold_start: waits, still no slot
            await until(lambda: messages(players[2], "final"))
            assert messages(players[2], "final")[0]["error"] == ERR_UNAVAILABLE
            await answer(room, players[0])  # frees a slot when done
            await until(lambda: limit.open == 1)
            await room.end_round("host")
            await until(lambda: limit.open == 0 and bridge.open_count() == 0)
            assert fake.max_open == 2
    run(main())


@pytest.mark.parametrize("answered", [True, False])
def test_next_question_gets_slots_while_last_rounds_streams_close(answered):
    """Cap full with round 1's streams (answered ones linger ~2 s until the
    server closes them); round 2 starting right away must still get streams."""
    async def main():
        async with fake_server(close_after=2.0) as (fake, url):
            limit = StreamLimit(2)
            bridge = TranscribeBridge(url, "m", limit=limit, quiet_gap=0.1)
            room, players = make_room(bridge, "A", "B")
            await room.start_question()
            await until(lambda: fake.open == 2)
            if answered:
                for p in players:
                    await answer(room, p)
                await until(lambda: all(messages(p, "final") for p in players))
                assert limit.open == 2  # still waiting for the server to close
            await room.end_round("host")
            await room.start_question()
            round2 = bridge.metrics[2:]
            await until(lambda: all(m.config_sent for m in round2))
            assert [m.outcome for m in round2] == ["open", "open"] and limit.open == 2
            assert [m.outcome for m in bridge.metrics[:2]] == (["final"] * 2 if answered else ["unused"] * 2)
            await bridge.aclose()
            assert limit.open == 0
    run(main())


def test_slot_wait_gets_a_slot_freed_later():
    async def main():
        async with fake_server() as (fake, url):
            limit = StreamLimit(1)
            bridge = TranscribeBridge(url, "m", limit=limit, slot_wait=5)
            room, [a, b] = make_room(bridge, "A", "B")
            await room.start_question()
            await until(lambda: bridge.metrics[0].config_sent)
            await answer(room, a)  # A's stream closes, B's takes the slot
            await until(lambda: bridge.metrics[1].config_sent)
            assert bridge.metrics[1].outcome == "open" and fake.max_open == 1
            await bridge.aclose()
    run(main())


def test_aclose_before_streams_start_marks_them_cancelled():
    async def main():
        bridge = TranscribeBridge("ws://127.0.0.1:9/", "m", limit=None)
        room, _ = make_room(bridge, "A")
        await room.start_question()
        bridge.streams.clear()  # a task the bridge no longer tracks by player
        await bridge.aclose()
        assert bridge.metrics[0].outcome == "cancelled" and bridge.metrics[0].closed
    run(main())


def test_demo_server_gets_the_cap_unless_allowed(monkeypatch):
    demo = "wss://demo.cobaltspeech.com/transcribe/api/transcribe/v5/streaming-recognize"
    assert default_limit(demo) is DEMO_LIMIT and DEMO_LIMIT.max == 4
    assert default_limit("wss://DEMO.cobaltspeech.com./x") is DEMO_LIMIT
    assert default_limit("wss://transcribe.example.com/x") is None
    monkeypatch.setattr(config, "ALLOW_DEMO_LOAD", True)
    assert default_limit(demo) is None


def test_bridge_env_var(monkeypatch):
    monkeypatch.setattr(config, "BRIDGE", "logging")
    assert isinstance(default_bridge(), LoggingBridge)
    monkeypatch.setattr(config, "BRIDGE", "transcribe")
    assert isinstance(default_bridge(), TranscribeBridge)
