"""Host screen end to end (CQ-11): headless Chromium on /host, two players over
WebSockets, the real TranscribeBridge + MetricsRecorder against the fake
Transcribe server (in its own thread), so the page gets the real final,
leaderboard, stream_metrics and metrics_summary messages.

Skips without playwright/Chromium, like tests/test_phone_e2e.py.
HOST_SCREENSHOT=/path.png saves a 1920x1080 screenshot after the round.
"""
import asyncio
import json
import os
import threading
import time

import pytest

sync_api = pytest.importorskip("playwright.sync_api")
import websockets  # noqa: E402
from websockets.sync.client import connect as ws_connect  # noqa: E402

from server.app import create_app  # noqa: E402
from server.metrics import MetricsRecorder  # noqa: E402
from server.transcribe_bridge import TranscribeBridge  # noqa: E402
from tests.test_phone_e2e import QUESTIONS, ServerThread  # noqa: E402
from tests.test_transcribe_bridge import CHUNK, FakeTranscribe  # noqa: E402


class FakeTranscribeThread:
    """FakeTranscribe served from its own event loop in a thread."""

    def __init__(self, **kwargs):
        self.fake = FakeTranscribe(**kwargs)
        self.ready = threading.Event()
        self.url = None
        self.thread = threading.Thread(target=lambda: asyncio.run(self._serve()), daemon=True)

    async def _serve(self):
        self.stop = asyncio.Event()
        async with websockets.serve(self.fake.handler, "127.0.0.1", 0) as srv:
            self.url = f"ws://127.0.0.1:{srv.sockets[0].getsockname()[1]}/"
            self.ready.set()
            await self.stop.wait()

    def __enter__(self):
        self.thread.start()
        assert self.ready.wait(10)
        return self

    def __exit__(self, *exc):
        self.thread.join(0.1)  # daemon: dies with the test process


class Player:
    def __init__(self, server, room, name, simulated):
        self.ws = ws_connect(server.ws + "/ws/play")
        self.ws.send(json.dumps({"type": "join", "room": room, "name": name, "simulated": simulated}))
        self.expect("joined")

    def expect(self, kind, timeout=10):
        deadline = time.monotonic() + timeout
        while True:
            msg = json.loads(self.ws.recv(timeout=max(0.01, deadline - time.monotonic())))
            if msg["type"] == kind:
                return msg

    def answer(self, said=None):
        start = {"type": "hold_start"}
        if said:
            start.update(said=said, expect_correct=True)
        self.ws.send(json.dumps(start))
        for _ in range(4):
            self.ws.send(CHUNK)
            time.sleep(0.05)
        self.ws.send(json.dumps({"type": "hold_end"}))


@pytest.fixture(scope="module")
def browser():
    with sync_api.sync_playwright() as p:
        try:
            b = p.chromium.launch()
        except Exception as e:
            pytest.skip(f"Chromium not available: {str(e).splitlines()[0]}")
        yield b
        b.close()


def text(page, selector):
    return page.locator(selector).inner_text()


def test_round_run_from_the_host_screen(browser):
    with FakeTranscribeThread(close_after=0.1) as fake:
        bridge = TranscribeBridge(fake.url, "m", limit=None, quiet_gap=0.1, recorder=MetricsRecorder(None))
        with ServerThread(create_app(QUESTIONS, bridge, 30)) as server:
            page = browser.new_page(viewport={"width": 1920, "height": 1080})
            errors = []
            page.on("pageerror", lambda e: errors.append(str(e)))
            page.goto(server.http + "/host")
            page.wait_for_function("/^[A-Z]{4}$/.test(document.getElementById('room-code').textContent)")
            room = text(page, "#room-code")
            assert f"/play?room={room}" in text(page, "#join-url")
            assert page.locator("#join-warn").is_visible()  # plain http: mic warning
            page.wait_for_function("document.getElementById('qr').naturalWidth > 0")

            alice = Player(server, room, "Alice", simulated=False)
            sim = Player(server, room, "Sim-01", simulated=True)
            page.wait_for_function("document.getElementById('players').textContent === '2 players'")

            page.click("#btn-next")
            page.wait_for_function("document.getElementById('q-text').textContent.includes('capital of France')")
            assert page.locator("#btn-next").is_disabled() and page.locator("#btn-end").is_enabled()
            alice.expect("question")
            sim.expect("question")
            alice.answer()
            sim.answer(said="Paris")
            page.wait_for_function("document.querySelectorAll('#feed li').length === 2")
            page.wait_for_function("document.getElementById('q-answers').textContent === '2 / 2 answers in'")
            # Leaderboard: both scored, only the phone player tagged
            rows = page.locator("#board li")
            assert rows.count() == 2
            assert page.locator("#board .tag.real").count() == 1
            assert "Alice" in page.locator("#board li:has(.tag.real)").inner_text()
            # Live metrics from stream_metrics; feed latency filled in
            page.wait_for_function("document.getElementById('m-p50').textContent.endsWith('s')")
            page.wait_for_function("[...document.querySelectorAll('#feed .lat')].every(e => e.textContent.endsWith('s'))")
            assert text(page, "#m-errors") == "0/2"

            page.keyboard.press("e")  # end the round from the keyboard
            page.wait_for_function("!document.getElementById('q-reveal').hidden")
            assert "Answer: paris" in text(page, "#q-reveal")
            # The server's summary replaces the live numbers once the streams are done
            page.wait_for_function("document.getElementById('m-scope').textContent === 'round 1, final'", timeout=15000)
            assert not page.locator("#by-streams").is_hidden()
            cells = page.locator("#by-streams tbody tr").first.locator("td").all_inner_texts()
            assert cells[:3] == ["2", "1", "2"]
            assert text(page, "#m-wer") == "100%"  # simulated player said "Paris", fake heard "Paris."
            assert page.locator("#btn-next").is_enabled()
            if os.environ.get("HOST_SCREENSHOT"):
                page.screenshot(path=os.environ["HOST_SCREENSHOT"])
            assert not errors, errors
            alice.ws.close()
            sim.ws.close()
            page.close()
