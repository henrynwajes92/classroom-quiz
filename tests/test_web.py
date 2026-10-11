"""Phone page routes (CQ-6): the page, room pre-fill and its static files.

The page itself is tested in a real browser by tests/test_phone_e2e.py.
"""
import pytest
from fastapi.testclient import TestClient

from server.app import create_app
from server.bridge import Bridge
from server.questions import Question

QUESTIONS = [Question("france-capital", "What is the capital of France?", ["paris"])]


@pytest.fixture(scope="module")
def client():
    with TestClient(create_app(QUESTIONS, Bridge(), 30)) as c:
        yield c


@pytest.mark.parametrize("path", ["/", "/play"])
def test_page_served(client, path):
    r = client.get(path)
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/html")
    assert r.headers["cache-control"] == "no-cache"
    html = r.text
    assert 'id="hold"' in html and 'id="join-form"' in html
    assert "/static/play.js" in html and "/static/capture-worklet.js" in html
    assert 'id="room" name="room" value=""' in html  # no room given: empty, placeholder gone
    assert "__ROOM__" not in html


@pytest.mark.parametrize("room, shown", [("KXQB", "KXQB"), ("kxqb", "KXQB"), (" kxqb ", "KXQB"),
                                         ("KXQ", ""), ("KXQBB", ""), ("K1QB", ""),
                                         ('"><script>alert(1)</script>', "")])
def test_room_prefill(client, room, shown):
    html = client.get("/play", params={"room": room}).text
    assert f'id="room" name="room" value="{shown}"' in html
    assert "<script>alert" not in html


@pytest.mark.parametrize("path, kind", [("/static/play.js", "javascript"),
                                        ("/static/capture-worklet.js", "javascript"),
                                        ("/static/play.css", "text/css")])
def test_static_files(client, path, kind):
    r = client.get(path)
    assert r.status_code == 200
    assert kind in r.headers["content-type"]
    assert r.headers["cache-control"] == "no-cache"


def test_static_no_escape(client):
    assert client.get("/static/../server/config.py").status_code == 404
    assert client.get("/static/nope.js").status_code == 404


def test_health_still_works(client):
    assert client.get("/health").json()["ok"] is True


def test_host_page_and_join_link(client, tmp_path, monkeypatch):
    from server import config
    r = client.get("/host")
    assert r.status_code == 200 and 'id="room-code"' in r.text and "/static/host.js" in r.text
    monkeypatch.setattr(config, "TUNNEL_URL_FILE", tmp_path / "none")
    info = client.get("/join-info", params={"room": "kxqb"}).json()
    assert info == {"url": "http://testserver/play?room=KXQB", "https": False, "tunnel": False}
    assert client.get("/join-info", params={"room": "<b>"}).json()["url"] == "http://testserver/play"
    (tmp_path / "tunnel").write_text("https://abc-def.trycloudflare.com\n")
    monkeypatch.setattr(config, "TUNNEL_URL_FILE", tmp_path / "tunnel")
    info = client.get("/join-info", params={"room": "KXQB"}).json()
    assert info == {"url": "https://abc-def.trycloudflare.com/play?room=KXQB", "https": True, "tunnel": True}
    (tmp_path / "tunnel").write_text("javascript:alert(1)")
    assert client.get("/join-info").json()["tunnel"] is False  # anything odd in the file is ignored
    qr = client.get("/join-qr.svg", params={"room": "KXQB"})
    assert qr.headers["content-type"] == "image/svg+xml" and qr.text.lstrip().startswith(("<?xml", "<svg"))
