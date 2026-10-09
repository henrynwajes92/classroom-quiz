"""FastAPI app: WebSocket endpoints for the host and players.

    uvicorn server.app:app --host 0.0.0.0 --port 8000

/ws/host  one per room; connecting creates the room
/ws/play  players (phones and simulator); first message must be "join"
/, /play  the phone page (web/play.html); /play?room=KXQB pre-fills the room
/static/  the phone page's JS/CSS (web/)

The message contract is in docs/protocol.md.
"""
import base64
import binascii
import json
import logging
import math
import re
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from . import config
from .bridge import Bridge, LoggingBridge
from .transcribe_bridge import TranscribeBridge
from .game import Client, GameError, Lobby, Player, Room
from .questions import Question, load_questions

log = logging.getLogger("quiz.app")

WEB_DIR = config.ROOT / "web"
# No caching: phones should pick up page fixes on reload during the hackathon.
NO_CACHE = {"Cache-Control": "no-cache"}


def phone_page(room: str | None) -> str:
    """web/play.html with the room field pre-filled. Only a 4-letter code is
    used (anything else is dropped), so nothing from the URL reaches the HTML."""
    room = (room or "").strip().upper()
    if not re.fullmatch(r"[A-Z]{4}", room):
        room = ""
    return (WEB_DIR / "play.html").read_text(encoding="utf-8").replace("__ROOM__", room)


class NoCacheStatic(StaticFiles):
    async def get_response(self, path, scope):
        response = await super().get_response(path, scope)
        response.headers.update(NO_CACHE)
        return response


async def serve(ws: WebSocket, client: Client, on_message, on_binary=None):
    """Run one connection: a writer task for outgoing messages, and a read loop
    that hands each incoming message to on_message(dict) / on_binary(bytes).
    GameErrors become error messages; the connection stays open. Any other
    exception is logged and reported as internal_error, so one bad message (or
    a bug) can't close a room for everyone."""
    client.start()
    try:
        while True:
            event = await ws.receive()
            if event["type"] == "websocket.disconnect":
                return
            try:
                if event.get("bytes") is not None:
                    if on_binary is None:
                        raise GameError("bad_message", "binary messages are not accepted here")
                    await on_binary(event["bytes"])
                else:
                    try:
                        msg = json.loads(event.get("text") or "")
                    except ValueError:
                        raise GameError("bad_message", "messages must be JSON")
                    if not isinstance(msg, dict) or not isinstance(msg.get("type"), str):
                        raise GameError("bad_message", 'messages need a "type"')
                    await on_message(msg)
            except GameError as e:
                client.send({"type": "error", "code": e.code, "message": e.message})
            except Exception:
                log.exception("error handling a message")
                client.send({"type": "error", "code": "internal_error", "message": "server error"})
    finally:
        client.stop()


def valid_time_limit(value) -> bool:
    return (isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)
            and config.MIN_TIME_LIMIT <= value <= config.MAX_TIME_LIMIT)


def default_bridge() -> Bridge:
    """The bridge picked by the BRIDGE env var."""
    if config.BRIDGE == "logging":
        return LoggingBridge()
    if config.BRIDGE == "transcribe":
        return TranscribeBridge()
    raise ValueError(f"BRIDGE must be 'transcribe' or 'logging', not {config.BRIDGE!r}")


def create_app(questions: list[Question] | None = None, bridge: Bridge | None = None,
               round_seconds: float | None = None) -> FastAPI:
    bridge = bridge or default_bridge()

    @asynccontextmanager
    async def lifespan(app):
        yield
        await bridge.aclose()  # no Transcribe streams left open on shutdown

    app = FastAPI(title="Classroom quiz", lifespan=lifespan)
    lobby = Lobby(questions or load_questions(config.QUESTIONS_FILE), bridge,
                  round_seconds or config.ROUND_SECONDS)
    app.state.lobby = lobby

    @app.get("/health")
    async def health():
        return {"ok": True, "rooms": len(lobby.rooms),
                "players": sum(len(r.players) for r in lobby.rooms.values()),
                "transcribe": bridge.status()}  # the simulator's demo-server check reads this

    @app.get("/", response_class=HTMLResponse)
    @app.get("/play", response_class=HTMLResponse)
    async def play_page(room: str | None = None):
        return HTMLResponse(phone_page(room), headers=NO_CACHE)

    app.mount("/static", NoCacheStatic(directory=WEB_DIR), name="static")

    @app.websocket("/ws/host")
    async def host_ws(ws: WebSocket):
        await ws.accept()
        client = Client(ws)
        room = lobby.create_room(client)
        client.send({"type": "room_created", "room": room.code,
                     "questions": len(room.questions), "round_seconds": room.round_seconds})

        async def on_message(msg):
            kind = msg["type"]
            if kind == "start_question":
                index, time_limit = msg.get("index"), msg.get("time_limit")
                if index is not None and (isinstance(index, bool) or not isinstance(index, int)):
                    raise GameError("bad_message", "index must be an integer")
                if time_limit is not None and not valid_time_limit(time_limit):
                    raise GameError("bad_message", f"time_limit must be {config.MIN_TIME_LIMIT}-"
                                                   f"{config.MAX_TIME_LIMIT} seconds")
                await room.start_question(index, time_limit)
            elif kind == "end_round":
                await room.end_round("host")
            else:
                raise GameError("unknown_type", f"unknown message type {kind!r}")

        try:
            await serve(ws, client, on_message)
        finally:
            await lobby.close_room(room)

    @app.websocket("/ws/play")
    async def play_ws(ws: WebSocket):
        await ws.accept()
        client = Client(ws)
        room: Room | None = None
        player: Player | None = None

        async def on_message(msg):
            nonlocal room, player
            kind = msg["type"]
            if kind == "join":
                if player:
                    raise GameError("already_joined", "already in a room")
                r = lobby.get(msg.get("room", ""))
                player = r.add_player(msg.get("name", ""), client)
                room = r
            elif player is None:
                raise GameError("not_joined", 'send "join" first')
            elif kind == "hold_start":
                await room.hold_start(player)
            elif kind == "audio":
                try:
                    chunk = base64.b64decode(msg.get("data", ""), validate=True)
                except (binascii.Error, TypeError):
                    raise GameError("bad_message", "audio data must be base64")
                await room.audio(player, chunk)
            elif kind == "hold_end":
                await room.hold_end(player)
            else:
                raise GameError("unknown_type", f"unknown message type {kind!r}")

        async def on_binary(data: bytes):
            if player is None:
                raise GameError("not_joined", 'send "join" first')
            await room.audio(player, data)

        try:
            await serve(ws, client, on_message, on_binary)
        finally:
            if room and player:
                await room.remove_player(player)

    return app


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
app = create_app()
