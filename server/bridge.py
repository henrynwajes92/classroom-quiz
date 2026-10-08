"""Hooks between the game and speech recognition. CQ-4 plugs Transcribe in here.

The game calls a Bridge at fixed points in a round, always passing the round
(``rnd``) the call belongs to, so each Transcribe stream can be tied to one
player in one round. A bridge reports recognition results back with
``room.on_partial(rnd, player, text)`` and ``room.on_final(rnd, player, text)``,
passing the same ``rnd`` it got in ``hold_start``. The room forwards results
to the player, scores finals and updates the leaderboard; results for a round
that is no longer current are dropped.

All hooks run on the server's event loop and are awaited by the caller, so
they must not block. ``question_started`` and ``round_ended`` are awaited while
the room is locked: start slow work (opening N Transcribe streams) with
``asyncio.create_task`` instead of awaiting it there. Exceptions from hooks are
logged by the room and otherwise ignored.

``LoggingBridge`` is the placeholder used until CQ-4: it counts audio bytes
and logs, and never produces results.
"""
import logging

from .config import BYTES_PER_SEC

log = logging.getLogger("quiz.bridge")


class Bridge:
    async def question_started(self, room, rnd):
        """A question was broadcast. CQ-4: open one Transcribe stream per player
        in ``room.players`` and send the config, so connect time is off the
        answer latency. Players who join mid-round have no stream yet; open
        one for them on hold_start."""

    async def hold_start(self, room, rnd, player):
        """Player pressed "hold to answer" in round ``rnd``."""

    async def audio(self, room, rnd, player, chunk: bytes):
        """One chunk (never empty) of 16 kHz mono 16-bit PCM. CQ-4: forward to
        the player's stream as {"audio": {"data": base64}}."""

    async def hold_end(self, room, rnd, player):
        """The answer is complete: the player released, or the round ended while
        they were still holding (``rnd.answers[player.id].cut_off``). Called
        exactly once per hold_start. CQ-4: send the empty audio message
        {"audio": {"data": ""}} that ends the stream."""

    async def round_ended(self, room, rnd):
        """Time is up or the host ended the round. Runs after hold_end for any
        answers that were cut off. CQ-4: close streams nobody used. Streams
        that got hold_end may still deliver a final; the room scores it until
        the next question starts."""

    async def player_left(self, room, player):
        """Player disconnected. CQ-4: close their stream if open."""


class LoggingBridge(Bridge):
    """Placeholder: no recognition, just log what would be forwarded."""

    async def question_started(self, room, rnd):
        log.info("room %s round %d: would open %d streams", room.code, rnd.number, len(room.players))

    async def hold_end(self, room, rnd, player):
        answer = rnd.answers[player.id]
        log.info("room %s round %d %s: hold_end%s, %d bytes (%.2f s of audio)", room.code,
                 rnd.number, player.name, " (cut off)" if answer.cut_off else "",
                 answer.audio_bytes, answer.audio_bytes / BYTES_PER_SEC)

    async def round_ended(self, room, rnd):
        log.info("room %s round %d ended: %d answers", room.code, rnd.number, len(rnd.answers))
