"""Hooks between the game and speech recognition.

The game calls a Bridge at fixed points in a round, always passing the round
(``rnd``) the call belongs to, so each Transcribe stream can be tied to one
player in one round. A bridge reports recognition results back with
``room.on_partial(rnd, player, text)`` and
``room.on_final(rnd, player, text, error=None)``, passing the same ``rnd`` it
got in ``hold_start``; ``error`` is a code when recognition failed. The room
forwards results to the player, scores finals and updates the leaderboard;
results for a round that is no longer current are dropped.

All hooks run on the server's event loop and are awaited by the caller, so
they must not block. ``question_started`` and ``round_ended`` are awaited while
the room is locked: start slow work (opening N Transcribe streams) with
``asyncio.create_task`` instead of awaiting it there. Exceptions from hooks are
logged by the room and otherwise ignored.

The real bridge is ``TranscribeBridge`` in ``transcribe_bridge.py`` (the
default). ``LoggingBridge`` (``BRIDGE=logging``) only counts audio bytes and
logs, and never produces results.
"""
import logging

from .config import BYTES_PER_SEC

log = logging.getLogger("quiz.bridge")


class Bridge:
    async def question_started(self, room, rnd):
        """A question was broadcast. TranscribeBridge opens one stream per player
        in ``room.players`` here (in the background), so connect time is off
        the answer latency."""

    async def hold_start(self, room, rnd, player):
        """Player pressed "hold to answer" in round ``rnd``. TranscribeBridge
        opens a stream here for players who have none (joined mid-round)."""

    async def audio(self, room, rnd, player, chunk: bytes):
        """One chunk (never empty) of 16 kHz mono 16-bit PCM. TranscribeBridge
        forwards it to the player's stream, buffering until it is connected."""

    async def hold_end(self, room, rnd, player):
        """The answer is complete: the player released, or the round ended while
        they were still holding (``rnd.answers[player.id].cut_off``). Called
        exactly once per hold_start. TranscribeBridge sends the end-of-audio
        message and reports the final once the answer is recognised."""

    async def round_ended(self, room, rnd):
        """Time is up or the host ended the round. Runs after hold_end for any
        answers that were cut off. TranscribeBridge closes streams nobody used.
        Streams that got hold_end may still deliver a final; the room scores
        it until the next question starts."""

    async def player_left(self, room, player):
        """Player disconnected. TranscribeBridge closes their stream if open."""

    async def aclose(self):
        """Server shutdown: release anything still open."""

    def status(self) -> dict | None:
        """For /health: {"host": Transcribe host or None, "stream_cap": max
        concurrent streams or None}. None means unknown."""
        return None


class LoggingBridge(Bridge):
    """Placeholder: no recognition, just log what would be forwarded."""

    def status(self):
        return {"host": None, "stream_cap": None}  # no Transcribe at all

    async def question_started(self, room, rnd):
        log.info("room %s round %d: would open %d streams", room.code, rnd.number, len(room.players))

    async def hold_end(self, room, rnd, player):
        answer = rnd.answers[player.id]
        log.info("room %s round %d %s: hold_end%s, %d bytes (%.2f s of audio)", room.code,
                 rnd.number, player.name, " (cut off)" if answer.cut_off else "",
                 answer.audio_bytes, answer.audio_bytes / BYTES_PER_SEC)

    async def round_ended(self, room, rnd):
        log.info("room %s round %d ended: %d answers", room.code, rnd.number, len(rnd.answers))
