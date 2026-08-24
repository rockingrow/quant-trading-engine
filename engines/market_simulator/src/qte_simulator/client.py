"""Talking to a running simulator: connect, send one command, read the answer.

Every CLI subcommand except ``serve`` is this class plus argument parsing. It
is deliberately a short-lived connection rather than a session — a command that
holds the socket open for the duration of a paced replay is still one command,
and when it returns there is nothing left to keep.
"""

from __future__ import annotations

import uuid
from types import TracebackType
from typing import Any

import websockets
from qte_shared.providers.simulator.protocol import dumps, loads

from qte_simulator.settings import simulator_settings

#: An acknowledgement carries one expected candle per replayed bar, so a
#: 5000-bar replay answers with something well past the 1 MB default.
MAX_ACK_BYTES = 32 * 1024 * 1024


class ControlError(RuntimeError):
    """The simulator refused the command, and this is what it said."""


class SimulatorUnreachable(ConnectionError):
    """Nothing is listening on the control URL."""


class ControlClient:
    """One connection to ``/control``, used as an async context manager."""

    def __init__(self, url: str | None = None, *, open_timeout: float = 5.0) -> None:
        self.url = url or simulator_settings.control_url
        self._open_timeout = open_timeout
        self._socket: Any = None

    async def __aenter__(self) -> ControlClient:
        try:
            self._socket = await websockets.connect(
                self.url,
                open_timeout=self._open_timeout,
                max_size=MAX_ACK_BYTES,
                # A paced replay can hold this open for minutes without a frame
                # in either direction; the default ping keeps it alive.
                ping_interval=20,
                ping_timeout=None,
            )
        except OSError as exc:
            raise SimulatorUnreachable(
                f"No simulator answering at {self.url} — start one with `make sim` "
                f"(or `qte-simulator serve`). Underlying error: {exc}"
            ) from exc
        await self._socket.recv()  # the welcome frame
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._socket is not None:
            await self._socket.close()
            self._socket = None

    async def send(self, op: str, **arguments: Any) -> dict[str, Any]:
        """Send a command and return its ``result``, raising on a refusal.

        Frames that are not the acknowledgement for *this* command are skipped
        rather than treated as the answer, so a future server that pushes a
        progress frame mid-replay does not break this client.
        """
        if self._socket is None:
            raise RuntimeError("ControlClient is not connected — use `async with`")
        command_id = uuid.uuid4().hex[:8]
        await self._socket.send(dumps({"op": op, "id": command_id, **arguments}))

        while True:
            frame = loads(await self._socket.recv())
            if frame.get("type") == "error":
                raise ControlError(frame.get("message") or "the simulator refused the command")
            if frame.get("type") == "ack" and frame.get("id") in (command_id, None):
                return frame.get("result") or {}


__all__ = ["ControlClient", "ControlError", "SimulatorUnreachable"]
