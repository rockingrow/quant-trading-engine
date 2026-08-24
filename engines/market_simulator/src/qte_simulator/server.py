"""The WebSocket server: a feed on ``/stream``, a control plane on ``/control``.

One process, two paths, because they are two halves of one fixture — the thing
that sends the data and the thing you tell what to send. Splitting them into
two services would mean two ports, two lifecycles and a shared-state problem
between them.

The server holds no market state beyond :class:`~qte_simulator.hub.SimulatorHub`:
it does not track candles, does not talk to NATS, Redis or Postgres, and does
not know what a strategy is. Everything downstream of the socket is the real
pipeline, which is the only way an end-to-end rehearsal proves anything.
"""

from __future__ import annotations

import asyncio
import contextlib
import signal

from qte_shared.dev_only import require_dev_env
from qte_shared.logging_setup import configure_logging, get_logger
from qte_shared.providers.simulator.protocol import (
    CONTROL_PATH,
    STREAM_PATH,
    dumps,
    error_frame,
    loads,
    welcome_frame,
)
from websockets.asyncio.server import ServerConnection, serve

from qte_simulator.control import CommandError, dispatch
from qte_simulator.hub import SimulatorHub
from qte_simulator.settings import simulator_settings

log = get_logger(__name__)

SERVICE_NAME = "market-simulator"


class SimulatorServer:
    """Serves the feed and the control plane until stopped."""

    def __init__(self, host: str | None = None, port: int | None = None) -> None:
        # The refusal happens here, before a socket is bound: a process that
        # should not exist should not have taken a port either.
        require_dev_env("The market data simulator")
        self.host = host or simulator_settings.host
        self.port = port if port is not None else simulator_settings.port
        self.hub = SimulatorHub()
        #: The port actually bound. Differs from :attr:`port` only when that is
        #: 0 — which is how a test gets a port nobody else on the box has.
        self.bound_port = self.port
        #: Set once the socket is listening, so a caller can proceed without
        #: sleeping and hoping.
        self.ready = asyncio.Event()
        self._stopping = asyncio.Event()

    # ── Lifecycle ─────────────────────────────────────────────────────

    async def serve_forever(self) -> None:
        async with serve(self._handle, self.host, self.port) as server:
            if server.sockets:
                self.bound_port = server.sockets[0].getsockname()[1]
            self.ready.set()
            log.info(
                "Simulator listening on ws://%s:%d — feed %s, control %s",
                self.host,
                self.bound_port,
                STREAM_PATH,
                CONTROL_PATH,
            )
            await self._stopping.wait()
            await self.hub.stop_generators()
            self.ready.clear()
            server.close()
        log.info("Simulator stopped after %d ticks", self.hub.ticks_sent)

    def request_stop(self) -> None:
        """Ask :meth:`serve_forever` to unwind. Safe from a signal handler."""
        self._stopping.set()

    # ── Routing ───────────────────────────────────────────────────────

    async def _handle(self, connection: ServerConnection) -> None:
        path = (connection.request.path if connection.request else "") or ""
        route = path.split("?")[0].rstrip("/") or STREAM_PATH
        if route == STREAM_PATH:
            await self._serve_stream(connection)
        elif route == CONTROL_PATH:
            await self._serve_control(connection)
        else:
            await connection.send(
                dumps(error_frame(f"Unknown path {path!r}; use {STREAM_PATH} or {CONTROL_PATH}"))
            )
            await connection.close()

    # ── The feed ──────────────────────────────────────────────────────

    async def _serve_stream(self, connection: ServerConnection) -> None:
        subscriber = self.hub.attach(str(connection.remote_address), connection.send)
        try:
            await connection.send(dumps(welcome_frame(STREAM_PATH)))
            async for raw in connection:
                await self._on_stream_frame(connection, subscriber, raw)
        except Exception as exc:
            log.debug("Feed client id=%d closed: %s", subscriber.id, exc)
        finally:
            self.hub.detach(subscriber)

    async def _on_stream_frame(self, connection, subscriber, raw: str | bytes) -> None:
        try:
            frame = loads(raw)
        except ValueError:
            await connection.send(dumps(error_frame("frames must be JSON objects")))
            return
        if frame.get("op") != "subscribe":
            await connection.send(dumps(error_frame(f"unknown op {frame.get('op')!r}")))
            return
        symbols = frame.get("symbols") or []
        subscriber.symbols = {str(symbol).upper() for symbol in symbols}
        log.info(
            "Feed client id=%d subscribed to %s",
            subscriber.id,
            ",".join(sorted(subscriber.symbols)) or "*",
        )
        await connection.send(dumps({"type": "subscribed", "symbols": sorted(subscriber.symbols)}))

    # ── The control plane ─────────────────────────────────────────────

    async def _serve_control(self, connection: ServerConnection) -> None:
        await connection.send(dumps(welcome_frame(CONTROL_PATH)))
        async for raw in connection:
            await self._on_control_frame(connection, raw)

    async def _on_control_frame(self, connection, raw: str | bytes) -> None:
        try:
            command = loads(raw)
        except ValueError as exc:
            await connection.send(dumps(error_frame(f"unreadable command: {exc}")))
            return

        op = str(command.get("op") or "")
        try:
            result = await dispatch(self.hub, command)
        except CommandError as exc:
            await connection.send(dumps(error_frame(str(exc), op=op)))
            return
        except Exception as exc:
            # A defect in a command handler is a bug in this fixture, not a
            # reason to drop a session someone is mid-test in.
            log.exception("Command %s failed", op)
            await connection.send(dumps(error_frame(f"{type(exc).__name__}: {exc}", op=op)))
            return

        await connection.send(
            dumps({"type": "ack", "op": op, "id": command.get("id"), "result": result})
        )


async def main(host: str | None = None, port: int | None = None) -> None:
    configure_logging()
    server = SimulatorServer(host, port)
    loop = asyncio.get_running_loop()
    for signal_name in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(signal_name, server.request_stop)
    await server.serve_forever()


__all__ = ["SERVICE_NAME", "SimulatorServer", "main"]
