"""The client half of the simulator: a live feed that reads its ticks off a socket.

Structurally this is the same object as a vendor's WebSocket feed — connect,
subscribe, parse, call the handler, reconnect on failure — which is the point.
Ingestion cannot tell the difference, so what the simulator exercises is the
real ingestion path rather than a test double of it.

Two differences from a vendor feed, both deliberate:

* **One socket for every symbol.** A vendor splits FX and crypto because its
  endpoints do; the simulator has no such constraint and splitting anyway would
  only add a way for a test to be wrong.
* **No credentials.** There is nothing to authenticate to. The protection
  against pointing production at it is :func:`~qte_shared.dev_only.require_dev_env`
  on the provider, not a key.
"""

from __future__ import annotations

import asyncio
import random

import websockets

from qte_shared.interfaces.market_data import LiveFeed, TickHandler
from qte_shared.logging_setup import get_logger
from qte_shared.providers.simulator.protocol import (
    decode_tick,
    dumps,
    loads,
    subscribe_frame,
)
from qte_shared.providers.simulator.settings import SimulatorSettings

log = get_logger(__name__)


class SimulatorLiveFeed(LiveFeed):
    """Streams ticks from a running ``qte-simulator serve`` until stopped."""

    def __init__(
        self,
        symbols: list[str],
        on_tick: TickHandler,
        config: SimulatorSettings | None = None,
    ) -> None:
        self.name = "simulator"
        self._symbols = tuple(sorted({symbol.upper() for symbol in symbols}))
        self._on_tick = on_tick
        self._config = config or SimulatorSettings()
        self._running = False
        self._task: asyncio.Task[None] | None = None
        #: Frames seen since the process started — `qte-simulator status` shows
        #: what was sent, this is what arrived, and a gap between the two is the
        #: first thing worth knowing when a candle does not appear.
        self.ticks_received = 0

    # ── Lifecycle ─────────────────────────────────────────────────────

    @property
    def symbols(self) -> tuple[str, ...]:
        return self._symbols

    @property
    def url(self) -> str:
        return self._config.url

    def start(self) -> asyncio.Task[None] | None:
        if not self._symbols:
            log.info("Simulator feed has no symbols — not connecting")
            return None
        self._running = True
        self._task = asyncio.create_task(self._run(), name=self.name)
        return self._task

    async def stop(self) -> None:
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    # ── Connect loop ──────────────────────────────────────────────────

    async def _run(self) -> None:
        attempt = 0
        while self._running:
            try:
                async with websockets.connect(
                    self.url,
                    ping_interval=self._config.ping_interval,
                    max_queue=4096,
                ) as socket:
                    await socket.send(dumps(subscribe_frame(list(self._symbols))))
                    attempt = 0
                    log.info(
                        "Simulator feed open url=%s symbols=%s",
                        self.url,
                        ",".join(self._symbols),
                    )
                    async for raw in socket:
                        await self._handle_raw(raw)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if not self._running:
                    return
                attempt += 1
                delay = self._backoff(attempt)
                log.warning(
                    "Simulator feed dropped (attempt %d): %s — retrying in %.1fs",
                    attempt,
                    exc,
                    delay,
                )
                await asyncio.sleep(delay)

    def _backoff(self, attempt: int) -> float:
        base = min(self._config.max_backoff_seconds, 2.0 ** min(attempt, 6))
        return base * (0.5 + random.random() / 2)

    async def _handle_raw(self, raw: str | bytes) -> None:
        try:
            frame = loads(raw)
        except ValueError:
            log.warning("Simulator sent a non-JSON frame: %.120r", raw)
            return

        kind = frame.get("type")
        if kind == "tick":
            tick = decode_tick(frame)
            if tick is None:
                log.warning("Simulator sent an unusable tick frame: %s", frame)
                return
            self.ticks_received += 1
            # The handler's failures are the handler's. Letting one reach the
            # connect loop would tear down a healthy socket, lose every tick
            # sent during the backoff, and log it as the simulator dropping the
            # feed — sending whoever reads that log to the wrong service.
            try:
                await self._on_tick(tick)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception(
                    "Tick handler failed symbol=%s ts=%s — the feed stays open",
                    tick.symbol,
                    tick.ts.isoformat(),
                )
        elif kind == "error":
            log.error("Simulator refused a frame: %s", frame.get("message"))
        elif kind in ("welcome", "subscribed"):
            log.info("Simulator %s: %s", kind, frame)
