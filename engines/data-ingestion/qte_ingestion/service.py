"""Wires the Tiingo sockets to the resamplers, Redis and NATS.

The flow is one-way and never blocks on a consumer:

    Tiingo WS → Resampler → Redis (state) + NATS (event)

Redis is written first and NATS second on purpose. The runner rebuilds its
warm-up window from Redis when it starts, so a candle that reached the bus but
not the cache would be a bar the engine acts on now and cannot see after a
restart.
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime

from qte_shared.bus import NatsBus, Subjects
from qte_shared.cache import RedisState
from qte_shared.config import settings
from qte_shared.db import EventRepository
from qte_shared.logging_setup import get_logger
from qte_shared.models import Candle, CandleClosedEvent, Tick, TickEvent
from qte_shared.symbols import build_specs
from qte_shared.timeframes import normalize_timeframe

from qte_ingestion.resampler import Resampler
from qte_ingestion.settings import ingestion_settings
from qte_ingestion.tiingo_ws import TiingoWebSocketClient

log = get_logger(__name__)

SERVICE_NAME = "data-ingestion"


class IngestionService:
    """Owns the sockets, the resamplers and the publish loop."""

    def __init__(self) -> None:
        self.specs = build_specs(settings.engine.symbols, ingestion_settings.market_overrides)
        self.timeframes = [normalize_timeframe(tf) for tf in settings.engine.timeframes]
        self.bus = NatsBus(name="qte-ingestion")
        self.state = RedisState()
        self.subjects = Subjects()
        self.events = EventRepository()
        self._resamplers: dict[str, Resampler] = {
            spec.symbol: Resampler(spec.symbol, self.timeframes) for spec in self.specs
        }
        self._clients: list[TiingoWebSocketClient] = []
        self._flush_task: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()

    # ── Lifecycle ─────────────────────────────────────────────────────

    async def start(self) -> None:
        await self.bus.connect()
        await self.state.connect()
        await self._restore_open_candles()

        for market in ("fx", "crypto"):
            client = TiingoWebSocketClient(market, self.specs, self._handle_tick)  # type: ignore[arg-type]
            if client.start() is not None:
                self._clients.append(client)

        if not self._clients:
            raise RuntimeError("No market sockets started — check QTE_ENGINE__SYMBOLS")

        self._flush_task = asyncio.create_task(self._flush_loop(), name="candle-flush")
        await self.events.record_event(
            service=SERVICE_NAME,
            event="started",
            payload={
                "symbols": [spec.symbol for spec in self.specs],
                "timeframes": self.timeframes,
            },
        )
        log.info(
            "Ingestion started symbols=%s timeframes=%s",
            [spec.symbol for spec in self.specs],
            self.timeframes,
        )

    def request_stop(self) -> None:
        """Ask :meth:`run_forever` to unwind. Safe to call from a signal handler."""
        self._stopping.set()

    async def stop(self) -> None:
        self._stopping.set()
        for client in self._clients:
            await client.stop()
        if self._flush_task is not None:
            self._flush_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._flush_task
        await self.events.record_event(service=SERVICE_NAME, event="stopped")
        await self.bus.close()
        await self.state.close()
        log.info("Ingestion stopped")

    async def run_forever(self) -> None:
        await self.start()
        try:
            await self._stopping.wait()
        finally:
            await self.stop()

    async def _restore_open_candles(self) -> None:
        """Reload bars that were mid-build when this process last died."""
        if not ingestion_settings.persist_open_candles:
            return
        for spec in self.specs:
            for timeframe in self.timeframes:
                candle = await self.state.get_open_candle(spec.symbol, timeframe)
                if candle is not None:
                    self._resamplers[spec.symbol].restore(candle)
                    log.info(
                        "Restored open bar symbol=%s tf=%s open_time=%s",
                        spec.symbol,
                        timeframe,
                        candle.open_time,
                    )

    # ── Tick path ─────────────────────────────────────────────────────

    async def _handle_tick(self, tick: Tick) -> None:
        await self.state.set_last_tick(tick)
        if ingestion_settings.publish_ticks:
            await self.bus.publish(
                self.subjects.tick(tick.symbol),
                TickEvent(symbol=tick.symbol, tick=tick).model_dump(mode="json"),
            )
        resampler = self._resamplers.get(tick.symbol)
        if resampler is None:
            return
        for candle in resampler.add_tick(tick):
            await self._emit_candle(candle)
        if ingestion_settings.persist_open_candles:
            for open_candle in resampler.open_candles():
                await self.state.set_open_candle(open_candle)

    async def _flush_loop(self) -> None:
        """Close bars on the clock so a quiet market still produces candles."""
        while not self._stopping.is_set():
            await asyncio.sleep(ingestion_settings.flush_interval)
            now = datetime.now(UTC)
            for resampler in self._resamplers.values():
                for candle in resampler.flush(now):
                    await self._emit_candle(candle)

    async def _emit_candle(self, candle: Candle) -> None:
        await self.state.push_candle(candle)
        await self.bus.publish(
            self.subjects.candle_closed(candle.symbol, candle.timeframe),
            CandleClosedEvent(
                symbol=candle.symbol, timeframe=candle.timeframe, candle=candle
            ).model_dump(mode="json"),
        )
        log.info(
            "Candle closed %s %s open_time=%s o=%s h=%s l=%s c=%s ticks=%d",
            candle.symbol,
            candle.timeframe,
            candle.open_time.isoformat(),
            candle.open,
            candle.high,
            candle.low,
            candle.close,
            candle.tick_count,
        )
