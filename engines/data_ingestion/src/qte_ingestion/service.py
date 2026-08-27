"""Wires the market data feeds to the resamplers, Redis and NATS.

The flow is one-way and never blocks on a consumer:

    Live feed → Resampler → Redis (state) + NATS (event)

Which vendor sits at the left-hand end is configuration
(``QTE_MARKET_DATA__PROVIDER``): this service asks
:func:`~qte_shared.providers.create_provider` for feeds and only ever sees
:class:`~qte_shared.interfaces.market_data.LiveFeed` objects emitting ticks.

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
from qte_shared.interfaces.market_data import Capability, LiveFeed
from qte_shared.logging_setup import get_logger
from qte_shared.models import Candle, CandleClosedEvent, Tick, TickEvent
from qte_shared.providers import create_provider
from qte_shared.symbols import build_specs
from qte_shared.timeframes import normalize_timeframe

from qte_ingestion.resampler import Resampler
from qte_ingestion.settings import ingestion_settings

log = get_logger(__name__)

SERVICE_NAME = "data-ingestion"


class IngestionService:
    """Owns the live feeds, the resamplers and the publish loop."""

    def __init__(self) -> None:
        self.specs = build_specs(settings.engine.symbols, ingestion_settings.market_overrides)
        self.timeframes = [normalize_timeframe(tf) for tf in settings.engine.timeframes]
        self.bus = NatsBus(name="qte-ingestion")
        self.state = RedisState()
        self.subjects = Subjects()
        self.events = EventRepository()
        self.provider = create_provider(capability=Capability.LIVE)
        self._resamplers: dict[str, Resampler] = {
            spec.symbol: Resampler(spec.symbol, self.timeframes) for spec in self.specs
        }
        self._feeds: list[LiveFeed] = []
        self._flush_task: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()
        self._outbox_lock = asyncio.Lock()
        self._cleaned = False

    # ── Lifecycle ─────────────────────────────────────────────────────

    async def start(self) -> None:
        self._cleaned = False
        try:
            await self.bus.connect()
            await self.state.connect()
            # Drain before accepting fresh ticks so a candle delayed by a restart
            # cannot arrive after a newer close for the same strategy window.
            await self._drain_candle_outbox()
            await self._restore_open_candles()

            started = 0
            for feed in self.provider.live_feeds(self.specs, self._handle_tick):
                # Track before start: a provider may acquire a socket and then
                # raise, and that half-started feed still needs its stop hook.
                self._feeds.append(feed)
                if feed.start() is not None:
                    started += 1

            if not started:
                raise RuntimeError(
                    f"Provider {self.provider.name!r} started no feeds — check QTE_ENGINE__SYMBOLS"
                )

            self._flush_task = asyncio.create_task(self._flush_loop(), name="candle-flush")
            await self.events.record_event(
                service=SERVICE_NAME,
                event="started",
                payload={
                    "provider": self.provider.name,
                    "symbols": [spec.symbol for spec in self.specs],
                    "timeframes": self.timeframes,
                },
            )
            log.info(
                "Ingestion started provider=%s symbols=%s timeframes=%s",
                self.provider.name,
                [spec.symbol for spec in self.specs],
                self.timeframes,
            )
        except BaseException:
            await self._close_resources(record_event=False)
            raise

    def request_stop(self) -> None:
        """Ask :meth:`run_forever` to unwind. Safe to call from a signal handler."""
        self._stopping.set()

    async def stop(self) -> None:
        await self._close_resources(record_event=True)

    async def _close_resources(self, *, record_event: bool) -> None:
        """Release resources in reverse acquisition order after any start state."""
        if getattr(self, "_cleaned", False):
            return
        cleanup_failed = False
        self._stopping.set()
        if self._flush_task is not None:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
            except Exception:
                cleanup_failed = True
                log.exception("Flush task cleanup failed during ingestion shutdown")
            self._flush_task = None
        failed_feeds: list[LiveFeed] = []
        for feed in reversed(self._feeds):
            try:
                await feed.stop()
            except Exception:
                cleanup_failed = True
                failed_feeds.append(feed)
                log.exception("Feed cleanup failed during ingestion shutdown")
        self._feeds = list(reversed(failed_feeds))
        if record_event:
            with contextlib.suppress(Exception):
                await self.events.record_event(service=SERVICE_NAME, event="stopped")
        for close in (self.state.close, self.bus.close):
            try:
                await close()
            except Exception:
                cleanup_failed = True
                log.exception("Resource cleanup failed during ingestion shutdown")
        self._cleaned = not cleanup_failed
        log.info("Ingestion stopped")

    async def run_forever(self) -> None:
        try:
            await self.start()
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
        closed = resampler.add_tick(tick)
        try:
            await self._emit_candles(closed)
        finally:
            # `add_tick` may have opened the next bucket before publishing the
            # previous one failed.  Persist it even on that failure, otherwise
            # a restart loses the first tick of the new bar.
            if ingestion_settings.persist_open_candles:
                for open_candle in resampler.open_candles():
                    await self.state.set_open_candle(open_candle)

    async def _flush_loop(self) -> None:
        """Close bars on the clock so a quiet market still produces candles.

        One failed cycle costs one interval, never the loop. This task is the
        only thing that closes a bar in a market too quiet to push the bucket
        over with a tick, and nothing awaits it until :meth:`stop` — so an
        exception escaping here would end wall-clock closing for the life of
        the process, with the service still looking healthy.
        """
        while not self._stopping.is_set():
            await asyncio.sleep(ingestion_settings.flush_interval)
            try:
                await self._drain_candle_outbox()
                now = datetime.now(UTC)
                closed: list[Candle] = []
                for resampler in self._resamplers.values():
                    closed.extend(resampler.flush(now))
                await self._emit_candles(closed)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Flush cycle failed — retrying at the next interval")

    async def _emit_candle(self, candle: Candle) -> None:
        await self._emit_candles([candle])

    async def _emit_candles(self, candles: list[Candle]) -> None:
        """Stage every retired bucket before attempting any network publish."""
        for candle in candles:
            await self.state.stage_closed_candle(candle)
        await self._drain_candle_outbox()

    async def _drain_candle_outbox(self) -> None:
        """Publish staged closes oldest-first, acknowledging only on success.

        Core NATS has no server acknowledgement.  A crash immediately after
        publish can therefore replay one duplicate, which the runner already
        rejects by candle open time.  The opposite outcome — deleting before
        publish and losing a strategy decision — is never allowed.
        """
        # Several live feeds call this method concurrently. Without a single
        # consumer lock, two callbacks can peek A, both publish A, then pop A
        # and B — silently acknowledging B without ever publishing it.
        lock = getattr(self, "_outbox_lock", None)
        if lock is None:
            lock = self._outbox_lock = asyncio.Lock()
        async with lock:
            while candle := await self.state.peek_pending_candle():
                await self.bus.publish(
                    self.subjects.candle_closed(candle.symbol, candle.timeframe),
                    CandleClosedEvent(
                        symbol=candle.symbol, timeframe=candle.timeframe, candle=candle
                    ).model_dump(mode="json"),
                )
                await self.state.ack_pending_candle()
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
