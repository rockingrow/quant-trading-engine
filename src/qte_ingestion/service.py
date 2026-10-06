"""Wires the market data feeds to the resamplers, Redis and NATS.

The flow is one-way and never blocks on a consumer:

    Live feed → Resampler → Redis (state) + NATS (event)

Which vendors sit at the left-hand end is configuration
(``QTE_MARKET_DATA__PROVIDER``, one name or a comma-separated list): this
service asks :func:`~qte_shared.providers.create_provider` for each one's feeds
and only ever sees :class:`~qte_shared.interfaces.market_data.LiveFeed` objects
emitting ticks. Each provider feeds the symbols of its own plan, stamps them
with its own origin, and completes and backfills them from its own history.

A vendor that closes its own bars — the MT5 ingester — emits candles instead
(``Capability.LIVE_BARS``), and those skip the resampler: each bar goes through
the same Redis outbox and candle subject as a resampled one, so the runner
cannot tell the two apart. Whether a bar is new is still decided in Redis, whose
stage drops a bucket it already holds, so a replay or a redelivery is harmless.

Redis is written first and NATS second on purpose. The runner rebuilds its
warm-up window from Redis when it starts, so a candle that reached the bus but
not the cache would be a bar the engine acts on now and cannot see after a
restart.

Start-up also *seeds* that cache from vendor history when the provider serves
it -- see :mod:`qte_ingestion.backfill`. Without it a cold Redis means the
runner has no indicator window until enough bars have printed live, which on
M15 is days.

A bar feed that cannot serve history can still fill that window by *replaying*
it: the MT5 ingester marks such bars ``warmup_bar`` and sends them as a counted
batch, which arrives here on a separate handler
(:meth:`IngestionService._handle_warmup_bar`). Those bars are history, not
events — they are buffered until the batch is whole, then merged into the
stored window in one transaction, and they are never staged, published or
decided on. Staging them would not work: the watermark that makes a redelivery
harmless reads any older bar as a duplicate, and the bars a half-filled window
is missing are the old ones. Because the runner re-reads Redis on every live
close, a merged window reaches its indicator buffer on the next bar without a
restart.

Start-up guards that cache in a fixed order. Candle state another provider
wrote, or dated after now, is discarded before anything reads it back
(:mod:`qte_ingestion.state_guard`). A bar restored from before a restart whose
bucket ended during the downtime is closed and completed from vendor history
before backfill runs, so the top-up merges around it instead of landing beside
it. And the moment ticks start arriving is recorded on every resampler, so the
bar of the bucket already under way is completed when it closes rather than
published half-built (:mod:`qte_ingestion.repair`).
"""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass
from datetime import UTC, datetime

from qte_ingestion.backfill import HistoryBackfiller, open_history_source
from qte_ingestion.repair import PartialBarRepairer, RoutedBarRepairer
from qte_ingestion.resampler import Resampler
from qte_ingestion.settings import ingestion_settings
from qte_ingestion.state_guard import discard_foreign_candle_state
from qte_shared.bus import NatsBus, Subjects
from qte_shared.cache import RedisState
from qte_shared.config import market_data_plan, settings
from qte_shared.db import EventRepository
from qte_shared.interfaces.market_data import (
    Capability,
    LiveFeed,
    MarketDataProvider,
    ProviderError,
    UnsupportedCapability,
    WarmupBatch,
)
from qte_shared.logging_setup import get_logger
from qte_shared.market_data_plan import SymbolFeed
from qte_shared.models import Candle, CandleClosedEvent, Tick, TickEvent
from qte_shared.providers import create_provider
from qte_shared.state_scope import MarketDataOrigin, stamp_market_data
from qte_shared.timeframes import CLOCK_TOLERANCE, bucket_close

log = get_logger(__name__)

SERVICE_NAME = "data-ingestion"


@dataclass
class RetiredCandle:
    """A retired bucket retained until Redis acknowledges durable staging."""

    candle: Candle
    partial: bool
    close_is_current: bool


def resolve_subscriptions() -> list[SymbolFeed]:
    """Use the shared precedence rules, preserving an intentionally empty plan."""
    return settings.engine.resolve_subscriptions(
        market_data_plan(), ingestion_settings.market_overrides
    )


class IngestionService:
    """Owns the live feeds, the resamplers and the publish loop."""

    def __init__(self) -> None:
        self.subscriptions = resolve_subscriptions()
        if not self.subscriptions:
            raise ValueError(
                "Market-data configuration enables no subscriptions. Enable a planned symbol "
                "or configure QTE_ENGINE__SYMBOLS and timeframes before starting ingestion."
            )
        self.specs = [feed.spec for feed in self.subscriptions]
        #: Every timeframe anything is resampled to — for the log, the start
        #: event and the outbox drain. What a given symbol gets is its own
        #: :attr:`~qte_shared.market_data_plan.SymbolFeed.timeframes`.
        self.timeframes = list(
            dict.fromkeys(tf for feed in self.subscriptions for tf in feed.timeframes)
        )
        self.bus = NatsBus(name="qte-ingestion")
        self.state = RedisState()
        self.subjects = Subjects()
        self.events = EventRepository()
        self._scope = settings.state_scope
        #: Every configured provider, by name, in the order configured.
        self.providers: dict[str, MarketDataProvider] = {
            provider_name: create_provider(
                provider_name, capability=(Capability.LIVE, Capability.LIVE_BARS)
            )
            for provider_name in self._scope.providers
        }
        #: The provenance each provider stamps on what it feeds.
        self._origins: dict[str, MarketDataOrigin] = {
            provider_name: self._scope.origin(provider=provider_name, synthetic=provider.synthetic)
            for provider_name, provider in self.providers.items()
        }
        #: Symbol to the provider that feeds it.
        self._symbol_providers: dict[str, str] = {
            feed.symbol: feed.provider for feed in self.subscriptions
        }
        self._resamplers: dict[str, Resampler] = {
            feed.symbol: Resampler(feed.symbol, list(feed.timeframes))
            for feed in self.subscriptions
        }
        self._feeds: list[LiveFeed] = []
        #: Completes partial bars from vendor history; built in :meth:`start`.
        self._repairer: RoutedBarRepairer | None = None
        self._flush_task: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()
        self._outbox_lock = asyncio.Lock()
        self._processing_lock = asyncio.Lock()
        self._retired_candles: dict[tuple[str, str, datetime], RetiredCandle] = {}
        #: Replayed warm-up bars per (symbol, timeframe), keyed by open time so
        #: a redelivered bar collapses, held until the batch is whole.
        self._warmup_batches: dict[tuple[str, str], dict[datetime, Candle]] = {}
        self._cleaned = False

    # ── Lifecycle ─────────────────────────────────────────────────────

    async def start(self) -> None:
        self._cleaned = False
        try:
            await self.bus.connect()
            await self.state.connect()
            # Before anything reads the cache back: bars another provider wrote,
            # or dated after now, are not this market's history.
            await discard_foreign_candle_state(
                self.state,
                self.subscriptions,
                provider_key=self._scope.provider,
                synthetic=any(provider.synthetic for provider in self.providers.values()),
            )
            # Drain before accepting fresh ticks so a candle delayed by a restart
            # cannot arrive after a newer close for the same strategy window.
            await self._drain_candle_outbox()
            self._repairer = self._build_repairer()
            await self._restore_open_candles()
            # A restored bar whose bucket ended while this process was down closes
            # now, completed from vendor history — its close included, since this
            # process heard its last tick before the outage — before backfill reads
            # the list. Closed any later, it would land behind the bars the backfill
            # tops up with, beside the vendor's copy of its own bucket.
            await self._emit_candles(
                self._close_ended_bars(datetime.now(UTC)), close_is_current=False
            )
            # Before the first live tick: a bar backfilled from the vendor must
            # not land behind one this process just resampled.
            for provider_name in self.providers:
                await HistoryBackfiller(
                    self.state,
                    self._subscriptions_of(provider_name),
                    provider_name=provider_name,
                ).run()

            # Whatever bucket is under way now was not listened to from its open.
            joined_at = datetime.now(UTC)
            for resampler in self._resamplers.values():
                resampler.mark_joined(joined_at)

            started = dict.fromkeys(self.providers, 0)
            for provider_name, feed in self._open_feeds():
                # Track before start: a provider may acquire a socket and then
                # raise, and that half-started feed still needs its stop hook.
                self._feeds.append(feed)
                if feed.start() is not None:
                    started[provider_name] += 1

            # Each provider on its own: one quiet vendor must not hide behind
            # another that started, or its symbols would silently never trade.
            silent = [provider_name for provider_name, count in started.items() if not count]
            if silent:
                planned_in = [str(plan_path) for plan_path in market_data_plan().sources]
                raise RuntimeError(
                    f"Provider {', '.join(map(repr, silent))} started no feeds — check the "
                    f"symbols in {', '.join(planned_in) or 'QTE_ENGINE__SYMBOLS'}"
                )

            self._flush_task = asyncio.create_task(self._flush_loop(), name="candle-flush")
            await self.events.record_event(
                service=SERVICE_NAME,
                event="started",
                payload={
                    "provider": self._scope.provider,
                    "providers": list(self.providers),
                    "symbols": [spec.symbol for spec in self.specs],
                    "timeframes": self.timeframes,
                },
            )
            log.info(
                "Ingestion started providers=%s symbols=%s timeframes=%s",
                ",".join(self.providers),
                [spec.symbol for spec in self.specs],
                self.timeframes,
            )
        except BaseException:
            await self._close_resources(record_event=False)
            raise

    def _subscriptions_of(self, provider_name: str) -> list[SymbolFeed]:
        """The subscriptions *provider_name* feeds."""
        single_provider = len(self.providers) == 1
        return [
            feed
            for feed in self.subscriptions
            if self._symbol_providers.get(feed.symbol, "") == provider_name
            or (single_provider and not self._symbol_providers.get(feed.symbol))
        ]

    def _origin_for(self, symbol: str) -> MarketDataOrigin:
        """The origin stamped on *symbol*'s ticks and bars: its provider's."""
        provider_name = self._symbol_providers.get(symbol)
        if not provider_name and len(self._origins) == 1:
            return next(iter(self._origins.values()))
        if not provider_name:
            raise ValueError(f"No configured provider feeds {symbol}")
        return self._origins[provider_name]

    def _open_feeds(self) -> list[tuple[str, LiveFeed]]:
        """Each provider's feeds over its own symbols, tagged with its name.

        Bar feeds for a provider that closes its own bars, tick feeds otherwise.
        """
        opened: list[tuple[str, LiveFeed]] = []
        for provider_name, provider in self.providers.items():
            subscriptions = self._subscriptions_of(provider_name)
            if provider.supports(Capability.LIVE_BARS):
                feeds = provider.bar_feeds(
                    subscriptions, self._handle_closed_bar, self._handle_warmup_bar
                )
            else:
                specs = [symbol_feed.spec for symbol_feed in subscriptions]
                feeds = provider.live_feeds(specs, self._handle_tick)
            opened.extend((provider_name, feed) for feed in feeds)
        return opened

    def _build_repairer(self) -> RoutedBarRepairer:
        """Per provider, a repairer over its history, or one that publishes as built."""
        repairers_by_symbol: dict[str, PartialBarRepairer] = {}
        for provider_name in self.providers:
            markets = {
                symbol_feed.symbol: symbol_feed.market
                for symbol_feed in self._subscriptions_of(provider_name)
            }
            try:
                source = open_history_source(provider_name)
            except UnsupportedCapability:
                source = None
            except ProviderError as failure:
                log.warning(
                    "Partial bars from %r will be published as built: %s", provider_name, failure
                )
                source = None
            repairer = PartialBarRepairer(source, markets)
            repairers_by_symbol.update(dict.fromkeys(markets, repairer))
        return RoutedBarRepairer(repairers_by_symbol)

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
        for feed in self.subscriptions:
            for timeframe in feed.timeframes:
                candle = await self.state.get_open_candle(feed.symbol, timeframe)
                if candle is not None:
                    self._resamplers[feed.symbol].restore(candle)
                    log.info(
                        "Restored open bar symbol=%s tf=%s open_time=%s",
                        feed.symbol,
                        timeframe,
                        candle.open_time,
                    )

    # ── Tick path ─────────────────────────────────────────────────────

    async def _handle_tick(self, tick: Tick) -> None:
        async with self._processing_guard():
            # Do not retire another batch while Redis cannot retain the first.
            # This bounds memory by the configured series, rather than ticks.
            if getattr(self, "_retired_candles", None):
                await self._emit_candles([])
            await self._handle_tick_serialized(tick)

    async def _handle_closed_bar(self, candle: Candle) -> None:
        """Stage a bar the vendor already closed; no resampling, no repair.

        A bar whose bucket has not ended yet cannot be a close. From a live
        vendor it means the vendor's clock is off — for MT5, almost always
        ``MT5_SERVER_TIMEZONE`` on the ingester — and staging it would put a
        future-dated bar in Redis, which the next start discards as foreign.
        """
        closes_at = bucket_close(candle.open_time, candle.timeframe)
        if closes_at > datetime.now(UTC) + CLOCK_TOLERANCE:
            log.warning(
                "Dropping %s %s bar open_time=%s: its bucket ends at %s, after now. "
                "Check the vendor's clock (MT5_SERVER_TIMEZONE on the ingester).",
                candle.symbol,
                candle.timeframe,
                candle.open_time.isoformat(),
                closes_at.isoformat(),
            )
            return
        async with self._processing_guard():
            # Retained before any await inside, so a Redis failure leaves this
            # bar with the flush loop to retry instead of losing it.
            await self._emit_candles([candle])

    async def _handle_warmup_bar(self, candle: Candle, batch: WarmupBatch) -> None:
        """Buffer a replayed bar, merging the window once its batch is whole.

        The batch is held in memory rather than written bar by bar because a
        merge rewrites the whole stored window: a 150-bar batch applied one bar
        at a time would rewrite it 150 times. A crash mid-batch therefore loses
        the partial batch, which is the right trade — a warm-up batch is
        re-sendable, and nothing has been published from it.

        Nothing here goes through :meth:`_emit_candles`. A replayed bar is not a
        close: no partial-bar repair, no outbox, no NATS event, no decision.
        """
        closes_at = bucket_close(candle.open_time, candle.timeframe)
        if closes_at > datetime.now(UTC) + CLOCK_TOLERANCE:
            log.warning(
                "Dropping %s %s warm-up bar open_time=%s: its bucket ends at %s, after now. "
                "Check the vendor's clock (MT5_SERVER_TIMEZONE on the ingester).",
                candle.symbol,
                candle.timeframe,
                candle.open_time.isoformat(),
                closes_at.isoformat(),
            )
            return
        candle = stamp_market_data(candle, self._origin_for(candle.symbol))
        series = (candle.symbol, candle.timeframe)
        async with self._processing_guard():
            buffered = self._warmup_batches.setdefault(series, {})
            buffered[candle.open_time] = candle
            if len(buffered) == 1 and batch.total > settings.redis.candle_history:
                # Not an error: the batch is simply larger than the window kept
                # for it, and the merge trims to the newest. Said once a batch.
                log.info(
                    "%s %s warm-up batch is %d bars but QTE_REDIS__CANDLE_HISTORY is %d; "
                    "the oldest will be trimmed",
                    candle.symbol,
                    candle.timeframe,
                    batch.total,
                    settings.redis.candle_history,
                )
            # The cap is a safety valve, not the normal path: an ingester that
            # never sends its last bar would otherwise buffer without bound.
            # Flushing at the window size loses nothing, since the merge trims
            # to it anyway.
            overflowing = len(buffered) >= settings.redis.candle_history
            if not batch.is_last and not overflowing:
                return
            if overflowing and not batch.is_last:
                log.warning(
                    "Flushing the %s %s warm-up batch at %d bars without its last bar "
                    "(expected %d) — the window size is the cap",
                    candle.symbol,
                    candle.timeframe,
                    len(buffered),
                    batch.total,
                )
            pending = self._warmup_batches.pop(series, {})
            await self.state.merge_history_candles(
                candle.symbol, candle.timeframe, [pending[moment] for moment in sorted(pending)]
            )

    def _processing_guard(self) -> asyncio.Lock:
        if not hasattr(self, "_processing_lock"):
            self._processing_lock = asyncio.Lock()
        return self._processing_lock

    async def _handle_tick_serialized(self, tick: Tick) -> None:
        tick = stamp_market_data(tick, self._origin_for(tick.symbol))
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
                async with self._processing_guard():
                    if getattr(self, "_retired_candles", None):
                        await self._emit_candles([])
                    await self._emit_candles(self._close_ended_bars(datetime.now(UTC)))
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Flush cycle failed — retrying at the next interval")

    def _close_ended_bars(self, moment: datetime) -> list[Candle]:
        """Every open bar, across all symbols, whose bucket has ended by *moment*."""
        closed_bars: list[Candle] = []
        for resampler in self._resamplers.values():
            closed_bars.extend(resampler.flush(moment))
        return closed_bars

    async def _emit_candle(self, candle: Candle) -> None:
        await self._emit_candles([candle])

    async def _emit_candles(self, candles: list[Candle], *, close_is_current: bool = True) -> None:
        """Stage every retired bucket before attempting any network publish.

        A bar the resampler built from only part of its bucket is completed from
        vendor history first, so what reaches Redis and the runner is the whole
        bar. Whole bars are staged and published before any repair starts, so
        one symbol's vendor request never holds back another symbol's close. A
        batch never holds two bars of one symbol and timeframe — a tick or a
        flush retires at most one per timeframe — so no series is reordered.

        *close_is_current* is false only for the bars closed at start-up after an
        outage that outlasted their bucket: this process's last tick for them
        predates the outage, so their close comes from the vendor as well.
        """
        if not hasattr(self, "_retired_candles"):
            self._retired_candles = {}
        # Retain the entire batch and its partial flags before the first await.
        # A failure on any item must leave that item and all later ones intact.
        for candle in candles:
            candle = stamp_market_data(candle, self._origin_for(candle.symbol))
            resampler = self._resamplers.get(candle.symbol)
            marker = (candle.symbol, candle.timeframe, candle.open_time)
            if marker not in self._retired_candles:
                self._retired_candles[marker] = RetiredCandle(
                    candle=candle,
                    partial=resampler is not None and resampler.take_partial(candle),
                    close_is_current=close_is_current,
                )
        for marker, retired in list(self._retired_candles.items()):
            if not retired.partial:
                await self.state.stage_closed_candle(retired.candle)
                del self._retired_candles[marker]
        await self._drain_candle_outbox()
        had_partial_bars = bool(self._retired_candles)
        for marker, retired in list(self._retired_candles.items()):
            if self._repairer is not None:
                retired.candle = await self._repairer.repair(
                    retired.candle, close_is_current=retired.close_is_current
                )
                retired.candle = stamp_market_data(
                    retired.candle, self._origin_for(retired.candle.symbol)
                )
            retired.partial = False
            await self.state.stage_closed_candle(retired.candle)
            del self._retired_candles[marker]
        if had_partial_bars:
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
                if not self._scope.accepts(candle.origin):
                    raise ValueError("Refusing to publish a candle from another state scope")
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
