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

A bar feed whose vendor is another process is warmed the same way, by asking:
the MT5 ingester answers one request per series with the newest closed bars,
and this service decides which series and how many. Three things follow from
the vendor being a process that can be down. **This service starts without
it**: a boot that finds nobody to ask carries on with what Redis holds. It then
does not ask on a timer — it waits for the ingester to say it has connected
(:meth:`IngestionService._handle_vendor_online`), and only then checks every
planned window against Redis and requests the ones that are short
(:meth:`IngestionService._history_loop`). And a live bar that does not follow
the newest stored one — the ingester restarted and skipped the closes in
between — has the bars before it fetched
*before* it is staged (:meth:`IngestionService._fill_gap_before`), so the
runner never decides on a window with a hole in it.

An ingester from before that change fills the window by *replaying* it instead:
it marks such bars ``warmup_bar`` and sends them as a counted
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
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

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
    HistoryNotServed,
    HistoryOffline,
    LiveFeed,
    MarketDataProvider,
    ProviderError,
    UnsupportedCapability,
    WarmupBatch,
)
from qte_shared.logging_setup import get_logger
from qte_shared.market_data_plan import SymbolFeed
from qte_shared.models import Candle, CandleClosedEvent, Tick, TickEvent
from qte_shared.notifications import ServiceStatusNotifier, TelegramErrorNotifier
from qte_shared.providers import create_provider
from qte_shared.state_scope import MarketDataOrigin, stamp_market_data
from qte_shared.symbols import SymbolSpec
from qte_shared.timeframes import CLOCK_TOLERANCE, bucket_close, timeframe_seconds

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
        #: Telegram, if it is configured: the service's own UP/DOWN message and
        #: its ERROR logs. Both are no-ops without a token and a chat, and
        #: neither is ever on the bar path.
        self.status = ServiceStatusNotifier(SERVICE_NAME)
        self.telegram_errors = TelegramErrorNotifier()
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
        #: The backfiller that warms each provider's series; built in :meth:`start`.
        self._backfillers: dict[str, HistoryBackfiller] = {}
        #: Series whose history has been asked for and not answered yet.
        self._history_owed: dict[tuple[str, str], SymbolSpec] = {}
        self._history_task: asyncio.Task[None] | None = None
        self._history_wake = asyncio.Event()
        #: True from a request that found no vendor to the vendor's next
        #: announcement: while it is set nothing is asked on a timer.
        self._vendor_offline = False
        #: When each series last had a gap topped up inline.
        self._gap_asked_at: dict[tuple[str, str], datetime] = {}
        self._cleaned = False

    # ── Lifecycle ─────────────────────────────────────────────────────

    async def start(self) -> None:
        self._cleaned = False
        try:
            # First of all, so everything below is covered: a boot that fails on
            # the very next line is exactly the error worth a message. Inside the
            # try, so its own failure still unwinds through _close_resources.
            await self.telegram_errors.start(SERVICE_NAME)
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
            self._backfillers = {}
            self._history_owed = {}
            self._history_wake = asyncio.Event()
            self._vendor_offline = False
            self._gap_asked_at = {}
            for provider_name in self.providers:
                backfiller = HistoryBackfiller(
                    self.state,
                    self._subscriptions_of(provider_name),
                    provider_name=provider_name,
                )
                self._backfillers[provider_name] = backfiller
                # Listening before the first request: a vendor that connects
                # between the two is then heard, not missed.
                await self._watch_vendor(provider_name, backfiller)
                for spec, timeframe in await backfiller.run():
                    self._history_owed[(spec.symbol, timeframe)] = spec
                if backfiller.offline:
                    self._vendor_offline = True

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

            # The operator's plane, once the feeds are actually open: what a
            # status, a warm-up or a flush answers about is the running service,
            # so it is subscribed where the service starts being one.
            await self.bus.subscribe(self.subjects.ingestion_control(), self._on_control_message)

            self._flush_task = asyncio.create_task(self._flush_loop(), name="candle-flush")
            if any(backfiller.retryable for backfiller in self._backfillers.values()):
                # Started even with nothing owed: the vendor's announcement and
                # a gap found later are both handed to this loop.
                if self._history_owed and self._vendor_offline:
                    log.warning(
                        "Started without history for %s: the vendor is not connected. "
                        "Carrying on with what Redis holds; the windows are checked and "
                        "requested as soon as it announces itself.",
                        ", ".join(
                            f"{symbol} {timeframe}" for symbol, timeframe in self._history_owed
                        ),
                    )
                elif self._history_owed:
                    self._history_wake.set()
                self._history_task = asyncio.create_task(
                    self._history_loop(), name="history-requests"
                )
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
                "Ingestion started providers=%s symbols=%s timeframes=%s telegram status=%s",
                ",".join(self.providers),
                [spec.symbol for spec in self.specs],
                self.timeframes,
                self.status.describe(),
            )
            # Last, so "UP" means the feeds are actually subscribed and Redis,
            # Postgres and NATS all answered.
            await self.status.announce_started(
                {
                    "Book": self._scope.namespace,
                    "Providers": list(self.providers),
                    "Symbols": [spec.symbol for spec in self.specs],
                    "Timeframes": self.timeframes,
                }
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

    # -- Control plane ------------------------------------------------

    async def _on_control_message(self, msg) -> None:
        """Answer the operator: what is feeding, warm this up, flush that.

        Request/reply over NATS, the same shape the runner answers on its own
        subject. These three actions have to be served *here* rather than by
        whoever asked, because ingestion owns both halves of the state they
        touch: the vendor request protocol, and the staging watermark that
        decides whether a replayed bar counts as a duplicate.
        """
        try:
            command = json.loads(msg.data)
        except json.JSONDecodeError:
            log.warning("Unparseable control message: %.120r", msg.data)
            return

        action = command.get("action")
        if action in {"ping", "status"}:
            await self._reply(msg, await self._status_payload())
        elif action == "warmup":
            await self._reply(msg, await self._warmup_on_request(command.get("symbols")))
        elif action == "flush":
            await self._reply(msg, await self._flush_on_request(command.get("symbols")))
        else:
            log.warning("Unknown ingestion control action: %r", action)

    async def _reply(self, msg, payload: dict) -> None:
        if msg.reply:
            await self.bus.nc.publish(msg.reply, json.dumps(payload, default=str).encode())

    async def _status_payload(self) -> dict:
        """What is subscribed, and how full each series' window is in Redis."""
        series = []
        for feed in self.subscriptions:
            for timeframe in feed.timeframes:
                held = await self.state.count_candles(feed.symbol, timeframe)
                newest = await self._newest_bar(feed.symbol, timeframe)
                series.append(
                    {
                        "symbol": feed.symbol,
                        "timeframe": timeframe,
                        "provider": feed.provider or self._scope.provider,
                        "market": feed.market,
                        "bars": held,
                        # What a full window is: the same figure the
                        # backfiller fills to.
                        "target": settings.redis.candle_history,
                        "newest_bar": newest.open_time.isoformat() if newest else None,
                        "owed": (feed.symbol, timeframe) in self._history_owed,
                    }
                )
        return {
            "service": SERVICE_NAME,
            "namespace": self._scope.namespace,
            "providers": list(self.providers),
            "vendor_offline": self._vendor_offline,
            "series": series,
        }

    async def _newest_bar(self, symbol: str, timeframe: str) -> Candle | None:
        held = await self.state.get_candles(symbol, timeframe, count=1)
        return held[-1] if held else None

    def _requested_series(self, symbols) -> tuple[list[tuple[SymbolSpec, str]], list[str]]:
        """Resolve a symbol list against what is actually subscribed.

        ``None`` or an empty list means every subscribed series -- the ``all``
        the bot offers. A name nothing feeds is reported back rather than
        silently ignored: it is almost always a typo, and pretending to have
        warmed it would be worse than saying so.
        """
        wanted = {str(name).upper() for name in symbols} if symbols else None
        resolved: list[tuple[SymbolSpec, str]] = []
        for feed in self.subscriptions:
            if wanted is not None and feed.symbol.upper() not in wanted:
                continue
            for timeframe in feed.timeframes:
                resolved.append((feed.spec, timeframe))
        known = {feed.symbol.upper() for feed in self.subscriptions}
        unknown = sorted(wanted - known) if wanted else []
        return resolved, unknown

    async def _warmup_on_request(self, symbols) -> dict:
        """Ask the vendor for these windows again, now, and report what came back.

        Forced: an operator asking by hand wants the request to go out even
        when Redis looks full, because what they are usually checking is
        whether what it holds is right.
        """
        resolved, unknown = self._requested_series(symbols)
        warmed: list[dict] = []
        for spec, timeframe in resolved:
            backfiller = self._backfiller_for(spec.symbol)
            if backfiller is None:
                warmed.append(
                    {
                        "symbol": spec.symbol,
                        "timeframe": timeframe,
                        "error": "no history source is configured for this symbol",
                    }
                )
                continue
            before = await self.state.count_candles(spec.symbol, timeframe)
            try:
                await backfiller.backfill_series(spec, timeframe, force=True)
            except asyncio.CancelledError:
                raise
            except Exception as failure:
                log.warning(
                    "Requested warm-up of %s %s failed: %s", spec.symbol, timeframe, failure
                )
                warmed.append(
                    {"symbol": spec.symbol, "timeframe": timeframe, "error": str(failure)}
                )
                continue
            after = await self.state.count_candles(spec.symbol, timeframe)
            self._history_owed.pop((spec.symbol, timeframe), None)
            warmed.append(
                {
                    "symbol": spec.symbol,
                    "timeframe": timeframe,
                    "bars_before": before,
                    "bars": after,
                }
            )
        return {"warmed": warmed, "unknown": unknown}

    async def _flush_on_request(self, symbols) -> dict:
        """Drop these windows from Redis, and forget what was built on them.

        Three things go together or none of them does: the stored bars, the
        staging watermark that `discard_candle_state` clears with them, and the
        resampler's in-flight bucket. A window flushed while its resampler
        still holds a half-built bar would publish that bar next, as a close
        with no history behind it; re-marking the join point drops it instead.
        """
        resolved, unknown = self._requested_series(symbols)
        flushed: list[dict] = []
        joined_at = datetime.now(UTC)
        for spec, timeframe in resolved:
            held = await self.state.count_candles(spec.symbol, timeframe)
            await self.state.discard_candle_state(spec.symbol, timeframe)
            self._history_owed[(spec.symbol, timeframe)] = spec
            self._gap_asked_at.pop((spec.symbol, timeframe), None)
            flushed.append({"symbol": spec.symbol, "timeframe": timeframe, "dropped": held})
        for spec, _ in resolved:
            resampler = self._resamplers.get(spec.symbol)
            if resampler is not None:
                resampler.mark_joined(joined_at)
        if flushed:
            log.warning(
                "Flushed %d window(s) on request: %s",
                len(flushed),
                ", ".join(f"{row['symbol']} {row['timeframe']}" for row in flushed),
            )
        return {"flushed": flushed, "unknown": unknown}

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
        history_task, self._history_task = getattr(self, "_history_task", None), None
        if history_task is not None:
            history_task.cancel()
            try:
                await history_task
            except asyncio.CancelledError:
                pass
            except Exception:
                cleanup_failed = True
                log.exception("History task cleanup failed during ingestion shutdown")
        for backfiller in getattr(self, "_backfillers", {}).values():
            try:
                await backfiller.close()
            except Exception:
                cleanup_failed = True
                log.exception("Closing a history source failed during ingestion shutdown")
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
        # Announced on every shutdown, including one that began with a failed
        # start, and awaited rather than queued — see the runner for why.
        with contextlib.suppress(Exception):
            await self.status.announce_stopped()
        for close in (
            self.telegram_errors.stop,
            self.status.aclose,
            self.state.close,
            self.bus.close,
        ):
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
        await self._fill_gap_before(candle)
        async with self._processing_guard():
            # Retained before any await inside, so a Redis failure leaves this
            # bar with the flush loop to retry instead of losing it.
            await self._emit_candles([candle])

    # ── History from a vendor that is another process ─────────────────

    def _backfiller_for(self, symbol: str) -> HistoryBackfiller | None:
        """The backfiller that can re-ask *symbol*'s history, if any can."""
        backfillers = getattr(self, "_backfillers", None)
        if not backfillers:
            return None
        provider_name = self._symbol_providers.get(symbol)
        if not provider_name and len(backfillers) == 1:
            provider_name = next(iter(backfillers))
        backfiller = backfillers.get(provider_name or "")
        return backfiller if backfiller is not None and backfiller.retryable else None

    def _spec_for(self, symbol: str) -> SymbolSpec | None:
        return next((feed.spec for feed in self.subscriptions if feed.symbol == symbol), None)

    async def _watch_vendor(self, provider_name: str, backfiller: HistoryBackfiller) -> None:
        """Have *provider_name*'s vendor wake the history loop when it connects."""

        async def vendor_online() -> None:
            await self._handle_vendor_online(provider_name)

        try:
            watching = await backfiller.watch_online(vendor_online)
        except Exception as failure:
            log.warning(
                "Cannot listen for %r announcing itself (%s) — its windows are checked "
                "again only when a live bar arrives, or by the probe if one is configured",
                provider_name,
                failure,
            )
            return
        if watching:
            log.info("Listening for %r to announce that it is connected", provider_name)

    async def _handle_vendor_online(self, provider_name: str) -> None:
        """The vendor says it is connected: check every one of its windows.

        Every planned series is queued, not only the ones owed from boot. The
        vendor may have been down across closes it will never publish, and the
        check itself is one Redis read per series: a window that is full and
        current is left alone without a request being sent.
        """
        self._vendor_offline = False
        queued = []
        for feed in self._subscriptions_of(provider_name):
            for timeframe in feed.timeframes:
                self._history_owed[(feed.symbol, timeframe)] = feed.spec
                queued.append(f"{feed.symbol} {timeframe}")
        log.info(
            "%r is connected — checking Redis for %s and requesting the windows that are short",
            provider_name,
            ", ".join(queued) or "nothing planned",
        )
        self._history_wake.set()

    async def _warm_series(
        self, spec: SymbolSpec, timeframe: str, *, before: datetime | None = None
    ) -> bool:
        """Check one series against Redis and ask for it if short.

        True when there is nothing left to ask. That covers a final refusal as
        well as an answer: a symbol the vendor does not carry is settled, and
        asking again would only repeat the log.
        """
        series = (spec.symbol, timeframe)
        backfiller = self._backfiller_for(spec.symbol)
        if backfiller is None:
            self._history_owed.pop(series, None)
            return True
        try:
            await backfiller.backfill_series(spec, timeframe, before=before)
        except asyncio.CancelledError:
            raise
        except HistoryOffline as silence:
            self._vendor_offline = True
            log.warning(
                "History for %s %s cannot be asked for: %s. Waiting for the vendor to "
                "announce itself.",
                spec.symbol,
                timeframe,
                silence,
            )
            return False
        except HistoryNotServed as refusal:
            log.warning(
                "No history for %s %s: %s. Its window fills from live closes only.",
                spec.symbol,
                timeframe,
                refusal,
            )
        except Exception as failure:
            log.warning(
                "History for %s %s is still not available: %s", spec.symbol, timeframe, failure
            )
            return False
        self._history_owed.pop(series, None)
        return True

    async def _history_loop(self) -> None:
        """Check and request every series that is owed its history.

        Three waits, for three situations. Nothing owed: sleep until something
        is. The vendor not connected: sleep until it announces itself, with no
        timer (``history_offline_probe_interval`` adds one for a vendor that
        cannot announce). The vendor connected but not ready (a terminal still
        logging in): ask again shortly, backing off.

        The merge each request ends in is checked against the staging
        watermark, so it is safe beside the live path.
        """
        delay = ingestion_settings.history_retry_interval
        while True:
            if not self._history_owed:
                self._history_wake.clear()
                await self._history_wake.wait()
                delay = ingestion_settings.history_retry_interval
            elif self._vendor_offline:
                self._history_wake.clear()
                probe = ingestion_settings.history_offline_probe_interval
                try:
                    await asyncio.wait_for(self._history_wake.wait(), timeout=probe or None)
                except TimeoutError:
                    log.info("Probing for the history vendor (no announcement heard)")
                delay = ingestion_settings.history_retry_interval
            elif self._history_wake.is_set():
                # Woken on purpose — an announcement, a gap: act now.
                self._history_wake.clear()
            else:
                await asyncio.sleep(delay)
                delay = min(delay * 2, ingestion_settings.history_retry_max_interval)
            for (_, timeframe), spec in list(self._history_owed.items()):
                settled = await self._warm_series(spec, timeframe)
                if self._vendor_offline:
                    # Nobody to ask: the rest would hear the same silence.
                    break
                if settled:
                    log.info(
                        "History for %s %s settled; %d series still owed",
                        spec.symbol,
                        timeframe,
                        len(self._history_owed),
                    )

    async def _fill_gap_before(self, candle: Candle) -> None:
        """Fetch the bars a live close does not follow, before it is staged.

        The ingester publishes nothing it found already closed when it started,
        so after it has been down the next bar arrives with the closes in
        between missing — and nothing else would ever fetch them. Asked for
        here, ahead of staging, the runner reads a whole window when this
        bar's event reaches it. Only bars older than *candle* are merged: the
        merge raises the staging watermark to its newest bar, and one that
        included *candle* would have it dropped as its own duplicate.

        A market's session break looks the same and costs one request, which
        comes back with nothing new. A failure never holds the bar back: it is
        staged as it is and the series is left with the retry loop.
        """
        backfiller = self._backfiller_for(candle.symbol)
        if backfiller is None:
            return
        newest = await self.state.get_candles(candle.symbol, candle.timeframe, count=1)
        step = timedelta(seconds=timeframe_seconds(candle.timeframe))
        if newest and candle.open_time - newest[-1].open_time <= step:
            # The next bucket, or one already held: nothing is missing.
            return
        series = (candle.symbol, candle.timeframe)
        moment = datetime.now(UTC)
        asked_at = self._gap_asked_at.get(series)
        cooldown = ingestion_settings.history_gap_cooldown
        if asked_at is not None and (moment - asked_at).total_seconds() < cooldown:
            return
        self._gap_asked_at[series] = moment
        spec = self._spec_for(candle.symbol)
        if spec is None:
            return
        log.info(
            "%s %s bar open_time=%s does not follow the newest stored bar (%s) — asking for "
            "the bars in between before it is staged",
            candle.symbol,
            candle.timeframe,
            candle.open_time.isoformat(),
            newest[-1].open_time.isoformat() if newest else "none held",
        )
        if not await self._warm_series(spec, candle.timeframe, before=candle.open_time):
            self._history_owed[series] = spec
            if not self._vendor_offline:
                self._history_wake.set()

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
            opens_batch = series not in self._warmup_batches
            buffered = self._warmup_batches.setdefault(series, {})
            buffered[candle.open_time] = candle
            if opens_batch:
                log.info(
                    "Warm-up batch opened %s %s: first bar is %d/%d, open_time=%s; Redis "
                    "holds %d bar(s) now. Buffering in memory until bar %d/%d arrives",
                    candle.symbol,
                    candle.timeframe,
                    batch.index,
                    batch.total,
                    candle.open_time.isoformat(),
                    await self.state.count_candles(candle.symbol, candle.timeframe),
                    batch.total,
                    batch.total,
                )
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
            ordered = sorted(pending)
            held_before = await self.state.count_candles(candle.symbol, candle.timeframe)
            log.log(
                logging.INFO if len(pending) >= batch.total else logging.WARNING,
                "Warm-up batch closed %s %s on bar %d/%d: %d of %d open time(s) buffered%s, span "
                "%s..%s. Merging into Redis by open time (window held %d)",
                candle.symbol,
                candle.timeframe,
                batch.index,
                batch.total,
                len(pending),
                batch.total,
                "" if len(pending) >= batch.total else " — FEWER THAN ANNOUNCED",
                ordered[0].isoformat(),
                ordered[-1].isoformat(),
                held_before,
            )
            held_after = await self.state.merge_history_candles(
                candle.symbol, candle.timeframe, [pending[moment] for moment in ordered]
            )
            if not held_after:
                # The merge gave up and said why; the window is as it was.
                return
            log.info(
                "Warm-up merge done %s %s: Redis window %d -> %d bar(s) (%+d new open "
                "times, %d overwritten in place)",
                candle.symbol,
                candle.timeframe,
                held_before,
                held_after,
                held_after - held_before,
                len(pending) - (held_after - held_before),
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
