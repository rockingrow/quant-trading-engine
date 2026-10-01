"""Fill Redis with vendor history before the live feed opens.

The runner warms its indicator window from Redis and nothing else, so on a cold
cache a restarted engine sees only the bars that have printed since ingestion
came up. A strategy wanting 200 bars of EMA on M15 therefore sits idle for two
days after a deploy, and one wanting a fair-value gap from last month never
sees it at all -- the bar it needs was never in the cache to begin with.

This module closes that gap at boot: for every symbol and timeframe, if Redis
holds fewer bars than ``QTE_REDIS__CANDLE_HISTORY``, the missing history is
fetched from the provider and the list is rewritten whole. A list that is full
but *stale* -- its newest bar older than the last completed bucket, because
ingestion was down across one or more closes -- is topped up from that newest
bar instead. Counting alone cannot see an outage: the list is just as long
before it as after.

Four limits are deliberate:

* **It runs only when the provider serves history.** With several providers
  configured, each warms the symbols of its own plan from its own history. The simulator
  does not, and pointing a dev stack at it is exactly when nobody wants a
  vendor request. That stack warms itself by hand instead --
  ``make warmup-cache`` replays the parquet the last real fetch left behind.
* **It never fails the service.** A vendor outage at boot must not stop
  ingestion from recording the market; a short warm-up window is recoverable,
  a dead feed is not.
* **It never stores a bar whose bucket has not ended.** The vendor's answer for
  today includes the bucket still forming; written as closed, that snapshot
  would shadow the live close of the same bucket.
* **It writes what it fetched to the history cache**, so the next boot in a dev
  loop costs nothing. That needs ``pyarrow``, which this engine declares as the
  optional ``history-cache`` extra rather than a dependency -- it is 152 MB and
  a production image has no use for it, because ``QTE_ENV=prod`` bypasses the
  cache anyway. Without it the warm-up still runs, it just always asks the
  vendor. See :mod:`qte_shared.history_cache`. A top-up never reads the cache:
  the bars it asks for are the ones a cached file cannot hold yet.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import pandas as pd

from qte_ingestion.settings import ingestion_settings
from qte_shared.config import provider_market_data_plan, settings
from qte_shared.history_cache import HistoryCache, fetch_history
from qte_shared.interfaces.market_data import (
    Capability,
    HistoryRequest,
    HistorySource,
    ProviderError,
    UnsupportedCapability,
    drop_unfinished_bars,
)
from qte_shared.logging_setup import get_logger
from qte_shared.market_data_plan import SymbolFeed
from qte_shared.models import Candle
from qte_shared.providers import create_provider
from qte_shared.state_scope import stamp_market_data
from qte_shared.symbols import SymbolSpec
from qte_shared.timeframes import floor_to_bucket, timeframe_seconds

log = get_logger(__name__)

#: FX and CFD venues are shut roughly two days in seven. A window sized on
#: calendar days alone would come back a third short of the bars asked for.
_CALENDAR_TO_SESSION_RATIO = 7 / 5

_SECONDS_PER_DAY = 86_400


def open_history_source(provider_name: str | None = None) -> HistorySource:
    """*provider_name*'s history client; the configured provider's when omitted.

    Raises :class:`UnsupportedCapability` when the provider serves no history,
    as the simulator does, and :class:`ProviderError` when it should but cannot
    -- a missing API key. Each caller decides which of the two deserves a
    warning.
    """
    return create_provider(provider_name, capability=Capability.HISTORY).history_source()


class HistoryBackfiller:
    """Tops Redis up to the configured candle history from one provider."""

    def __init__(
        self,
        state,
        subscriptions: list[SymbolFeed],
        cache: HistoryCache | None = None,
        utc_clock: Callable[[], datetime] | None = None,
        provider_name: str | None = None,
    ) -> None:
        self.state = state
        #: The provider whose history warms *subscriptions*. Omitted means the
        #: only configured one; with several, each gets a backfiller of its own.
        self.provider_name = provider_name or _sole_provider()
        # One list of (symbol, market, timeframes) rather than a symbol list
        # crossed with a timeframe list: warming a pair nothing resamples costs
        # a vendor request against a rate-limited plan and fills Redis with a
        # window no strategy reads.
        self.subscriptions = subscriptions
        self.target = settings.redis.candle_history
        # Injectable so a test can point the parquet cache at a temporary
        # directory instead of the repository's own data/.
        self.cache = cache
        #: What "now" is when deciding which buckets have completed. Injectable
        #: so a test can pin it.
        self.utc_clock = utc_clock or (lambda: datetime.now(UTC))

    async def run(self) -> None:
        """Warm every symbol and timeframe. Never raises."""
        if not self._enabled():
            log.info(
                "History backfill disabled for %r ([provider].backfill_history = false)",
                self.provider_name,
            )
            return

        source = self._history_source()
        if source is None:
            return

        cache = self.cache or HistoryCache(self.provider_name)
        for feed in self.subscriptions:
            for timeframe in feed.timeframes:
                try:
                    await self._backfill_one(source, cache, feed.spec, timeframe)
                except Exception:
                    # One bad symbol must not cost the others their warm-up,
                    # and none of them may cost the service its start.
                    log.exception(
                        "History backfill failed for %s %s — starting on what Redis has",
                        feed.symbol,
                        timeframe,
                    )

    # -- One symbol / timeframe -------------------------------------------

    async def _backfill_one(
        self, source, cache: HistoryCache, spec: SymbolSpec, timeframe: str
    ) -> None:
        moment = self.utc_clock()
        provider_name = self.provider_name
        held_count = await self.state.count_candles(spec.symbol, timeframe)
        newest = await self._newest_held(spec.symbol, timeframe) if held_count else None
        last_completed = floor_to_bucket(moment, timeframe) - timedelta(
            seconds=timeframe_seconds(timeframe)
        )
        topping_up = held_count >= self.target and newest is not None

        if topping_up and newest.open_time >= last_completed:
            log.info(
                "Warm-up not needed %s %s: Redis holds %d/%d bars, newest %s",
                spec.symbol,
                timeframe,
                held_count,
                self.target,
                newest.open_time.isoformat(),
            )
            return

        if topping_up:
            log.info(
                "Topping up %s %s from %s: Redis holds %d/%d bars but the newest opened %s, "
                "before the last completed bucket %s",
                spec.symbol,
                timeframe,
                provider_name,
                held_count,
                self.target,
                newest.open_time.isoformat(),
                last_completed.isoformat(),
            )
            request = HistoryRequest(
                symbol=spec.symbol,
                timeframe=timeframe,
                start=newest.open_time.date(),
                end=moment.date(),
                market=spec.market,
            ).normalized()
        else:
            log.info(
                "Warming %s %s from %s: Redis holds %d/%d bars",
                spec.symbol,
                timeframe,
                provider_name,
                held_count,
                self.target,
            )
            request = self._request_for(spec, timeframe, moment)

        # A top-up asks for exactly the bars a cached file cannot hold yet, and
        # the cache's coverage tolerance would answer it with none of them.
        history = await fetch_history(
            source, request, cache=cache, use_cache=False if topping_up else None
        )
        origin = settings.state_scope.origin(provider=provider_name)
        fetched = [
            stamp_market_data(candle, origin)
            for candle in _frame_to_candles(
                drop_unfinished_bars(history, timeframe, moment), spec.symbol, timeframe
            )
        ]
        if not fetched:
            if topping_up:
                log.info(
                    "No newer %s %s bars from %s for %s..%s — the market was closed",
                    spec.symbol,
                    timeframe,
                    provider_name,
                    request.start,
                    request.end,
                )
            else:
                log.warning(
                    "Provider returned no history for %s %s %s..%s",
                    spec.symbol,
                    timeframe,
                    request.start,
                    request.end,
                )
            return

        existing = await self.state.get_candles(spec.symbol, timeframe)
        merged = _merge_candles(existing, fetched)
        if merged == existing:
            log.info(
                "Warm-up of %s %s unchanged: Redis already holds every bar %s returned",
                spec.symbol,
                timeframe,
                provider_name,
            )
            return
        written = await self.state.replace_candles(
            spec.symbol, timeframe, merged, max_len=self.target
        )
        log.info(
            "Warmed %s %s: %d bars in Redis (%d fetched, %d already held), span %s..%s",
            spec.symbol,
            timeframe,
            written,
            len(fetched),
            held_count,
            merged[-written].open_time.isoformat() if written else "-",
            merged[-1].open_time.isoformat(),
        )

    async def _newest_held(self, symbol: str, timeframe: str) -> Candle | None:
        newest_bars = await self.state.get_candles(symbol, timeframe, count=1)
        return newest_bars[-1] if newest_bars else None

    def _request_for(self, spec: SymbolSpec, timeframe: str, moment: datetime) -> HistoryRequest:
        """A range wide enough to yield ``target`` bars on a part-time market."""
        last_day = moment.date()
        return HistoryRequest(
            symbol=spec.symbol,
            timeframe=timeframe,
            start=last_day - self._span_for(timeframe, spec.market),
            end=last_day,
            market=spec.market,
        ).normalized()

    def _span_for(self, timeframe: str, market: str) -> timedelta:
        bars_per_session_day = _SECONDS_PER_DAY / timeframe_seconds(timeframe)
        days = self.target / bars_per_session_day
        if market != "crypto":
            days *= _CALENDAR_TO_SESSION_RATIO
        # A whole extra week absorbs public holidays at either end of the span.
        return timedelta(days=int(days) + 7)

    # -- Provider ----------------------------------------------------------

    def _enabled(self) -> bool:
        """``QTE_INGESTION__BACKFILL_HISTORY`` for everyone, else this provider's plan."""
        if not ingestion_settings.backfill_history:
            return False
        if "backfill_history" in ingestion_settings.model_fields_set:
            return True
        return bool(provider_market_data_plan(self.provider_name).option("backfill_history", True))

    def _history_source(self):
        """The configured provider's history source, or ``None`` to skip.

        Gated on the capability rather than on the vendor's name: any provider
        that can serve history should warm the cache, and the one that cannot
        is exactly the one a developer chose to avoid the network.
        """
        try:
            # Building the source is where a missing API key surfaces, so it
            # belongs inside the same guard as the capability check.
            return open_history_source(self.provider_name)
        except UnsupportedCapability:
            # The expected, deliberate case: someone pointed a dev stack at the
            # simulator precisely so nothing would reach the network.
            log.info(
                "Provider %r serves no history — skipping backfill. Warm the engine "
                "by hand instead (make warmup-cache).",
                self.provider_name,
            )
            return None
        except ProviderError as error:
            # Anything else is a misconfiguration of a provider that was
            # supposed to work: a missing API key, an unknown name. Redis stays
            # cold and the runner will not trade until it fills, so this must
            # not read like the deliberate skip above.
            log.warning(
                "History backfill unavailable for provider %r: %s. Redis will not be "
                "warmed and the runner has no indicator window until enough live "
                "bars have printed.",
                self.provider_name,
                error,
            )
            return None


def _frame_to_candles(frame: pd.DataFrame, symbol: str, timeframe: str) -> list[Candle]:
    """Canonical OHLCV rows to closed candles, oldest first.

    ``tick_count`` stays 0: these bars were resampled by the vendor and no tick
    count survives that, and inventing one would make a backfilled bar
    indistinguishable from one this engine built itself.
    """
    return [
        Candle(
            symbol=symbol,
            timeframe=timeframe,
            open_time=index.to_pydatetime(),
            open=float(row.open),
            high=float(row.high),
            low=float(row.low),
            close=float(row.close),
            volume=float(row.volume),
            tick_count=0,
            is_closed=True,
        )
        for index, row in zip(frame.index, frame.itertuples(index=False), strict=True)
    ]


def _merge_candles(existing: list[Candle], fetched: list[Candle]) -> list[Candle]:
    """Union by open time, oldest first, preferring the bar this engine built.

    A bar ingestion resampled from live ticks is the more trustworthy of the
    two: it is what the strategy already acted on, and replacing it with the
    vendor's rounding of the same minute would make a restart change history.

    A held *vendor* bar -- ``tick_count`` 0 -- has no such claim. It may be a
    snapshot of a bucket that was still forming when it was fetched, and the
    answer just received is at least as complete, so the fresh one wins.
    """
    by_open_time: dict[datetime, Candle] = {candle.open_time: candle for candle in fetched}
    for candle in existing:
        if candle.tick_count > 0 or candle.open_time not in by_open_time:
            by_open_time[candle.open_time] = candle
    return [by_open_time[open_time] for open_time in sorted(by_open_time)]


def _sole_provider() -> str:
    """The configured provider, when there is exactly one."""
    providers = settings.market_data.providers
    if len(providers) > 1:
        raise ValueError(
            f"{len(providers)} providers are configured ({', '.join(providers)}); "
            "name the one whose history to backfill"
        )
    return providers[0]


__all__ = ["HistoryBackfiller", "open_history_source"]
