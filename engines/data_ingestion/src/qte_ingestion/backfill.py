"""Fill Redis with vendor history before the live feed opens.

The runner warms its indicator window from Redis and nothing else, so on a cold
cache a restarted engine sees only the bars that have printed since ingestion
came up. A strategy wanting 200 bars of EMA on M15 therefore sits idle for two
days after a deploy, and one wanting a fair-value gap from last month never
sees it at all -- the bar it needs was never in the cache to begin with.

This module closes that gap at boot: for every symbol and timeframe, if Redis
holds fewer bars than ``QTE_REDIS__CANDLE_HISTORY``, the missing history is
fetched from the provider and the list is rewritten whole.

Three limits are deliberate:

* **It runs only when the configured provider serves history.** The simulator
  does not, and pointing a dev stack at it is exactly when nobody wants a
  vendor request. That stack warms itself by hand instead --
  ``make warmup-cache`` replays the parquet the last real fetch left behind.
* **It never fails the service.** A vendor outage at boot must not stop
  ingestion from recording the market; a short warm-up window is recoverable,
  a dead feed is not.
* **It writes what it fetched to the history cache**, so the next boot in a dev
  loop costs nothing. That needs ``pyarrow``, which this engine declares as the
  optional ``history-cache`` extra rather than a dependency -- it is 152 MB and
  a production image has no use for it, because ``QTE_ENV=prod`` bypasses the
  cache anyway. Without it the warm-up still runs, it just always asks the
  vendor. See :mod:`qte_shared.history_cache`.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pandas as pd
from qte_shared.config import settings
from qte_shared.history_cache import HistoryCache, fetch_history
from qte_shared.interfaces.market_data import (
    Capability,
    HistoryRequest,
    ProviderError,
    UnsupportedCapability,
)
from qte_shared.logging_setup import get_logger
from qte_shared.models import Candle
from qte_shared.providers import create_provider
from qte_shared.symbols import SymbolSpec
from qte_shared.timeframes import timeframe_seconds

from qte_ingestion.settings import ingestion_settings

log = get_logger(__name__)

#: FX and CFD venues are shut roughly two days in seven. A window sized on
#: calendar days alone would come back a third short of the bars asked for.
_CALENDAR_TO_SESSION_RATIO = 7 / 5

_SECONDS_PER_DAY = 86_400


class HistoryBackfiller:
    """Tops Redis up to the configured candle history from the vendor."""

    def __init__(
        self,
        state,
        specs: list[SymbolSpec],
        timeframes: list[str],
        cache: HistoryCache | None = None,
    ) -> None:
        self.state = state
        self.specs = specs
        self.timeframes = timeframes
        self.target = settings.redis.candle_history
        # Injectable so a test can point the parquet cache at a temporary
        # directory instead of the repository's own data/.
        self.cache = cache

    async def run(self) -> None:
        """Warm every symbol and timeframe. Never raises."""
        if not ingestion_settings.backfill_history:
            log.info("History backfill disabled (QTE_INGESTION__BACKFILL_HISTORY=false)")
            return

        source = self._history_source()
        if source is None:
            return

        cache = self.cache or HistoryCache(settings.market_data.provider)
        for spec in self.specs:
            for timeframe in self.timeframes:
                try:
                    await self._backfill_one(source, cache, spec, timeframe)
                except Exception:
                    # One bad symbol must not cost the others their warm-up,
                    # and none of them may cost the service its start.
                    log.exception(
                        "History backfill failed for %s %s — starting on what Redis has",
                        spec.symbol,
                        timeframe,
                    )

    # -- One symbol / timeframe -------------------------------------------

    async def _backfill_one(
        self, source, cache: HistoryCache, spec: SymbolSpec, timeframe: str
    ) -> None:
        held = await self.state.count_candles(spec.symbol, timeframe)
        if held >= self.target:
            log.info(
                "Warm-up not needed %s %s: Redis holds %d/%d bars",
                spec.symbol,
                timeframe,
                held,
                self.target,
            )
            return

        log.info(
            "Warming %s %s from %s: Redis holds %d/%d bars",
            spec.symbol,
            timeframe,
            settings.market_data.provider,
            held,
            self.target,
        )
        request = self._request_for(spec, timeframe)
        frame = await fetch_history(source, request, cache=cache)
        if frame.empty:
            log.warning(
                "Provider returned no history for %s %s %s..%s",
                spec.symbol,
                timeframe,
                request.start,
                request.end,
            )
            return

        fetched = _frame_to_candles(frame, spec.symbol, timeframe)
        existing = await self.state.get_candles(spec.symbol, timeframe)
        merged = _merge_candles(existing, fetched)
        written = await self.state.replace_candles(
            spec.symbol, timeframe, merged, max_len=self.target
        )
        log.info(
            "Warmed %s %s: %d bars in Redis (%d fetched, %d already held), span %s..%s",
            spec.symbol,
            timeframe,
            written,
            len(fetched),
            held,
            merged[-written].open_time.isoformat() if written else "-",
            merged[-1].open_time.isoformat(),
        )

    def _request_for(self, spec: SymbolSpec, timeframe: str) -> HistoryRequest:
        """A range wide enough to yield ``target`` bars on a part-time market."""
        end = datetime.now(UTC).date()
        return HistoryRequest(
            symbol=spec.symbol,
            timeframe=timeframe,
            start=end - self._span_for(timeframe, spec.market),
            end=end,
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

    def _history_source(self):
        """The configured provider's history source, or ``None`` to skip.

        Gated on the capability rather than on the vendor's name: any provider
        that can serve history should warm the cache, and the one that cannot
        is exactly the one a developer chose to avoid the network.
        """
        try:
            provider = create_provider(capability=Capability.HISTORY)
            # Building the source is where a missing API key surfaces, so it
            # belongs inside the same guard as the capability check.
            return provider.history_source()
        except UnsupportedCapability:
            # The expected, deliberate case: someone pointed a dev stack at the
            # simulator precisely so nothing would reach the network.
            log.info(
                "Provider %r serves no history — skipping backfill. Warm the engine "
                "by hand instead (make warmup-cache).",
                settings.market_data.provider,
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
                settings.market_data.provider,
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
    """
    by_open_time: dict[datetime, Candle] = {candle.open_time: candle for candle in fetched}
    by_open_time.update({candle.open_time: candle for candle in existing})
    return [by_open_time[key] for key in sorted(by_open_time)]


__all__ = ["HistoryBackfiller"]
