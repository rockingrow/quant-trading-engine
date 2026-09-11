"""Complete a bar ingestion only saw part of, from vendor history.

Ingestion builds a bar from the ticks it receives, so a bucket it joined late —
the process started, or restarted, after the bucket opened — closes with the
ticks from that moment on and none from before. Published as it stands, that bar
carries the wrong open and misses whatever high or low printed while nobody was
listening, and the strategy decides on it as though it were whole.

When the configured provider serves history, the vendor has the whole bucket.
:class:`PartialBarRepairer` fetches that one bar as soon as ingestion closes the
partial one, and merges the two:

* **open** from the vendor — the engine never saw it;
* **high / low** as the extremes of both — either side may have missed a print;
* **close** from the engine while it was still listening when the bucket ended,
  because its last tick is the freshest price — and from the vendor when it was
  not: a bar restored after an outage that outlasted its bucket heard its last
  tick before the outage began;
* **volume and tick count** from the engine — they describe what this process
  actually received.

A repair that cannot happen — a provider with no history, a vendor error, a
vendor slower than :data:`REPAIR_TIMEOUT_SECONDS`, a bucket the vendor has no
bar for yet — publishes the bar as it was built. Withholding it would cost the
strategy that decision outright; the log says which bar went out incomplete. The
timeout matters because the repair runs on the path that publishes closes and,
for a tick-driven close, reads the socket.
"""

from __future__ import annotations

import asyncio

import pandas as pd

from qte_shared.interfaces.market_data import HistoryRequest, HistorySource
from qte_shared.logging_setup import get_logger
from qte_shared.models import Candle
from qte_shared.symbols import Market

log = get_logger(__name__)

#: How long a close waits for the vendor's bar. A Tiingo single-day request
#: measured 1.3-1.9 s; anything much slower costs the socket more than a
#: partial bar costs the strategy.
REPAIR_TIMEOUT_SECONDS = 5.0


class PartialBarRepairer:
    """Completes partial bars from the configured provider's history."""

    def __init__(
        self,
        source: HistorySource | None,
        markets: dict[str, Market],
        timeout_seconds: float = REPAIR_TIMEOUT_SECONDS,
    ) -> None:
        #: ``None`` when the provider serves no history, as the simulator does.
        self.source = source
        #: Symbol to market: a history request has to name the endpoint to ask.
        self.markets = dict(markets)
        self.timeout_seconds = timeout_seconds

    async def repair(self, candle: Candle, *, close_is_current: bool = True) -> Candle:
        """The whole bar when the vendor has it, otherwise *candle* unchanged.

        *close_is_current* says whether this process was still listening when the
        bucket ended. When it was not, the vendor's close replaces the engine's.
        """
        if self.source is None:
            log.info(
                "Published partial bar %s %s open_time=%s as built: the provider serves no "
                "history to complete it from",
                candle.symbol,
                candle.timeframe,
                candle.open_time.isoformat(),
            )
            return candle
        try:
            vendor_bar = await asyncio.wait_for(
                self._vendor_bar(candle), timeout=self.timeout_seconds
            )
        except Exception as failure:
            log.warning(
                "Published partial bar %s %s open_time=%s unrepaired: fetching the vendor's "
                "bar failed (%r). Its open, high and low cover only the ticks this process "
                "received.",
                candle.symbol,
                candle.timeframe,
                candle.open_time.isoformat(),
                failure,
            )
            return candle
        if vendor_bar is None:
            log.warning(
                "Published partial bar %s %s open_time=%s unrepaired: the vendor returned no "
                "bar for that bucket. Its open, high and low cover only the ticks this process "
                "received.",
                candle.symbol,
                candle.timeframe,
                candle.open_time.isoformat(),
            )
            return candle

        repaired = merge_partial_bar(candle, vendor_bar, close_is_current=close_is_current)
        log.info(
            "Repaired partial bar %s %s open_time=%s from vendor history: open %s -> %s, "
            "high %s -> %s, low %s -> %s, close %s -> %s",
            candle.symbol,
            candle.timeframe,
            candle.open_time.isoformat(),
            candle.open,
            repaired.open,
            candle.high,
            repaired.high,
            candle.low,
            repaired.low,
            candle.close,
            repaired.close,
        )
        return repaired

    async def _vendor_bar(self, candle: Candle) -> Candle | None:
        """The vendor's bar for *candle*'s bucket, or ``None`` when it has none."""
        market = self.markets.get(candle.symbol)
        if market is None:
            raise LookupError(f"no market is configured for {candle.symbol}")
        bucket_day = candle.open_time.date()
        history = await self.source.fetch(
            HistoryRequest(
                symbol=candle.symbol,
                timeframe=candle.timeframe,
                start=bucket_day,
                end=bucket_day,
                market=market,
            )
        )
        bucket_open = pd.Timestamp(candle.open_time)
        if history.empty or bucket_open not in history.index:
            return None
        vendor_row = history.loc[bucket_open]
        return Candle(
            symbol=candle.symbol,
            timeframe=candle.timeframe,
            open_time=candle.open_time,
            open=float(vendor_row["open"]),
            high=float(vendor_row["high"]),
            low=float(vendor_row["low"]),
            close=float(vendor_row["close"]),
            volume=float(vendor_row["volume"]),
            tick_count=0,
            is_closed=True,
        )


def merge_partial_bar(
    partial: Candle, vendor_bar: Candle, *, close_is_current: bool = True
) -> Candle:
    """The vendor's open, the extremes of both, and whichever close is the later price."""
    merged_fields = {
        "open": vendor_bar.open,
        "high": max(partial.high, vendor_bar.high),
        "low": min(partial.low, vendor_bar.low),
    }
    if not close_is_current:
        merged_fields["close"] = vendor_bar.close
    return partial.model_copy(update=merged_fields)


__all__ = ["REPAIR_TIMEOUT_SECONDS", "PartialBarRepairer", "merge_partial_bar"]
