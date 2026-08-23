"""Ticks in, completed candles out.

A bar belongs to the bucket its *timestamp* falls into, and it closes when the
clock passes the bucket's end — not when the next tick happens to arrive. Those
are different rules and only the first one is safe: in a thin session the next
XAUUSD tick can be two minutes late, and a resampler that waits for it emits the
M15 bar two minutes after every worker downstream expected it. So
:meth:`Resampler.flush` closes bars against the wall clock, and the ingestion
loop calls it on a timer regardless of feed activity.

A bucket with no ticks produces no candle. Forward-filling a flat synthetic bar
would feed strategies a body that never traded, which quietly corrupts any
indicator with a range in it (ATR most of all).
"""

from __future__ import annotations

from datetime import datetime, timedelta

from qte_shared.logging_setup import get_logger
from qte_shared.models import Candle, Tick
from qte_shared.timeframes import floor_to_bucket, normalize_timeframe, timeframe_seconds

log = get_logger(__name__)


class _BarBuilder:
    """Accumulates ticks into the one bar currently open for a timeframe."""

    __slots__ = (
        "symbol",
        "timeframe",
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "tick_count",
    )

    def __init__(self, symbol: str, timeframe: str, open_time: datetime, price: float) -> None:
        self.symbol = symbol
        self.timeframe = timeframe
        self.open_time = open_time
        self.open = price
        self.high = price
        self.low = price
        self.close = price
        self.volume = 0.0
        self.tick_count = 0

    def update(self, price: float, volume: float) -> None:
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price
        self.volume += volume
        self.tick_count += 1

    def snapshot(self, *, is_closed: bool) -> Candle:
        return Candle(
            symbol=self.symbol,
            timeframe=self.timeframe,
            open_time=self.open_time,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=self.volume,
            tick_count=self.tick_count,
            is_closed=is_closed,
        )


class Resampler:
    """Builds candles for one symbol across several timeframes at once."""

    def __init__(self, symbol: str, timeframes: list[str]) -> None:
        self.symbol = symbol
        self.timeframes = [normalize_timeframe(tf) for tf in timeframes]
        self._builders: dict[str, _BarBuilder] = {}

    # ── Feeding ───────────────────────────────────────────────────────

    def add_tick(self, tick: Tick) -> list[Candle]:
        """Fold *tick* into every timeframe; return any bars it closed.

        A tick landing in a later bucket closes the one before it, which is how
        a busy feed closes bars without waiting for the flush timer.
        """
        price = tick.price
        closed: list[Candle] = []
        for timeframe in self.timeframes:
            bucket = floor_to_bucket(tick.ts, timeframe)
            builder = self._builders.get(timeframe)
            if builder is None:
                self._builders[timeframe] = _BarBuilder(self.symbol, timeframe, bucket, price)
            elif bucket > builder.open_time:
                closed.append(builder.snapshot(is_closed=True))
                self._builders[timeframe] = _BarBuilder(self.symbol, timeframe, bucket, price)
            elif bucket < builder.open_time:
                # Out-of-order tick from a reconnect replay: it belongs to a bar
                # already published, and reopening that bar would repaint a
                # candle strategies have acted on. Drop it and say so.
                log.warning(
                    "Dropping late tick symbol=%s tf=%s tick_bucket=%s open_bucket=%s",
                    self.symbol,
                    timeframe,
                    bucket,
                    builder.open_time,
                )
                continue
            self._builders[timeframe].update(price, tick.volume)
        return closed

    def flush(self, now: datetime) -> list[Candle]:
        """Close every bar whose bucket has ended by *now*.

        Call this on a timer. It is what makes a candle close on schedule in a
        market so quiet that no tick arrives to push the bar over.
        """
        closed: list[Candle] = []
        for timeframe, builder in list(self._builders.items()):
            bucket_end = builder.open_time + timedelta(seconds=timeframe_seconds(timeframe))
            if now >= bucket_end:
                closed.append(builder.snapshot(is_closed=True))
                del self._builders[timeframe]
        return closed

    # ── Inspection ────────────────────────────────────────────────────

    def open_candle(self, timeframe: str) -> Candle | None:
        """The in-progress bar, for state persistence and dashboards."""
        builder = self._builders.get(normalize_timeframe(timeframe))
        return builder.snapshot(is_closed=False) if builder else None

    def open_candles(self) -> list[Candle]:
        return [builder.snapshot(is_closed=False) for builder in self._builders.values()]

    def restore(self, candle: Candle) -> None:
        """Resume a partially-built bar recovered from Redis after a restart."""
        timeframe = normalize_timeframe(candle.timeframe)
        if timeframe not in self.timeframes:
            return
        builder = _BarBuilder(self.symbol, timeframe, candle.open_time, candle.open)
        builder.high = candle.high
        builder.low = candle.low
        builder.close = candle.close
        builder.volume = candle.volume
        builder.tick_count = candle.tick_count
        self._builders[timeframe] = builder
