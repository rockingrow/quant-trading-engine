"""Bars in, ticks out — and the candle those ticks must produce.

Ingestion has no notion of a bar. It has a
:class:`~qte_ingestion.resampler.Resampler` that folds ticks into buckets, and
that is precisely the thing an end-to-end test should be exercising. So "send a
bar" here means *synthesise the ticks a bar is made of* and let the real
resampler rebuild it. If the candle that comes out the far end does not match
the bar that went in, something in the pipeline is wrong — which is the whole
value of the exercise, and would be lost if the simulator published a
ready-made candle instead.

Four ticks per bar, at fixed offsets inside the bucket:

    open ─────── low ─────── high ─────── close        (bullish: close ≥ open)
    open ─────── high ────── low ──────── close        (bearish)
    t+0        t+¼d        t+½d         t+d−1s

The path differs by direction because a bullish bar that printed its high
before its low is a bar that fell and then rose, and an M1 strategy reading
intrabar sequence would be reading a lie. The resampled OHLC is identical
either way; only the story is.

The final tick sits at ``d − 1`` seconds, not at ``d``: a tick on the boundary
belongs to the *next* bucket and would open the following bar with this bar's
close.

Two invariants this module holds, both of which the verifier depends on:

* ``price`` is carried in ``last``, so :attr:`~qte_shared.models.Tick.price`
  returns it exactly. A bar rebuilt from bid/ask midpoints would drift by half
  a spread and every comparison would need a tolerance.
* Volume is split so the four shares sum to the bar's volume exactly — the
  remainder goes on the last tick rather than being divided four ways.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from qte_shared.models import Candle, Tick
from qte_shared.timeframes import floor_to_bucket, normalize_timeframe, timeframe_seconds

#: Ticks emitted per bar. Fixed, so `expected_candle().tick_count` is knowable
#: before anything is sent and a mismatch is a real finding rather than noise.
TICKS_PER_BAR = 4


class BarError(ValueError):
    """Raised for a bar that could not have printed."""


@dataclass(frozen=True, slots=True)
class BarSpec:
    """One OHLCV bar to be played into the feed, keyed by its **open** time."""

    symbol: str
    timeframe: str
    open_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0

    def __post_init__(self) -> None:
        if self.high < self.low:
            raise BarError(f"{self.symbol} bar has high {self.high} below low {self.low}")
        if self.high < max(self.open, self.close) or self.low > min(self.open, self.close):
            raise BarError(
                f"{self.symbol} bar at {self.open_time} does not contain its own body: "
                f"o={self.open} h={self.high} l={self.low} c={self.close}"
            )
        if self.volume < 0:
            raise BarError(f"{self.symbol} bar has negative volume {self.volume}")

    @property
    def is_bullish(self) -> bool:
        return self.close >= self.open

    @property
    def duration(self) -> timedelta:
        return timedelta(seconds=timeframe_seconds(self.timeframe))

    @property
    def close_time(self) -> datetime:
        """When the bucket ends — the open time of the bar after this one."""
        return self.open_time + self.duration

    def to_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "open_time": self.open_time.isoformat(),
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
        }


def bar_ticks(bar: BarSpec, *, spread: float = 0.0) -> list[Tick]:
    """The four ticks whose resampling reproduces *bar* exactly.

    *spread* decorates each tick with a bid and an ask around the price. It
    changes nothing about the candle — ``last`` is what the resampler reads —
    and exists so a strategy or a dashboard that looks at the quote sees a
    two-sided one.
    """
    seconds = timeframe_seconds(bar.timeframe)
    offsets = (0, seconds // 4, seconds // 2, seconds - 1)
    path = (
        (bar.open, bar.low, bar.high, bar.close)
        if bar.is_bullish
        else (bar.open, bar.high, bar.low, bar.close)
    )
    share = bar.volume / TICKS_PER_BAR
    volumes = [share] * (TICKS_PER_BAR - 1)
    volumes.append(bar.volume - sum(volumes))

    half = spread / 2 if spread > 0 else None
    return [
        Tick(
            symbol=bar.symbol,
            ts=bar.open_time + timedelta(seconds=offset),
            last=price,
            bid=None if half is None else price - half,
            ask=None if half is None else price + half,
            volume=volume,
        )
        for offset, price, volume in zip(offsets, path, volumes, strict=True)
    ]


def expected_candle(bar: BarSpec) -> Candle:
    """The candle ingestion must publish for *bar*, field for field.

    This is the assertion target for ``--verify``. It is derived from the bar
    rather than from the ticks on purpose: deriving it from the ticks would
    make the synthesis agree with itself no matter what it did.
    """
    return Candle(
        symbol=bar.symbol,
        timeframe=normalize_timeframe(bar.timeframe),
        open_time=bar.open_time,
        open=bar.open,
        high=bar.high,
        low=bar.low,
        close=bar.close,
        volume=bar.volume,
        tick_count=TICKS_PER_BAR,
        is_closed=True,
    )


def seal_tick(bar: BarSpec) -> Tick:
    """A single tick in the bucket *after* *bar*, to force its close now.

    A resampler closes a bar when a tick lands in a later bucket, or when the
    wall clock passes the bucket's end. Bars anchored to the future never meet
    the second condition, so the last bar of a replay would sit open
    indefinitely; this is the tick that closes it.

    It has a cost worth knowing about: it opens a one-tick bar in the next
    bucket, which will print as a doji the next time anything advances past it.
    That is a real candle rather than an artefact — one tick in a bucket is
    what a very thin market looks like — but it is there because of this tick,
    not because of your data.
    """
    return Tick(symbol=bar.symbol, ts=bar.close_time, last=bar.close, volume=0.0)


# ── Anchoring ─────────────────────────────────────────────────────────────


def anchor_open_times(
    count: int,
    timeframe: str,
    *,
    mode: str = "past",
    now: datetime | None = None,
    cursor: datetime | None = None,
) -> list[datetime]:
    """Where on the clock a run of *count* bars is placed, and why it matters.

    The choice is not cosmetic. Ingestion closes bars on the wall clock as well
    as on bucket advance (``Resampler.flush``), and a replay of historical
    timestamps runs into that timer: every bucket it fills is already over, so
    the flush can fire *between* two ticks of the same bar and publish half of
    it. Once per flush interval, for as long as the replay runs.

    ``"past"``
        The run ends on the last **completed** bucket. Every bar is over
        already, so the wall-clock flush is what closes the final one — within
        ``QTE_INGESTION__FLUSH_INTERVAL``. Right for one bar or a few: the
        window in which a flush could split a bar is the few hundred
        microseconds its four ticks take to arrive.

    ``"next"``
        The run starts at the current bucket (or at the bucket after whatever
        this symbol/timeframe last received) and marches **forward**. No
        bucket's end has passed, so the flush never touches them: each bar is
        closed by the arrival of the next one, and the last by an explicit
        :func:`seal_tick`. Deterministic at any length, which is why a replay
        of 300 warm-up bars uses it.

    The cost of ``"next"`` is that candle timestamps run ahead of the clock —
    fine in a dev stack, and the reason
    :func:`~qte_shared.dev_only.require_dev_env` guards the provider.
    """
    if count < 1:
        return []
    moment = now or datetime.now(UTC)
    duration = timedelta(seconds=timeframe_seconds(timeframe))

    if mode == "past":
        last = floor_to_bucket(moment, timeframe) - duration
        first = last - duration * (count - 1)
    elif mode == "next":
        first = cursor + duration if cursor is not None else floor_to_bucket(moment, timeframe)
    else:
        raise BarError(f"Unknown anchor {mode!r}; use 'past' or 'next'")

    return [first + duration * index for index in range(count)]


# ── Generated data ────────────────────────────────────────────────────────


def generate_bars(
    symbol: str,
    timeframe: str,
    open_times: list[datetime],
    *,
    start_price: float,
    volatility: float = 0.002,
    drift: float = 0.0,
    seed: int | None = None,
) -> list[BarSpec]:
    """A contiguous run of synthetic bars, gapless and seedable.

    It is a random walk with a body and two wicks — enough to warm an indicator
    window and move a strategy off the fence, and nothing more. It is not a
    market model, and a backtest over it would measure nothing; that is why the
    simulator serves no history.

    Each bar opens exactly where the previous closed, because a resampled feed
    cannot produce a gap: the tick that closes one bucket and the tick that
    opens the next are consecutive prints.
    """
    rng = random.Random(seed)
    bars: list[BarSpec] = []
    price = start_price
    for open_time in open_times:
        step = rng.gauss(drift, volatility) * price
        close = max(price + step, price * 0.5)
        span = abs(rng.gauss(0, volatility)) * price
        high = max(price, close) + span * rng.random()
        low = min(price, close) - span * rng.random()
        bars.append(
            BarSpec(
                symbol=symbol,
                timeframe=normalize_timeframe(timeframe),
                open_time=open_time,
                open=round(price, 6),
                high=round(high, 6),
                low=round(low, 6),
                close=round(close, 6),
                volume=round(rng.uniform(50, 500), 3),
            )
        )
        price = bars[-1].close
    return bars


#: Somewhere plausible to start a walk when nobody says. A dev fixture, not a
#: quote — the numbers only have to be the right order of magnitude for a stop
#: distance in ATR multiples to look sane in a log.
REFERENCE_PRICES: dict[str, float] = {
    "XAUUSD": 2400.0,
    "XAGUSD": 30.0,
    "EURUSD": 1.08,
    "GBPUSD": 1.27,
    "USDJPY": 156.0,
    "BTCUSDT": 60000.0,
    "BTCUSD": 60000.0,
    "ETHUSDT": 3000.0,
}


def reference_price(symbol: str) -> float:
    return REFERENCE_PRICES.get(symbol.upper(), 100.0)


__all__ = [
    "TICKS_PER_BAR",
    "BarError",
    "BarSpec",
    "anchor_open_times",
    "bar_ticks",
    "expected_candle",
    "generate_bars",
    "reference_price",
    "seal_tick",
]
