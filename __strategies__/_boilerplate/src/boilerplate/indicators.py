"""Indicators this repository owns, computed with pandas and numpy only.

The engine ships its own set. Borrowing them would put the edge back on top of
the delivery layer, so the ones a strategy needs live here instead — and the
strategy is then testable, portable and reviewable with nothing but its own
lockfile installed.

Two things are worth keeping if you rewrite these:

* **seed the recursion with an SMA**, exactly as TradingView's ``ta.ema`` does.
  ``Series.ewm(adjust=False)`` on its own starts from the *first* value, which
  leaves a visible offset against the same EMA plotted in Pine — and a strategy
  ported from a chart then crosses at different bars than it does on screen;
* **skip leading NaNs rather than folding them into the seed**, so composing an
  indicator on top of another does not quietly shift the series by however many
  bars the inner one needed.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

# ── Moving averages ───────────────────────────────────────────────────


def sma(series: pd.Series, length: int) -> pd.Series:
    """Simple moving average. NaN until *length* values exist."""
    return series.rolling(length, min_periods=length).mean()


def ema(series: pd.Series, length: int) -> pd.Series:
    """Exponential MA seeded with an SMA — TradingView's ``ta.ema``."""
    return _seeded_ewm(series, length, alpha=2.0 / (length + 1.0))


def rma(series: pd.Series, length: int) -> pd.Series:
    """Wilder's smoothing (``ta.rma``) — the basis of RSI and ATR. Same seed."""
    return _seeded_ewm(series, length, alpha=1.0 / length)


def _seeded_ewm(series: pd.Series, length: int, *, alpha: float) -> pd.Series:
    """Recursive smoothing whose first output is the SMA of the first *length*."""
    values = pd.to_numeric(series, errors="coerce").astype(float)
    result = pd.Series(np.nan, index=values.index, dtype=float)
    if length <= 1:
        return values.copy()

    valid = values.dropna()
    if len(valid) < length:
        return result

    tail = valid.iloc[length - 1 :].copy()
    tail.iloc[0] = valid.iloc[:length].mean()
    result.loc[tail.index] = tail.ewm(alpha=alpha, adjust=False).mean()
    return result


# ── Volatility ────────────────────────────────────────────────────────


def true_range(df: pd.DataFrame) -> pd.Series:
    """``max(high - low, |high - prev_close|, |low - prev_close|)``."""
    previous_close = df["close"].shift(1)
    spans = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - previous_close).abs(),
            (df["low"] - previous_close).abs(),
        ],
        axis=1,
    )
    return spans.max(axis=1)


def atr(df: pd.DataFrame, length: int = 14) -> pd.Series:
    """Average true range — the distance a stop should respect."""
    return rma(true_range(df), length)


# ── Crosses ───────────────────────────────────────────────────────────


def crossover(fast: pd.Series, slow: pd.Series) -> pd.Series:
    """True on the bar where *fast* crosses **above** *slow*.

    The previous bar has to be strictly below, so a series that merely stays
    above does not report a cross on every bar of the run.
    """
    return (fast > slow) & (fast.shift(1) <= slow.shift(1))


def crossunder(fast: pd.Series, slow: pd.Series) -> pd.Series:
    """True on the bar where *fast* crosses **below** *slow*."""
    return (fast < slow) & (fast.shift(1) >= slow.shift(1))
