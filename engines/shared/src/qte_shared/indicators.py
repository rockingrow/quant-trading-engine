"""Pure technical indicators: arrays in, arrays out, no trading logic.

Nothing here knows what a position is. Every function takes a
:class:`pandas.Series` (or the OHLCV frame) and returns a Series/DataFrame
aligned to the same index, so a strategy can compose them freely and a backtest
gets bit-identical values to the live runner.

**On ``pandas_ta``**: it is deliberately *not* a dependency of the engine. It
drags in ``numba`` — pinned, and the binding constraint on which Python the
whole system may run — for four functions the engine itself uses, and it has no
WaveTrend, the one indicator the broker's payload schema actually names. So the
implementations below are first-party and self-contained, with TradingView's SMA
seeding for :func:`ema`/:func:`rma` so a strategy ported off a Pine chart
crosses at the same bars.

A strategy *repository* may decide otherwise — it owns its own dependencies,
and the one this engine loads today builds its indicators on ``pandas_ta``
wherever the library agrees with TradingView. :func:`pandas_ta_frame` is the
escape hatch on this side: if ``pandas-ta`` is installed it exposes the whole
library on a frame, otherwise it raises a message telling you so.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

OHLCV_COLUMNS = ("open", "high", "low", "close", "volume")


# ── Moving averages ───────────────────────────────────────────────────────


def sma(series: pd.Series, length: int) -> pd.Series:
    return series.rolling(length, min_periods=length).mean()


def ema(series: pd.Series, length: int) -> pd.Series:
    """Exponential MA seeded with an SMA, exactly as TradingView's ``ta.ema`` is.

    The seeding is the whole point. ``Series.ewm(adjust=False)`` on its own
    starts the recursion from the *first* value, which leaves a visible offset
    against the same EMA plotted in Pine — and a strategy ported from a chart
    would then cross at different bars in the backtest than it does on screen.
    """
    return _seeded_ewm(series, length, alpha=2.0 / (length + 1.0))


def rma(series: pd.Series, length: int) -> pd.Series:
    """Wilder's smoothing (``ta.rma``) — the basis of RSI, ATR and ADX.

    Same SMA seed as :func:`ema`, for the same reason.
    """
    return _seeded_ewm(series, length, alpha=1.0 / length)


def _seeded_ewm(series: pd.Series, length: int, *, alpha: float) -> pd.Series:
    """Recursive smoothing whose first output is the SMA of the first *length*.

    Leading NaNs (from a ``diff`` or an upstream indicator still warming up) are
    skipped rather than folded into the seed, so composing indicators does not
    quietly shift the series by however many bars the inner one needed.
    """
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


def wma(series: pd.Series, length: int) -> pd.Series:
    weights = np.arange(1, length + 1, dtype=float)
    return series.rolling(length, min_periods=length).apply(
        lambda window: float(np.dot(window, weights) / weights.sum()), raw=True
    )


def hma(series: pd.Series, length: int) -> pd.Series:
    half = wma(series, max(1, length // 2))
    full = wma(series, length)
    return wma(2 * half - full, max(1, int(np.sqrt(length))))


# ── Volatility ────────────────────────────────────────────────────────────


def true_range(df: pd.DataFrame) -> pd.Series:
    """max(high-low, |high-prev_close|, |low-prev_close|)."""
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
    return rma(true_range(df), length)


def bollinger(series: pd.Series, length: int = 20, mult: float = 2.0) -> pd.DataFrame:
    """Basis / upper / lower, using the population stddev TradingView uses."""
    basis = sma(series, length)
    deviation = series.rolling(length, min_periods=length).std(ddof=0) * mult
    return pd.DataFrame({"basis": basis, "upper": basis + deviation, "lower": basis - deviation})


def keltner(df: pd.DataFrame, length: int = 20, mult: float = 2.0) -> pd.DataFrame:
    basis = ema(df["close"], length)
    band = atr(df, length) * mult
    return pd.DataFrame({"basis": basis, "upper": basis + band, "lower": basis - band})


# ── Oscillators ───────────────────────────────────────────────────────────


def rsi(series: pd.Series, length: int = 14) -> pd.Series:
    delta = series.diff()
    gain = rma(delta.clip(lower=0.0), length)
    loss = rma((-delta).clip(lower=0.0), length)
    # A zero-loss window is a legitimate 100, not a division error.
    strength = gain / loss.replace(0.0, np.nan)
    result = 100.0 - (100.0 / (1.0 + strength))
    return result.where(loss != 0.0, 100.0).where(gain.notna())


def macd(series: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
    line = ema(series, fast) - ema(series, slow)
    signal_line = ema(line, signal)
    return pd.DataFrame({"macd": line, "signal": signal_line, "histogram": line - signal_line})


def stochastic(df: pd.DataFrame, k: int = 14, d: int = 3, smooth: int = 3) -> pd.DataFrame:
    lowest = df["low"].rolling(k, min_periods=k).min()
    highest = df["high"].rolling(k, min_periods=k).max()
    span = (highest - lowest).replace(0.0, np.nan)
    raw_k = 100.0 * (df["close"] - lowest) / span
    smoothed_k = sma(raw_k, smooth)
    return pd.DataFrame({"k": smoothed_k, "d": sma(smoothed_k, d)})


def wavetrend(
    df: pd.DataFrame, channel_length: int = 10, average_length: int = 21, signal_length: int = 4
) -> pd.DataFrame:
    """LazyBear's WaveTrend oscillator — ``wt1``/``wt2`` in the broker payload.

    Kept first-party because ``pandas_ta`` has no equivalent and the broker's
    ``IndicatorsSchema`` names these fields explicitly.
    """
    typical = (df["high"] + df["low"] + df["close"]) / 3.0
    esa = ema(typical, channel_length)
    deviation = ema((typical - esa).abs(), channel_length)
    # 0.015 is the constant from the original script; the guard keeps a flat
    # channel (deviation == 0) from producing infinities.
    scaled = (typical - esa) / (0.015 * deviation.replace(0.0, np.nan))
    tci = ema(scaled, average_length)
    return pd.DataFrame({"wt1": tci, "wt2": sma(tci, signal_length)})


def adx(df: pd.DataFrame, length: int = 14) -> pd.DataFrame:
    up_move = df["high"].diff()
    down_move = -df["low"].diff()
    plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)
    atr_value = rma(true_range(df), length).replace(0.0, np.nan)
    plus_di = 100.0 * rma(plus_dm, length) / atr_value
    minus_di = 100.0 * rma(minus_dm, length) / atr_value
    di_sum = (plus_di + minus_di).replace(0.0, np.nan)
    return pd.DataFrame(
        {
            "plus_di": plus_di,
            "minus_di": minus_di,
            "adx": rma(100.0 * (plus_di - minus_di).abs() / di_sum, length),
        }
    )


# ── Volume / price levels ─────────────────────────────────────────────────


def vwap(df: pd.DataFrame) -> pd.Series:
    """Session-less running VWAP over whatever slice you pass in."""
    typical = (df["high"] + df["low"] + df["close"]) / 3.0
    volume = df["volume"].fillna(0.0)
    cumulative_volume = volume.cumsum().replace(0.0, np.nan)
    return (typical * volume).cumsum() / cumulative_volume


def highest(series: pd.Series, length: int) -> pd.Series:
    return series.rolling(length, min_periods=length).max()


def lowest(series: pd.Series, length: int) -> pd.Series:
    return series.rolling(length, min_periods=length).min()


def volume_spike(df: pd.DataFrame, lookback: int = 20, mult: float = 2.0) -> pd.Series:
    """True where this bar's volume exceeds *mult* × its trailing average."""
    average = sma(df["volume"], lookback)
    return df["volume"] > (average * mult)


# ── Crossing helpers ──────────────────────────────────────────────────────


def crossover(left: pd.Series, right: pd.Series) -> pd.Series:
    """True on the bar where *left* crosses from below *right* to above it."""
    return (left > right) & (left.shift(1) <= right.shift(1))


def crossunder(left: pd.Series, right: pd.Series) -> pd.Series:
    return (left < right) & (left.shift(1) >= right.shift(1))


# ── Optional pandas_ta bridge ─────────────────────────────────────────────


def pandas_ta_frame(df: pd.DataFrame):
    """Return ``df.ta`` when ``pandas-ta`` is installed, else explain why not.

    QTE does not depend on it (see the module docstring), but a strategy that
    wants its 270-odd extra indicators can ``uv add pandas-ta`` and reach them
    through here without importing it at module scope everywhere. It is already
    installed if you ran ``make strategy-deps`` for a plugin repo that uses it.
    """
    try:
        import pandas_ta  # noqa: F401
    except ImportError as exc:  # pragma: no cover - depends on the environment
        raise ImportError(
            "pandas-ta is not installed. QTE ships its own indicators; install "
            "pandas-ta only if you need something this module does not cover "
            "(`uv add pandas-ta`), and note that it pins numba, which decides "
            "which Python versions the whole engine can run on."
        ) from exc
    return df.ta
