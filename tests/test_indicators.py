import numpy as np
import pandas as pd
from qte_shared.indicators import (
    atr,
    bollinger,
    crossover,
    crossunder,
    ema,
    rsi,
    sma,
    true_range,
    wavetrend,
)


def test_sma_is_nan_until_the_window_is_full():
    series = pd.Series([1.0, 2.0, 3.0, 4.0])
    result = sma(series, 3)
    assert result.isna().tolist() == [True, True, False, False]
    assert result.iloc[2] == 2.0


def test_ema_matches_the_recursive_definition():
    series = pd.Series([10.0, 11.0, 12.0, 13.0, 14.0])
    result = ema(series, 3)
    alpha = 2 / 4
    expected = 11.0  # seed = mean of the first 3
    for value in (13.0, 14.0):
        expected = value * alpha + expected * (1 - alpha)
    assert result.iloc[-1] == round(expected, 10)


def test_rsi_reports_100_when_nothing_ever_falls():
    # A zero-loss window is a legitimate 100, not a divide-by-zero.
    series = pd.Series(np.arange(1.0, 30.0))
    assert rsi(series, 14).iloc[-1] == 100.0


def test_rsi_stays_inside_its_bounds():
    rng = np.random.default_rng(7)
    series = pd.Series(100 + rng.normal(0, 1, 200).cumsum())
    values = rsi(series, 14).dropna()
    assert values.between(0, 100).all()


def test_true_range_uses_the_previous_close():
    frame = pd.DataFrame({"high": [10.0, 12.0], "low": [9.0, 11.5], "close": [9.5, 11.8]})
    # Bar 2 gapped up: |high - prev_close| = 2.5 beats its own 0.5 range.
    assert true_range(frame).iloc[1] == 2.5


def test_atr_is_positive_and_warms_up():
    frame = pd.DataFrame(
        {
            "high": np.linspace(10, 20, 50) + 0.5,
            "low": np.linspace(10, 20, 50) - 0.5,
            "close": np.linspace(10, 20, 50),
        }
    )
    values = atr(frame, 14)
    assert values.iloc[:13].isna().all()
    assert values.dropna().gt(0).all()


def test_bollinger_bands_straddle_the_basis():
    series = pd.Series(np.linspace(100, 120, 60))
    bands = bollinger(series, 20, 2.0).dropna()
    assert (bands["upper"] > bands["basis"]).all()
    assert (bands["lower"] < bands["basis"]).all()


def test_wavetrend_survives_a_perfectly_flat_channel():
    # deviation == 0 would divide by zero; the guard must yield NaN, not inf.
    frame = pd.DataFrame({"high": [5.0] * 60, "low": [5.0] * 60, "close": [5.0] * 60})
    result = wavetrend(frame)
    assert not np.isinf(result.to_numpy(dtype=float)).any()


def test_crossover_fires_only_on_the_crossing_bar():
    left = pd.Series([1.0, 2.0, 3.0, 4.0])
    right = pd.Series([2.0, 2.0, 2.0, 2.0])
    assert crossover(left, right).tolist() == [False, False, True, False]
    assert crossunder(right, left).tolist() == [False, False, True, False]
