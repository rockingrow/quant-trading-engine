"""Backtest and live must hand a strategy the same amount of history.

Before ``StrategyBase.history_window`` existed, the live runner kept a bounded
deque while the replay passed the entire parquet file. A strategy using a
running sum, a session VWAP, or anything else that reads the whole frame
computed one thing in the backtest and another in production — silently, and in
the direction that flatters the backtest.
"""

from __future__ import annotations

import pandas as pd
import pytest
from qte_backtest.replay import BacktestEngine
from qte_shared.strategy_base import MIN_HISTORY_WINDOW, StrategyBase


class Recorder(StrategyBase):
    """Records how many bars it was handed on every call."""

    name = "RECORDER"
    timeframe = "M15"
    warmup = 30

    def __init__(self, params=None):
        super().__init__(params)
        self.sizes: list[int] = []
        self.first_index: list[pd.Timestamp] = []

    def on_candle_closed(self, df, context):
        self.sizes.append(len(df))
        self.first_index.append(df.index[0])
        return None


def test_the_default_window_is_warmup_doubled_with_a_floor():
    class Small(Recorder):
        warmup = 30

    class Large(Recorder):
        warmup = 500

    assert Small().history_window() == MIN_HISTORY_WINDOW  # floor wins
    assert Large().history_window() == 1000  # warmup * 2 wins


def test_an_explicit_max_history_is_honoured():
    class Custom(Recorder):
        warmup = 30
        max_history = 75

    assert Custom().history_window() == 75


def test_zero_means_unbounded():
    class Unbounded(Recorder):
        max_history = 0

    assert Unbounded().history_window() is None


def test_the_replay_never_hands_over_more_than_the_window(trending_frame):
    class Windowed(Recorder):
        warmup = 30
        max_history = 50

    strategy = Windowed()
    BacktestEngine(strategy, symbol="XAUUSD").run(trending_frame)

    assert max(strategy.sizes) == 50
    # The window slides rather than growing: once full, its first bar advances.
    assert strategy.first_index[-1] > strategy.first_index[0]


def test_the_window_still_ends_on_the_bar_being_decided(trending_frame):
    class Windowed(Recorder):
        warmup = 30
        max_history = 50

    strategy = Windowed()
    BacktestEngine(strategy, symbol="XAUUSD").run(trending_frame)

    # Clipping the *start* must not disturb the invariant that df.iloc[-1] is
    # the bar just closed — that is what makes the frame non-repainting.
    assert strategy.sizes[0] == 31  # warmup bars + the one being decided
    assert len(strategy.sizes) == len(trending_frame) - 30


def test_an_unbounded_strategy_really_sees_everything(trending_frame):
    class Unbounded(Recorder):
        warmup = 30
        max_history = 0

    strategy = Unbounded()
    BacktestEngine(strategy, symbol="XAUUSD").run(trending_frame)

    assert max(strategy.sizes) == len(trending_frame)
    assert strategy.first_index[0] == strategy.first_index[-1]  # never slides


def test_the_live_runner_bounds_its_buffer_by_the_same_rule():
    # The two drivers must read one source of truth, not two copies of a formula.
    from qte_strategy_engine.runner import StrategySlot

    class Windowed(Recorder):
        warmup = 30
        max_history = 77

    strategy = Windowed()
    slot = StrategySlot(strategy, "XAUUSD", factory=None)
    assert slot.buffer.maxlen == strategy.history_window() == 77


def test_an_unbounded_strategy_gets_an_unbounded_live_buffer():
    from qte_strategy_engine.runner import StrategySlot

    class Unbounded(Recorder):
        max_history = 0

    slot = StrategySlot(Unbounded(), "XAUUSD", factory=None)
    assert slot.buffer.maxlen is None


def test_describe_reports_the_window_so_it_lands_in_the_backtest_report():
    class Windowed(Recorder):
        warmup = 30
        max_history = 60

    assert Windowed().describe()["history_window"] == 60


@pytest.mark.parametrize("max_history", [40, 100, 0])
def test_replay_cost_stays_linear_in_the_number_of_bars(max_history, trending_frame):
    """A quadratic replay is what passing the whole frame every bar produced.

    Not a timing assertion — those are flaky. This counts the rows actually
    handed over, which is what drove the cost.
    """

    class Windowed(Recorder):
        warmup = 30

    Windowed.max_history = max_history
    strategy = Windowed()
    BacktestEngine(strategy, symbol="XAUUSD").run(trending_frame)

    bars = len(trending_frame) - 30
    total_rows = sum(strategy.sizes)
    if max_history == 0:
        assert total_rows > bars * 100  # unbounded: grows with the file
    else:
        assert total_rows <= bars * max_history  # bounded per call
