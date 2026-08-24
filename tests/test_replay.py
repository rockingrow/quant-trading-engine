"""End-to-end replay: a scripted strategy through the engine and out as signals."""

from __future__ import annotations

import pandas as pd
import pytest
from qte_backtest.execution import CostModel
from qte_backtest.replay import BacktestEngine
from qte_shared.models import SignalAction
from qte_shared.strategy_base import IntentResult, SignalIntent, StrategyBase, StrategyContext


class BuyOnceStrategy(StrategyBase):
    """Goes long on the first bar after warm-up and never trades again."""

    name = "BUY_ONCE"
    timeframe = "M15"
    warmup = 5

    def __init__(self, params=None):
        super().__init__(params)
        self.calls = 0
        self.fired = False

    def on_candle_closed(self, df: pd.DataFrame, context: StrategyContext) -> IntentResult:
        self.calls += 1
        if self.fired:
            return None
        self.fired = True
        close = float(df["close"].iloc[-1])
        return SignalIntent(
            action=SignalAction.LONG,
            price=close,
            quantity=1.0,
            sl=close - 5,
            tp1=close + 5,
            tp2=close + 10,
            tp1_percent=50.0,
        )


class AlwaysLongStrategy(BuyOnceStrategy):
    name = "ALWAYS_LONG"

    def on_candle_closed(self, df, context):
        self.fired = False
        return super().on_candle_closed(df, context)


def test_the_strategy_only_ever_sees_closed_history(trending_frame):
    seen: list[int] = []

    class Recorder(BuyOnceStrategy):
        name = "RECORDER"

        def on_candle_closed(self, df, context):
            seen.append(len(df))
            # The last row must be the bar we were called for, never a later one.
            assert df.index[-1] == context.now
            return None

    BacktestEngine(Recorder(), symbol="XAUUSD").run(trending_frame)
    assert seen == list(range(6, len(trending_frame) + 1))


def test_a_full_run_produces_matching_positions_and_signals(trending_frame):
    result = BacktestEngine(
        BuyOnceStrategy(), symbol="XAUUSD", costs=CostModel(spread=0.2), starting_equity=10_000
    ).run(trending_frame)

    assert len(result.positions) == 1
    assert len(result.signals) == 1
    signal = result.signals[0]
    assert signal.strategy == "BUY_ONCE"
    assert signal.symbol == "XAUUSD"
    assert signal.timeframe == "15"
    assert signal.position.action is SignalAction.LONG
    signal.validate_shape()


def test_metrics_are_computed_over_realised_trades(trending_frame):
    result = BacktestEngine(BuyOnceStrategy(), symbol="XAUUSD").run(trending_frame)
    metrics = result.metrics
    assert metrics.trades == 1
    assert metrics.wins + metrics.losses == metrics.trades
    assert metrics.equity_curve[0] == 0.0
    assert "BUY_ONCE" in result.report()


def test_a_second_entry_is_rejected_while_a_position_is_open(trending_frame):
    # Mirrors the worker, which answers REJECTED rather than stacking positions.
    result = BacktestEngine(AlwaysLongStrategy(), symbol="XAUUSD").run(trending_frame)
    assert result.rejected > 0
    open_at_once = [p for p in result.positions if p.is_open]
    assert len(open_at_once) == 0


def test_a_position_still_open_at_the_end_is_marked_out(trending_frame):
    class NeverExits(BuyOnceStrategy):
        name = "NEVER_EXITS"

        def on_candle_closed(self, df, context):
            if self.fired:
                return None
            self.fired = True
            close = float(df["close"].iloc[-1])
            # No reachable stop or target: the position can only end at the data's edge.
            return SignalIntent(
                action=SignalAction.LONG,
                price=close,
                quantity=1.0,
                sl=close - 10_000,
                tp1=close + 10_000,
            )

    result = BacktestEngine(NeverExits(), symbol="XAUUSD").run(trending_frame)
    assert result.metrics.trades == 1
    assert result.positions[0].exit_reason == "END_OF_DATA"


def test_too_little_history_for_the_warmup_is_an_error(trending_frame):
    class Hungry(BuyOnceStrategy):
        warmup = 10_000

    with pytest.raises(ValueError, match="warm-up"):
        BacktestEngine(Hungry(), symbol="XAUUSD").run(trending_frame)


def test_trade_rows_are_shaped_for_the_audit_table(trending_frame):
    result = BacktestEngine(BuyOnceStrategy(), symbol="XAUUSD").run(trending_frame)
    row = result.trades_as_rows()[0]
    assert set(row) == {
        "signal_uxid",
        "symbol",
        "direction",
        "opened_at",
        "closed_at",
        "entry_price",
        "exit_price",
        "quantity",
        "sl",
        "tp1",
        "tp2",
        "exit_reason",
        "gross_pnl",
        "fees",
        "net_pnl",
    }
