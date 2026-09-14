"""Live and replay decide on the same candle that completes warm-up."""

from datetime import UTC, datetime, timedelta

import pytest
from test_runner_catch_up import CandleStore, DecisionRecorder, bar_at, build_runner, deliver_live

from qte_backtest.replay import BacktestEngine
from qte_shared.strategies.strategy_base import candles_to_frame
from qte_strategy_engine import runner as runner_module


@pytest.mark.parametrize("required_bars,extra_bars", [(1, 0), (1, 2), (3, 0), (3, 2), (5, 0)])
async def test_first_decision_and_benchmark_match_live_warmup(
    monkeypatch, required_bars, extra_bars
):
    candles = [
        bar_at(datetime(2026, 1, 1, tzinfo=UTC) + timedelta(minutes=15 * offset))
        for offset in range(required_bars + extra_bars)
    ]

    class MarketClock(datetime):
        current_time = candles[0].open_time

        @classmethod
        def now(cls, timezone=None):
            return cls.current_time.astimezone(timezone)

    monkeypatch.setattr(runner_module, "datetime", MarketClock)
    runner, strategy_slot, live_strategy = build_runner(
        monkeypatch, CandleStore([]), provider="simulator"
    )
    live_strategy.warmup = required_bars
    await runner.start()
    try:
        for candle in candles:
            MarketClock.current_time = candle.open_time + timedelta(minutes=15, seconds=1)
            await deliver_live(runner, candle)
    finally:
        await runner.stop()

    replay_strategy = DecisionRecorder()
    replay_strategy.warmup = required_bars
    replay_result = BacktestEngine(replay_strategy, symbol="XAUUSD").run(candles_to_frame(candles))
    expected = [candle.open_time for candle in candles[required_bars - 1 :]]
    assert strategy_slot.is_warm
    assert live_strategy.decided_on == replay_strategy.decided_on == expected
    assert replay_result.market.benchmark_from == expected[0]
    assert replay_result.market.benchmark_close == candles[required_bars - 1].close


def test_history_one_bar_short_of_warmup_is_rejected():
    strategy = DecisionRecorder()
    strategy.warmup = 3
    candles = [
        bar_at(datetime(2026, 1, 1, tzinfo=UTC) + timedelta(minutes=15 * offset))
        for offset in range(2)
    ]
    with pytest.raises(ValueError, match="warm-up"):
        BacktestEngine(strategy, symbol="XAUUSD").run(candles_to_frame(candles))
