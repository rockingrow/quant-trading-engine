"""The documented sample must load, pass audit and emit after the seeded warm-up."""

from datetime import UTC, datetime, timedelta

from qte_shared.config import REPO_ROOT
from qte_shared.models import SignalAction
from qte_shared.strategies.plugin_loader import StrategyLoader
from qte_shared.strategies.strategy_base import StrategyContext, candles_to_frame
from qte_simulator.bars import expected_candle, generate_bars
from qte_strategy_audit import StrategyAuditor


def test_documented_example_warms_then_emits_a_long():
    directory = REPO_ROOT / "examples" / "__strategies__"
    audit_report = StrategyAuditor(directory).run()
    assert not audit_report.errors
    strategy = StrategyLoader(directory).load_one("QTE_EXAMPLE_EMA_ATR")
    start_time = datetime(2026, 1, 1, tzinfo=UTC)
    open_times = [start_time + timedelta(minutes=15 * bar_index) for bar_index in range(360)]
    warmup_bars = generate_bars("XAUUSD", "M15", open_times[:300], start_price=2400.0, seed=7)
    signal_bars = generate_bars(
        "XAUUSD",
        "M15",
        open_times[300:],
        start_price=warmup_bars[-1].close,
        seed=3,
        drift=0.004,
        volatility=0.0015,
    )
    history = []
    emitted = []
    for candle in [expected_candle(candle) for candle in warmup_bars + signal_bars]:
        history.append(candle)
        if len(history) < strategy.warmup:
            continue
        context = StrategyContext("XAUUSD", "M15", candle.open_time)
        produced = strategy.on_candle_closed(candles_to_frame(history), context)
        if len(history) <= 300:
            assert produced == []
        emitted.extend(produced)
    assert len(emitted) == 1
    assert emitted[0].action == SignalAction.LONG
    assert emitted[0].sl < emitted[0].price < emitted[0].tp1 < emitted[0].tp2
