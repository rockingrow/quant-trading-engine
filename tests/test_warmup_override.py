"""``warmup`` set per strategy in the mapping table, instead of only in the repo.

A strategy declares the candles its slowest indicator needs; the deployment may
run it on a different count. The override travels as an ordinary pair param, so
the live runner and the replay resolve it from the same dict and still start
deciding on the same bar — the property the whole engine is arranged around.

What the engine refuses is only what is not a judgement call: a value that is
not a positive whole number, and one above the strategy's own ``max_history``,
which the runner's buffer could never reach. Asking for fewer bars than the
strategy declares is the operator's call and is logged, because what a strategy
really needs is a property of its indicators and nothing here can measure it.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

import pytest
from test_runner_catch_up import CandleStore, DecisionRecorder, bar_at, build_runner, deliver_live
from test_runner_mapping import Edge, table

from qte_backtest.replay import BacktestEngine
from qte_shared.config import settings
from qte_shared.models import SignalAction
from qte_shared.strategies.signal_factory import SignalFactory
from qte_shared.strategies.sizing import PositionSizer
from qte_shared.strategies.strategy_base import SignalIntent, candles_to_frame
from qte_shared.strategies.strategy_settings import (
    PORT_ONLY_PARAMS,
    WARMUP_PARAM,
    describe_warmup,
    resolve_warmup,
)
from qte_strategy_engine import runner as runner_module
from qte_strategy_engine.runner import StrategySlot

SYMBOL = "XAUUSD"
DECLARED = 150
WINDOW = 300
MOMENT = datetime(2026, 1, 1, tzinfo=UTC)


# ── The resolver ──────────────────────────────────────────────────────


@pytest.mark.parametrize("params", [{}, {WARMUP_PARAM: None}, {"risk_percent": 2.0}])
def test_without_the_key_the_declaration_stands(params):
    assert resolve_warmup(DECLARED, WINDOW, params) == DECLARED


def test_a_higher_count_inside_the_window_is_honoured():
    assert resolve_warmup(DECLARED, WINDOW, {WARMUP_PARAM: 250}) == 250


def test_a_lower_count_is_honoured_and_says_so(caplog):
    with caplog.at_level(logging.WARNING):
        resolved = resolve_warmup(DECLARED, WINDOW, {WARMUP_PARAM: 40}, subject="PROBE on XAUUSD")

    assert resolved == 40
    warning = "\n".join(record.getMessage() for record in caplog.records)
    # Both numbers, so the log says what was replaced and not merely that
    # something was.
    assert "40" in warning
    assert str(DECLARED) in warning
    assert "PROBE on XAUUSD" in warning


def test_a_count_equal_to_the_declaration_passes_quietly(caplog):
    with caplog.at_level(logging.WARNING):
        assert resolve_warmup(DECLARED, WINDOW, {WARMUP_PARAM: DECLARED}) == DECLARED

    assert caplog.records == []


@pytest.mark.parametrize("wanted", [0, -1, True, False, "120", 12.5, [120]])
def test_a_count_that_is_not_a_positive_whole_number_is_refused(wanted):
    with pytest.raises(ValueError, match=WARMUP_PARAM):
        resolve_warmup(DECLARED, WINDOW, {WARMUP_PARAM: wanted})


def test_a_count_above_the_strategys_window_is_refused():
    # The runner's buffer is a deque capped at history_window(), so this pair
    # would warm up forever and trade nothing.
    with pytest.raises(ValueError, match="never finish warming up"):
        resolve_warmup(DECLARED, WINDOW, {WARMUP_PARAM: WINDOW + 1})


def test_an_unbounded_window_has_no_ceiling_to_exceed():
    assert resolve_warmup(DECLARED, None, {WARMUP_PARAM: 10_000}) == 10_000


@pytest.mark.parametrize(
    "effective,declared,expected",
    [(150, 150, "150"), (120, 150, "120 (declared 150)"), (250, 150, "250 (declared 150)")],
)
def test_the_log_field_names_the_declaration_it_replaced(effective, declared, expected):
    assert describe_warmup(effective, declared) == expected


# ── The live gate ─────────────────────────────────────────────────────


def slot_with(warmup: int | None) -> StrategySlot:
    strategy = DecisionRecorder()
    strategy.warmup = 2
    factory = SignalFactory(strategy.name, timeframe="M15", token="test")
    return StrategySlot(strategy, SYMBOL, factory, warmup=warmup)


def test_a_slot_without_an_override_defers_to_the_strategy():
    slot = slot_with(None)

    assert slot.warmup == 2
    slot.buffer.append(bar_at(MOMENT))
    assert not slot.is_warm
    slot.buffer.append(bar_at(MOMENT + timedelta(minutes=15)))
    assert slot.is_warm


def test_a_slot_with_an_override_warms_on_the_overridden_count():
    slot = slot_with(3)

    assert slot.warmup == 3
    for offset in range(2):
        slot.buffer.append(bar_at(MOMENT + timedelta(minutes=15 * offset)))
    assert not slot.is_warm, "two candles is the strategy's count, not this pair's"
    slot.buffer.append(bar_at(MOMENT + timedelta(minutes=30)))
    assert slot.is_warm


# ── The mapping table, through the runner's own slot builder ──────────


@pytest.fixture
def mapped_runner(monkeypatch, tmp_path):
    """A runner whose loader publishes one strategy declaring ``warmup = 10``."""
    from qte_shared.strategies.plugin_loader import LoadedStrategy

    monkeypatch.setattr(
        "qte_strategy_engine.runner.load_strategies",
        lambda *args, **kwargs: [
            LoadedStrategy(name=Edge.name, cls=Edge, source=tmp_path / "edge.py")
        ],
    )
    return runner_module.StrategyRunner()


def test_the_strategies_table_sets_the_warmup_for_every_symbol(
    mapped_runner, monkeypatch, tmp_path
):
    monkeypatch.setattr(
        settings.engine,
        "mapping_file",
        table(
            tmp_path,
            """
            [symbols.XAUUSD]
            strategies = ["GOLD_M15"]

            [symbols.BTCUSDT]
            strategies = ["GOLD_M15"]

            [strategies.GOLD_M15]
            warmup = 25
            """,
        ),
    )

    mapped_runner._build_slots()

    assert [slot.warmup for slot in mapped_runner.slots] == [25, 25]
    assert all(slot.strategy.warmup == 10 for slot in mapped_runner.slots), (
        "the declaration is untouched; only the gate moved"
    )


def test_a_pair_can_restate_the_warmup_for_one_symbol(mapped_runner, monkeypatch, tmp_path):
    monkeypatch.setattr(
        settings.engine,
        "mapping_file",
        table(
            tmp_path,
            """
            [symbols.XAUUSD]
            strategies = ["GOLD_M15"]

            [symbols.BTCUSDT]
            strategies = ["GOLD_M15"]

            [strategies.GOLD_M15]
            warmup = 25

            [symbols.BTCUSDT.params.GOLD_M15]
            warmup = 40
            """,
        ),
    )

    mapped_runner._build_slots()

    assert {(slot.symbol, slot.warmup) for slot in mapped_runner.slots} == {
        ("XAUUSD", 25),
        ("BTCUSDT", 40),
    }


def test_an_unusable_warmup_stops_the_start_and_names_the_pair(
    mapped_runner, monkeypatch, tmp_path
):
    monkeypatch.setattr(
        settings.engine,
        "mapping_file",
        table(
            tmp_path,
            """
            [symbols.XAUUSD]
            strategies = ["GOLD_M15"]

            [strategies.GOLD_M15]
            warmup = 0
            """,
        ),
    )

    with pytest.raises(ValueError, match="GOLD_M15 on XAUUSD"):
        mapped_runner._build_slots()


# ── Both drivers, one number ──────────────────────────────────────────


async def test_the_override_moves_the_first_decision_in_both_drivers(monkeypatch):
    """The runner and the replay decide on the same bars under the same override."""
    required_bars = 3
    monkeypatch.setattr(settings.market_data, "provider", "simulator")
    candles = [bar_at(MOMENT + timedelta(minutes=15 * offset)) for offset in range(5)]

    class MarketClock(datetime):
        current_time = candles[0].open_time

        @classmethod
        def now(cls, timezone=None):
            return cls.current_time.astimezone(timezone)

    monkeypatch.setattr(runner_module, "datetime", MarketClock)
    runner, strategy_slot, live_strategy = build_runner(
        monkeypatch, CandleStore([]), provider="simulator"
    )
    # The strategy declares 1; this pair is mapped to 3, as the runner's own
    # resolve_warmup would hand it over.
    live_strategy.warmup = 1
    strategy_slot._warmup_override = required_bars
    await runner.start()
    try:
        for candle in candles:
            MarketClock.current_time = candle.open_time + timedelta(minutes=15, seconds=1)
            await deliver_live(runner, candle)
    finally:
        await runner.stop()

    # The replay gets the same number the only way a deployment states it: as a
    # pair param on the instance it replays.
    replay_strategy = DecisionRecorder({WARMUP_PARAM: required_bars})
    replay_strategy.warmup = 1
    replay_result = BacktestEngine(replay_strategy, symbol=SYMBOL).run(candles_to_frame(candles))

    expected = [candle.open_time for candle in candles[required_bars - 1 :]]
    assert [moment.replace(tzinfo=UTC) for moment in live_strategy.decided_on] == expected
    assert [moment.replace(tzinfo=UTC) for moment in replay_strategy.decided_on] == expected
    assert replay_result.warmup == required_bars


def test_the_replay_refuses_a_count_above_the_strategys_window():
    strategy = DecisionRecorder({WARMUP_PARAM: 10_000})
    candles = [bar_at(MOMENT + timedelta(minutes=15 * offset)) for offset in range(3)]

    with pytest.raises(ValueError, match="never finish warming up"):
        BacktestEngine(strategy, symbol=SYMBOL).run(candles_to_frame(candles))


# ── The broker never hears about it ───────────────────────────────────


def test_the_payload_carries_the_pairs_params_but_not_the_warmup():
    factory = SignalFactory(
        "WARMUP_PROBE",
        timeframe="M15",
        token="test",
        inputs={WARMUP_PARAM: 120, "risk_percent": 1.0},
        sizer=PositionSizer(capital=1000.0, risk_percent=1.0),
    )
    intent = SignalIntent(action=SignalAction.LONG, price=2000.0, sl=1990.0, tp2=2020.0)

    signal = factory.build(intent, symbol=SYMBOL, moment=MOMENT)

    assert WARMUP_PARAM not in factory.inputs
    assert WARMUP_PARAM not in signal.inputs
    assert signal.inputs["risk_percent"] == 1.0
    assert PORT_ONLY_PARAMS == {WARMUP_PARAM}
