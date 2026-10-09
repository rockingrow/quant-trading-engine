"""Executable spec for ``/prevent`` and ``/allow``.

Two halves. The gate itself is a value object, so its rules are asserted
directly: what a scope blocks, what releasing one takes back, and what a
malformed flag in Redis must not do. Then the property the whole feature rests
on, asserted through the runner: a paused pair keeps filling its window and
only skips the decision, so ``/allow`` resumes on the next bar rather than on a
window with a hole in it.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from test_runner_delivery import FakeState, RecordingSignals, _runner, _slot

from qte_shared.bar_gate import BAR_GATE_FLAG, BarGate
from qte_shared.config import settings
from qte_shared.models import Candle
from qte_shared.strategies.strategy_base import StrategyBase
from qte_shared.timeframes import floor_to_bucket

BAR_LENGTH = timedelta(minutes=15)


# ── The gate ──────────────────────────────────────────────────────────────


def test_an_empty_gate_blocks_nothing():
    gate = BarGate()
    assert not gate.blocking
    assert not gate.blocks(symbol="XAUUSD", strategy="PROBE")
    assert gate.describe() == "nothing"


def test_a_symbol_is_blocked_by_name_and_case_does_not_matter():
    gate = BarGate().prevent(symbol="xauusd")
    assert gate.blocks(symbol="XAUUSD", strategy="PROBE")
    assert not gate.blocks(symbol="EURUSD", strategy="PROBE")


def test_a_strategy_is_blocked_across_every_symbol_it_trades():
    gate = BarGate().prevent(strategy="PROBE")
    assert gate.blocks(symbol="XAUUSD", strategy="PROBE")
    assert gate.blocks(symbol="EURUSD", strategy="PROBE")
    assert not gate.blocks(symbol="XAUUSD", strategy="OTHER")


def test_everything_outranks_the_named_sets():
    gate = BarGate().prevent(everything=True)
    assert gate.blocks(symbol="ANYTHING", strategy="ANY")
    assert gate.describe() == "everything"


def test_releasing_one_scope_leaves_the_others_paused():
    gate = BarGate().prevent(symbol="XAUUSD").prevent(strategy="PROBE")
    gate.allow(symbol="XAUUSD")
    assert not gate.blocks(symbol="XAUUSD", strategy="OTHER")
    assert gate.blocks(symbol="EURUSD", strategy="PROBE")


def test_allowing_everything_clears_the_named_sets_too():
    """ "Allow all" means trading resumes, not "resume all but yesterday's pause"."""
    gate = BarGate(everything=True, symbols={"XAUUSD"}, strategies={"PROBE"})
    gate.allow(everything=True)
    assert not gate.blocking
    assert not gate.blocks(symbol="XAUUSD", strategy="PROBE")


def test_a_gate_survives_a_round_trip_through_redis():
    gate = BarGate().prevent(symbol="XAUUSD").prevent(strategy="PROBE")
    restored = BarGate.from_payload(gate.to_payload())
    assert restored == gate


def test_a_malformed_flag_reads_as_nothing_blocked():
    """A runner that refused every bar over one bad value would be worse."""
    for payload in (None, "paused", 7, {"symbols": "XAUUSD"}, {"everything": None}):
        assert not BarGate.from_payload(payload).blocking


# ── Through the runner ────────────────────────────────────────────────────


def _bar(open_time: datetime, close: float = 2340.0) -> Candle:
    return Candle(
        origin=settings.state_scope.origin(),
        symbol="XAUUSD",
        timeframe="M15",
        open_time=open_time,
        open=close,
        high=close,
        low=close,
        close=close,
        tick_count=5,
    )


class GateState(FakeState):
    """FakeState plus the candle-window surface the decision path touches."""

    def __init__(self, gate: BarGate | None = None) -> None:
        super().__init__()
        if gate is not None:
            self.flags[BAR_GATE_FLAG] = gate.to_payload()
        self.decided: list[datetime] = []

    async def set_decided_open_time(self, strategy, symbol, timeframe, open_time):
        self.decided.append(open_time)

    async def get_decided_open_time(self, strategy, symbol, timeframe):
        return self.decided[-1] if self.decided else None


class CountingStrategy(StrategyBase):
    """Records the bars it was asked about, and trades nothing."""

    name = "DELIVERY_PROBE"
    timeframe = "M15"
    warmup = 1
    max_history = 50

    def __init__(self, params=None) -> None:
        super().__init__(params)
        self.decisions: list[datetime] = []

    def on_candle_closed(self, candles_frame, context):
        self.decisions.append(context.now)
        return None


async def _feed(gate: BarGate | None, bar_count: int = 2):
    """Feed *bar_count* bars to a slot, with *gate* stored in Redis."""
    runner = _runner(state=GateState(gate))
    runner.signals = RecordingSignals()
    slot = _slot()
    counting = CountingStrategy()
    slot.strategy = counting
    runner.slots = [slot]

    first_open = floor_to_bucket(datetime.now(UTC), "M15") - BAR_LENGTH * bar_count
    for index in range(bar_count):
        await runner._feed_candle_serialized(slot, _bar(first_open + BAR_LENGTH * index))
    return runner, slot, counting


async def test_a_paused_pair_still_fills_its_window():
    """The bar is stored and recorded; only the decision is skipped."""
    runner, slot, strategy = await _feed(BarGate().prevent(symbol="XAUUSD"))

    assert strategy.decisions == [], "the strategy was never asked"
    assert len(slot.buffer) == 2, "but the window kept filling"
    assert len(runner.state.decided) == 2, "and each bar was recorded as seen"


async def test_a_pair_nobody_paused_decides_as_usual():
    _, slot, strategy = await _feed(None)
    assert len(strategy.decisions) == 2
    assert len(slot.buffer) == 2


async def test_pausing_a_strategy_blocks_it_on_every_symbol():
    _, _, strategy = await _feed(BarGate().prevent(strategy="DELIVERY_PROBE"))
    assert strategy.decisions == []


async def test_pausing_everything_blocks_a_pair_nobody_named():
    _, _, strategy = await _feed(BarGate().prevent(everything=True))
    assert strategy.decisions == []


async def test_releasing_the_gate_resumes_on_the_next_bar_with_a_whole_window():
    """What ``/allow`` is for: no hole, so the first decision is a real one."""
    runner, slot, strategy = await _feed(BarGate().prevent(symbol="XAUUSD"))
    assert strategy.decisions == []

    runner.state.flags[BAR_GATE_FLAG] = BarGate().to_payload()
    next_open = slot.buffer[-1].open_time + BAR_LENGTH
    await runner._feed_candle_serialized(slot, _bar(next_open))

    assert strategy.decisions == [next_open]
    assert len(slot.buffer) == 3, "the paused bars are still behind it"


async def test_an_unreadable_redis_keeps_the_last_known_gate():
    """A Redis blip must not silently resume a pair an operator paused."""
    runner, slot, strategy = await _feed(BarGate().prevent(everything=True))

    async def refuse(*arguments, **keywords):
        raise RuntimeError("Redis is unreachable")

    runner.state.get_flag = refuse
    await runner._feed_candle_serialized(slot, _bar(slot.buffer[-1].open_time + BAR_LENGTH))

    assert strategy.decisions == []
