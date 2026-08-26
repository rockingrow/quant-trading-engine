"""The live runner commits position state only after broker delivery — and
then writes it where a restart can find it again.

Two rules are pinned here. Delivery first: a signal the broker never accepted
must leave no trace, or the runner ends up holding a position that does not
exist (or forgetting one that does). Then persistence: what it *does* hold goes
to Redis and Postgres both, and boot prefers the cache and falls back to the
table — because an empty cache is otherwise indistinguishable from being flat.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from qte_shared.models import OpenPosition, SignalAction
from qte_shared.signal_factory import SignalFactory
from qte_shared.sizing import PositionSizer
from qte_shared.strategy_base import SignalIntent, StrategyBase
from qte_strategy_engine.broker_sink import DeliveryResult
from qte_strategy_engine.runner import StrategyRunner, StrategySlot

MOMENT = datetime(2026, 5, 1, tzinfo=UTC)


class ProbeStrategy(StrategyBase):
    name = "DELIVERY_PROBE"
    timeframe = "M15"
    warmup = 1

    def on_candle_closed(self, df, context):
        return None


class FailedSink:
    shadow_mode = False

    async def send(self, signal):
        return DeliveryResult(status="failed", transport="nats", detail="no ack")


class RecordingSignals:
    def __init__(self):
        self.rows = []

    async def record_signal(self, signal, **delivery):
        self.rows.append((signal, delivery))


class RecordingBus:
    def __init__(self):
        self.messages = []

    async def publish(self, subject, payload):
        self.messages.append((subject, payload))


async def test_failed_entry_delivery_does_not_create_a_ghost_live_cycle():
    runner = StrategyRunner(sink=FailedSink())
    runner.signals = RecordingSignals()
    runner.bus = RecordingBus()
    factory = SignalFactory("DELIVERY_PROBE", timeframe="M15", token="test")
    slot = StrategySlot(ProbeStrategy(), "XAUUSD", factory)

    await runner._emit(
        slot,
        SignalIntent(action=SignalAction.LONG, price=2000.0, quantity=1.0, sl=1990.0),
        fallback_price=2000.0,
        moment=datetime(2026, 5, 1, tzinfo=UTC),
    )

    assert factory.open_cycle("XAUUSD") is None
    assert runner.signals.rows[0][1]["delivery_status"] == "failed"
    assert runner.bus.messages[0][1]["delivery"]["status"] == "failed"


# ── Position state is written twice and read back in order ───────────────
#
# Redis is the hot copy and Postgres the durable one. The failure this guards
# against is a re-provisioned cache reading as "flat": the runner would mint a
# second cycle against a position the broker is still carrying, and nothing
# would ever close the first.


class AcceptingSink:
    shadow_mode = False

    async def send(self, signal):
        return DeliveryResult(status="sent", transport="nats")


class FakeState:
    """The parts of RedisState the runner touches for cycle state."""

    def __init__(self, held: dict | None = None) -> None:
        self.held = dict(held or {})
        self.cleared: list[tuple[str, str]] = []

    async def get_open_position(self, strategy, symbol):
        return self.held.get((strategy, symbol))

    async def set_open_position(self, position):
        self.held[(position.strategy, position.symbol)] = position

    async def clear_open_cycle(self, strategy, symbol):
        self.held.pop((strategy, symbol), None)
        self.cleared.append((strategy, symbol))


class FakePositions:
    """The same, for the ``open_positions`` table."""

    def __init__(self, held: dict | None = None) -> None:
        self.held = dict(held or {})
        self.cleared: list[tuple[str, str]] = []

    async def get(self, strategy, symbol):
        return self.held.get((strategy, symbol))

    async def upsert(self, position):
        self.held[(position.strategy, position.symbol)] = position
        return True

    async def clear(self, strategy, symbol):
        self.held.pop((strategy, symbol), None)
        self.cleared.append((strategy, symbol))
        return True


def _runner(state=None, positions=None, sink=None):
    runner = StrategyRunner(sink=sink or AcceptingSink())
    runner.signals = RecordingSignals()
    runner.bus = RecordingBus()
    runner.state = state or FakeState()
    runner.positions = positions or FakePositions()
    return runner


def _slot() -> StrategySlot:
    factory = SignalFactory(
        "DELIVERY_PROBE",
        timeframe="M15",
        token="test",
        sizer=PositionSizer(capital=1000.0, risk_percent=3.0),
    )
    return StrategySlot(ProbeStrategy(), "XAUUSD", factory)


async def _enter(runner, slot, **kwargs):
    await runner._emit(
        slot,
        SignalIntent(
            action=SignalAction.LONG, price=2334.50, sl=2329.50, tp1_percent=30.0, **kwargs
        ),
        fallback_price=2334.50,
        moment=MOMENT,
    )


async def _close(runner, slot, action, price, **kwargs):
    await runner._emit(
        slot,
        SignalIntent(action=action, price=price, **kwargs),
        fallback_price=price,
        moment=MOMENT,
    )


async def test_a_delivered_entry_lands_in_both_redis_and_postgres():
    runner, slot = _runner(), _slot()
    await _enter(runner, slot)

    key = ("DELIVERY_PROBE", "XAUUSD")
    for store in (runner.state.held, runner.positions.held):
        assert store[key].signal_uxid == slot.factory.open_cycle("XAUUSD")
        assert store[key].remaining == pytest.approx(6.0)


async def test_a_partial_close_updates_both_stores_rather_than_clearing_them():
    runner, slot = _runner(), _slot()
    await _enter(runner, slot)
    await _close(runner, slot, SignalAction.TP1, 2345.0)

    key = ("DELIVERY_PROBE", "XAUUSD")
    assert runner.state.held[key].remaining == pytest.approx(4.2)
    assert runner.positions.held[key].remaining == pytest.approx(4.2)
    assert runner.state.cleared == []


async def test_a_terminal_close_clears_both_stores():
    runner, slot = _runner(), _slot()
    await _enter(runner, slot)
    await _close(runner, slot, SignalAction.TP2, 2350.0)

    assert runner.state.held == {}
    assert runner.positions.held == {}
    assert runner.state.cleared == runner.positions.cleared == [("DELIVERY_PROBE", "XAUUSD")]


async def test_a_tp1_taking_the_whole_entry_clears_the_cycle_like_any_other_exit():
    # The exit rule, end to end: TP1 is a partial by contract and an exit by
    # arithmetic, and only the second reading leaves the runner able to trade.
    runner, slot = _runner(), _slot()
    await _enter(runner, slot)
    await _close(runner, slot, SignalAction.TP1, 2345.0, quantity=6.0)

    assert slot.factory.open_cycle("XAUUSD") is None
    assert runner.state.held == {}
    assert runner.positions.held == {}


async def test_a_failed_delivery_persists_nothing_at_all():
    runner, slot = _runner(sink=FailedSink()), _slot()
    await _enter(runner, slot)

    assert runner.state.held == {}
    assert runner.positions.held == {}


# ── Restoring after a restart ────────────────────────────────────────────


def _held(**kwargs) -> OpenPosition:
    defaults = {
        "signal_uxid": "9F2C4B7E18A3D605",
        "strategy": "DELIVERY_PROBE",
        "symbol": "XAUUSD",
        "action": SignalAction.LONG,
        "price": 2334.50,
        "quantity": 6.0,
        "remaining": 4.2,
    }
    return OpenPosition(**{**defaults, **kwargs})


async def test_boot_reads_the_cycle_out_of_redis():
    key = ("DELIVERY_PROBE", "XAUUSD")
    runner = _runner(state=FakeState({key: _held()}))
    slot = _slot()

    await runner._restore_position(slot)

    restored = slot.factory.open_position("XAUUSD")
    assert restored.signal_uxid == "9F2C4B7E18A3D605"
    assert restored.remaining == pytest.approx(4.2)


async def test_an_empty_cache_falls_back_to_the_table_and_re_seeds_it():
    # A flushed Redis is indistinguishable from "flat" without this.
    key = ("DELIVERY_PROBE", "XAUUSD")
    runner = _runner(state=FakeState(), positions=FakePositions({key: _held()}))
    slot = _slot()

    await runner._restore_position(slot)

    assert slot.factory.open_cycle("XAUUSD") == "9F2C4B7E18A3D605"
    assert runner.state.held[key].signal_uxid == "9F2C4B7E18A3D605"


async def test_nothing_stored_anywhere_means_the_slot_really_is_flat():
    runner, slot = _runner(), _slot()
    await runner._restore_position(slot)
    assert slot.factory.open_position("XAUUSD") is None


async def test_a_restored_runner_closes_the_position_at_the_size_that_is_left():
    key = ("DELIVERY_PROBE", "XAUUSD")
    runner = _runner(state=FakeState({key: _held()}))
    slot = _slot()
    await runner._restore_position(slot)

    await _close(runner, slot, SignalAction.SL, 2329.50)

    signal, _ = runner.signals.rows[0]
    assert signal.signal_uxid == "9F2C4B7E18A3D605"
    assert signal.position.quantity == pytest.approx(4.2)
    assert runner.state.held == {}
