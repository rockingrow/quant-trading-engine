"""Several trade cycles on one (strategy, symbol) pair, each with its own ``signal_uxid``.

Off by default: a pair holds one cycle and a second entry is refused, which is
what every driver did before ``allow_multiple_cycles`` existed. Switched on in the
mapping table, a pair holds up to ``max_open_cycles`` positions A, B, C side by
side. Each keeps its own bracket, each close names the cycle it closes, and the
factory, the replay and the runner all agree on the limit because all three read
it from the same pair params.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest
from fakeredis.aioredis import FakeRedis
from test_runner_delivery import FakePositions, FakeState, _runner

from qte_backtest.replay import BacktestEngine
from qte_shared.cache.redis_state import RedisState, cycle_field, cycle_symbol
from qte_shared.config import settings
from qte_shared.models import OpenPosition, SignalAction
from qte_shared.strategies.signal_factory import SignalFactory
from qte_shared.strategies.sizing import PositionSizer
from qte_shared.strategies.strategy_base import (
    SignalIntent,
    SignalStrategy,
    StrategyBase,
    StrategyContext,
)
from qte_shared.strategies.strategy_settings import (
    SINGLE_CYCLE,
    CyclePolicy,
    WeekendFlatPolicy,
    WeeklyMoment,
    resolve_cycle_policy,
)
from qte_strategy_engine.runner import StrategySlot

SYMBOL = "XAUUSD"
STRATEGY_NAME = "MULTI_PROBE"
MOMENT = datetime(2026, 5, 1, tzinfo=UTC)
MULTIPLE = {"allow_multiple_cycles": True, "max_open_cycles": 3}


def factory_for(params: dict | None = None) -> SignalFactory:
    return SignalFactory(
        STRATEGY_NAME,
        timeframe="M15",
        token="test",
        inputs=params or {},
        sizer=PositionSizer(capital=1000.0, risk_percent=1.0),
    )


def entry(uxid: str | None = None, price: float = 2000.0) -> SignalIntent:
    return SignalIntent(
        action=SignalAction.LONG, price=price, sl=price - 10, tp2=price + 20, signal_uxid=uxid
    )


def close(action: SignalAction, uxid: str | None = None) -> SignalIntent:
    return SignalIntent(action=action, price=2005.0, signal_uxid=uxid)


# ── The policy ──────────────────────────────────────────────────────────


def test_a_pair_that_says_nothing_holds_one_cycle():
    assert resolve_cycle_policy({}) is SINGLE_CYCLE
    assert SINGLE_CYCLE.limit == 1


def test_the_limit_applies_only_when_multiple_cycles_are_allowed():
    assert resolve_cycle_policy(MULTIPLE) == CyclePolicy(True, 3)
    assert resolve_cycle_policy(MULTIPLE).limit == 3
    assert resolve_cycle_policy({"max_open_cycles": 3}).limit == 1


@pytest.mark.parametrize(
    "params",
    [
        {"allow_multiple_cycles": "yes"},
        {"allow_multiple_cycles": True, "max_open_cycles": 0},
        {"allow_multiple_cycles": True, "max_open_cycles": 2.5},
        {"allow_multiple_cycles": True, "max_open_cycles": True},
    ],
)
def test_a_value_that_cannot_be_honoured_is_refused(params):
    with pytest.raises(ValueError):
        resolve_cycle_policy(params)


# ── The factory ─────────────────────────────────────────────────────────


def test_a_single_cycle_pair_still_refuses_a_second_entry():
    factory = factory_for()
    factory.build(entry(), symbol=SYMBOL, moment=MOMENT)
    with pytest.raises(ValueError, match="would replace open cycle"):
        factory.build(entry(), symbol=SYMBOL, moment=MOMENT)


def test_a_multiple_cycle_pair_holds_up_to_its_limit_and_no_more():
    factory = factory_for(MULTIPLE)
    for uxid in ("A000000000000001", "B000000000000002", "C000000000000003"):
        factory.build(entry(uxid), symbol=SYMBOL, moment=MOMENT)
    assert factory.open_cycles(SYMBOL) == (
        "A000000000000001",
        "B000000000000002",
        "C000000000000003",
    )
    assert factory.open_cycle(SYMBOL) == "C000000000000003", "the most recent"
    with pytest.raises(ValueError, match="at most 3"):
        factory.build(entry("D000000000000004"), symbol=SYMBOL, moment=MOMENT)


def test_an_entry_without_an_id_gets_a_fresh_one():
    factory = factory_for(MULTIPLE)
    first = factory.build(entry(), symbol=SYMBOL, moment=MOMENT)
    second = factory.build(entry(), symbol=SYMBOL, moment=MOMENT)
    assert first.signal_uxid != second.signal_uxid
    assert factory.open_cycles(SYMBOL) == (first.signal_uxid, second.signal_uxid)


def test_an_open_cycle_id_cannot_be_reused():
    factory = factory_for(MULTIPLE)
    factory.build(entry("A000000000000001"), symbol=SYMBOL, moment=MOMENT)
    with pytest.raises(ValueError, match="reuses open cycle id"):
        factory.build(entry("A000000000000001"), symbol=SYMBOL, moment=MOMENT)


def test_each_close_touches_only_the_cycle_it_names():
    factory = factory_for(MULTIPLE)
    for uxid in ("A000000000000001", "B000000000000002", "C000000000000003"):
        factory.build(entry(uxid), symbol=SYMBOL, moment=MOMENT)
    before_b = factory.open_position(SYMBOL, "B000000000000002").remaining

    closing = factory.build(
        close(SignalAction.SL, "A000000000000001"), symbol=SYMBOL, moment=MOMENT
    )
    assert closing.signal_uxid == "A000000000000001"
    assert closing.position.quantity == pytest.approx(before_b), "sized off A, as big as B"
    assert factory.open_cycles(SYMBOL) == ("B000000000000002", "C000000000000003")

    factory.build(close(SignalAction.TP2, "C000000000000003"), symbol=SYMBOL, moment=MOMENT)
    assert factory.open_cycles(SYMBOL) == ("B000000000000002",)
    assert factory.open_position(SYMBOL, "B000000000000002").remaining == pytest.approx(before_b)


def test_each_cycle_keeps_its_own_partial():
    factory = factory_for(MULTIPLE)
    factory.build(entry("A000000000000001"), symbol=SYMBOL, moment=MOMENT)
    factory.build(entry("B000000000000002"), symbol=SYMBOL, moment=MOMENT)
    full = factory.open_position(SYMBOL, "A000000000000001").quantity
    partial = SignalIntent(
        action=SignalAction.TP1, price=2010.0, signal_uxid="A000000000000001", tp1_percent=50.0
    )
    factory.build(partial, symbol=SYMBOL, moment=MOMENT)
    assert factory.open_position(SYMBOL, "A000000000000001").remaining == pytest.approx(full / 2)
    assert factory.open_position(SYMBOL, "B000000000000002").remaining == pytest.approx(full)


def test_an_unnamed_close_is_refused_while_several_cycles_are_open():
    factory = factory_for(MULTIPLE)
    factory.build(entry("A000000000000001"), symbol=SYMBOL, moment=MOMENT)
    factory.build(entry("B000000000000002"), symbol=SYMBOL, moment=MOMENT)
    with pytest.raises(ValueError, match="names no cycle"):
        factory.build(close(SignalAction.R_SL), symbol=SYMBOL, moment=MOMENT)


def test_an_unnamed_close_still_reaches_the_only_open_cycle():
    factory = factory_for(MULTIPLE)
    factory.build(entry("A000000000000001"), symbol=SYMBOL, moment=MOMENT)
    closing = factory.build(close(SignalAction.R_SL), symbol=SYMBOL, moment=MOMENT)
    assert closing.signal_uxid == "A000000000000001"
    assert factory.open_cycles(SYMBOL) == ()


# ── The strategy contract ───────────────────────────────────────────────


def test_can_open_counts_the_cycles_the_driver_reported():
    context = StrategyContext(symbol=SYMBOL, timeframe="M15", now=MOMENT)
    assert context.can_open
    context.open_uxid = "A000000000000001"
    assert not context.can_open, "a driver that only set open_uxid holds one"
    context.open_uxids = ("A000000000000001", "B000000000000002")
    context.max_open_cycles = 3
    assert context.can_open
    context.open_uxids = ("A000000000000001", "B000000000000002", "C000000000000003")
    assert not context.can_open


class OneOfEach(SignalStrategy):
    name = STRATEGY_NAME

    def long(self, candles_frame, context):
        return entry()

    def short(self, candles_frame, context):
        return None

    def tp1(self, candles_frame, context):
        return None

    def tp2(self, candles_frame, context):
        return None

    def sl(self, candles_frame, context):
        return SignalIntent(action=SignalAction.SL, signal_uxid=context.open_uxids[0])


def test_the_dispatcher_asks_for_an_entry_beside_the_exits_while_room_is_left():
    strategy = OneOfEach()
    context = StrategyContext(
        symbol=SYMBOL,
        timeframe="M15",
        now=MOMENT,
        open_uxid="A000000000000001",
        open_uxids=("A000000000000001",),
        max_open_cycles=2,
    )
    actions = [intent.action for intent in strategy.on_candle_closed(pd.DataFrame(), context)]
    assert actions == [SignalAction.SL, SignalAction.LONG]
    context.max_open_cycles = 1
    actions = [intent.action for intent in strategy.on_candle_closed(pd.DataFrame(), context)]
    assert actions == [SignalAction.SL], "a single-cycle pair is never asked while holding"


# ── The replay ──────────────────────────────────────────────────────────


def bars(closes: list[float], start: datetime = MOMENT) -> pd.DataFrame:
    index = pd.DatetimeIndex(
        [start + timedelta(minutes=15 * offset) for offset in range(len(closes))]
    )
    return pd.DataFrame(
        {
            "open": closes,
            "high": [close_price + 0.5 for close_price in closes],
            "low": [close_price - 0.5 for close_price in closes],
            "close": closes,
            "volume": [1.0] * len(closes),
        },
        index=index,
    )


class ScriptedCycles(StrategyBase):
    """Opens a cycle on bars 0, 1, 2 and 3, and closes the second one by name on bar 4."""

    name = STRATEGY_NAME
    timeframe = "M15"
    warmup = 1
    max_history = 5

    def __init__(self, params=None):
        super().__init__(params)
        self.seen: list[tuple[str, ...]] = []
        self.bar_number = -1

    def on_candle_closed(self, candles_frame, context):
        self.bar_number += 1
        self.seen.append(context.open_uxids)
        price = float(candles_frame["close"].iloc[-1])
        if self.bar_number <= 3:
            return SignalIntent(
                action=SignalAction.LONG,
                price=price,
                sl=price - 50,
                tp2=price + 50,
                signal_uxid=f"CYCLE{self.bar_number:011d}",
            )
        if self.bar_number == 4:
            return SignalIntent(
                action=SignalAction.TP2, price=price, signal_uxid="CYCLE00000000001"
            )
        return None


def replay(params: dict, closes: list[float], **engine_options) -> tuple:
    strategy = ScriptedCycles(params)
    engine = BacktestEngine(
        strategy,
        symbol=SYMBOL,
        timeframe="M15",
        starting_equity=1000.0,
        sizer=PositionSizer(capital=1000.0, risk_percent=1.0),
        **engine_options,
    )
    return engine.run(bars(closes)), strategy


def test_the_replay_holds_one_cycle_by_default():
    result, strategy = replay({}, [2000.0] * 8)
    assert result.rejected == 3
    assert strategy.seen[1] == ("CYCLE00000000000",)
    assert [position.signal_uxid for position in result.positions] == ["CYCLE00000000000"]


def test_the_replay_holds_every_cycle_up_to_the_limit_and_closes_them_by_name():
    result, strategy = replay(MULTIPLE, [2000.0] * 8)
    assert result.rejected == 1, "the fourth entry is over the limit of three"
    assert strategy.seen[3] == ("CYCLE00000000000", "CYCLE00000000001", "CYCLE00000000002")
    assert strategy.seen[5] == ("CYCLE00000000000", "CYCLE00000000002"), (
        "only the named cycle closed"
    )
    assert [position.signal_uxid for position in result.positions] == [
        "CYCLE00000000000",
        "CYCLE00000000001",
        "CYCLE00000000002",
    ]
    closed_by_strategy = result.positions[1]
    assert closed_by_strategy.exit_reason == "TP2"
    assert [leg.reason.value for leg in result.positions[0].legs] == ["END_OF_DATA"]


def test_each_cycle_is_closed_by_its_own_bracket_or_by_name():
    # Each entry rests its stop 50 below and its target 50 above. Cycle 0
    # (2000) reaches its target on bar 2; that frees room, so bar 3 opens
    # cycle 3 at 2080. Cycle 1 is closed by name on bar 4, and the fall to 2025
    # on bar 5 stops out cycles 2 and 3 (stops at 2030) but no other.
    closes = [2000.0, 2040.0, 2080.0, 2080.0, 2080.0, 2025.0, 2025.0]
    result, strategy = replay(MULTIPLE, closes)
    by_id = {position.signal_uxid: position for position in result.positions}
    assert by_id["CYCLE00000000000"].exit_reason == "TP2"
    # Its target, 2090, is never reached: this TP2 is the strategy's, by name.
    assert by_id["CYCLE00000000001"].exit_reason == "TP2"
    assert by_id["CYCLE00000000002"].exit_reason == "SL"
    assert by_id["CYCLE00000000003"].exit_reason == "SL"
    assert result.rejected == 0


def test_the_weekend_flat_closes_every_cycle_by_name():
    friday_evening = datetime(2026, 5, 1, 16, 15, tzinfo=UTC)
    policy = WeekendFlatPolicy(
        enabled=True,
        flat_from=WeeklyMoment(weekday=4, minute_of_day=17 * 60),
        flat_until=WeeklyMoment(weekday=6, minute_of_day=22 * 60),
    )
    strategy = ScriptedCycles(MULTIPLE)
    engine = BacktestEngine(
        strategy,
        symbol=SYMBOL,
        timeframe="M15",
        starting_equity=1000.0,
        sizer=PositionSizer(capital=1000.0, risk_percent=1.0),
        weekend_flat=policy,
    )
    result = engine.run(bars([2000.0] * 8, start=friday_evening))
    flats = [signal for signal in result.signals if signal.position.action is SignalAction.FLAT]
    assert sorted(signal.signal_uxid for signal in flats) == [
        "CYCLE00000000000",
        "CYCLE00000000001",
        "CYCLE00000000002",
    ]
    assert all(not position.is_open for position in result.positions)


# ── The runner ──────────────────────────────────────────────────────────


class CycleState(FakeState):
    """Redis surface keyed by cycle, as the real hash now is."""

    def __init__(self, held=None):
        super().__init__()
        self.cycles: dict[tuple[str, str, str], OpenPosition] = dict(held or {})

    async def get_open_positions_for(self, strategy, symbol):
        return [
            position
            for (held_strategy, held_symbol, _), position in self.cycles.items()
            if (held_strategy, held_symbol) == (strategy, symbol)
        ]

    async def set_open_position(self, position):
        self.cycles[(position.strategy, position.symbol, position.signal_uxid)] = position

    async def clear_open_position(self, strategy, symbol, signal_uxid):
        self.cycles.pop((strategy, symbol, signal_uxid), None)


class CyclePositions(FakePositions):
    def __init__(self, held=None):
        super().__init__()
        self.cycles: dict[tuple[str, str, str], OpenPosition] = dict(held or {})

    async def list_for(self, strategy, symbol):
        return [
            position
            for (held_strategy, held_symbol, _), position in self.cycles.items()
            if (held_strategy, held_symbol) == (strategy, symbol)
        ]

    async def upsert(self, position):
        self.cycles[(position.strategy, position.symbol, position.signal_uxid)] = position
        return True

    async def clear(self, strategy, symbol, signal_uxid=None):
        for key in [key for key in self.cycles if key[:2] == (strategy, symbol)]:
            if signal_uxid in (None, key[2]):
                del self.cycles[key]
        return True

    async def list_open(self, strategy=None):
        return list(self.cycles.values())


class SilentStrategy(StrategyBase):
    name = STRATEGY_NAME
    timeframe = "M15"
    warmup = 1

    def on_candle_closed(self, candles_frame, context):
        return None


def multi_slot() -> StrategySlot:
    return StrategySlot(SilentStrategy(), SYMBOL, factory_for(MULTIPLE))


def stored(uxid: str, opened_minutes_ago: int) -> OpenPosition:
    opened = datetime.now(UTC) - timedelta(minutes=opened_minutes_ago)
    return OpenPosition(
        state_namespace=settings.state_scope.namespace,
        signal_uxid=uxid,
        strategy=STRATEGY_NAME,
        symbol=SYMBOL,
        action=SignalAction.LONG,
        price=2000.0,
        quantity=1.0,
        remaining=1.0,
        opened_at=opened,
        updated_at=opened,
    )


async def test_the_runner_delivers_and_persists_each_cycle_on_its_own():
    runner = _runner(state=CycleState(), positions=CyclePositions())
    strategy_slot = multi_slot()
    for uxid in ("A000000000000001", "B000000000000002"):
        await runner._emit(strategy_slot, entry(uxid), 2000.0, MOMENT)
    assert set(runner.state.cycles) == {
        (STRATEGY_NAME, SYMBOL, "A000000000000001"),
        (STRATEGY_NAME, SYMBOL, "B000000000000002"),
    }
    assert set(runner.positions.cycles) == set(runner.state.cycles)

    await runner._emit(strategy_slot, close(SignalAction.SL, "A000000000000001"), 2000.0, MOMENT)
    assert set(runner.state.cycles) == {(STRATEGY_NAME, SYMBOL, "B000000000000002")}
    assert set(runner.positions.cycles) == {(STRATEGY_NAME, SYMBOL, "B000000000000002")}
    assert strategy_slot.factory.open_cycles(SYMBOL) == ("B000000000000002",)


async def test_the_runner_restores_every_cycle_a_pair_holds():
    held = {
        (STRATEGY_NAME, SYMBOL, uxid): stored(uxid, minutes)
        for uxid, minutes in (("A000000000000001", 30), ("B000000000000002", 20))
    }
    runner = _runner(state=CycleState(), positions=CyclePositions(held))
    strategy_slot = multi_slot()
    await runner._restore_position(strategy_slot)
    assert strategy_slot.factory.open_cycles(SYMBOL) == ("A000000000000001", "B000000000000002")
    assert set(runner.state.cycles) == set(held), "Postgres re-seeded Redis"


async def test_the_context_reports_every_cycle_and_the_limit():
    strategy_slot = multi_slot()
    for uxid in ("A000000000000001", "B000000000000002"):
        strategy_slot.factory.restore_position(stored(uxid, 10))
    context = _runner()._strategy_context(strategy_slot, MOMENT)
    assert context.open_uxids == ("A000000000000001", "B000000000000002")
    assert context.open_uxid == "B000000000000002"
    assert context.max_open_cycles == 3
    assert context.can_open


async def test_the_runner_weekend_flat_names_every_cycle():
    runner = _runner(state=CycleState(), positions=CyclePositions())
    strategy_slot = multi_slot()
    for uxid in ("A000000000000001", "B000000000000002"):
        await runner._emit(strategy_slot, entry(uxid), 2000.0, MOMENT)
    await runner._flatten_for_the_weekend(strategy_slot, 2001.0, MOMENT)
    flats = [
        signal for signal, _ in runner.signals.rows if signal.position.action is SignalAction.FLAT
    ]
    assert [signal.signal_uxid for signal in flats] == ["A000000000000001", "B000000000000002"]
    assert strategy_slot.factory.open_cycles(SYMBOL) == ()
    assert runner.state.cycles == {}


# ── Redis layout ────────────────────────────────────────────────────────


@pytest.fixture
async def cycle_store():
    redis_state = RedisState(prefix="cycle-test")
    redis_state._client = FakeRedis(decode_responses=True)
    try:
        yield redis_state
    finally:
        await redis_state.close()


def test_a_cycle_field_names_its_symbol_and_its_id():
    assert cycle_field(SYMBOL, "A000000000000001") == "XAUUSD|A000000000000001"
    assert cycle_symbol("XAUUSD|A000000000000001") == SYMBOL
    assert cycle_symbol(SYMBOL) == SYMBOL, "a legacy field is the bare symbol"


async def test_redis_keeps_one_field_per_cycle(cycle_store):
    for uxid, minutes in (("A000000000000001", 30), ("B000000000000002", 20)):
        await cycle_store.set_open_position(stored(uxid, minutes))
    held = await cycle_store.get_open_positions_for(STRATEGY_NAME, SYMBOL)
    assert [position.signal_uxid for position in held] == ["A000000000000001", "B000000000000002"]

    await cycle_store.clear_open_position(STRATEGY_NAME, SYMBOL, "A000000000000001")
    held = await cycle_store.get_open_positions_for(STRATEGY_NAME, SYMBOL)
    assert [position.signal_uxid for position in held] == ["B000000000000002"]
    assert await cycle_store.get_open_positions_for(STRATEGY_NAME, "USOIL") == []


async def test_a_legacy_symbol_field_is_read_and_moved_on_its_next_write(cycle_store):
    legacy = stored("A000000000000001", 30)
    hash_key = cycle_store.key("cycle", STRATEGY_NAME)
    await cycle_store.client.hset(hash_key, SYMBOL, legacy.model_dump_json())
    held = await cycle_store.get_open_positions_for(STRATEGY_NAME, SYMBOL)
    assert [position.signal_uxid for position in held] == ["A000000000000001"]

    await cycle_store.set_open_position(legacy)
    assert set(await cycle_store.client.hkeys(hash_key)) == {
        cycle_field(SYMBOL, "A000000000000001")
    }


async def test_clearing_a_pair_forgets_every_cycle_on_it(cycle_store):
    for uxid, minutes in (("A000000000000001", 30), ("B000000000000002", 20)):
        await cycle_store.set_open_position(stored(uxid, minutes))
    await cycle_store.clear_open_cycle(STRATEGY_NAME, SYMBOL)
    assert await cycle_store.get_open_positions_for(STRATEGY_NAME, SYMBOL) == []
