"""An outage leaves a position open; the pair must not come back locked on it.

One ``(strategy, symbol)`` pair holds one cycle at a time, so a row that
outlived a long downtime refuses every entry the strategy proposes afterwards —
against a position whose bracket the market left behind hours ago. The runner
closes those on start with ``R_SL``.

Age is what separates that from an ordinary restart, and the split is the thing
most worth pinning: a deploy comes back inside the window and keeps its
positions, because the Redis/Postgres recovery path exists precisely so a
restart does not forget a position the broker still holds.

The second half of the file is the other direction of the pair rule — two pairs
must never claim one ``signal_uxid``, because the broker groups a trade by it
and a close on either would then close the other's position.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from test_runner_delivery import (
    AcceptingSink,
    FakePositions,
    FakeState,
    RecordingBus,
    RecordingSignals,
)

from qte_shared.config import settings
from qte_shared.models import Candle, OpenPosition, SignalAction
from qte_shared.strategies.signal_factory import SignalFactory
from qte_shared.strategies.sizing import PositionSizer
from qte_shared.strategies.strategy_base import StrategyBase
from qte_strategy_engine.runner import StrategyRunner, StrategySlot

STRATEGY_NAME = "STALE_PROBE"
SYMBOL = "XAUUSD"
CYCLE_UXID = "9F2C4B7E18A3D605"


class QuietStrategy(StrategyBase):
    name = STRATEGY_NAME
    timeframe = "M15"
    warmup = 1

    def on_candle_closed(self, candles_frame, context):
        return None


def held_position(**overrides) -> OpenPosition:
    """A cycle as the table hands it back, aged by ``updated_at``."""
    defaults = {
        "state_namespace": settings.state_scope.namespace,
        "signal_uxid": CYCLE_UXID,
        "strategy": STRATEGY_NAME,
        "symbol": SYMBOL,
        "action": SignalAction.LONG,
        "price": 2334.50,
        "quantity": 6.0,
        "remaining": 4.2,
        "opened_at": datetime.now(UTC) - timedelta(hours=9),
        "updated_at": datetime.now(UTC) - timedelta(hours=9),
    }
    return OpenPosition(**{**defaults, **overrides})


def aged(seconds: float) -> datetime:
    return datetime.now(UTC) - timedelta(seconds=seconds)


def build_runner(*, positions: dict | None = None, sink=None):
    """A runner holding one slot, wired to the recording transports."""
    runner = StrategyRunner(sink=sink or AcceptingSink())
    runner.signals = RecordingSignals()
    runner.bus = RecordingBus()
    runner.state = FakeState()
    runner.state.owner_id = runner._owner_id
    runner._ownership_acquired = True
    runner.positions = FakePositions(positions or {})
    strategy_slot = StrategySlot(
        QuietStrategy(),
        SYMBOL,
        SignalFactory(
            STRATEGY_NAME,
            timeframe="M15",
            token="test",
            sizer=PositionSizer(capital=10_000.0, risk_percent=1.0),
        ),
    )
    runner.slots.append(strategy_slot)
    return runner, strategy_slot


def restore(strategy_slot: StrategySlot, position: OpenPosition) -> None:
    """What ``_restore_state`` would have done before the flush runs."""
    strategy_slot.factory.restore_position(position, symbol=SYMBOL)


def with_history(strategy_slot: StrategySlot, close: float) -> None:
    """Give the slot one bar, as Redis warm-up would have."""
    strategy_slot.buffer.append(
        Candle(
            origin=settings.state_scope.origin(),
            symbol=SYMBOL,
            timeframe="M15",
            open_time=datetime.now(UTC) - timedelta(minutes=15),
            open=close,
            high=close,
            low=close,
            close=close,
            tick_count=5,
        )
    )


def emitted(runner) -> list:
    return [signal for signal, _ in runner.signals.rows]


def flush_mode(monkeypatch, mode: str, *, max_age: float = 3600.0) -> None:
    monkeypatch.setattr(
        "qte_strategy_engine.runner.runner_settings.flush_stale_positions", mode, raising=False
    )
    monkeypatch.setattr(
        "qte_strategy_engine.runner.runner_settings.stale_position_max_age",
        max_age,
        raising=False,
    )


# ── Flushing what the outage left behind ─────────────────────────────────


async def test_a_stale_cycle_is_closed_with_one_r_sl(monkeypatch):
    """The whole point: the pair comes back free to trade."""
    flush_mode(monkeypatch, "close")
    position = held_position(updated_at=aged(9 * 3600))
    runner, strategy_slot = build_runner(positions={(STRATEGY_NAME, SYMBOL): position})
    restore(strategy_slot, position)
    with_history(strategy_slot, 2410.0)

    await runner._flush_stale_positions()

    signals = emitted(runner)
    assert [signal.position.action for signal in signals] == [SignalAction.R_SL]
    assert signals[0].signal_uxid == CYCLE_UXID, "the close has to name the cycle it closes"
    assert strategy_slot.factory.open_cycle(SYMBOL) is None, "the pair is still locked"


async def test_the_close_carries_the_whole_remaining_size_and_the_freshest_price(monkeypatch):
    flush_mode(monkeypatch, "close")
    position = held_position(updated_at=aged(9 * 3600), remaining=4.2)
    runner, strategy_slot = build_runner(positions={(STRATEGY_NAME, SYMBOL): position})
    restore(strategy_slot, position)
    with_history(strategy_slot, 2410.0)

    await runner._flush_stale_positions()

    block = emitted(runner)[0].position
    assert block.quantity == pytest.approx(4.2)
    assert block.price == pytest.approx(2410.0), "the last bar, not the entry price"
    assert block.is_running is False, "a terminal close leaves nothing running"


async def test_the_price_falls_back_to_the_entry_when_redis_held_nothing(monkeypatch):
    """After a long outage the cache may hold no recent bar at all."""
    flush_mode(monkeypatch, "close")
    position = held_position(updated_at=aged(9 * 3600), price=2334.50)
    runner, strategy_slot = build_runner(positions={(STRATEGY_NAME, SYMBOL): position})
    restore(strategy_slot, position)

    await runner._flush_stale_positions()

    assert emitted(runner)[0].position.price == pytest.approx(2334.50)


async def test_a_cycle_of_unknown_size_still_closes(monkeypatch):
    """A record restored from a bare uxid closes by id, with no size."""
    flush_mode(monkeypatch, "close")
    position = held_position(updated_at=aged(9 * 3600), quantity=None, remaining=None)
    runner, strategy_slot = build_runner(positions={(STRATEGY_NAME, SYMBOL): position})
    restore(strategy_slot, position)

    await runner._flush_stale_positions()

    signals = emitted(runner)
    assert [signal.position.action for signal in signals] == [SignalAction.R_SL]
    assert signals[0].position.quantity is None
    assert strategy_slot.factory.open_cycle(SYMBOL) is None


async def test_a_fresh_cycle_survives_a_quick_restart(monkeypatch):
    """A deploy must not close the book. This is the case the age test exists for."""
    flush_mode(monkeypatch, "close", max_age=3600.0)
    position = held_position(updated_at=aged(120))
    runner, strategy_slot = build_runner(positions={(STRATEGY_NAME, SYMBOL): position})
    restore(strategy_slot, position)

    await runner._flush_stale_positions()

    assert emitted(runner) == []
    assert strategy_slot.factory.open_cycle(SYMBOL) == CYCLE_UXID


async def test_a_zero_window_closes_regardless_of_age(monkeypatch):
    """The unconditional flush, for an operator who wants it."""
    flush_mode(monkeypatch, "close", max_age=0.0)
    position = held_position(updated_at=aged(1))
    runner, strategy_slot = build_runner(positions={(STRATEGY_NAME, SYMBOL): position})
    restore(strategy_slot, position)

    await runner._flush_stale_positions()

    assert [signal.position.action for signal in emitted(runner)] == [SignalAction.R_SL]


@pytest.mark.parametrize("mode", ["off", "warn"])
async def test_only_close_mode_sends_anything(monkeypatch, mode):
    flush_mode(monkeypatch, mode)
    position = held_position(updated_at=aged(9 * 3600))
    runner, strategy_slot = build_runner(positions={(STRATEGY_NAME, SYMBOL): position})
    restore(strategy_slot, position)

    await runner._flush_stale_positions()

    assert emitted(runner) == []
    assert strategy_slot.factory.open_cycle(SYMBOL) == CYCLE_UXID


async def test_warn_mode_says_what_close_mode_would_do(monkeypatch, caplog):
    """The rollout path: see the list before letting it act on it."""
    flush_mode(monkeypatch, "warn")
    position = held_position(updated_at=aged(9 * 3600))
    runner, strategy_slot = build_runner(positions={(STRATEGY_NAME, SYMBOL): position})
    restore(strategy_slot, position)

    with caplog.at_level("WARNING"):
        await runner._flush_stale_positions()

    reported = [
        record.getMessage() for record in caplog.records if CYCLE_UXID in record.getMessage()
    ]
    assert reported, [record.getMessage() for record in caplog.records]
    assert "would close" in reported[0]


async def test_an_unreconciled_pair_is_left_for_the_outbox(monkeypatch, caplog):
    """Recovery settles an ambiguous delivery first; flushing over it would
    manufacture a second command against the same cycle."""
    flush_mode(monkeypatch, "close")
    position = held_position(updated_at=aged(9 * 3600))
    runner, strategy_slot = build_runner(positions={(STRATEGY_NAME, SYMBOL): position})
    restore(strategy_slot, position)
    runner._uncertain_pairs.add((STRATEGY_NAME, SYMBOL))

    with caplog.at_level("WARNING"):
        await runner._flush_stale_positions()

    assert emitted(runner) == []
    assert any("unreconciled" in record.getMessage() for record in caplog.records)


async def test_a_position_for_a_strategy_we_do_not_run_is_reported_not_closed(monkeypatch, caplog):
    """An orphan row: the strategy was unmapped while it held a position.

    Not closed, because ``OpenPosition`` records no timeframe and the broker
    payload needs one — the engine cannot speak for a strategy it is not
    driving. Loud, because nobody else will ever close it either.
    """
    flush_mode(monkeypatch, "close")
    orphan = held_position(
        strategy="RETIRED_STRATEGY", signal_uxid="1111111111111111", updated_at=aged(9 * 3600)
    )
    runner, strategy_slot = build_runner(positions={("RETIRED_STRATEGY", SYMBOL): orphan})

    with caplog.at_level("ERROR"):
        await runner._flush_stale_positions()

    assert emitted(runner) == []
    reported = [record.getMessage() for record in caplog.records]
    assert any("RETIRED_STRATEGY" in message for message in reported), reported
    assert any("no running strategy" in message for message in reported)


async def test_nothing_reaches_the_broker_in_shadow_mode(monkeypatch):
    flush_mode(monkeypatch, "close")
    sink = AcceptingSink()
    sink.shadow_mode = True
    position = held_position(updated_at=aged(9 * 3600))
    runner, strategy_slot = build_runner(positions={(STRATEGY_NAME, SYMBOL): position}, sink=sink)
    restore(strategy_slot, position)

    await runner._flush_stale_positions()

    assert sink.delivery_ids == [], "a shadowed runner sends nothing to the broker"


async def test_an_empty_table_is_not_an_error(monkeypatch):
    flush_mode(monkeypatch, "close")
    runner, _ = build_runner()

    await runner._flush_stale_positions()

    assert emitted(runner) == []


# ── One pair per cycle id ────────────────────────────────────────────────


async def test_two_pairs_sharing_a_cycle_id_refuse_to_start(monkeypatch):
    """A close on either would close the other's position at the broker."""
    flush_mode(monkeypatch, "close")
    runner, _ = build_runner(
        positions={
            (STRATEGY_NAME, SYMBOL): held_position(),
            (STRATEGY_NAME, "EURUSD"): held_position(symbol="EURUSD"),
        }
    )

    with pytest.raises(RuntimeError, match=CYCLE_UXID) as excinfo:
        await runner._flush_stale_positions()

    message = str(excinfo.value)
    assert f"{STRATEGY_NAME}/{SYMBOL}" in message
    assert f"{STRATEGY_NAME}/EURUSD" in message


@pytest.mark.parametrize("mode", ["off", "warn", "close"])
async def test_the_duplicate_check_runs_in_every_mode(monkeypatch, mode):
    """Turning the flush off must not hide a correctness problem."""
    flush_mode(monkeypatch, mode)
    runner, _ = build_runner(
        positions={
            (STRATEGY_NAME, SYMBOL): held_position(),
            (STRATEGY_NAME, "EURUSD"): held_position(symbol="EURUSD"),
        }
    )

    with pytest.raises(RuntimeError, match="share a trade-cycle id"):
        await runner._flush_stale_positions()


async def test_distinct_cycle_ids_are_fine(monkeypatch):
    flush_mode(monkeypatch, "off")
    runner, _ = build_runner(
        positions={
            (STRATEGY_NAME, SYMBOL): held_position(),
            (STRATEGY_NAME, "EURUSD"): held_position(
                symbol="EURUSD", signal_uxid="2222222222222222"
            ),
        }
    )

    await runner._flush_stale_positions()


# ── Which store decides ──────────────────────────────────────────────────


async def test_a_cycle_redis_restored_but_absent_from_the_table_is_still_flushed(monkeypatch):
    """The lock lives on the slot, not in the table, so that is what decides.

    ``_persist_position`` writes Redis first and ``upsert`` swallows its own
    failure, so the two stores can disagree. Deciding from the table would leave
    the pair locked on a position the flush never looked at — the exact failure
    this feature exists to prevent.
    """
    flush_mode(monkeypatch, "close")
    position = held_position(updated_at=aged(9 * 3600))
    # Nothing in the table at all; the cycle exists only on the restored slot.
    runner, strategy_slot = build_runner(positions={})
    restore(strategy_slot, position)

    await runner._flush_stale_positions()

    assert [signal.position.action for signal in emitted(runner)] == [SignalAction.R_SL]
    assert strategy_slot.factory.open_cycle(SYMBOL) is None


async def test_duplicate_ids_are_caught_among_live_slots_with_no_table_at_all(monkeypatch):
    """``list_open`` answers ``[]`` when it could not read, so the check cannot
    rest on the table alone or a database blip makes it pass on no data."""
    flush_mode(monkeypatch, "close")
    runner, first_slot = build_runner(positions={})
    second_slot = StrategySlot(
        QuietStrategy(),
        "EURUSD",
        SignalFactory(STRATEGY_NAME, timeframe="M15", token="test"),
    )
    runner.slots.append(second_slot)
    restore(first_slot, held_position())
    second_slot.factory.restore_position(held_position(symbol="EURUSD"), symbol="EURUSD")

    with pytest.raises(RuntimeError, match="share a trade-cycle id") as excinfo:
        await runner._flush_stale_positions()

    assert f"{STRATEGY_NAME}/EURUSD" in str(excinfo.value)


async def test_a_naive_stored_timestamp_does_not_stop_the_runner(monkeypatch):
    """A row from an older build can deserialize naive; the flush runs inside
    ``start()``, so raising here would refuse the boot over one bad row."""
    flush_mode(monkeypatch, "close")
    position = held_position()
    # What pydantic gives back for a stored value that carried no offset.
    position.updated_at = datetime.now(UTC).replace(tzinfo=None) - timedelta(hours=9)
    runner, strategy_slot = build_runner(positions={(STRATEGY_NAME, SYMBOL): position})
    restore(strategy_slot, position)

    await runner._flush_stale_positions()

    assert [signal.position.action for signal in emitted(runner)] == [SignalAction.R_SL]


async def test_an_unknown_price_is_absent_rather_than_zero(monkeypatch):
    """A zero would claim the trade closed at nothing. The schema allows none."""
    flush_mode(monkeypatch, "close")
    position = held_position(updated_at=aged(9 * 3600), price=None, quantity=None, remaining=None)
    runner, strategy_slot = build_runner(positions={(STRATEGY_NAME, SYMBOL): position})
    restore(strategy_slot, position)

    await runner._flush_stale_positions()

    assert emitted(runner)[0].position.price is None


# ── Where it runs in the boot sequence ───────────────────────────────────


async def test_start_flushes_after_recovery_and_before_subscribing(monkeypatch):
    """The call site, not the method — every test above would pass without it.

    Order matters twice over. After recovery, so the outbox has settled every
    ambiguous delivery and the book is what it says it is. Before the
    subscriptions, so no live close races the flush and no strategy is asked to
    decide a bar against a position that is about to be closed.
    """
    from test_runner_catch_up import CandleStore
    from test_runner_catch_up import build_runner as build_catch_up_runner

    runner, _, _ = build_catch_up_runner(monkeypatch, CandleStore([]))
    sequence: list[str] = []

    async def record(name, original):
        sequence.append(name)
        await original()

    for name in ("_recover_pending_deliveries", "_flush_stale_positions", "_subscribe"):
        original = getattr(runner, name)
        monkeypatch.setattr(
            runner, name, (lambda name=name, original=original: record(name, original))
        )

    await runner.start()
    try:
        assert sequence == [
            "_recover_pending_deliveries",
            "_flush_stale_positions",
            "_subscribe",
        ]
    finally:
        await runner.stop()


def test_the_table_enforces_one_pair_per_cycle_id():
    """The constraint behind the runtime check, so the two cannot drift.

    Needs no database: the declaration is what the migration is checked
    against by ``alembic check``.
    """
    from qte_strategy_engine.db import OpenPositionRow

    constraints = {
        constraint.name
        for constraint in OpenPositionRow.__table__.constraints
        if constraint.name is not None
    }
    assert "uq_open_positions_uxid" in constraints
    assert "uq_open_positions_pair" in constraints
    indexes = {index.name for index in OpenPositionRow.__table__.indexes}
    assert "ix_open_positions_uxid" not in indexes, "the unique constraint replaced it"


@pytest.fixture(autouse=True)
def live_state_scope(monkeypatch):
    """A live book with fake transports, as the delivery tests use."""
    monkeypatch.setattr(settings, "env", "prod")
    monkeypatch.setattr(settings.state_config, "execution_mode", "live")
    monkeypatch.setattr(settings.market_data, "provider", "tiingo")
