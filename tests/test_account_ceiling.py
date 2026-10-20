"""The account-wide ceiling on open positions, and what it must never block.

``QTE_ACCOUNT__MAX_OPEN_POSITIONS`` is the engine's copy of a limit the broker
account enforces across every strategy and symbol at once. The engine never
hears about a refusal down there — the entry is simply not placed and nothing
comes back — so a book that overruns the account goes on managing cycles that
do not exist, sending TP1/TP2/SL for positions nobody holds.

Four properties are pinned here. The ceiling counts the whole book and not one
pair, because a per-pair cap cannot see the overrun. Closes always pass, because
a position that cannot be got out of is worse than one position too many. Zero
changes nothing, so a backtested figure stays comparable to a traded one unless
an operator opts in. And an entry in flight counts, so two pairs whose bars
close on the same boundary cannot both take the last slot.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest
from test_runner_delivery import AcceptingSink, FakePositions, FakeState, _runner

from qte_shared.config import settings
from qte_shared.models import SignalAction
from qte_shared.strategies.signal_factory import SignalFactory
from qte_shared.strategies.sizing import PositionSizer
from qte_shared.strategies.strategy_base import SignalIntent, StrategyBase
from qte_strategy_engine.broker_sink import DeliveryResult
from qte_strategy_engine.runner import StrategySlot

MOMENT = datetime(2026, 5, 1, tzinfo=UTC)


class ProbeStrategy(StrategyBase):
    timeframe = "M15"
    warmup = 1

    def __init__(self, name: str) -> None:
        super().__init__()
        self.name = name

    def on_candle_closed(self, df, context):
        return None


class RecordingEvents:
    """``EventRepository`` with no database behind it."""

    def __init__(self) -> None:
        self.rows: list[dict] = []

    async def record_event(self, *, service, event, level="INFO", payload=None):
        self.rows.append({"service": service, "event": event, "level": level, **(payload or {})})


@pytest.fixture(autouse=True)
def live_state_scope(monkeypatch):
    """A live book with fake transports, as the delivery tests use.

    Paper mode short-circuits the sink, so the ceiling would be measured on a
    path no real entry takes.
    """
    monkeypatch.setattr(settings, "env", "prod")
    monkeypatch.setattr(settings.state_config, "execution_mode", "live")


def _slot(name: str = "CEILING_PROBE", symbol: str = "XAUUSD") -> StrategySlot:
    factory = SignalFactory(
        name,
        timeframe="M15",
        token="test",
        sizer=PositionSizer(capital=1000.0, risk_percent=1.0),
    )
    return StrategySlot(ProbeStrategy(name), symbol, factory)


def _book(*slots: StrategySlot):
    runner = _runner(state=FakeState(), positions=FakePositions())
    runner.events = RecordingEvents()
    runner.slots = list(slots)
    return runner


async def _enter(runner, slot, price: float = 2000.0):
    await runner._emit(
        slot,
        SignalIntent(action=SignalAction.LONG, price=price, sl=price - 10.0, tp2=price + 20.0),
        fallback_price=price,
        moment=MOMENT,
    )


async def _close(runner, slot, action, uxid, price: float = 2010.0):
    await runner._emit(
        slot,
        SignalIntent(action=action, price=price, signal_uxid=uxid),
        fallback_price=price,
        moment=MOMENT,
    )


def _open_count(runner) -> int:
    return sum(len(slot.factory.open_cycles(slot.symbol)) for slot in runner.slots)


# ── The ceiling is on the account, not on a pair ─────────────────────────


async def test_a_ceiling_of_zero_lets_a_pair_stack_as_before(monkeypatch):
    monkeypatch.setattr(settings.account, "max_open_positions", 0)
    slot = _slot()
    runner = _book(slot)

    for _ in range(6):
        await _enter(runner, slot)

    assert len(slot.factory.open_cycles("XAUUSD")) == 6
    assert runner.events.rows == []


async def test_the_entry_that_would_exceed_the_ceiling_is_refused(monkeypatch):
    monkeypatch.setattr(settings.account, "max_open_positions", 2)
    slot = _slot()
    runner = _book(slot)

    for _ in range(4):
        await _enter(runner, slot)

    assert len(slot.factory.open_cycles("XAUUSD")) == 2


async def test_a_refused_entry_never_reaches_the_broker(monkeypatch):
    monkeypatch.setattr(settings.account, "max_open_positions", 1)
    slot = _slot()
    sink = AcceptingSink()
    runner = _book(slot)
    runner.sink = sink

    await _enter(runner, slot)
    await _enter(runner, slot)

    # One entry, one outbox row, one publish: the refusal leaves no trace on
    # the wire and no row to reconcile later.
    assert len(sink.delivery_ids) == 1
    assert len(runner.signals.rows) == 1


async def test_the_budget_is_shared_across_pairs(monkeypatch):
    """The failure a per-pair cap cannot see: two pairs, one account."""
    monkeypatch.setattr(settings.account, "max_open_positions", 3)
    gold = _slot("GOLD_PROBE", "XAUUSD")
    oil = _slot("OIL_PROBE", "USOIL")
    runner = _book(gold, oil)

    await _enter(runner, gold)
    await _enter(runner, gold)
    await _enter(runner, oil)
    await _enter(runner, oil)

    assert len(gold.factory.open_cycles("XAUUSD")) == 2
    # The fourth is refused although this pair holds only one of its own.
    assert len(oil.factory.open_cycles("USOIL")) == 1
    assert _open_count(runner) == 3


async def test_a_refusal_is_recorded_as_an_event(monkeypatch):
    monkeypatch.setattr(settings.account, "max_open_positions", 1)
    slot = _slot()
    runner = _book(slot)

    await _enter(runner, slot)
    await _enter(runner, slot)

    assert [row["event"] for row in runner.events.rows] == ["entry_refused_account_full"]
    refusal = runner.events.rows[0]
    assert refusal["level"] == "WARNING"
    assert refusal["strategy"] == "CEILING_PROBE"
    assert refusal["symbol"] == "XAUUSD"
    assert (refusal["open_positions"], refusal["ceiling"]) == (1, 1)


async def test_a_refusal_is_logged_with_both_numbers(monkeypatch, caplog):
    monkeypatch.setattr(settings.account, "max_open_positions", 1)
    slot = _slot()
    runner = _book(slot)

    await _enter(runner, slot)
    with caplog.at_level("WARNING"):
        await _enter(runner, slot)

    assert "QTE_ACCOUNT__MAX_OPEN_POSITIONS" in caplog.text
    assert "ceiling of 1" in caplog.text


# ── A full book must never trap a position ───────────────────────────────


@pytest.mark.parametrize(
    "action",
    [SignalAction.TP1, SignalAction.TP2, SignalAction.SL, SignalAction.R_SL, SignalAction.FLAT],
)
async def test_every_close_passes_while_the_book_is_full(monkeypatch, action):
    monkeypatch.setattr(settings.account, "max_open_positions", 1)
    slot = _slot()
    runner = _book(slot)

    await _enter(runner, slot)
    uxid = slot.factory.open_cycle("XAUUSD")
    assert uxid is not None

    await _close(runner, slot, action, uxid)

    # Delivered, not refused. Whether the cycle ends here is the factory's
    # business — a partial `TP1` leaves it open — and not the ceiling's.
    staged = [signal.position.action for signal, _ in runner.signals.rows]
    assert staged[-1] is action
    assert runner.events.rows == []


async def test_a_closed_cycle_gives_its_slot_back(monkeypatch):
    monkeypatch.setattr(settings.account, "max_open_positions", 1)
    slot = _slot()
    runner = _book(slot)

    await _enter(runner, slot)
    await _close(runner, slot, SignalAction.SL, slot.factory.open_cycle("XAUUSD"))
    await _enter(runner, slot)

    assert len(slot.factory.open_cycles("XAUUSD")) == 1


# ── The reservation, which is what makes the ceiling hold ────────────────


async def test_two_pairs_cannot_both_take_the_last_slot(monkeypatch):
    """Both read the book before either one's entry has reached it.

    The sink suspends mid-send so the two emits are genuinely interleaved —
    which is what happens when two pairs close a bar on the same boundary.
    """
    monkeypatch.setattr(settings.account, "max_open_positions", 1)
    gold = _slot("GOLD_PROBE", "XAUUSD")
    oil = _slot("OIL_PROBE", "USOIL")
    runner = _book(gold, oil)

    in_send = asyncio.Event()
    release = asyncio.Event()

    class SuspendingSink(AcceptingSink):
        async def send(self, signal, *, delivery_id=None):
            self.delivery_ids.append(delivery_id)
            in_send.set()
            await release.wait()
            return DeliveryResult(status="sent", transport="nats")

    runner.sink = SuspendingSink()

    first = asyncio.create_task(_enter(runner, gold))
    await in_send.wait()
    # Gold's entry is on the wire and in no book yet. Oil asks now.
    await _enter(runner, oil)
    release.set()
    await first

    assert len(gold.factory.open_cycles("XAUUSD")) == 1
    assert oil.factory.open_cycle("USOIL") is None


async def test_a_refused_entry_gives_its_reservation_back(monkeypatch):
    """An intent the factory throws out must not leak a slot."""
    monkeypatch.setattr(settings.account, "max_open_positions", 1)
    slot = _slot()
    runner = _book(slot)

    # No stop and no size: `build()` refuses it inside the reservation.
    await runner._emit(
        slot,
        SignalIntent(action=SignalAction.LONG, price=2000.0, quantity=0.0),
        fallback_price=2000.0,
        moment=MOMENT,
    )

    assert runner._entries_in_flight == 0
    await _enter(runner, slot)
    assert len(slot.factory.open_cycles("XAUUSD")) == 1
