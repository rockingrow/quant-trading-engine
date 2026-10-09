"""Executable spec for the control actions the Telegram bot and CLI call.

Request/reply over NATS is the whole control plane in this repository — there
is no HTTP endpoint to test instead — so these go at the handlers directly,
with a message double standing in for the reply subject.

Two services answer: the runner (``status``, ``flat``, ``set_bar_gate``) and
ingestion (``status``, ``warmup``, ``flush``). What each must get right is the
same in both cases: answer only when asked, say plainly what it refused, and
never let a bad request through as a silent no-op.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

from test_runner_delivery import AcceptingSink, FakeState, RecordingSignals, _runner, _slot

from qte_shared.bar_gate import BAR_GATE_FLAG, BarGate
from qte_shared.config import settings
from qte_shared.models import Candle, SignalAction
from qte_shared.strategies.strategy_base import SignalIntent, StrategyBase

BAR_LENGTH = timedelta(minutes=15)
MOMENT = datetime(2026, 5, 1, tzinfo=UTC)


class ControlMessage:
    """The parts of a NATS message a control handler touches."""

    def __init__(self, payload: dict, *, reply: str = "reply.subject") -> None:
        self.data = json.dumps(payload).encode()
        self.reply = reply


class ReplyBus:
    """Captures what a handler publishes to the reply subject."""

    def __init__(self) -> None:
        self.messages: list[tuple[str, dict]] = []
        self.published: list[tuple[str, dict]] = []
        self.nc = self

    async def publish(self, subject, payload=None):
        # Two shapes meet here: the handler's reply (bytes, through ``nc``) and
        # the runner's own event publishes (a dict, through the bus).
        if isinstance(payload, bytes):
            self.messages.append((subject, json.loads(payload)))
        else:
            self.published.append((subject, payload))

    async def subscribe(self, subject, handler, queue: str = ""):
        return None

    async def close(self):
        return None


class ProbeStrategy(StrategyBase):
    name = "DELIVERY_PROBE"
    timeframe = "M15"
    warmup = 1
    max_history = 50

    def on_candle_closed(self, candles_frame, context):
        return None


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


def _ready_runner(*, with_bar: bool = True):
    """A runner with one warm slot, a reply bus and nothing real behind it."""
    runner = _runner(state=FakeState(), sink=AcceptingSink())
    runner.signals = RecordingSignals()
    runner.bus = ReplyBus()
    slot = _slot()
    slot.strategy = ProbeStrategy()
    runner.slots = [slot]
    if with_bar:
        slot.buffer.append(_bar(MOMENT))
    return runner, slot


async def _ask(runner, payload: dict, *, reply: str = "reply.subject") -> dict | None:
    await runner._on_control_message(ControlMessage(payload, reply=reply))
    replies = [body for subject, body in runner.bus.messages if subject == reply]
    return replies[-1] if replies else None


# ── The runner: status ────────────────────────────────────────────────────


async def test_status_reports_every_slot_and_whether_it_can_decide():
    runner, slot = _ready_runner()

    answer = await _ask(runner, {"action": "status"})

    assert answer is not None
    assert answer["namespace"] == settings.state_scope.namespace
    [reported] = answer["strategies"]
    assert reported["strategy"] == "DELIVERY_PROBE"
    assert reported["symbol"] == "XAUUSD"
    assert reported["timeframe"] == "M15"
    assert reported["bars"] == 1 and reported["warmup"] == 1
    assert reported["warm"] is True
    assert reported["ready"] is True
    assert reported["open_cycles"] == []


async def test_status_says_why_a_cold_slot_is_not_ready():
    runner, _ = _ready_runner(with_bar=False)

    answer = await _ask(runner, {"action": "status"})

    [reported] = answer["strategies"]
    assert reported["warm"] is False and reported["ready"] is False
    assert reported["bars"] == 0


async def test_status_marks_a_paused_pair():
    runner, _ = _ready_runner()
    runner.state.flags[BAR_GATE_FLAG] = BarGate().prevent(symbol="XAUUSD").to_payload()

    answer = await _ask(runner, {"action": "status"})

    [reported] = answer["strategies"]
    assert reported["gated"] is True and reported["ready"] is False
    assert answer["gate"]["symbols"] == ["XAUUSD"]


async def test_status_marks_a_pair_an_unreconciled_delivery_is_blocking():
    runner, slot = _ready_runner()
    runner._uncertain_pairs.add(slot.key)

    answer = await _ask(runner, {"action": "status"})

    [reported] = answer["strategies"]
    assert reported["uncertain"] is True and reported["ready"] is False


async def test_a_broadcast_with_no_reply_subject_is_answered_with_nothing():
    """``set_shadow_mode`` is a fan-out; a status request is not."""
    runner, _ = _ready_runner()
    await runner._on_control_message(ControlMessage({"action": "status"}, reply=""))
    assert runner.bus.messages == []


async def test_an_unknown_action_changes_nothing():
    runner, _ = _ready_runner()
    assert await _ask(runner, {"action": "reboot"}) is None


async def test_an_unparseable_message_is_ignored():
    runner, _ = _ready_runner()
    message = ControlMessage({"action": "status"})
    message.data = b"{not json"
    await runner._on_control_message(message)
    assert runner.bus.messages == []


# ── The runner: the bar gate ──────────────────────────────────────────────


async def test_set_bar_gate_rereads_the_stored_flag_rather_than_trusting_the_message():
    """The caller stored it; a delayed broadcast must not overwrite a newer one."""
    runner, _ = _ready_runner()
    runner.state.flags[BAR_GATE_FLAG] = BarGate().prevent(everything=True).to_payload()

    answer = await _ask(runner, {"action": "set_bar_gate", "gate": {"everything": False}})

    assert answer["gate"]["everything"] is True
    assert runner._bar_gate.blocks(symbol="ANY", strategy="ANY")


# ── The runner: FLAT ──────────────────────────────────────────────────────


async def _open_a_cycle(runner, slot) -> str:
    await runner._emit(
        slot,
        SignalIntent(action=SignalAction.LONG, price=2334.50, sl=2329.50),
        fallback_price=2334.50,
        moment=MOMENT,
    )
    cycles = slot.factory.open_cycles(slot.symbol)
    assert cycles, "the probe never opened a cycle"
    return cycles[-1]


async def test_flat_closes_the_named_symbol_and_reports_the_cycle():
    runner, slot = _ready_runner()
    uxid = await _open_a_cycle(runner, slot)

    answer = await _ask(runner, {"action": "flat", "scope": {"symbol": "xauusd"}})

    assert [row["signal_uxid"] for row in answer["closed"]] == [uxid]
    assert answer["refused"] == []
    assert slot.factory.open_cycles(slot.symbol) == ()


async def test_flat_closes_by_strategy_name_too():
    runner, slot = _ready_runner()
    await _open_a_cycle(runner, slot)

    answer = await _ask(runner, {"action": "flat", "scope": {"strategy": "DELIVERY_PROBE"}})

    assert len(answer["closed"]) == 1


async def test_flat_on_everything_closes_what_is_open():
    runner, slot = _ready_runner()
    await _open_a_cycle(runner, slot)

    answer = await _ask(runner, {"action": "flat", "scope": {"everything": True}})

    assert len(answer["closed"]) == 1


async def test_flat_goes_out_as_an_ordinary_flat_signal():
    """No second delivery route: the outbox and the audit see a normal exit."""
    runner, slot = _ready_runner()
    await _open_a_cycle(runner, slot)

    await _ask(runner, {"action": "flat", "scope": {"everything": True}})

    entry_signal, flat_signal = (row[0] for row in runner.signals.rows)
    assert entry_signal.position.action is SignalAction.LONG
    assert flat_signal.position.action is SignalAction.FLAT
    # The FLAT names the cycle it closes, which is what lets the broker group
    # the whole trade and what the factory refuses to go without.
    assert flat_signal.signal_uxid == entry_signal.signal_uxid
    # And it is labelled, so the audit trail distinguishes an operator's close
    # from a strategy's exit and from the weekend flat.
    reasons = [payload.get("reason") for _, payload in runner.bus.published]
    assert reasons[-1] == "MANUAL_FLAT"


async def test_flat_leaves_a_pair_that_is_not_in_scope_alone():
    runner, slot = _ready_runner()
    await _open_a_cycle(runner, slot)

    answer = await _ask(runner, {"action": "flat", "scope": {"symbol": "EURUSD"}})

    assert answer == {"closed": [], "refused": []}
    assert slot.factory.open_cycles(slot.symbol)


async def test_flat_without_a_scope_is_refused_rather_than_read_as_everything():
    runner, slot = _ready_runner()
    await _open_a_cycle(runner, slot)

    answer = await _ask(runner, {"action": "flat", "scope": {}})

    assert "scope" in answer["error"]
    assert slot.factory.open_cycles(slot.symbol), "nothing was closed"


async def test_flat_refuses_a_pair_whose_delivery_is_unreconciled():
    runner, slot = _ready_runner()
    await _open_a_cycle(runner, slot)
    runner._uncertain_pairs.add(slot.key)

    answer = await _ask(runner, {"action": "flat", "scope": {"everything": True}})

    assert answer["closed"] == []
    [refusal] = answer["refused"]
    assert "unreconciled" in refusal["reason"]
    assert slot.factory.open_cycles(slot.symbol), "the cycle is still open"


async def test_a_cycle_that_is_still_open_afterwards_is_reported_not_claimed():
    """The reply says what actually happened, not what was attempted.

    A delivery that never completes leaves the cycle open -- the factory only
    commits a transition the broker took -- and the operator has to be told
    that, because the next thing they do depends on it.
    """
    runner, slot = _ready_runner()
    await _open_a_cycle(runner, slot)
    slot.factory.commit = lambda *arguments, **keywords: None

    answer = await _ask(runner, {"action": "flat", "scope": {"everything": True}})

    assert answer["closed"] == []
    [refusal] = answer["refused"]
    assert refusal["reason"] == "the close was not delivered"
    assert slot.factory.open_cycles(slot.symbol), "the cycle is still open"
