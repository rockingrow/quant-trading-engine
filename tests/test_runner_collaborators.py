"""The runner's collaborators work on their own, against the hooks protocol.

The point of splitting the runner was that the outbox, recovery and control
plane can be driven without assembling a runner. These tests do exactly that,
with a stand-in for :class:`RunnerHooks`, and pin the one property the split
must keep: every collaborator sees the runner's *current* repositories.
"""

from __future__ import annotations

from datetime import UTC, datetime

from test_runner_delivery import (
    AcceptingSink,
    FakePositions,
    FakeState,
    RecordingBus,
    RecordingSignals,
    _runner,
    _slot,
)

from qte_shared.bus import Subjects
from qte_shared.config import settings
from qte_shared.db import EventRepository
from qte_shared.models import SignalAction
from qte_shared.strategies.strategy_base import SignalIntent
from qte_strategy_engine.delivery import DeliveryPipeline
from qte_strategy_engine.runner_context import RunnerContext
from qte_strategy_engine.telegram_notify import TelegramLifecycleNotifier

MOMENT = datetime(2026, 5, 1, tzinfo=UTC)


class StubHooks:
    """Everything the pipeline may ask of a runner, and a count of the guards."""

    def __init__(self, context: RunnerContext) -> None:
        self._context = context
        self.ownership_checks = 0
        self.shadow_refreshes = 0

    async def check_ownership(self) -> None:
        self.ownership_checks += 1

    async def refresh_shadow_mode(self) -> None:
        # What the runner's refresh guarantees: a paper book is always shadow.
        self.shadow_refreshes += 1
        self._context.sink.shadow_mode = self._context.scope.is_paper

    async def live_delivery_paused(self) -> bool:
        return False

    async def refresh_bar_gate(self) -> None:
        return None

    async def emit(self, slot, intent, fallback_price, moment) -> None:
        raise AssertionError("the pipeline never re-enters the decision path")

    async def flatten_cycle(self, slot, uxid, price, moment, *, reason) -> None:
        raise AssertionError("the pipeline never re-enters the decision path")

    async def feed_candle_serialized(self, slot, candle) -> None:
        raise AssertionError("the pipeline never re-enters the decision path")


def standalone_pipeline() -> tuple[DeliveryPipeline, RunnerContext, StubHooks]:
    context = RunnerContext(
        bus=RecordingBus(),
        state=FakeState(),
        subjects=Subjects(),
        events=EventRepository(),
        signals=RecordingSignals(),
        positions=FakePositions(),
        sink=AcceptingSink(),
        telegram=TelegramLifecycleNotifier(),
        scope=settings.state_scope,
    )
    hooks = StubHooks(context)
    return DeliveryPipeline(context, hooks), context, hooks


async def test_the_pipeline_delivers_and_commits_without_a_runner():
    pipeline, context, hooks = standalone_pipeline()
    slot = _slot()
    context.slots.append(slot)

    await pipeline.emit_intent(
        slot,
        SignalIntent(action=SignalAction.LONG, price=2334.50, sl=2329.50),
        fallback_price=2334.50,
        moment=MOMENT,
    )

    assert len(context.signals.rows) == 1
    assert slot.factory.open_cycles(slot.symbol), "the delivered entry opened a cycle"
    assert slot.key not in pipeline.uncertain_pairs
    # The caller checks ownership once per intent; the pipeline adds only the
    # checks at the broker boundary — one before the paper/live split, and a
    # second right before a live send.
    expected_boundary_checks = 1 if context.scope.is_paper else 2
    assert hooks.ownership_checks == expected_boundary_checks


async def test_a_repository_swapped_on_the_runner_reaches_every_collaborator():
    runner = _runner()
    replacement = RecordingSignals()

    runner.signals = replacement

    assert runner.context.signals is replacement
    for collaborator in (runner.delivery, runner.recovery, runner.history, runner.control):
        assert collaborator._context.signals is replacement
