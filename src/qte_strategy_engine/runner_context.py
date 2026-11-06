"""What the runner's collaborators share, and what they may ask of the runner.

:class:`~qte_strategy_engine.runner.StrategyRunner` is a coordinator over four
collaborators — :class:`~qte_strategy_engine.delivery.DeliveryPipeline`,
:class:`~qte_strategy_engine.recovery.PositionRecovery`,
:class:`~qte_strategy_engine.history_sync.HistorySync` and
:class:`~qte_strategy_engine.control_plane.ControlPlane`. Each one used to be a
region of a 2,300-line class; they are objects now so the safety-critical parts
(the delivery state machine above all) can be read and tested on their own.

Two things connect them, both defined here:

* :class:`RunnerContext` — the connections, repositories and shared book every
  collaborator reads. It is one mutable object, held by reference, so swapping a
  repository on the runner (``runner.signals = ...``, which a test or a future
  driver does) reaches every collaborator at once instead of leaving stale
  copies behind.
* :class:`RunnerHooks` — the few things only the runner can do: guard the
  ownership claim and the shadow switch, and decide on a bar. Collaborators
  depend on this protocol, not on the runner class, so the dependency points one
  way and each collaborator can be driven by a stand-in.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Protocol

from qte_shared.bar_gate import BarGate
from qte_shared.bus import NatsBus, Subjects
from qte_shared.cache import RedisState
from qte_shared.config import settings
from qte_shared.db import EventRepository
from qte_shared.interfaces.market_data import ProviderError
from qte_shared.models import Candle
from qte_shared.providers import get_provider_class
from qte_shared.state_scope import StateScope
from qte_shared.strategies.strategy_base import SignalIntent
from qte_strategy_engine.broker_sink import BrokerSink
from qte_strategy_engine.db import OpenPositionRepository, SignalRepository
from qte_strategy_engine.slot import StrategySlot
from qte_strategy_engine.telegram_notify import TelegramLifecycleNotifier

SERVICE_NAME = "strategy-runner"


@dataclass(slots=True)
class RunnerContext:
    """The runner's connections, repositories and book, shared by reference."""

    bus: NatsBus
    state: RedisState
    subjects: Subjects
    events: EventRepository
    signals: SignalRepository
    positions: OpenPositionRepository
    sink: BrokerSink
    telegram: TelegramLifecycleNotifier
    scope: StateScope
    #: Every (strategy, symbol) this process drives.
    slots: list[StrategySlot] = field(default_factory=list)
    #: Every ``(strategy, symbol)`` the mapping table declares, whether or not
    #: a slot was built for it, and ``None`` when no table was read. Orphan
    #: settlement needs the declaration and not the slots: a pair missing from
    #: both is retired, while one the table still claims is a strategy that
    #: failed to load, and those two call for opposite answers.
    declared_pairs: set[tuple[str, str]] | None = None
    #: When this process began listening; a close after it is a live event.
    history_started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    #: Which pairs may not be decided on, as last read from Redis.
    bar_gate: BarGate = field(default_factory=BarGate)
    #: Set once shutdown begins; every loop and every decision checks it.
    stopping: asyncio.Event = field(default_factory=asyncio.Event)


class RunnerHooks(Protocol):
    """What a collaborator may ask of the runner that owns it."""

    async def check_ownership(self) -> None:
        """Raise, and pause delivery, unless this process still owns the book."""

    async def refresh_shadow_mode(self) -> None:
        """Re-read the durable shadow switch; fail closed when it cannot be read."""

    async def live_delivery_paused(self) -> bool:
        """Whether a live book is currently holding its deliveries back."""

    async def refresh_bar_gate(self) -> None:
        """Re-read which pairs ``/prevent`` paused."""

    async def emit(
        self,
        slot: StrategySlot,
        intent: SignalIntent,
        fallback_price: float | None,
        moment: datetime,
    ) -> None:
        """Deliver one intent through every guard the decision path applies."""

    async def flatten_cycle(
        self,
        slot: StrategySlot,
        uxid: str,
        price: float | None,
        moment: datetime,
        *,
        reason: str,
    ) -> None:
        """Close one open cycle with a ``FLAT`` on the ordinary path."""

    async def feed_candle_serialized(self, slot: StrategySlot, candle: Candle) -> None:
        """Feed one closed bar to its slot and let the strategy decide on it."""


def provider_is_synthetic(provider_name: str | None = None) -> bool:
    """Whether *provider_name* invents its prices, as the dev simulator does.

    Without a name: whether any configured provider does. A synthetic one never
    shares a state scope with another (:class:`StateScope`), so that is the
    answer for the whole feed.
    """
    names = (provider_name,) if provider_name else settings.market_data.providers
    try:
        return any(get_provider_class(name).synthetic for name in names)
    except (ProviderError, ImportError):
        return False


__all__ = ["SERVICE_NAME", "RunnerContext", "RunnerHooks", "provider_is_synthetic"]
