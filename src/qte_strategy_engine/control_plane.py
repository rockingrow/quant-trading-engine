"""The runner's control plane: what ``qte-control`` and the Telegram bot can ask.

Request/reply over NATS rather than an HTTP endpoint, because this repository
has no control-plane service to hang one on and does not want one — see
:mod:`qte_strategy_engine.control`. Five actions, each a registered handler on a
:class:`~qte_shared.bus.ControlRouter`:

``set_shadow_mode`` / ``set_bar_gate``
    Broadcasts. The caller already persisted the change in Redis; this only
    makes the running process re-read it, so a delayed broadcast can never
    overwrite a newer setting.
``ping`` / ``status``
    Answered from memory, which is the point of asking the *running* process:
    how full a window is, and whether a pair is blocked by an unreconciled
    delivery, are not things Redis or Postgres can be queried for.
``flat``
    Closes the cycles a scope names, through the ordinary path — the slots'
    own factories for pairs this runner drives, the orphan close for the rest.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from nats.aio.msg import Msg

from qte_shared.bus import ControlRouter
from qte_shared.logging_setup import get_logger
from qte_shared.state_scope import StateScope
from qte_strategy_engine.delivery import DeliveryPipeline
from qte_strategy_engine.recovery import PositionRecovery
from qte_strategy_engine.runner_context import SERVICE_NAME, RunnerContext, RunnerHooks
from qte_strategy_engine.slot import StrategySlot

log = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class RunnerIdentity:
    """Who answers: the ownership id and when this process started."""

    owner_id: str
    started_at: datetime


class ControlPlane:
    """Answers the runner's control subject."""

    def __init__(
        self,
        context: RunnerContext,
        hooks: RunnerHooks,
        delivery: DeliveryPipeline,
        recovery: PositionRecovery,
        identity: RunnerIdentity,
    ) -> None:
        self._context = context
        self._hooks = hooks
        self._delivery = delivery
        self._recovery = recovery
        self._identity = identity
        self._router = ControlRouter(SERVICE_NAME)
        self._router.register("set_shadow_mode", self._on_shadow_mode_changed)
        self._router.register("set_bar_gate", self._on_bar_gate_changed)
        self._router.register("ping", self._on_ping)
        self._router.register("status", self._on_status)
        self._router.register("flat", self._on_flat)

    @property
    def _owner_id(self) -> str:
        return self._identity.owner_id

    @property
    def _started_at(self) -> datetime:
        return self._identity.started_at

    async def dispatch(self, msg: Msg) -> None:
        """Answer one control message over the runner's current bus."""
        await self._router.dispatch(msg, bus=self._context.bus)

    # ── Handlers ──────────────────────────────────────────────────────

    async def _on_shadow_mode_changed(self, command: dict[str, Any]) -> None:
        # The CLI already persisted the command. Re-read it instead of
        # allowing an older, delayed broadcast to overwrite a newer mode.
        await self._hooks.refresh_shadow_mode()
        await self._context.events.record_event(
            service=SERVICE_NAME,
            event="shadow_mode_changed",
            level="WARNING",
            payload={"enabled": self._context.sink.shadow_mode},
        )

    async def _on_bar_gate_changed(self, command: dict[str, Any]) -> dict[str, Any]:
        # Same contract as shadow mode: the caller stored it, and this only
        # makes the running process pick it up without waiting for a bar.
        await self._hooks.refresh_bar_gate()
        log.warning("Bar gate changed - now blocking %s", self._context.bar_gate.describe())
        await self._context.events.record_event(
            service=SERVICE_NAME,
            event="bar_gate_changed",
            level="WARNING",
            payload=self._context.bar_gate.to_payload(),
        )
        return {"gate": self._context.bar_gate.to_payload()}

    async def _on_ping(self, command: dict[str, Any]) -> dict[str, Any]:
        return self.ping_payload()

    async def _on_status(self, command: dict[str, Any]) -> dict[str, Any]:
        return await self.status_payload()

    async def _on_flat(self, command: dict[str, Any]) -> dict[str, Any]:
        return await self.flat_on_request(command.get("scope") or {})

    # ── Answers ───────────────────────────────────────────────────────

    def ping_payload(self) -> dict[str, Any]:
        return {
            "service": SERVICE_NAME,
            "owner_id": self._owner_id,
            "slots": len(self._context.slots),
            "namespace": self._context.scope.namespace,
            "execution_mode": self._context.scope.execution_mode,
            "delivery_paused": not self._context.scope.is_paper and self._context.sink.shadow_mode,
            "shadow_mode": self._context.sink.shadow_mode,
            "ready": all(slot.is_warm for slot in self._context.slots)
            and not self._delivery.uncertain_pairs,
        }

    async def status_payload(self) -> dict[str, Any]:
        """Every slot, and whether it could decide on the next bar.

        Answered from memory, which is the point of asking the *running*
        process: how full a window is, and whether a pair is blocked by an
        unreconciled delivery, are not things Redis or Postgres can be queried
        for.
        """
        await self._hooks.refresh_shadow_mode()
        await self._hooks.refresh_bar_gate()
        delivery_paused = not self._context.scope.is_paper and self._context.sink.shadow_mode
        strategies = []
        for slot in self._context.slots:
            gated = self._context.bar_gate.blocks(symbol=slot.symbol, strategy=slot.strategy.name)
            uncertain = slot.key in self._delivery.uncertain_pairs
            strategies.append(
                {
                    "strategy": slot.strategy.name,
                    "symbol": slot.symbol,
                    "timeframe": slot.timeframe,
                    "bars": len(slot.buffer),
                    "warmup": slot.warmup,
                    "warm": slot.is_warm,
                    "newest_bar": slot.buffer[-1].open_time.isoformat() if slot.buffer else None,
                    "open_cycles": list(slot.factory.open_cycles(slot.symbol)),
                    "gated": gated,
                    "uncertain": uncertain,
                    # What the chat actually asks: can this pair produce a
                    # signal on its next closed bar, or is something holding it?
                    "ready": slot.is_warm and not gated and not uncertain and not delivery_paused,
                }
            )
        return {
            **self.ping_payload(),
            "started_at": self._started_at.isoformat(),
            "gate": self._context.bar_gate.to_payload(),
            "transport": self._context.sink.transport,
            "strategies": strategies,
        }

    async def flat_on_request(self, scope: dict[str, Any]) -> dict[str, Any]:
        """Close every open cycle the scope names, through the ordinary path.

        The scope is the one the bot offers: everything, one symbol, or one
        strategy. A pair already flat contributes nothing, and a refusal is
        reported per pair rather than failing the whole request -- an operator
        asking for everything wants whatever can be closed to be closed, and to
        be told plainly about the rest.

        The book this walks is the whole stored book, not only the pairs this
        runner drives. A row whose strategy was unmapped or removed while it
        still held a position has no slot, and skipping it silently is what made
        ``/flat`` report "nothing was open" while ``/positions`` kept listing it
        and the broker never heard anything. Those rows are closed by
        :meth:`_flatten_orphan_cycle` instead.
        """
        everything = bool(scope.get("everything"))
        symbol = (scope.get("symbol") or "").upper() or None
        strategy = scope.get("strategy") or None
        if not (everything or symbol or strategy):
            return {"error": "the scope must name everything, a symbol or a strategy"}

        closed: list[dict[str, str]] = []
        refused: list[dict[str, str]] = []
        moment = datetime.now(UTC)
        for slot in self._context.slots:
            if not everything and slot.symbol != symbol and slot.strategy.name != strategy:
                continue
            if not slot.factory.open_cycles(slot.symbol):
                continue
            refusal = _flat_refusal(
                slot,
                self._delivery.uncertain_pairs,
                self._context.sink.shadow_mode,
                self._context.scope,
            )
            if refusal is not None:
                refused.append(
                    {"strategy": slot.strategy.name, "symbol": slot.symbol, "reason": refusal}
                )
                continue
            async with slot.lock:
                for uxid in list(slot.factory.open_cycles(slot.symbol)):
                    position = slot.factory.open_position(slot.symbol, uxid)
                    price = (
                        self._recovery.last_known_price(slot, position)
                        if position is not None
                        else (slot.buffer[-1].close if slot.buffer else None)
                    )
                    log.warning(
                        "Operator FLAT: closing %s on %s uxid=%s",
                        slot.strategy.name,
                        slot.symbol,
                        uxid,
                    )
                    await self._hooks.flatten_cycle(slot, uxid, price, moment, reason="MANUAL_FLAT")
                    record = {
                        "strategy": slot.strategy.name,
                        "symbol": slot.symbol,
                        "signal_uxid": uxid,
                    }
                    if uxid in slot.factory.open_cycles(slot.symbol):
                        refused.append({**record, "reason": "the close was not delivered"})
                    else:
                        closed.append(record)

        for position in await self._recovery.orphan_positions(everything, symbol, strategy):
            record = {
                "strategy": position.strategy,
                "symbol": position.symbol,
                "signal_uxid": position.signal_uxid,
            }
            refusal = await self._recovery.flatten_orphan_cycle(position, moment)
            if refusal is None:
                closed.append(record)
            else:
                refused.append({**record, "reason": refusal})
        return {"closed": closed, "refused": refused}


def _flat_refusal(
    slot: StrategySlot,
    uncertain_pairs: set[tuple[str, str]],
    shadow_mode: bool,
    state_scope: StateScope,
) -> str | None:
    """Why this pair cannot be flattened right now, or ``None`` when it can.

    A pair that has not seen a bar yet is not refused: a ``FLAT`` carries no
    level to the broker, and the audit price falls back to the entry's — the
    same rule the stale and orphan closes follow.
    """
    if slot.key in uncertain_pairs:
        return "an unreconciled delivery is blocking the pair"
    if not state_scope.is_paper and shadow_mode:
        return "live delivery is paused (shadow mode)"
    return None


__all__ = ["ControlPlane", "RunnerIdentity"]
