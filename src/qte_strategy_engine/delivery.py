"""The signal outbox: stage, send, settle — and reconcile what is left unsettled.

Every signal is written to Postgres before the broker is called; that row's id
is the stable delivery id, so an ambiguous send can be retried without
manufacturing a second command, and a restart can tell a signal that went from
one that may have. This class owns the three pieces of in-memory state that make
that safe:

* ``uncertain_pairs`` — pairs with a delivery whose outcome is not known. No new
  intent is emitted for one until the outbox has reconciled it.
* ``pending_results`` — outcomes the broker returned whose checkpoint could not
  be written yet; the retry loop finishes them.
* ``unconfirmed_staging`` — pairs whose outbox insert may or may not have
  committed; a clean outbox scan is what unblocks them.

The moves a row may make are :mod:`qte_strategy_engine.delivery_status`.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

from qte_shared.logging_setup import get_logger
from qte_shared.models import BrokerSignal, OpenPosition
from qte_shared.strategies.signal_serialization import signal_record
from qte_shared.strategies.strategy_base import SignalIntent
from qte_strategy_engine.broker_sink import DeliveryResult
from qte_strategy_engine.delivery_status import (
    AMBIGUOUS,
    DELIVERED,
    LIVE,
    PAPER,
    SETTLED,
    DeliveryStatus,
)
from qte_strategy_engine.runner_context import RunnerContext, RunnerHooks
from qte_strategy_engine.settings import RunnerSettings, runner_settings
from qte_strategy_engine.slot import StrategySlot

log = get_logger(__name__)


class DeliveryPipeline:
    """Owns the outbox: every send, every checkpoint, every reconciliation."""

    def __init__(
        self,
        context: RunnerContext,
        hooks: RunnerHooks,
        *,
        settings: RunnerSettings | None = None,
    ) -> None:
        self._context = context
        self._hooks = hooks
        #: Read at call time, so a changed retry horizon reaches the next pass.
        self._settings = settings or runner_settings
        self.uncertain_pairs: set[tuple[str, str]] = set()
        self.unconfirmed_staging: set[tuple[str, str]] = set()
        self.pending_results: dict[str, DeliveryResult] = {}

    async def emit_intent(
        self,
        slot: StrategySlot,
        intent: SignalIntent,
        fallback_price: float | None,
        moment: datetime,
    ) -> None:
        """Stage, send and settle one intent the runner has cleared to emit.

        The caller (:meth:`StrategyRunner.emit`) has already checked ownership
        for this intent; the two checks that must sit right against the broker
        boundary are in :meth:`send_staged_signal`.
        """
        await self._hooks.refresh_shadow_mode()
        if not self._context.scope.is_paper and self._context.sink.shadow_mode:
            return
        if slot.key in self.uncertain_pairs:
            log.error(
                "Dropped intent for uncertain delivery strategy=%s symbol=%s; "
                "the durable outbox must reconcile first",
                slot.strategy.name,
                slot.symbol,
            )
            return
        if intent.price is None:
            intent.price = fallback_price

        # Size is not filled in here: `build()` risk-sizes the entry against
        # the account and rescales the strategy's closes to match, so the
        # backtest and this loop put the same number on the wire.
        try:
            signal = slot.factory.build(
                intent,
                symbol=slot.symbol,
                moment=moment,
                commit=False,
            )
        except ValueError as exc:
            log.warning("Dropped intent from %s: %s", slot.strategy.name, exc)
            return

        original_shadow = self._context.sink.shadow_mode
        delivery_id = await self._context.signals.stage_signal(
            signal,
            transport=self._context.sink.transport,
            shadow=original_shadow,
            recovery_context=slot.factory.pending_delivery_context(slot.symbol),
        )
        if delivery_id is None:
            # The insert may have committed before its connection failed.
            # Do not create a second command until a successful outbox scan
            # proves whether this pair has an unfinished row.
            self.uncertain_pairs.add(slot.key)
            self.unconfirmed_staging.add(slot.key)
            slot.factory.discard_pending_delivery_context(slot.symbol)
            log.error(
                "Signal not sent because its outbox row could not be persisted "
                "strategy=%s symbol=%s uxid=%s",
                slot.strategy.name,
                slot.symbol,
                signal.signal_uxid,
            )
            return

        self.uncertain_pairs.add(slot.key)
        result = await self.send_staged_signal(signal, delivery_id, shadow=original_shadow)
        if result is None:
            return
        if await self.finish_delivery(slot, signal, delivery_id, result):
            self.uncertain_pairs.discard(slot.key)

        try:
            await self._context.bus.publish(
                self._context.subjects.signal_emitted(),
                {
                    "signal": signal_record(signal),
                    "delivery": {
                        "id": delivery_id,
                        "status": result.status,
                        "transport": result.transport,
                    },
                    "reason": intent.reason,
                    "emitted_at": datetime.now(UTC).isoformat(),
                },
            )
        except Exception as mirror_error:  # noqa: BLE001 - a mirror must not stop a decision
            # The signal itself went out over the broker's connection and is in
            # Postgres. Raising here would skip the rest of this bar's intents
            # — a stop after a target, the weekend FLAT — over a copy.
            log.warning(
                "Could not mirror signal %s on %s: %s",
                signal.signal_uxid,
                self._context.subjects.signal_emitted(),
                mirror_error,
            )

    async def send_staged_signal(
        self, signal: BrokerSignal, delivery_id: str, *, shadow: bool
    ) -> DeliveryResult | None:
        """Record uncertainty before crossing the broker boundary."""
        await self._hooks.check_ownership()
        await self._hooks.refresh_shadow_mode()
        if shadow != self._context.scope.is_paper:
            raise ValueError("Outbox execution mode does not match the state namespace")
        if shadow:
            return DeliveryResult(
                status=DeliveryStatus.SHADOW, transport=self._context.sink.transport
            )
        if self._context.sink.shadow_mode:
            log.error(
                "Cannot recover live delivery id=%s while shadow mode is enabled", delivery_id
            )
            return None
        if not await self._context.signals.mark_delivery(
            delivery_id, status=DeliveryStatus.UNKNOWN
        ):
            return None
        await self._hooks.check_ownership()
        await self._hooks.refresh_shadow_mode()
        if self._context.sink.shadow_mode:
            return None
        try:
            return await self._context.sink.send(signal, delivery_id=delivery_id)
        except Exception:
            log.exception("Broker call raised after staging delivery id=%s", delivery_id)
            return DeliveryResult(
                status=DeliveryStatus.UNKNOWN, transport=self._context.sink.transport
            )

    async def finish_delivery(
        self,
        strategy_slot: StrategySlot,
        signal: BrokerSignal,
        delivery_id: str,
        outcome: DeliveryResult,
    ) -> bool:
        """Checkpoint broker acceptance separately from local state persistence.

        A known result stays in memory if its checkpoint fails. After a crash,
        the pre-send unknown marker prevents blind resends outside a configured
        deduplication horizon. Accepted checkpoints only retry local writes.

        Also where the chat is told about the action, for both callers at once:
        the emit path and the outbox recovery both land here with the same
        delivery id, so a send that was first reported as ambiguous and later
        confirmed updates its own line in the message instead of appending a
        second one. Queuing only, so Telegram is never in front of the next bar.
        """
        self._context.telegram.note_signal(
            signal,
            delivery_id=delivery_id,
            delivery_status=outcome.status,
            transport=outcome.transport,
            detail=outcome.detail or None,
        )
        if outcome.status is not DeliveryStatus.UNKNOWN:
            self.pending_results[delivery_id] = outcome
        try:
            if outcome.status in DELIVERED:
                if not await self._context.signals.mark_delivery(
                    delivery_id, status=outcome.status.checkpoint
                ):
                    return False
                strategy_slot.factory.commit(signal, delivery_id=delivery_id)
                if not await self.track_cycle(strategy_slot, signal.signal_uxid):
                    return False
            finalized = await self._context.signals.mark_delivery(
                delivery_id, status=outcome.status, error=outcome.detail or None
            )
            if not finalized or outcome.status is DeliveryStatus.UNKNOWN:
                return False
        except Exception:
            log.exception("Could not reconcile delivery id=%s; pair remains blocked", delivery_id)
            return False
        self.pending_results.pop(delivery_id, None)
        strategy_slot.factory.discard_pending_delivery_context(strategy_slot.symbol)
        return True

    async def recover_pending_deliveries(self) -> None:
        """Reconcile every unfinished phase without starving rows beyond page one."""
        await self._hooks.check_ownership()
        await self._hooks.refresh_shadow_mode()
        active_slots = {strategy_slot.key: strategy_slot for strategy_slot in self._context.slots}
        blocked_pairs: set[tuple[str, str]] = set()
        resolved_pairs: set[tuple[str, str]] = set()
        after_cursor = None
        while True:
            pending_rows = await self._context.signals.pending_deliveries(
                after_cursor=after_cursor, include_ids=tuple(self.pending_results)
            )
            if not pending_rows:
                break
            after_cursor = (pending_rows[-1].created_at, pending_rows[-1].id)
            for pending_row in pending_rows:
                pairing = (pending_row.strategy, pending_row.symbol.upper())
                self.uncertain_pairs.add(pairing)
                strategy_slot = active_slots.get(pairing)
                if strategy_slot is None or pairing in blocked_pairs:
                    blocked_pairs.add(pairing)
                    continue
                try:
                    async with strategy_slot.lock:
                        resolved = await self.recover_delivery(strategy_slot, pending_row)
                except Exception:
                    log.exception("Could not recover delivery id=%s", pending_row.id)
                    resolved = False
                if resolved:
                    resolved_pairs.add(pairing)
                else:
                    blocked_pairs.add(pairing)
        # Do not unblock between rows: an older unresolved row blocks every
        # later row of its pair, including those on another page.
        self.uncertain_pairs.difference_update(
            (resolved_pairs | self.unconfirmed_staging) - blocked_pairs
        )
        self.unconfirmed_staging.clear()

    async def recover_delivery(self, strategy_slot: StrategySlot, pending_row: Any) -> bool:
        await self._hooks.check_ownership()
        delivery_id = str(pending_row.id)
        # A scan may see a prepared row while its original sender still holds
        # this lock. Never resend that stale snapshot after the sender finishes.
        pending_row = await self._context.signals.get_delivery(delivery_id)
        if pending_row is None:
            raise ValueError("Outbox row disappeared during recovery")
        if (
            pending_row.namespace != self._context.scope.namespace
            or pending_row.shadow != self._context.scope.is_paper
        ):
            raise ValueError("Outbox record belongs to another state namespace or execution mode")
        stored_status = DeliveryStatus(pending_row.delivery_status)
        if stored_status in PAPER and not self._context.scope.is_paper:
            raise ValueError("Paper delivery cannot become a live position")
        if stored_status in LIVE and self._context.scope.is_paper:
            raise ValueError("Broker delivery cannot become a paper position")
        if stored_status in SETTLED:
            if delivery_id not in self.pending_results:
                return True
        signal = BrokerSignal.model_validate(pending_row.payload["payload"])
        if (signal.strategy, signal.symbol.upper()) != strategy_slot.key:
            raise ValueError("Outbox payload does not match its stored strategy and symbol")
        strategy_slot.factory.restore_pending_delivery_context(
            strategy_slot.symbol, self._context.signals.recovery_context(pending_row)
        )
        outcome = self.pending_results.get(delivery_id)
        if outcome is None and stored_status in {
            DeliveryStatus.SENT_PENDING,
            DeliveryStatus.SHADOW_PENDING,
        }:
            outcome = DeliveryResult(
                status=stored_status.settled,
                transport=pending_row.transport,
            )
        if outcome is None:
            if pending_row.transport != self._context.sink.transport:
                log.error("Transport changed for unfinished delivery id=%s", delivery_id)
                return False
            if not pending_row.shadow and stored_status is not DeliveryStatus.PREPARED:
                elapsed = (datetime.now(UTC) - pending_row.created_at).total_seconds()
                retry_horizon = self._settings.delivery_retry_max_age
                if retry_horizon <= 0 or not 0 <= elapsed < retry_horizon:
                    log.error(
                        "Delivery id=%s requires broker reconciliation: outside the configured "
                        "deduplication horizon; no automatic resend",
                        delivery_id,
                    )
                    return False
            was_ambiguous = stored_status in AMBIGUOUS
            outcome = await self.send_staged_signal(signal, delivery_id, shadow=pending_row.shadow)
            if outcome is not None and outcome.status is DeliveryStatus.FAILED and was_ambiguous:
                # Rejecting a retry does not disprove acceptance of the first
                # ambiguous attempt. Keep the pair blocked for reconciliation.
                outcome = DeliveryResult(
                    status=DeliveryStatus.UNKNOWN,
                    transport=outcome.transport,
                    detail=outcome.detail,
                )
        if outcome is None:
            return False
        return await self.finish_delivery(strategy_slot, signal, delivery_id, outcome)

    async def retry_loop(self) -> None:
        """Continuously reconcile ambiguous sends without requiring a restart."""
        while not self._context.stopping.is_set():
            await asyncio.sleep(self._settings.delivery_retry_interval)
            try:
                await self.recover_pending_deliveries()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Signal outbox retry failed — retrying at the next interval")

    async def track_cycle(self, slot: StrategySlot, signal_uxid: str) -> bool:
        """Mirror one of the slot's cycles into Redis and Postgres, or clear both.

        Written from what the factory now believes rather than from the action
        that was just sent, so the "a TP1 taking the whole entry ends the
        cycle" rule is decided once — in :class:`OpenPosition` — instead of
        being restated by everything that persists a transition.
        """
        position = slot.factory.open_position(slot.symbol, signal_uxid)
        if position is None:
            await self._context.state.clear_open_position(
                slot.strategy.name, slot.symbol, signal_uxid
            )
            return await self._context.positions.clear(slot.strategy.name, slot.symbol, signal_uxid)
        return await self.persist_position(position)

    async def persist_position(self, position: OpenPosition) -> bool:
        if position.state_namespace not in (None, self._context.scope.namespace):
            raise ValueError("Cannot persist a position from another state namespace")
        position.state_namespace = self._context.scope.namespace
        await self._context.state.set_open_position(position)
        return await self._context.positions.upsert(position)


__all__ = ["DeliveryPipeline"]
