"""What the runner does about positions it finds on start, or is asked to close.

Open positions survive a restart by design: every transition is written to
Redis *and* to the Postgres ``open_positions`` table, and
:meth:`PositionRecovery.restore_position` reloads each slot's cycles — from the
cache, or from the table when the cache came up empty.

Two start-up passes then decide what is *not* simply restored, and they are
separate because their evidence differs:

* :meth:`PositionRecovery.flush_stale_positions` — closes, with ``R_SL``, a cycle
  older than ``QTE_RUNNER__STALE_POSITION_MAX_AGE``. Off by default: age is not
  evidence, a strategy can be right to hold for days.
* :meth:`PositionRecovery.settle_orphan_positions` — closes, with ``FLAT``, a
  cycle the mapping table no longer claims. Nothing would ever decide on it.

The same orphan close serves the operator's ``/flat`` for rows no slot drives.
Every close goes through the ordinary outbox
(:class:`~qte_strategy_engine.delivery.DeliveryPipeline`).
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from datetime import UTC, datetime

from qte_shared.config import settings
from qte_shared.logging_setup import get_logger
from qte_shared.models import BrokerSignal, OpenPosition, PositionBlock, SignalAction
from qte_shared.strategies.strategy_base import SignalIntent
from qte_strategy_engine.delivery import DeliveryPipeline
from qte_strategy_engine.delivery_status import DELIVERED
from qte_strategy_engine.runner_context import RunnerContext, RunnerHooks
from qte_strategy_engine.settings import RunnerSettings, runner_settings
from qte_strategy_engine.slot import StrategySlot

log = get_logger(__name__)


class PositionRecovery:
    """Restores, flushes and settles the stored book around a start."""

    def __init__(
        self,
        context: RunnerContext,
        hooks: RunnerHooks,
        delivery: DeliveryPipeline,
        *,
        settings: RunnerSettings | None = None,
    ) -> None:
        self._context = context
        self._hooks = hooks
        self._delivery = delivery
        self._settings = settings or runner_settings

    async def restore_position(self, slot: StrategySlot) -> None:
        """Reload every cycle this slot holds — Redis first, Postgres behind it.

        The fallback is the point. Redis coming up empty is ambiguous: it means
        "flat" and it means "someone re-provisioned the cache", and acting on
        the wrong reading opens a second cycle against a position the broker is
        still carrying. The table settles it, and re-seeds the cache so the
        next boot is a cache hit again.
        """
        strategy, symbol = slot.strategy.name, slot.symbol
        positions = await self._context.state.get_open_positions_for(strategy, symbol)
        source = "redis"
        if not positions:
            positions = await self._context.positions.list_for(strategy, symbol)
            source = "postgres"
            for position in positions:
                if position.state_namespace != self._context.scope.namespace:
                    raise ValueError("Stored position has missing or foreign state provenance")
                await self._context.state.set_open_position(position)

        for position in positions:
            if position.state_namespace != self._context.scope.namespace:
                raise ValueError(
                    "Cannot restore a position with missing or foreign state provenance"
                )
            slot.factory.restore_position(position, symbol=symbol)
            log.info(
                "Restored open cycle from %s strategy=%s symbol=%s uxid=%s qty=%s remaining=%s",
                source,
                strategy,
                symbol,
                position.signal_uxid,
                position.quantity,
                position.remaining,
            )

    async def flush_stale_positions(self) -> None:
        """Close a cycle whose last transition is older than the window, if asked to.

        Off by default, and opt-in for a reason: age is not evidence. A strategy
        that holds a cycle until its own exit condition is met can be right to
        carry one for days, and an outage nobody can attend to outlasts any
        window worth setting — so the operator who wants the old behaviour asks
        for it with ``QTE_RUNNER__FLUSH_STALE_POSITIONS``. What genuinely has
        nobody deciding on it is closed by :meth:`settle_orphan_positions`
        instead, on the mapping table rather than on a clock.

        When it is on, a position past ``QTE_RUNNER__STALE_POSITION_MAX_AGE``
        is closed with ``R_SL``, a terminal action, so the cycle ends and its
        size stops being charged against the account on every later sizing. A
        deploy comes back inside the window and keeps its positions, which is
        what the Redis/Postgres recovery path is for. Zero makes every open
        position stale.

        ``warn`` reports and sends nothing, which is how to see what a first
        ``close`` run would do before letting it do it.

        The duplicate-id check below runs in every mode, ``off`` included: two
        pairs holding one ``signal_uxid`` is a correctness problem rather than
        a staleness one, and nothing else on the boot path would catch it.
        """
        mode = self._settings.flush_stale_positions
        # What locks a pair is the cycle `restore_position` put on its slot,
        # and that came from Redis in preference to Postgres. Deciding from the
        # table instead would miss a cycle Redis holds and Postgres does not —
        # `DeliveryPipeline.persist_position` writes Redis first and `upsert` swallows its own
        # failure, so the two can disagree, and the pair would come back locked
        # on a position this never looked at.
        locked_pairs = [
            (strategy_slot, position)
            for strategy_slot in self._context.slots
            for position in strategy_slot.factory.positions_for(strategy_slot.symbol)
        ]
        # The table is still read, for the one thing the slots cannot show:
        # duplicate cycle ids across the whole namespace. Rows belonging to no
        # running strategy are `settle_orphan_positions`' business. Best-effort
        # — `list_open` reports its own failures and answers with an empty list.
        listed = await self._context.positions.list_open()
        # Whatever the mode: a duplicated cycle id is a correctness problem
        # rather than a staleness one, and "off" must not hide it. Checked over
        # both sources so an unreadable table cannot make this pass on no data.
        self.verify_position_uxids(self._context.slots, listed)
        if mode == "off":
            return

        max_age = self._settings.stale_position_max_age
        moment = datetime.now(UTC)
        for strategy_slot, position in locked_pairs:
            if strategy_slot.key in self._delivery.uncertain_pairs:
                log.warning(
                    "Not flushing %s %s: its delivery is still unreconciled. The outbox "
                    "settles first; the next start will flush it if it is still stale.",
                    position.strategy,
                    position.symbol,
                )
                continue

            age = (moment - _as_aware(position.updated_at)).total_seconds()
            if age < max_age:
                log.info(
                    "Keeping open cycle %s on %s %s: %.0fs old, inside the %.0fs window",
                    position.signal_uxid,
                    strategy_slot.strategy.name,
                    strategy_slot.symbol,
                    age,
                    max_age,
                )
                continue

            log.warning(
                "Stale open cycle %s on %s %s: %.0fs since its last transition, over the "
                "%.0fs window — %s",
                position.signal_uxid,
                strategy_slot.strategy.name,
                strategy_slot.symbol,
                age,
                max_age,
                "closing it with R_SL" if mode == "close" else "would close it with R_SL",
            )
            if mode == "close":
                await self.close_stale_position(strategy_slot, position)

    async def settle_orphan_positions(self) -> None:
        """Close, or report, every stored cycle no running strategy decides on.

        The mapping table is the authority, not the slots. A pair missing from
        both was retired by the operator — dropped from a symbol's
        ``strategies`` list, or its symbol given ``enabled = false`` — and
        nothing will ever decide on its cycle again: no slot holds it, no bar
        reaches it, and its size is charged against the account on every later
        sizing. ``QTE_RUNNER__CLOSE_ORPHAN_POSITIONS`` says what to do about
        that, and ``close`` ends the cycle with ``FLAT``.

        A pair the table *still* claims is reported and never closed, however
        long its slot has been missing. That is not a retirement, it is a
        strategy that failed to load or a repository that is not mounted, and
        the two are indistinguishable from the slots alone. Reading the
        declaration instead is what stops a half-mounted ``__strategies__/``
        from flattening the whole book on one bad deploy.

        With no mapping table at all nothing declared any intent — the symbols
        come off each strategy's own class — so every orphan is reported and
        none is closed.

        Unlike :meth:`PositionRecovery.flush_stale_positions` this does not consult age. The
        table saying a pair is gone is evidence; a position being old is not.
        """
        mode = self._settings.close_orphan_positions
        orphans = await self.orphan_positions(True, None, None)
        if not orphans:
            return
        moment = datetime.now(UTC)
        for position in orphans:
            pair = (position.strategy, position.symbol.upper())
            if self._context.declared_pairs is None:
                log.error(
                    "Open position %s %s uxid=%s belongs to no running strategy, and no "
                    "mapping table was read — nothing declares whether it is retired, so "
                    "it is left alone. Map the strategy again and restart, or close it "
                    "with /flat.",
                    position.strategy,
                    position.symbol,
                    position.signal_uxid,
                )
                continue
            if pair in self._context.declared_pairs:
                log.error(
                    "Open position %s %s uxid=%s is still mapped in %s but no slot was "
                    "built for it — the strategy did not load, or its repository is not "
                    "mounted. Left open on purpose: a missing mount is not a retirement. "
                    "Fix the mount and restart, or close it with /flat.",
                    position.strategy,
                    position.symbol,
                    position.signal_uxid,
                    settings.engine.mapping_file,
                )
                continue
            if mode == "off":
                log.error(
                    "Open position %s %s uxid=%s belongs to no mapped strategy — this "
                    "runner is not closing it (QTE_RUNNER__CLOSE_ORPHAN_POSITIONS=off). "
                    "Nobody else will either.",
                    position.strategy,
                    position.symbol,
                    position.signal_uxid,
                )
                continue
            if pair in self._delivery.uncertain_pairs:
                # The same guard the age flush keeps, and it matters more here:
                # `DeliveryPipeline.recover_pending_deliveries` cannot reconcile a pair it has
                # no slot for, so it leaves it blocked. Sending over that would
                # put a second command against one cycle whose first one may
                # already have reached the broker — and, since an unconfirmed
                # close keeps the row, it would do so again on every start.
                log.error(
                    "Not closing orphan cycle %s on %s %s: a delivery of its own is still "
                    "unreconciled and no slot drives the pair the outbox would need to "
                    "settle it. Reconcile it with the broker and delete the row, or map "
                    "the strategy again so the outbox can finish.",
                    position.signal_uxid,
                    position.strategy,
                    position.symbol,
                )
                continue
            log.warning(
                "Open cycle %s on %s %s belongs to no mapped strategy — %s",
                position.signal_uxid,
                position.strategy,
                position.symbol,
                "closing it with FLAT" if mode == "close" else "would close it with FLAT",
            )
            if mode != "close":
                continue
            refusal = await self.flatten_orphan_cycle(position, moment)
            if refusal is not None:
                log.error(
                    "Could not close orphan cycle %s on %s %s: %s. The row stays, so the "
                    "next start tries again.",
                    position.signal_uxid,
                    position.strategy,
                    position.symbol,
                    refusal,
                )

    async def close_stale_position(
        self, strategy_slot: StrategySlot, position: OpenPosition
    ) -> None:
        """Send the ``R_SL`` that ends one stale cycle.

        ``R_SL`` rather than ``FLAT``: both are terminal, but ``FLAT`` is the
        broker's "close everything on this strategy" and this closes exactly one
        cycle, named by the uxid the factory carries. Only the action and a price
        are set — :meth:`SignalFactory._size_close` fills the quantity from what
        is left of the position, and ``_carry_entry_context`` restates the rest,
        so the payload says the same things a strategy's own exit would.

        Emitted through :meth:`StrategyRunner.emit`, so it is staged in the durable outbox,
        delivered and audited like any other exit and inherits every guard there
        — ownership, shadow mode, the uncertain-pair block.
        """
        price = self.last_known_price(strategy_slot, position)
        async with strategy_slot.lock:
            await self._hooks.emit(
                strategy_slot,
                SignalIntent(
                    action=SignalAction.R_SL,
                    symbol=strategy_slot.symbol,
                    price=price,
                    reason="STALE_POSITION_FLUSH",
                    signal_uxid=position.signal_uxid,
                ),
                price,
                datetime.now(UTC),
            )

    @staticmethod
    def last_known_price(strategy_slot: StrategySlot, position: OpenPosition) -> float | None:
        """The freshest price this pair has, for the audit trail.

        The broker closes at market, so this number is what an operator reads
        back later rather than what the trade fills at. Redis history is
        preferred because it is the most recent thing the engine saw; after a
        long outage the cache may hold nothing current, and the entry price is
        then the only honest answer available.

        ``None`` when there is neither — a cycle restored from a bare uxid, in a
        cache that came back empty. The broker's schema leaves a close's price
        optional for exactly this, and an absent price says "unknown" where a
        zero would have said the trade closed at nothing.
        """
        if strategy_slot.buffer:
            return strategy_slot.buffer[-1].close
        return position.price

    @staticmethod
    def verify_position_uxids(
        *sources: Sequence[OpenPosition] | Sequence[StrategySlot],
    ) -> None:
        """Refuse to trade a book where two pairs claim one cycle id.

        The broker groups a whole trade by ``signal_uxid``, so a close on either
        pair would close the other's position and the audit trail would show
        nothing wrong. ``uq_open_positions_uxid`` makes this impossible to
        write; this catches rows that predate the constraint, or were put there
        by hand, and it raises rather than warning because every entry and exit
        on either pair is unsafe until an operator says which is real.

        Every source is read because each one alone can be blind. The restored
        slots are the live book and always available; the table adds the pairs
        this runner is not driving, and it answers with an empty list when it
        could not be read at all — so checking only the table would let a
        transient database error pass this on no data.
        """
        holders: dict[str, set[str]] = defaultdict(set)
        for source in sources:
            for entry in source:
                held = (
                    entry.factory.positions_for(entry.symbol)
                    if isinstance(entry, StrategySlot)
                    else [entry]
                )
                for position in held:
                    holders[position.signal_uxid].add(f"{position.strategy}/{position.symbol}")

        duplicated = {uxid: pairs for uxid, pairs in holders.items() if len(pairs) > 1}
        if not duplicated:
            return
        described = "; ".join(
            f"{uxid} held by {', '.join(sorted(pairs))}"
            for uxid, pairs in sorted(duplicated.items())
        )
        raise RuntimeError(
            "Open positions share a trade-cycle id, so a close on one would close the "
            f"other at the broker: {described}. Reconcile with the broker, delete the row "
            "that is not real, and start again."
        )

    async def orphan_positions(
        self, everything: bool, symbol: str | None, strategy: str | None
    ) -> list[OpenPosition]:
        """Stored cycles inside the scope that no running slot can close.

        Read from Postgres rather than from the factories because that is the
        whole point: these are exactly the rows no factory knows about. The
        pairs this runner does drive are left to the loop above, which goes
        through the strategy's own factory and keeps the backtest and the live
        loop deciding alike.
        """
        driven = {slot.key for slot in self._context.slots}
        orphans: list[OpenPosition] = []
        for position in await self._context.positions.list_open():
            if (position.strategy, position.symbol.upper()) in driven:
                continue
            if (
                not everything
                and position.symbol.upper() != symbol
                and position.strategy != strategy
            ):
                continue
            orphans.append(position)
        return orphans

    async def flatten_orphan_cycle(self, position: OpenPosition, moment: datetime) -> str | None:
        """Send the ``FLAT`` that ends a cycle this runner drives no strategy for.

        Built here rather than by a :class:`SignalFactory` because there is no
        factory for an unmapped strategy, and minting one would need the
        mapping entry that is precisely what is gone. Everything else is the
        ordinary path: staged in the durable outbox, sent through the one sink,
        marked in the outbox, and only then reconciled locally.

        ``timeframe`` comes from the signals that opened the cycle — the broker
        payload requires one and :class:`OpenPosition` carries none, so it is
        read back from ``signals`` rather than stored twice where the two copies
        could disagree. Without it no truthful payload exists, so the row is
        refused with that said rather than closed with a guess.

        Both callers land here: the operator's ``/flat`` and, on start,
        :meth:`settle_orphan_positions`.

        Local state is dropped only on a confirmed ``sent`` whose outbox
        checkpoint also landed. A failed or ambiguous delivery, or a checkpoint
        that could not be written, leaves the row in place: the broker may
        still be carrying the trade, and a book that forgot it is worse than
        one that still shows it.

        Every failure is returned as a refusal reason rather than raised. One
        malformed row must not abort the reply: the operator would lose the
        report of every cycle the slots already closed, and could not tell
        which half of a ``/flat`` took effect.

        Returns the refusal reason, or ``None`` when the cycle is closed.
        """
        if position.state_namespace not in (None, self._context.scope.namespace):
            return "the stored position belongs to another state namespace"
        if not self._context.scope.is_paper and self._context.sink.shadow_mode:
            return "live delivery is paused (shadow mode)"
        timeframes = await self._context.signals.cycle_timeframes([position.signal_uxid])
        timeframe = timeframes.get(position.signal_uxid)
        if not timeframe:
            return "no signal records the timeframe this cycle trades, so no payload can be built"

        try:
            signal = BrokerSignal(
                strategy=position.strategy,
                symbol=position.symbol,
                timeframe=timeframe,
                timestamp=moment,
                signal_uxid=position.signal_uxid,
                position=PositionBlock(
                    action=SignalAction.FLAT,
                    price=position.price,
                    # Passed through as stored. ``or None`` here would read a
                    # remaining size of zero as "size unknown", which is a
                    # different thing to tell the broker and the audit trail.
                    quantity=position.remaining,
                ),
            )
            signal.validate_shape()
        except ValueError as invalid:
            # Both models validate on construction and pydantic's
            # ``ValidationError`` is a ``ValueError``, so a malformed uxid or a
            # non-finite price arrives here. Refused, never raised: see above.
            log.error(
                "Cannot build the FLAT for %s %s uxid=%s: %s",
                position.strategy,
                position.symbol,
                position.signal_uxid,
                invalid,
            )
            return "the stored position cannot be expressed as a broker payload"
        log.warning(
            "FLAT on an unmapped strategy: closing %s on %s uxid=%s",
            position.strategy,
            position.symbol,
            position.signal_uxid,
        )
        delivery_id = await self._context.signals.stage_signal(
            signal, transport=self._context.sink.transport, shadow=self._context.scope.is_paper
        )
        if delivery_id is None:
            return "the close could not be staged in the outbox, so it was not sent"
        outcome = await self._delivery.send_staged_signal(
            signal, delivery_id, shadow=self._context.scope.is_paper
        )
        if outcome is None:
            return "the close was not delivered"
        self._context.telegram.note_signal(
            signal,
            delivery_id=delivery_id,
            delivery_status=outcome.status,
            transport=outcome.transport,
            detail=outcome.detail or None,
        )
        # Checkpointed before the local writes and only then finalised, the
        # same order `DeliveryPipeline.finish_delivery` keeps. A checkpoint that cannot be
        # written is a refusal: clearing the book against an outbox row still
        # reading `prepared` would leave the position forgotten here and the
        # row unresolvable, since no slot drives the pair the retry loop would
        # need to reconcile it.
        checkpointed = True
        if outcome.status in DELIVERED:
            checkpointed = await self._context.signals.mark_delivery(
                delivery_id, status=outcome.status.checkpoint
            )
        finalized = await self._context.signals.mark_delivery(
            delivery_id, status=outcome.status, error=outcome.detail or None
        )
        if outcome.status not in DELIVERED:
            log.error(
                "FLAT for unmapped %s %s uxid=%s came back %s — the stored position is kept "
                "until the broker confirms",
                position.strategy,
                position.symbol,
                position.signal_uxid,
                outcome.status,
            )
            return f"the broker did not confirm the close ({outcome.status})"
        if not (checkpointed and finalized):
            log.error(
                "FLAT for unmapped %s %s uxid=%s was %s but its outbox row could not be "
                "marked — the stored position is kept for reconciliation",
                position.strategy,
                position.symbol,
                position.signal_uxid,
                outcome.status,
            )
            return "the broker took the close but the outbox could not record it"
        await self._context.state.clear_open_position(
            position.strategy, position.symbol, position.signal_uxid
        )
        if not await self._context.positions.clear(
            position.strategy, position.symbol, position.signal_uxid
        ):
            return "the broker took the close but the stored position could not be cleared"
        return None


def _as_aware(moment: datetime) -> datetime:
    """A stored timestamp as an aware one, so arithmetic on it cannot raise.

    ``OpenPosition.updated_at`` is typed ``datetime`` with no timezone
    requirement and pydantic accepts a naive value, so a row written by an older
    build or edited by hand can come back naive. Subtracting that from an aware
    ``now()`` raises, and the flush runs inside ``start()`` — the runner would
    refuse to boot over one malformed row. Read as UTC, which is what every
    writer in this engine stores.
    """
    if moment.tzinfo is None:
        return moment.replace(tzinfo=UTC)
    return moment


__all__ = ["PositionRecovery"]
