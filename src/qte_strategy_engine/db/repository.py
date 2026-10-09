"""Reads and writes against the runner's ``signals``, ``open_positions`` and
``telegram_cycle_messages`` tables."""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime

from sqlalchemy import delete, func, or_, select, tuple_, update
from sqlalchemy.dialects.postgresql import insert

from qte_shared.config import settings
from qte_shared.db.session import Database, get_database
from qte_shared.logging_setup import get_logger
from qte_shared.models import BrokerSignal, OpenPosition, SignalAction
from qte_shared.strategies.signal_serialization import signal_record
from qte_strategy_engine.db.models import (
    OpenPositionRow,
    SignalAudit,
    TelegramCycleMessage,
)

log = get_logger(__name__)

OUTBOX_CONTEXT_KEY = "__qte_outbox__"


class SignalRepository:
    """The audit trail of everything the runner emitted."""

    def __init__(self, database: Database | None = None) -> None:
        self._db = database or get_database()
        self._scope = settings.state_scope
        self._namespace = self._scope.namespace

    async def stage_signal(
        self,
        signal: BrokerSignal,
        *,
        transport: str,
        shadow: bool,
        recovery_context: dict | None = None,
    ) -> str | None:
        """Persist a signal before delivery and return its stable delivery id.

        This is the signal outbox.  Sending is deliberately conditional on
        this insert succeeding: without the row, a timeout cannot be retried
        with the same JetStream id or reconciled after a process restart.
        """
        delivery_id = uuid.uuid4()
        row = _signal_row(
            signal,
            namespace=self._namespace,
            row_id=delivery_id,
            transport=transport,
            delivery_status="prepared",
            shadow=shadow,
            recovery_context=recovery_context,
        )
        try:
            async with self._db.session() as session:
                session.add(row)
                await session.flush()
            return str(delivery_id)
        except Exception as exc:
            log.error("Could not stage signal %s %s: %s", signal.strategy, signal.symbol, exc)
            return None

    @staticmethod
    def recovery_context(row: SignalAudit) -> dict:
        """Private state stored beside, never inside, the broker payload."""
        value = (row.inputs or {}).get(OUTBOX_CONTEXT_KEY, {})
        return value if isinstance(value, dict) else {}

    async def mark_delivery(
        self,
        delivery_id: str,
        *,
        status: str,
        error: str | None = None,
    ) -> bool:
        """Record the latest outcome without deleting ambiguous outbox rows."""
        try:
            row_id = uuid.UUID(delivery_id)
            async with self._db.session() as session:
                execution = await session.execute(
                    update(SignalAudit)
                    .where(SignalAudit.id == row_id, SignalAudit.namespace == self._namespace)
                    .values(delivery_status=status, delivery_error=error)
                )
            return execution.rowcount == 1
        except Exception as exc:
            log.error("Could not mark signal delivery %s as %s: %s", delivery_id, status, exc)
            return False

    async def pending_deliveries(
        self,
        limit: int = 100,
        *,
        statuses: tuple[str, ...] = (
            "prepared",
            "pending",
            "unknown",
            "sent_pending",
            "shadow_pending",
        ),
        after_cursor: tuple[datetime, uuid.UUID] | None = None,
        include_ids: Sequence[str] = (),
    ) -> Sequence[SignalAudit]:
        """Outbox rows whose final broker outcome is not known yet."""
        statement = (
            select(SignalAudit)
            .where(SignalAudit.namespace == self._namespace)
            .where(
                or_(
                    SignalAudit.delivery_status.in_(statuses),
                    SignalAudit.id.in_([uuid.UUID(identifier) for identifier in include_ids]),
                )
            )
            .order_by(SignalAudit.created_at.asc(), SignalAudit.id.asc())
            .limit(limit)
        )
        if after_cursor is not None:
            statement = statement.where(
                tuple_(SignalAudit.created_at, SignalAudit.id) > after_cursor
            )
        async with self._db.session() as session:
            return (await session.execute(statement)).scalars().all()

    async def get_delivery(self, delivery_id: str) -> SignalAudit | None:
        """Refresh a scanned row after acquiring its pair lock."""
        async with self._db.session() as session:
            statement = select(SignalAudit).where(
                SignalAudit.id == uuid.UUID(delivery_id),
                SignalAudit.namespace == self._namespace,
            )
            return (await session.execute(statement)).scalar_one_or_none()

    async def record_signal(
        self,
        signal: BrokerSignal,
        *,
        transport: str,
        delivery_status: str,
        shadow: bool,
        delivery_error: str | None = None,
    ) -> str | None:
        """Persist one emitted signal. Returns the row id, or ``None`` on failure.

        Audit failures are swallowed on purpose: the trade has already been
        published to the broker by the time this runs, and raising here would
        turn a logging outage into a crashed runner that stops trading.
        """
        row = _signal_row(
            signal,
            namespace=self._namespace,
            transport=transport,
            delivery_status=delivery_status,
            delivery_error=delivery_error,
            shadow=shadow,
        )
        try:
            async with self._db.session() as session:
                session.add(row)
                await session.flush()
                return str(row.id)
        except Exception as exc:
            log.error("Audit write failed for %s %s: %s", signal.strategy, signal.symbol, exc)
            return None

    async def list_signals(
        self,
        *,
        strategy: str | None = None,
        symbol: str | None = None,
        since: datetime | None = None,
        limit: int = 100,
    ) -> Sequence[SignalAudit]:
        statement = (
            select(SignalAudit)
            .where(SignalAudit.namespace == self._namespace)
            .order_by(SignalAudit.created_at.desc())
            .limit(limit)
        )
        if strategy:
            statement = statement.where(SignalAudit.strategy == strategy)
        if symbol:
            statement = statement.where(SignalAudit.symbol == symbol)
        if since:
            statement = statement.where(SignalAudit.created_at >= since)
        async with self._db.session() as session:
            return (await session.execute(statement)).scalars().all()

    async def get_cycle(self, signal_uxid: str) -> Sequence[SignalAudit]:
        """Every action of one trade cycle, oldest first — the reconcile view."""
        statement = (
            select(SignalAudit)
            .where(SignalAudit.namespace == self._namespace)
            .where(SignalAudit.signal_uxid == signal_uxid)
            .order_by(SignalAudit.created_at.asc())
        )
        async with self._db.session() as session:
            return (await session.execute(statement)).scalars().all()

    async def cycle_timeframes(self, cycle_ids: Sequence[str]) -> dict[str, str]:
        """The timeframe each named cycle was signalled on.

        ``open_positions`` does not carry one — a cycle belongs to a (strategy,
        symbol) pair and the timeframe is the strategy's — but a table of open
        positions is unreadable without it. The signals that opened the cycles
        do carry it, so it is read back from there rather than added to a
        second table that could then disagree.
        """
        if not cycle_ids:
            return {}
        statement = select(SignalAudit.signal_uxid, SignalAudit.timeframe).where(
            SignalAudit.namespace == self._namespace,
            SignalAudit.signal_uxid.in_(tuple(cycle_ids)),
        )
        try:
            async with self._db.session() as session:
                rows = (await session.execute(statement)).all()
        except Exception as exc:
            log.error("Could not read the timeframes of %d cycle(s): %s", len(cycle_ids), exc)
            return {}
        return {row.signal_uxid: row.timeframe for row in rows if row.timeframe}

    async def closed_cycles(
        self, *, limit: int = 10, offset: int = 0
    ) -> tuple[list[ClosedCycle], int]:
        """Trade cycles that are over, newest close first, one page at a time.

        There is no ``closed_positions`` table and there should not be: the row
        in ``open_positions`` *is* the cycle, and it is deleted when the cycle
        ends. What survives is the audit trail, so "closed" is derived rather
        than stored — a cycle whose entry was delivered and whose id no longer
        has an open row.

        Derived that way on purpose, rather than by looking for a terminal
        action: a ``TP1`` that happens to take the whole entry ends a cycle
        too, and matching on actions would list every cycle except those. The
        absence of the open row is the one definition that agrees with what the
        runner actually believes, because the runner is what deletes it.
        """
        delivered = ("sent", "shadow")
        entries = tuple(action.value for action in (SignalAction.LONG, SignalAction.SHORT))

        entered_cycles = select(SignalAudit.signal_uxid).where(
            SignalAudit.namespace == self._namespace,
            SignalAudit.action.in_(entries),
            SignalAudit.delivery_status.in_(delivered),
        )
        still_open = select(OpenPositionRow.signal_uxid).where(
            OpenPositionRow.namespace == self._namespace
        )
        closed = (
            select(
                SignalAudit.signal_uxid.label("signal_uxid"),
                func.max(SignalAudit.created_at).label("closed_at"),
            )
            .where(
                SignalAudit.namespace == self._namespace,
                SignalAudit.signal_uxid.in_(entered_cycles),
                SignalAudit.signal_uxid.not_in(still_open),
            )
            .group_by(SignalAudit.signal_uxid)
        )
        page = closed.order_by(func.max(SignalAudit.created_at).desc()).limit(limit).offset(offset)

        try:
            async with self._db.session() as session:
                total = int(
                    (
                        await session.execute(select(func.count()).select_from(closed.subquery()))
                    ).scalar_one()
                )
                cycle_ids = [row.signal_uxid for row in (await session.execute(page)).all()]
                if not cycle_ids:
                    return [], total
                rows = (
                    (
                        await session.execute(
                            select(SignalAudit)
                            .where(
                                SignalAudit.namespace == self._namespace,
                                SignalAudit.signal_uxid.in_(cycle_ids),
                            )
                            .order_by(SignalAudit.created_at.asc())
                        )
                    )
                    .scalars()
                    .all()
                )
        except Exception as exc:
            log.error("Could not list closed cycles: %s", exc)
            return [], 0

        by_cycle: dict[str, list[SignalAudit]] = {cycle_id: [] for cycle_id in cycle_ids}
        for row in rows:
            by_cycle.setdefault(row.signal_uxid, []).append(row)
        return [_as_closed_cycle(cycle_id, by_cycle[cycle_id]) for cycle_id in cycle_ids], total


def _signal_row(
    signal: BrokerSignal,
    *,
    transport: str,
    delivery_status: str,
    shadow: bool,
    namespace: str | None = None,
    row_id: uuid.UUID | None = None,
    delivery_error: str | None = None,
    recovery_context: dict | None = None,
) -> SignalAudit:
    audit_inputs = dict(signal.inputs)
    if recovery_context:
        audit_inputs[OUTBOX_CONTEXT_KEY] = recovery_context
    return SignalAudit(
        namespace=namespace or settings.state_scope.namespace,
        id=row_id or uuid.uuid4(),
        signal_uxid=signal.signal_uxid,
        strategy=signal.strategy,
        symbol=signal.symbol,
        timeframe=signal.timeframe,
        action=signal.position.action.value,
        signal_time=signal.timestamp,
        price=signal.position.price,
        quantity=signal.position.quantity,
        sl=signal.position.sl,
        tp1=signal.position.tp1,
        tp2=signal.position.tp2,
        payload={"payload": signal_record(signal)},
        indicators=signal.indicators,
        inputs=audit_inputs,
        transport=transport,
        delivery_status=delivery_status,
        delivery_error=delivery_error,
        shadow=shadow,
    )


class OpenPositionRepository:
    """The durable copy of what each (strategy, symbol) pair currently holds.

    Redis is the hot path; this is the backstop. Both are written on every
    transition and the runner prefers Redis on boot, falling back here when the
    cache came up empty — a flushed or re-provisioned Redis is otherwise
    indistinguishable from "flat", and acting on that difference is what mints
    a second cycle against a position the broker still has open.

    Every method swallows its failures for the same reason the audit write
    does: by the time these run the signal is already with the broker, and
    raising would stop a runner that is holding real positions.
    """

    def __init__(self, database: Database | None = None) -> None:
        self._db = database or get_database()
        self._scope = settings.state_scope
        self._namespace = self._scope.namespace

    async def upsert(self, position: OpenPosition) -> bool:
        """Write one cycle's current state, replacing that cycle's previous row."""
        if position.state_namespace not in (None, self._namespace):
            raise ValueError("Cannot persist a position from another state namespace")
        position = position.model_copy(update={"state_namespace": self._namespace})
        values = {
            "signal_uxid": position.signal_uxid,
            "action": position.action.value,
            "opened_at": position.opened_at,
            "updated_at": position.updated_at,
            "price": position.price,
            "quantity": position.quantity,
            "remaining": position.remaining,
            "sl": position.sl,
            "tp1": position.tp1,
            "tp2": position.tp2,
            "state": position.model_dump(mode="json"),
        }
        statement = (
            insert(OpenPositionRow)
            .values(
                namespace=self._namespace,
                strategy=position.strategy,
                symbol=position.symbol,
                **values,
            )
            .on_conflict_do_update(constraint="uq_open_positions_uxid", set_=values)
        )
        try:
            async with self._db.session() as session:
                await session.execute(statement)
            return True
        except Exception as exc:
            log.error(
                "Could not persist open position %s %s: %s", position.strategy, position.symbol, exc
            )
            return False

    async def clear(self, strategy: str, symbol: str, signal_uxid: str | None = None) -> bool:
        """Drop one cycle's row, or every row of the pair when no cycle is named."""
        statement = delete(OpenPositionRow).where(
            OpenPositionRow.namespace == self._namespace,
            OpenPositionRow.strategy == strategy,
            OpenPositionRow.symbol == symbol.upper(),
        )
        if signal_uxid is not None:
            statement = statement.where(OpenPositionRow.signal_uxid == signal_uxid)
        try:
            async with self._db.session() as session:
                await session.execute(statement)
            return True
        except Exception as exc:
            log.error("Could not clear open position %s %s: %s", strategy, symbol, exc)
            return False

    async def get(self, strategy: str, symbol: str) -> OpenPosition | None:
        """The most recent cycle on the pair — the whole book for a single-cycle pair."""
        positions = await self.list_for(strategy, symbol)
        return positions[-1] if positions else None

    async def list_for(self, strategy: str, symbol: str) -> list[OpenPosition]:
        """Every cycle the pair holds, oldest first."""
        statement = (
            select(OpenPositionRow)
            .where(
                OpenPositionRow.namespace == self._namespace,
                OpenPositionRow.strategy == strategy,
                OpenPositionRow.symbol == symbol.upper(),
            )
            .order_by(OpenPositionRow.opened_at.asc())
        )
        try:
            async with self._db.session() as session:
                rows = (await session.execute(statement)).scalars().all()
        except Exception as exc:
            log.error("Could not read open positions %s %s: %s", strategy, symbol, exc)
            return []
        return [position for position in map(_as_position, rows) if position is not None]

    async def list_open(self, strategy: str | None = None) -> list[OpenPosition]:
        statement = (
            select(OpenPositionRow)
            .where(OpenPositionRow.namespace == self._namespace)
            .order_by(OpenPositionRow.opened_at.asc())
        )
        if strategy:
            statement = statement.where(OpenPositionRow.strategy == strategy)
        try:
            async with self._db.session() as session:
                rows = (await session.execute(statement)).scalars().all()
        except Exception as exc:
            log.error("Could not list open positions: %s", exc)
            return []
        return [position for position in map(_as_position, rows) if position is not None]


@dataclass(slots=True)
class ClosedCycle:
    """One finished trade cycle, folded from its audit rows."""

    signal_uxid: str
    strategy: str
    symbol: str
    timeframe: str
    #: The entry's own timestamp, not the row's: when the trade was decided.
    opened_at: datetime | None
    closed_at: datetime | None
    #: The last action the cycle recorded — TP2, SL, FLAT, or a closing TP1.
    closed_by: str
    entry_price: float | None
    exit_price: float | None
    quantity: float | None
    #: Every action the cycle emitted, entry included.
    actions: int


def _as_closed_cycle(cycle_id: str, rows: list[SignalAudit]) -> ClosedCycle:
    """Fold one cycle's rows, oldest first, into the row a table prints."""
    entry = next((row for row in rows if row.action in {"LONG", "SHORT"}), rows[0])
    closing = rows[-1]
    return ClosedCycle(
        signal_uxid=cycle_id,
        strategy=entry.strategy,
        symbol=entry.symbol,
        timeframe=entry.timeframe,
        opened_at=entry.signal_time,
        closed_at=closing.signal_time or closing.created_at,
        closed_by=closing.action,
        entry_price=entry.price,
        exit_price=closing.price,
        quantity=entry.quantity,
        actions=len(rows),
    )


def _as_position(row: OpenPositionRow | None) -> OpenPosition | None:
    """Rebuild the model from ``state``, which is the authoritative copy.

    The columns beside it are a queryable projection of the same record, so
    reading them back instead would only invite the two disagreeing.
    """
    if row is None:
        return None
    try:
        position = OpenPosition.model_validate(row.state)
    except ValueError:
        log.error("Unreadable open_positions.state for %s %s", row.strategy, row.symbol)
        return None

    if position.state_namespace != row.namespace:
        raise ValueError("Stored position has missing or foreign state provenance")
    return position


@dataclass(slots=True)
class TelegramCycleRecord:
    """One cycle's Telegram state, in memory: its events and its messages.

    The in-memory form of a :class:`TelegramCycleMessage` row, the way
    :class:`~qte_shared.models.OpenPosition` is for ``open_positions``. The
    notifier appends an event, re-renders the body from the whole list and
    stores the message ids it got back, then hands the record here to be
    written in one upsert.
    """

    strategy: str
    symbol: str
    signal_uxid: str
    timeframe: str = ""
    status: str = "RUNNING"
    events: list[dict] = field(default_factory=list)
    #: ``"<audience>:<chat id>"`` → Telegram ``message_id``.
    messages: dict[str, str] = field(default_factory=dict)


class TelegramCycleRepository:
    """The message each chat holds for a trade cycle, and the cycle's events.

    Swallows its failures like every other repository on this path: the signal
    is already with the broker by the time a notification is rendered, and a
    Telegram bookkeeping error must never stop a runner that is holding real
    positions. A failed write costs the next update its history, not the trade.
    """

    def __init__(self, database: Database | None = None) -> None:
        self._db = database or get_database()
        self._namespace = settings.state_scope.namespace

    async def load(
        self, strategy: str, symbol: str, signal_uxid: str
    ) -> TelegramCycleRecord | None:
        statement = select(TelegramCycleMessage).where(
            TelegramCycleMessage.namespace == self._namespace,
            TelegramCycleMessage.strategy == strategy,
            TelegramCycleMessage.symbol == symbol,
            TelegramCycleMessage.signal_uxid == signal_uxid,
        )
        try:
            async with self._db.session() as session:
                row = (await session.execute(statement)).scalar_one_or_none()
        except Exception as exc:
            log.error("Could not read the Telegram cycle %s %s: %s", strategy, signal_uxid, exc)
            return None
        if row is None:
            return None
        return TelegramCycleRecord(
            strategy=row.strategy,
            symbol=row.symbol,
            signal_uxid=row.signal_uxid,
            timeframe=row.timeframe or "",
            status=row.status,
            events=[event for event in (row.events or []) if isinstance(event, dict)],
            messages={str(key): str(value) for key, value in (row.messages or {}).items() if value},
        )

    async def save(self, record: TelegramCycleRecord) -> bool:
        values = {
            "timeframe": record.timeframe,
            "status": record.status,
            "events": record.events,
            "messages": record.messages,
        }
        statement = (
            insert(TelegramCycleMessage)
            .values(
                namespace=self._namespace,
                strategy=record.strategy,
                symbol=record.symbol,
                signal_uxid=record.signal_uxid,
                **values,
            )
            .on_conflict_do_update(
                constraint="uq_telegram_cycle_messages_cycle",
                set_={**values, "updated_at": func.now()},
            )
        )
        try:
            async with self._db.session() as session:
                await session.execute(statement)
            return True
        except Exception as exc:
            log.error(
                "Could not persist the Telegram cycle %s %s: %s",
                record.strategy,
                record.signal_uxid,
                exc,
            )
            return False
