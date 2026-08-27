"""Reads and writes against the runner's ``signals`` and ``open_positions`` tables."""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from datetime import datetime

from qte_shared.db.session import Database, get_database
from qte_shared.logging_setup import get_logger
from qte_shared.models import BrokerSignal, OpenPosition
from sqlalchemy import delete, select, update
from sqlalchemy.dialects.postgresql import insert

from qte_strategy_engine.db.models import OpenPositionRow, SignalAudit

log = get_logger(__name__)

OUTBOX_CONTEXT_KEY = "__qte_outbox__"


class SignalRepository:
    """The audit trail of everything the runner emitted."""

    def __init__(self, database: Database | None = None) -> None:
        self._db = database or get_database()

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
            row_id=delivery_id,
            transport=transport,
            delivery_status="pending",
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
                await session.execute(
                    update(SignalAudit)
                    .where(SignalAudit.id == row_id)
                    .values(delivery_status=status, delivery_error=error)
                )
            return True
        except Exception as exc:
            log.error("Could not mark signal delivery %s as %s: %s", delivery_id, status, exc)
            return False

    async def pending_deliveries(
        self,
        limit: int = 100,
        *,
        statuses: tuple[str, ...] = ("pending", "unknown"),
    ) -> Sequence[SignalAudit]:
        """Outbox rows whose final broker outcome is not known yet."""
        statement = (
            select(SignalAudit)
            .where(SignalAudit.delivery_status.in_(statuses))
            .order_by(SignalAudit.created_at.asc())
            .limit(limit)
        )
        async with self._db.session() as session:
            return (await session.execute(statement)).scalars().all()

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
        statement = select(SignalAudit).order_by(SignalAudit.created_at.desc()).limit(limit)
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
            .where(SignalAudit.signal_uxid == signal_uxid)
            .order_by(SignalAudit.created_at.asc())
        )
        async with self._db.session() as session:
            return (await session.execute(statement)).scalars().all()


def _signal_row(
    signal: BrokerSignal,
    *,
    transport: str,
    delivery_status: str,
    shadow: bool,
    row_id: uuid.UUID | None = None,
    delivery_error: str | None = None,
    recovery_context: dict | None = None,
) -> SignalAudit:
    audit_inputs = dict(signal.inputs)
    if recovery_context:
        audit_inputs[OUTBOX_CONTEXT_KEY] = recovery_context
    return SignalAudit(
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
        payload=signal.to_envelope(),
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

    async def upsert(self, position: OpenPosition) -> bool:
        """Write the pair's current cycle, replacing whatever was there."""
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
            .values(strategy=position.strategy, symbol=position.symbol, **values)
            .on_conflict_do_update(constraint="uq_open_positions_pair", set_=values)
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

    async def clear(self, strategy: str, symbol: str) -> bool:
        """Drop the pair's row — the cycle is over."""
        try:
            async with self._db.session() as session:
                await session.execute(
                    delete(OpenPositionRow).where(
                        OpenPositionRow.strategy == strategy,
                        OpenPositionRow.symbol == symbol.upper(),
                    )
                )
            return True
        except Exception as exc:
            log.error("Could not clear open position %s %s: %s", strategy, symbol, exc)
            return False

    async def get(self, strategy: str, symbol: str) -> OpenPosition | None:
        statement = select(OpenPositionRow).where(
            OpenPositionRow.strategy == strategy, OpenPositionRow.symbol == symbol.upper()
        )
        try:
            async with self._db.session() as session:
                row = (await session.execute(statement)).scalar_one_or_none()
        except Exception as exc:
            log.error("Could not read open position %s %s: %s", strategy, symbol, exc)
            return None
        return _as_position(row)

    async def list_open(self, strategy: str | None = None) -> list[OpenPosition]:
        statement = select(OpenPositionRow).order_by(OpenPositionRow.opened_at.asc())
        if strategy:
            statement = statement.where(OpenPositionRow.strategy == strategy)
        try:
            async with self._db.session() as session:
                rows = (await session.execute(statement)).scalars().all()
        except Exception as exc:
            log.error("Could not list open positions: %s", exc)
            return []
        return [position for position in map(_as_position, rows) if position is not None]


def _as_position(row: OpenPositionRow | None) -> OpenPosition | None:
    """Rebuild the model from ``state``, which is the authoritative copy.

    The columns beside it are a queryable projection of the same record, so
    reading them back instead would only invite the two disagreeing.
    """
    if row is None:
        return None
    try:
        return OpenPosition.model_validate(row.state)
    except ValueError:
        log.error("Unreadable open_positions.state for %s %s", row.strategy, row.symbol)
        return None
