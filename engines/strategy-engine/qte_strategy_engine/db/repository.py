"""Reads and writes against the ``signals`` table."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

from qte_shared.db.session import Database, get_database
from qte_shared.logging_setup import get_logger
from qte_shared.models import BrokerSignal
from sqlalchemy import select

from qte_strategy_engine.db.models import SignalAudit

log = get_logger(__name__)


class SignalRepository:
    """The audit trail of everything the runner emitted."""

    def __init__(self, database: Database | None = None) -> None:
        self._db = database or get_database()

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
        row = SignalAudit(
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
            inputs=signal.inputs,
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
