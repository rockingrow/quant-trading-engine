"""The runner's own tables — it is the only writer of both.

``signals`` is the append-only audit trail: ``payload`` holds the exact broker
envelope that was sent (or would have been, in shadow mode), so reconciliation
against ``algo-trading-broker``'s own ``signals`` table compares bytes rather
than a reconstruction.

``open_positions`` is the opposite kind of table — one mutable row per
(strategy, symbol), holding the trade cycle currently live on that pair. Redis
is where the runner reads it on the hot path; this is the copy that survives a
flushed cache, because the failure it guards against is expensive and silent:
a runner that forgets an open cycle mints a fresh one on the next entry and
leaves the broker holding a position nobody will ever close.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from qte_shared.db.base import Base, new_uuid
from sqlalchemy import Boolean, DateTime, Float, Index, String, Text, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column


class SignalAudit(Base):
    """One row per signal the runner produced — delivered, shadowed, or failed."""

    __tablename__ = "signals"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=new_uuid)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    signal_uxid: Mapped[str] = mapped_column(String(32), nullable=False)
    strategy: Mapped[str] = mapped_column(String(128), nullable=False)
    symbol: Mapped[str] = mapped_column(String(64), nullable=False)
    timeframe: Mapped[str] = mapped_column(String(16), nullable=False)
    action: Mapped[str] = mapped_column(String(16), nullable=False)
    signal_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    price: Mapped[float | None] = mapped_column(Float)
    quantity: Mapped[float | None] = mapped_column(Float)
    sl: Mapped[float | None] = mapped_column(Float)
    tp1: Mapped[float | None] = mapped_column(Float)
    tp2: Mapped[float | None] = mapped_column(Float)

    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    indicators: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    inputs: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)

    transport: Mapped[str] = mapped_column(String(16), default="nats")
    #: ``pending`` | ``unknown`` | ``sent`` | ``shadow`` | ``failed``.
    delivery_status: Mapped[str] = mapped_column(String(16), default="shadow")
    delivery_error: Mapped[str | None] = mapped_column(Text)
    shadow: Mapped[bool] = mapped_column(Boolean, default=True)

    __table_args__ = (
        Index("ix_signals_strategy_created", "strategy", "created_at"),
        Index("ix_signals_uxid", "signal_uxid"),
        Index("ix_signals_symbol_created", "symbol", "created_at"),
    )


class OpenPositionRow(Base):
    """The trade cycle live on one (strategy, symbol) pair — at most one.

    Mirrors :class:`qte_shared.models.OpenPosition`. ``state`` carries the whole
    record so a field added there does not need a migration to be persisted;
    the columns beside it exist because operating a book means querying it
    ("what is open right now, and how big"), and digging that out of JSONB in
    an incident is the wrong time to discover you cannot.
    """

    __tablename__ = "open_positions"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=new_uuid)
    strategy: Mapped[str] = mapped_column(String(128), nullable=False)
    symbol: Mapped[str] = mapped_column(String(64), nullable=False)
    signal_uxid: Mapped[str] = mapped_column(String(32), nullable=False)
    action: Mapped[str] = mapped_column(String(16), nullable=False)

    opened_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    price: Mapped[float | None] = mapped_column(Float)
    #: Size the entry was sent with — the denominator of every partial.
    quantity: Mapped[float | None] = mapped_column(Float)
    #: Size still open. Reaching zero closes the cycle and deletes the row.
    remaining: Mapped[float | None] = mapped_column(Float)
    sl: Mapped[float | None] = mapped_column(Float)
    tp1: Mapped[float | None] = mapped_column(Float)
    tp2: Mapped[float | None] = mapped_column(Float)

    state: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)

    __table_args__ = (
        # One live cycle per pair, enforced by the database rather than by the
        # runner remembering to check — two runner replicas share this table.
        UniqueConstraint("strategy", "symbol", name="uq_open_positions_pair"),
        Index("ix_open_positions_uxid", "signal_uxid"),
    )
