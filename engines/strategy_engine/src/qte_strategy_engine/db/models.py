"""The ``signals`` table — owned by the strategy runner, which is the only writer.

``payload`` holds the exact broker envelope that was sent (or would have been,
in shadow mode), so reconciliation against ``algo-trading-broker``'s own
``signals`` table compares bytes rather than a reconstruction.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from qte_shared.db.base import Base, new_uuid
from sqlalchemy import Boolean, DateTime, Float, Index, String, Text, func
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
    #: ``sent`` | ``shadow`` | ``failed`` — what actually happened to it.
    delivery_status: Mapped[str] = mapped_column(String(16), default="shadow")
    delivery_error: Mapped[str | None] = mapped_column(Text)
    shadow: Mapped[bool] = mapped_column(Boolean, default=True)

    __table_args__ = (
        Index("ix_signals_strategy_created", "strategy", "created_at"),
        Index("ix_signals_uxid", "signal_uxid"),
        Index("ix_signals_symbol_created", "symbol", "created_at"),
    )
