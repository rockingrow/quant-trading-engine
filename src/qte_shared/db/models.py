"""Tables owned by no single engine.

Only lifecycle breadcrumbs live here — every engine writes them, none of them
owns the table. Tables that belong to one engine live with that engine:
``qte_strategy_engine.db`` owns ``signals``, ``qte_backtest.db`` owns the
backtest tables.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, Index, Integer, String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from qte_shared.db.base import Base


class EngineEvent(Base):
    """Service lifecycle breadcrumbs — starts, stops, feed drops, mode changes.

    Written by ingestion, the strategy runner and ``qte-control`` alike, which
    is why it is the one table that sits in shared rather than with an engine.
    """

    __tablename__ = "engine_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    service: Mapped[str] = mapped_column(String(64), nullable=False)
    level: Mapped[str] = mapped_column(String(16), default="INFO")
    event: Mapped[str] = mapped_column(String(128), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)

    __table_args__ = (Index("ix_engine_events_service_created", "service", "created_at"),)
