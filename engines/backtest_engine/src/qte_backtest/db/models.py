"""The backtest tables — owned by the backtest engine, which is the only writer."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from qte_shared.db.base import Base, new_uuid
from sqlalchemy import DateTime, Float, ForeignKey, Index, Integer, String, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship


class BacktestRun(Base):
    """Header row for one replay: what was run, over what, and how it scored.

    ``metrics`` carries the report's metrics block plus its diagnostic findings,
    so a run's problems are queryable rather than only readable in the file.
    """

    __tablename__ = "backtest_runs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=new_uuid)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    strategy: Mapped[str] = mapped_column(String(128), nullable=False)
    symbol: Mapped[str] = mapped_column(String(64), nullable=False)
    timeframe: Mapped[str] = mapped_column(String(16), nullable=False)
    period_start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    period_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    params: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    metrics: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)

    # lazy="raise_on_sql" bans the implicit lazy load. It does not change the
    # error a *detached* run gives — SQLAlchemy raises DetachedInstanceError
    # before the strategy is consulted, and that message already names the
    # attribute. What it catches is the other case: a run still attached to a
    # session, where an unguarded `.trades` would quietly emit one SELECT per
    # run. Either way the fix is the same — ask for them up front with
    # list_backtests(with_trades=True).
    trades: Mapped[list[BacktestTrade]] = relationship(
        back_populates="run", cascade="all, delete-orphan", lazy="raise_on_sql"
    )


class BacktestTrade(Base):
    """One simulated round trip, already net of spread and commission."""

    __tablename__ = "backtest_trades"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("backtest_runs.id", ondelete="CASCADE"), nullable=False
    )
    signal_uxid: Mapped[str | None] = mapped_column(String(32))
    symbol: Mapped[str] = mapped_column(String(64), nullable=False)
    direction: Mapped[str] = mapped_column(String(8), nullable=False)
    opened_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    entry_price: Mapped[float] = mapped_column(Float, nullable=False)
    exit_price: Mapped[float | None] = mapped_column(Float)
    quantity: Mapped[float] = mapped_column(Float, nullable=False)
    sl: Mapped[float | None] = mapped_column(Float)
    tp1: Mapped[float | None] = mapped_column(Float)
    tp2: Mapped[float | None] = mapped_column(Float)
    exit_reason: Mapped[str | None] = mapped_column(String(16))
    gross_pnl: Mapped[float] = mapped_column(Float, default=0.0)
    fees: Mapped[float] = mapped_column(Float, default=0.0)
    net_pnl: Mapped[float] = mapped_column(Float, default=0.0)

    run: Mapped[BacktestRun] = relationship(back_populates="trades")

    __table_args__ = (Index("ix_backtest_trades_run", "run_id"),)
