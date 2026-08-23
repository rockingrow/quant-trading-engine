"""Reads and writes against the backtest tables."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any

from qte_shared.db.session import Database, get_database
from qte_shared.logging_setup import get_logger
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from qte_backtest.db.models import BacktestRun, BacktestTrade

log = get_logger(__name__)


class BacktestRepository:
    """Persists replays so runs can be compared later without re-running them."""

    def __init__(self, database: Database | None = None) -> None:
        self._db = database or get_database()

    async def record_backtest(
        self,
        *,
        strategy: str,
        symbol: str,
        timeframe: str,
        period_start: datetime | None,
        period_end: datetime | None,
        params: dict[str, Any],
        metrics: dict[str, Any],
        trades: Sequence[dict[str, Any]] = (),
    ) -> str | None:
        run = BacktestRun(
            strategy=strategy,
            symbol=symbol,
            timeframe=timeframe,
            period_start=period_start,
            period_end=period_end,
            params=params,
            metrics=metrics,
        )
        run.trades = [BacktestTrade(**trade) for trade in trades]
        try:
            async with self._db.session() as session:
                session.add(run)
                await session.flush()
                return str(run.id)
        except Exception as exc:
            log.error("Backtest audit write failed: %s", exc)
            return None

    async def list_backtests(
        self, limit: int = 50, *, with_trades: bool = False
    ) -> Sequence[BacktestRun]:
        """Recent runs, newest first.

        Trades are left unloaded unless asked for: a listing of 50 runs can
        carry thousands of trade rows and most callers want only the headline
        metrics. Since the session closes before this returns, an unloaded
        ``run.trades`` can never be lazy-loaded afterwards — the relationship is
        configured to say that plainly rather than fail from inside the ORM.
        """
        statement = select(BacktestRun).order_by(BacktestRun.created_at.desc()).limit(limit)
        if with_trades:
            # selectinload, not joinedload: one extra query rather than a join
            # that repeats every run row once per trade it owns.
            statement = statement.options(selectinload(BacktestRun.trades))
        async with self._db.session() as session:
            return (await session.execute(statement)).scalars().all()
