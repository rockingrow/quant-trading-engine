"""High-level backtest orchestration used by both the CLI and the API."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from qte_shared.config import settings
from qte_shared.db import AuditRepository
from qte_shared.logging_setup import get_logger
from qte_shared.plugin_loader import StrategyLoader
from qte_shared.signal_factory import BracketPolicy

from qte_backtest.data_store import ParquetStore
from qte_backtest.execution import CostModel
from qte_backtest.replay import BacktestEngine, BacktestResult

log = get_logger(__name__)


@dataclass(slots=True)
class BacktestRequest:
    strategy: str
    symbol: str
    timeframe: str = "M15"
    start: datetime | None = None
    end: datetime | None = None
    params: dict[str, Any] = field(default_factory=dict)
    spread: float = 0.0
    slippage: float = 0.0
    commission_per_unit: float = 0.0
    contract_size: float = 1.0
    quantity: float = 1.0
    starting_equity: float = 10_000.0
    persist: bool = False


async def run_backtest(
    request: BacktestRequest,
    *,
    strategies_dir: Path | None = None,
    parquet_dir: Path | None = None,
) -> BacktestResult:
    """Load the strategy and its history, replay, and optionally audit the run."""
    loader = StrategyLoader(strategies_dir or settings.engine.strategies_dir)
    strategy = loader.load_one(request.strategy, request.params)

    store = ParquetStore(parquet_dir)
    frame = store.load(request.symbol, request.timeframe, request.start, request.end)
    log.info(
        "Replaying %s on %s %s — %d bars",
        strategy.name,
        request.symbol,
        request.timeframe,
        len(frame),
    )

    engine = BacktestEngine(
        strategy,
        symbol=request.symbol,
        timeframe=request.timeframe,
        costs=CostModel(
            spread=request.spread,
            slippage=request.slippage,
            commission_per_unit=request.commission_per_unit,
            contract_size=request.contract_size,
        ),
        starting_equity=request.starting_equity,
        bracket=BracketPolicy(),
        default_quantity=request.quantity,
    )
    result = engine.run(frame)

    if request.persist and settings.postgres.enabled:
        run_id = await AuditRepository().record_backtest(
            strategy=result.strategy,
            symbol=result.symbol,
            timeframe=result.timeframe,
            period_start=result.metrics.period_start,
            period_end=result.metrics.period_end,
            params=result.params,
            metrics=result.metrics.to_dict(),
            trades=result.trades_as_rows(),
        )
        log.info("Backtest persisted run_id=%s", run_id)

    return result
