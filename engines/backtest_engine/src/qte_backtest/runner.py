"""High-level backtest orchestration used by both the CLI and the API."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from qte_shared.config import settings
from qte_shared.logging_setup import get_logger
from qte_shared.plugin_loader import StrategyLoader
from qte_shared.routing import SymbolRouting
from qte_shared.signal_factory import BracketPolicy
from qte_shared.sizing import PositionSizer

from qte_backtest.data_store import ParquetStore
from qte_backtest.db import BacktestRepository
from qte_backtest.execution import CostModel
from qte_backtest.replay import BacktestEngine
from qte_backtest.report import BacktestReport, build_report

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
    #: Defaulted from ``QTE_ACCOUNT__*`` so a run with no flags is priced and
    #: capitalised the way the live account is. Pass a value to override one
    #: without disturbing the rest.
    commission_per_unit: float = field(default_factory=lambda: settings.account.commission_per_unit)
    contract_size: float = field(default_factory=lambda: settings.account.contract_size)
    #: Fallback size for an entry the risk sizer could not size (no stop).
    quantity: float = 1.0
    starting_equity: float = field(default_factory=lambda: settings.account.capital)
    #: Percent of ``starting_equity`` risked per entry. ``None`` takes it from
    #: the routing table's entry for this pair, then from
    #: ``QTE_ACCOUNT__RISK_PERCENT``.
    risk_percent: float | None = None
    persist: bool = False
    #: Where to write the machine-readable report. ``None`` skips writing it;
    #: the report object is built either way, because the diagnostics are worth
    #: having in the return value even when nothing lands on disk.
    report_dir: Path | None = None
    report_formats: tuple[str, ...] = ("json", "md")
    report_include_signals: bool = True


async def run_backtest(
    request: BacktestRequest,
    *,
    strategies_dir: Path | None = None,
    parquet_dir: Path | None = None,
) -> BacktestReport:
    """Load the strategy and its history, replay it, and diagnose the outcome.

    Returns the :class:`~qte_backtest.report.BacktestReport` rather than the raw
    result: the diagnostics are the part a caller acts on, and building them is
    cheap enough that making it optional would only invite skipping it.
    """
    loader = StrategyLoader(strategies_dir or settings.engine.strategies_dir)
    # The routing table is what pairs a strategy with a symbol *and* states the
    # risk it runs at there. Reading it here is what makes a backtest size its
    # trades the way the runner will — without it, `--param risk_percent=…` on
    # the command line would be the only way to reproduce production, and
    # forgetting it would silently measure a different book.
    params = {**_routed_params(request), **request.params}
    strategy = loader.load_one(request.strategy, params)

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
        sizer=PositionSizer.from_settings(params, risk_percent=request.risk_percent).replace(
            capital=request.starting_equity, contract_size=request.contract_size
        ),
    )
    result = engine.run(frame)
    report = build_report(result)

    critical = report.severity_counts()["critical"]
    if critical:
        log.warning(
            "%s finished with %d critical finding(s) — treat its metrics as unproven: %s",
            result.strategy,
            critical,
            ", ".join(f.code for f in report.findings if f.severity == "critical"),
        )

    if request.report_dir is not None:
        report.write(
            request.report_dir,
            formats=request.report_formats,
            include_signals=request.report_include_signals,
        )

    if request.persist and settings.postgres.enabled:
        run_id = await BacktestRepository().record_backtest(
            strategy=result.strategy,
            symbol=result.symbol,
            timeframe=result.timeframe,
            period_start=result.metrics.period_start,
            period_end=result.metrics.period_end,
            params=result.params,
            metrics={
                **result.metrics.to_dict(),
                "diagnostics": [finding.to_dict() for finding in report.findings],
            },
            trades=result.trades_as_rows(),
        )
        log.info("Backtest persisted run_id=%s", run_id)

    return report


def _routed_params(request: BacktestRequest) -> dict[str, Any]:
    """This pair's overrides from ``config/strategies_mapping.toml``, if any.

    A missing table is normal — a fresh clone has none — and an unrouted pair
    is normal too: backtesting a symbol before deciding to trade it is the
    usual order of events. Both mean "no overrides", not an error.
    """
    routing = SymbolRouting.load(settings.engine.routing_file)
    if not routing:
        return {}
    routed = routing.params_for(request.symbol, request.strategy)
    if routed:
        log.info(
            "Applying %s overrides from %s for %s/%s",
            ", ".join(sorted(routed)),
            routing.source,
            request.symbol,
            request.strategy,
        )
    return routed
