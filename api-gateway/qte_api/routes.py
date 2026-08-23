"""Control-plane endpoints.

Everything here is off the trading path. The API reads the audit trail, lists
what is loaded, kicks off a backtest, and flips shadow mode — it never sits
between a strategy and the broker.
"""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status
from qte_backtest.data_store import ParquetStore
from qte_backtest.runner import BacktestRequest, run_backtest
from qte_shared.bus import Subjects
from qte_shared.cache import RedisState
from qte_shared.config import settings
from qte_shared.db import AuditRepository, get_database
from qte_shared.logging_setup import get_logger
from qte_shared.plugin_loader import StrategyLoader

from qte_api.deps import get_bus, require_api_key
from qte_api.schemas import (
    BacktestRunRequest,
    BacktestRunResponse,
    BacktestRunSummary,
    HealthResponse,
    HistoryEntry,
    ShadowModeRequest,
    ShadowModeResponse,
    SignalRow,
    StrategyInfo,
)

log = get_logger(__name__)
router = APIRouter()
subjects = Subjects()


def _repository() -> AuditRepository:
    return AuditRepository()


# ── Health ────────────────────────────────────────────────────────────


@router.get("/health", response_model=HealthResponse, tags=["ops"])
async def health() -> HealthResponse:
    """Liveness plus the state of every dependency, for a compose healthcheck.

    Reports ``degraded`` rather than failing when a dependency is down: the
    control plane being reachable *and* honest about a broken Postgres is more
    useful to an operator than a 503 that says nothing about which piece broke.
    """
    state = RedisState()
    redis_ok = False
    try:
        await state.connect()
        redis_ok = await state.ping()
    except Exception:
        redis_ok = False
    finally:
        await state.close()

    postgres_ok = await get_database().ping() if settings.postgres.enabled else True
    from qte_api import deps

    nats_ok = deps.bus is not None and deps.bus.is_connected
    dependencies = {"redis": redis_ok, "postgres": postgres_ok, "nats": nats_ok}

    shadow_mode = settings.broker.shadow_mode
    if redis_ok:
        try:
            await state.connect()
            shadow_mode = bool(await state.get_flag("shadow_mode", shadow_mode))
        except Exception:
            pass
        finally:
            await state.close()

    return HealthResponse(
        status="ok" if all(dependencies.values()) else "degraded",
        env=settings.env,
        shadow_mode=shadow_mode,
        transport=settings.broker.transport,
        dependencies=dependencies,
    )


# ── Strategies ────────────────────────────────────────────────────────


@router.get("/strategies", response_model=list[StrategyInfo], tags=["strategies"])
async def list_strategies() -> list[StrategyInfo]:
    """Every plugin discovered in ``user_strategies/``.

    Discovery runs per request rather than at boot so a strategy dropped into
    the mounted volume shows up without restarting the API.
    """
    loader = StrategyLoader(settings.engine.strategies_dir)
    infos = []
    for entry in loader.discover():
        described = entry.instantiate().describe()
        described["source"] = str(entry.source)
        infos.append(StrategyInfo(**described))
    return infos


# ── Signals ───────────────────────────────────────────────────────────


@router.get("/signals", response_model=list[SignalRow], tags=["signals"])
async def list_signals(
    strategy: str | None = None,
    symbol: str | None = None,
    since: datetime | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
) -> list[SignalRow]:
    """Newest-first audit trail of everything the runners emitted."""
    rows = await _repository().list_signals(
        strategy=strategy, symbol=symbol, since=since, limit=limit
    )
    return [_as_signal_row(row) for row in rows]


@router.get("/signals/cycle/{signal_uxid}", response_model=list[SignalRow], tags=["signals"])
async def get_cycle(signal_uxid: str) -> list[SignalRow]:
    """One whole trade cycle, oldest first — the reconciliation view.

    Entry and every TP/SL/FLAT that followed it, which is exactly the set the
    broker groups into one Telegram broadcast.
    """
    rows = await _repository().get_cycle(signal_uxid.upper())
    if not rows:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No such cycle")
    return [_as_signal_row(row) for row in rows]


# ── Shadow mode ───────────────────────────────────────────────────────


@router.post(
    "/admin/shadow-mode",
    response_model=ShadowModeResponse,
    tags=["admin"],
    dependencies=[Depends(require_api_key)],
)
async def set_shadow_mode(request: ShadowModeRequest) -> ShadowModeResponse:
    """Pause or resume delivery to the broker, across every running runner.

    The flag is written to Redis first and broadcast second, so a runner that
    starts after the broadcast still comes up in the mode you last chose.
    """
    state = RedisState()
    await state.connect()
    try:
        await state.set_flag("shadow_mode", request.enabled)
    finally:
        await state.close()

    broadcast = False
    try:
        await get_bus().publish(
            subjects.engine_control(),
            {"action": "set_shadow_mode", "enabled": request.enabled},
        )
        broadcast = True
    except HTTPException:
        log.error("Shadow mode stored but NOT broadcast — NATS is down")

    return ShadowModeResponse(enabled=request.enabled, broadcast=broadcast)


# ── Backtests ─────────────────────────────────────────────────────────


@router.get("/backtest/history", response_model=list[HistoryEntry], tags=["backtest"])
async def list_history() -> list[HistoryEntry]:
    """Parquet history available to replay."""
    store = ParquetStore()
    return [
        HistoryEntry(
            symbol=symbol,
            timeframe=timeframe,
            size_mb=round(store.path_for(symbol, timeframe).stat().st_size / 1_048_576, 3),
        )
        for symbol, timeframe in store.available()
    ]


@router.get("/backtest/runs", response_model=list[BacktestRunSummary], tags=["backtest"])
async def list_backtest_runs(
    limit: int = Query(default=50, ge=1, le=200),
) -> list[BacktestRunSummary]:
    runs = await _repository().list_backtests(limit=limit)
    return [
        BacktestRunSummary(
            id=str(run.id),
            created_at=run.created_at,
            strategy=run.strategy,
            symbol=run.symbol,
            timeframe=run.timeframe,
            period_start=run.period_start,
            period_end=run.period_end,
            metrics=run.metrics,
        )
        for run in runs
    ]


@router.post(
    "/backtest/run",
    response_model=BacktestRunResponse,
    tags=["backtest"],
    dependencies=[Depends(require_api_key)],
)
async def trigger_backtest(request: BacktestRunRequest) -> BacktestRunResponse:
    """Replay a strategy synchronously and return the report.

    Synchronous on purpose: a few years of M15 replays in seconds, and a job
    queue for something that fast is machinery nobody has to operate. If you
    move to M1 over a decade, that trade-off changes.
    """
    try:
        result = await run_backtest(
            BacktestRequest(
                strategy=request.strategy,
                symbol=request.symbol,
                timeframe=request.timeframe,
                start=request.start,
                end=request.end,
                params=request.params,
                spread=request.spread,
                slippage=request.slippage,
                commission_per_unit=request.commission_per_unit,
                contract_size=request.contract_size,
                quantity=request.quantity,
                starting_equity=request.starting_equity,
                persist=request.persist,
            )
        )
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    return BacktestRunResponse(
        strategy=result.strategy,
        symbol=result.symbol,
        timeframe=result.timeframe,
        metrics=result.metrics.to_dict(),
        rejected_entries=result.rejected,
        trades=[_serialise_trade(trade) for trade in result.trades_as_rows()],
        report=result.report(),
    )


# ── Helpers ───────────────────────────────────────────────────────────


def _as_signal_row(row) -> SignalRow:
    return SignalRow(
        id=str(row.id),
        created_at=row.created_at,
        signal_time=row.signal_time,
        signal_uxid=row.signal_uxid,
        strategy=row.strategy,
        symbol=row.symbol,
        timeframe=row.timeframe,
        action=row.action,
        price=row.price,
        quantity=row.quantity,
        sl=row.sl,
        tp1=row.tp1,
        tp2=row.tp2,
        transport=row.transport,
        delivery_status=row.delivery_status,
        delivery_error=row.delivery_error,
        shadow=row.shadow,
        payload=row.payload,
    )


def _serialise_trade(trade: dict) -> dict:
    return {
        key: (value.isoformat() if isinstance(value, datetime) else value)
        for key, value in trade.items()
    }
