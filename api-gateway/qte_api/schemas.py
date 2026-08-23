"""Request/response models for the control-plane API."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: str
    env: str
    shadow_mode: bool
    transport: str
    dependencies: dict[str, bool]


class StrategyInfo(BaseModel):
    name: str
    cls: str = Field(alias="class")
    module: str
    symbols: list[str]
    timeframe: str
    warmup: int
    params: dict[str, Any]
    source: str

    model_config = {"populate_by_name": True}


class SignalRow(BaseModel):
    id: str
    created_at: datetime
    signal_time: datetime
    signal_uxid: str
    strategy: str
    symbol: str
    timeframe: str
    action: str
    price: float | None
    quantity: float | None
    sl: float | None
    tp1: float | None
    tp2: float | None
    transport: str
    delivery_status: str
    delivery_error: str | None
    shadow: bool
    payload: dict[str, Any]


class ShadowModeRequest(BaseModel):
    enabled: bool = Field(description="True pauses delivery; false lets signals reach the broker.")


class ShadowModeResponse(BaseModel):
    enabled: bool
    broadcast: bool = Field(description="Whether the change reached the runners over NATS.")


class BacktestRunRequest(BaseModel):
    strategy: str
    symbol: str
    timeframe: str = "M15"
    start: datetime | None = None
    end: datetime | None = None
    params: dict[str, Any] = Field(default_factory=dict)
    quantity: float = 1.0
    spread: float = 0.0
    slippage: float = 0.0
    commission_per_unit: float = 0.0
    contract_size: float = 1.0
    starting_equity: float = 10_000.0
    persist: bool = True


class BacktestRunResponse(BaseModel):
    strategy: str
    symbol: str
    timeframe: str
    metrics: dict[str, Any]
    rejected_entries: int
    trades: list[dict[str, Any]]
    report: str


class BacktestRunSummary(BaseModel):
    id: str
    created_at: datetime
    strategy: str
    symbol: str
    timeframe: str
    period_start: datetime | None
    period_end: datetime | None
    metrics: dict[str, Any]


class HistoryEntry(BaseModel):
    symbol: str
    timeframe: str
    size_mb: float
