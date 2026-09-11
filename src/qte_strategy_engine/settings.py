"""Strategy-runner configuration (prefix ``QTE_RUNNER__``)."""

from __future__ import annotations

from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class RunnerSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="QTE_RUNNER__", extra="ignore")

    #: Whether the runner audits __strategies__/ before it loads them, and how
    #: much authority the verdict has. See :mod:`qte_strategy_engine.preflight`
    #: — "warn" is the default because it changes nothing about which
    #: strategies run, it only makes the loader's skipping legible.
    audit_on_start: Literal["off", "warn", "error", "strict"] = "warn"
    #: Default size attached to an entry whose strategy did not set one.
    default_quantity: float = 0.01
    #: NATS queue group. Two runner replicas in the same group split candles
    #: between them, so exactly one of them acts on each close.
    queue_group: str = "qte-runners"
    #: Subscribe to ticks. Only worth it when a strategy overrides ``on_tick``;
    #: the runner turns it on automatically when one does.
    subscribe_ticks: bool = False
    #: Seconds between retries of broker deliveries whose acknowledgement
    #: timed out. The durable row keeps every attempt on the same delivery ID.
    delivery_retry_interval: float = Field(default=5.0, gt=0)
    #: Level for the ``numba`` logger tree, independent of QTE_LOG_LEVEL. JIT
    #: compilation of pandas-ta indicators logs one DEBUG line per SSA/byteflow
    #: step, which drowns the runner's own output when QTE_LOG_LEVEL=DEBUG.
    numba_log_level: str = "WARNING"
    #: Seconds after a bar closed within which the runner still decides on a
    #: close it missed — one published while it was down or still starting. An
    #: older missed bar joins the window as history only: an entry decided
    #: minutes late would go out at a price the backtest never traded.
    catch_up_max_age: float = Field(default=120.0, ge=0)


runner_settings = RunnerSettings()
