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
    #: Subscription label. A Redis ownership claim enforces one active runner;
    #: queue distribution alone cannot share its candle or position state.
    queue_group: str = "qte-runners"
    #: Subscribe to ticks. Only worth it when a strategy overrides ``on_tick``;
    #: the runner turns it on automatically when one does.
    subscribe_ticks: bool = False
    #: Seconds between retries of broker deliveries whose acknowledgement
    #: timed out. The durable row keeps every attempt on the same delivery ID.
    delivery_retry_interval: float = Field(default=5.0, gt=0)
    #: Retry ambiguous live sends only inside a verified broker deduplication
    #: horizon. Zero requires operator reconciliation; never assume that an
    #: HTTP Idempotency-Key or a NATS message id is retained indefinitely.
    delivery_retry_max_age: float = Field(default=0.0, ge=0)
    #: Reconcile Redis history even if the last NATS close was lost entirely.
    history_sync_interval: float = Field(default=5.0, gt=0)
    #: Level for the ``numba`` logger tree, independent of QTE_LOG_LEVEL. JIT
    #: compilation of pandas-ta indicators logs one DEBUG line per SSA/byteflow
    #: step, which drowns the runner's own output when QTE_LOG_LEVEL=DEBUG.
    numba_log_level: str = "WARNING"
    #: Seconds after a bar closed within which the runner still decides on a
    #: close it missed — one published while it was down or still starting. An
    #: older missed bar joins the window as history only: an entry decided
    #: minutes late would go out at a price the backtest never traded.
    catch_up_max_age: float = Field(default=120.0, ge=0)
    #: Seconds between sweeps that flatten an open position whose strategy's
    #: declared weekend window has opened, with no candle close to trigger it.
    #: Bar-driven flattening is the path the backtest also takes, but it leaves
    #: a feed that stalls at 16:50 on a Friday holding the position all weekend.
    #: Zero disables the sweep, leaving only the bar-driven path.
    weekend_flat_sweep_interval: float = Field(default=60.0, ge=0)


runner_settings = RunnerSettings()
