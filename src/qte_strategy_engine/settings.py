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
    #: What to do on start about an open position the outage left behind. One
    #: pair holds one cycle at a time, so a stale row locks it: every entry the
    #: strategy proposes is refused while the position it is locked on was sized
    #: against a bracket the market left behind hours ago.
    #:
    #:   off     leave the table alone.
    #:   warn    report what would be closed, send nothing — look before acting.
    #:   close   send R_SL for it, which ends the cycle and frees the pair.
    flush_stale_positions: Literal["off", "warn", "close"] = "close"
    #: How old a position has to be, in seconds since its last transition,
    #: before the flush above counts it as stale. The restart this protects
    #: against is the long one; a deploy or a config change comes back inside
    #: this window and keeps its positions, which is what the Redis/Postgres
    #: recovery path exists for. Zero makes every open position stale, which is
    #: the unconditional flush.
    stale_position_max_age: float = Field(default=3600.0, ge=0)


runner_settings = RunnerSettings()
