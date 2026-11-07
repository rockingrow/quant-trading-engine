"""Strategy-runner configuration (prefix ``QTE_RUNNER__``).

Telegram lives in :class:`qte_shared.config.TelegramSettings` (``QTE_TELEGRAM__``)
instead: ingestion announces itself and forwards its errors through the same
chats, and a setting two services read belongs in ``qte_shared``."""

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
    #: Let a restart of the *same container* take back the claim its crashed
    #: predecessor left in Redis. Only the container's PID 1 qualifies; a
    #: recreated container, another host or a host process is still refused.
    #: See :mod:`qte_strategy_engine.instance`.
    reclaim_own_claim: bool = True
    #: Where that container keeps its identity. It must stay on the container's
    #: own writable layer — never on a mounted volume another container shares.
    instance_file: str = "/tmp/qte-runner.instance"
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
    #:
    #: Off by default. Age is not evidence that a position is wrong: a strategy
    #: that holds a cycle until its own exit condition is met can be right to
    #: carry one for days, and an outage nobody can attend to -- overnight, a
    #: weekend, a host that needs hands -- outlasts any window worth setting.
    #: Closing a cycle the strategy would still be managing costs more than
    #: leaving it, and what genuinely has nobody deciding on it is closed by
    #: ``close_orphan_positions`` below on a surer test than age.
    flush_stale_positions: Literal["off", "warn", "close"] = "off"
    #: How old a position has to be, in seconds since its last transition,
    #: before the flush above counts it as stale. Read only when that mode is
    #: not ``off``. The restart it protects against is the long one; a deploy or
    #: a config change comes back inside this window and keeps its positions,
    #: which is what the Redis/Postgres recovery path exists for. Zero makes
    #: every open position stale, which is the unconditional flush.
    stale_position_max_age: float = Field(default=3600.0, ge=0)
    #: What to do on start about an open cycle the mapping table no longer
    #: claims — a strategy dropped from a symbol's list, or a symbol carrying
    #: ``enabled = false``. Nothing decides on those: no slot holds them, no
    #: bar reaches them, and their size is still charged against the account on
    #: every later sizing. Unlike the age flush above this is not a guess —
    #: the operator's own table says the pair is retired.
    #:
    #:   off     leave the table alone, report it as today.
    #:   warn    report what would be closed, send nothing.
    #:   close   send FLAT for it, which ends the cycle.
    #:
    #: A pair the table still claims is never closed here, however long its
    #: slot has been missing: that is a strategy that failed to load or a
    #: repository that is not mounted, and a half-mounted
    #: ``__strategies__/`` must not flatten the book.
    close_orphan_positions: Literal["off", "warn", "close"] = "close"


runner_settings = RunnerSettings()
