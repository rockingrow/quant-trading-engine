"""Process configuration, read once from the environment / ``.env``.

Every service imports :data:`settings`. Nested blocks use their own env
prefixes (``QTE_NATS__URL``, ``QTE_BROKER__TOKEN``, …) so a section can be
overridden wholesale in compose without touching the rest.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _find_repo_root() -> Path:
    """Walk up from this file until the workspace root is found.

    Counting parent directories (``parents[3]``) works right up until a package
    moves — and then it does not fail, it silently resolves ``.env``,
    ``__strategies__/`` and ``data/`` one level off and the engine looks for
    everything in the wrong place. So the root is *identified* rather than
    counted: it is the ancestor whose ``pyproject.toml`` declares the uv
    workspace.

    Falls back to the package's grandparent for an installed (non-editable)
    copy, where there is no workspace above it and these defaults are expected
    to be overridden by environment variables anyway.
    """
    here = Path(__file__).resolve()
    for candidate in here.parents:
        manifest = candidate / "pyproject.toml"
        if manifest.is_file() and "[tool.uv.workspace]" in manifest.read_text(encoding="utf-8"):
            return candidate
    return here.parents[2]


REPO_ROOT = _find_repo_root()


class NatsSettings(BaseSettings):
    """QTE's own internal event bus (candles, ticks, engine control)."""

    model_config = SettingsConfigDict(env_prefix="QTE_NATS__", extra="ignore")

    url: str = "nats://localhost:4222"
    token: str = ""
    subject_prefix: str = "QTE"
    connect_timeout: float = 5.0
    max_reconnect_attempts: int = -1
    reconnect_time_wait: float = 2.0


class BrokerSettings(BaseSettings):
    """How signals reach ``algo-trading-broker``.

    ``transport="nats"`` publishes the webhook envelope straight onto the
    broker's JetStream ``SIGNALS.<strategy>`` subject — the same buffer its own
    HTTP endpoint writes to, minus the HTTP hop. ``transport="http"`` POSTs to
    ``/secret/webhook`` instead, which is the path that also verifies ``token``.

    ``nats_url`` defaults to empty meaning "reuse the internal QTE NATS URL",
    which is the right answer when QTE and the broker share one NATS cluster.
    """

    model_config = SettingsConfigDict(env_prefix="QTE_BROKER__", extra="ignore")

    transport: Literal["nats", "http"] = "nats"
    nats_url: str = ""
    nats_token: str = ""
    http_url: str = "http://localhost:8080"
    token: str = ""
    stream: str = "SIGNALS"
    subject_prefix: str = "SIGNALS"
    publish_timeout: float = 5.0
    # Shadow mode is the safety catch from phase 6: the runner does everything
    # it would do live — build the signal, log it, audit it — but stops short of
    # handing it to the broker.
    shadow_mode: bool = True


class RedisSettings(BaseSettings):
    """Hot state: last tick, in-flight candles, per-strategy position state."""

    model_config = SettingsConfigDict(env_prefix="QTE_REDIS__", extra="ignore")

    url: str = "redis://localhost:6379/0"
    key_prefix: str = "qte"
    candle_history: int = 500
    ttl_seconds: int = 0


class PostgresSettings(BaseSettings):
    """Audit trail. Plain PostgreSQL, JSONB rows, never on the hot path."""

    model_config = SettingsConfigDict(env_prefix="QTE_POSTGRES__", extra="ignore")

    dsn: str = "postgresql+asyncpg://qte:qte@localhost:5432/qte_audit"
    pool_size: int = 5
    max_overflow: int = 5
    echo: bool = False
    enabled: bool = True


class MarketDataSettings(BaseSettings):
    """Which market data vendor the process uses.

    Only the *choice* lives here. A vendor's own endpoints and credentials sit
    with its provider (``QTE_TIINGO__*`` with
    :mod:`qte_shared.providers.tiingo`), so adding a second vendor never edits
    this file — see :mod:`qte_shared.interfaces.market_data`.
    """

    model_config = SettingsConfigDict(env_prefix="QTE_MARKET_DATA__", extra="ignore")

    provider: str = "tiingo"


class EngineSettings(BaseSettings):
    """What the running engine trades and how much history it keeps warm."""

    model_config = SettingsConfigDict(env_prefix="QTE_ENGINE__", extra="ignore")

    symbols: list[str] = Field(default_factory=lambda: ["XAUUSD", "BTCUSDT"])
    timeframes: list[str] = Field(default_factory=lambda: ["M1", "M15"])
    signal_timeframe: str = "M15"
    warmup_candles: int = 300
    strategies_dir: Path = REPO_ROOT / "__strategies__"
    #: Symbol → strategies table. Git-ignored, with a tracked template beside
    #: it; see :mod:`qte_shared.routing`. Absent means every strategy keeps the
    #: symbols it declares on itself.
    routing_file: Path = REPO_ROOT / "config" / "strategies_mapping.toml"
    parquet_dir: Path = REPO_ROOT / "data" / "parquet"
    reports_dir: Path = REPO_ROOT / "data" / "reports"


class Settings(BaseSettings):
    """Root settings object — import :data:`settings`, not this class."""

    model_config = SettingsConfigDict(
        env_file=(REPO_ROOT / ".env"),
        env_file_encoding="utf-8",
        env_prefix="QTE_",
        extra="ignore",
    )

    env: Literal["dev", "staging", "prod"] = "dev"
    log_level: str = "INFO"

    nats: NatsSettings = Field(default_factory=NatsSettings)
    broker: BrokerSettings = Field(default_factory=BrokerSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    postgres: PostgresSettings = Field(default_factory=PostgresSettings)
    market_data: MarketDataSettings = Field(default_factory=MarketDataSettings)
    engine: EngineSettings = Field(default_factory=EngineSettings)

    @property
    def broker_nats_url(self) -> str:
        """Where to publish signals — the broker's own NATS, or ours if shared."""
        return self.broker.nats_url or self.nats.url

    @property
    def broker_nats_token(self) -> str:
        return self.broker.nats_token or self.nats.token


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
