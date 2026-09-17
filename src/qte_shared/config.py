"""Process configuration, read once from the environment / ``.env``.

Every service imports :data:`settings`. Nested blocks use their own env
prefixes (``QTE_NATS__URL``, ``QTE_BROKER__TOKEN``, …) so a section can be
overridden wholesale in compose without touching the rest.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from dotenv import load_dotenv
from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import URL

from qte_shared.market_data_plan import MarketDataPlan, SymbolFeed
from qte_shared.state_scope import ExecutionMode, StateScope
from qte_shared.symbols import build_specs
from qte_shared.timeframes import normalize_timeframe


def _find_repo_root() -> Path:
    """Walk up from this file until the workspace root is found.

    Counting parent directories (``parents[2]``) works right up until a package
    moves — and then it does not fail, it silently resolves ``.env``,
    ``__strategies__/`` and ``data/`` one level off and the engine looks for
    everything in the wrong place. So the root is *identified* rather than
    counted: it is the ancestor whose ``pyproject.toml`` carries the
    ``[tool.qte]`` marker table, which exists for no other purpose.

    Falls back to the package's grandparent for an installed (non-editable)
    copy, where there is no manifest above it and these defaults are expected
    to be overridden by environment variables anyway.
    """
    here = Path(__file__).resolve()
    for candidate in here.parents:
        manifest = candidate / "pyproject.toml"
        if manifest.is_file() and "[tool.qte]" in manifest.read_text(encoding="utf-8"):
            return candidate
    return here.parents[2]


REPO_ROOT = _find_repo_root()

# Load the workspace ``.env`` into ``os.environ`` once, at import. Every nested
# ``BaseSettings`` block reads from the process environment, so this is what
# makes ``QTE_NATS__URL=…`` in ``.env`` take effect for ``NatsSettings`` and its
# siblings — pydantic-settings only wires ``env_file`` into the class that
# declares it, not into ``Field(default_factory=NatsSettings)`` children.
# ``override=False`` keeps shell exports winning, so a targeted
# ``QTE_POSTGRES__DSN=… make db-upgrade`` still overrides the file.
load_dotenv(REPO_ROOT / ".env", override=False)


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
    #: Deployment safety override; persisted or broadcast controls cannot disable it.
    force_shadow_mode: bool = False


class AccountSettings(BaseSettings):
    """The account every driver sizes against, and what trading it costs.

    One capital figure for the whole engine rather than one per driver. A
    backtest that starts from a different balance than the runner sizes against
    is measuring a strategy nobody is going to trade, and the divergence is
    invisible in the report — both runs look internally consistent.

    ``risk_percent`` is only the *fallback*: what a pair risks is normally
    stated per (symbol, strategy) in ``config/strategies_mapping.toml``, and
    that value wins. This one covers the pair that declares none.

    ``commission_per_unit``, ``spread`` and ``slippage`` are backtest costs —
    live, the broker charges and fills its own — but they belong to the
    account rather than to a run, which is why they sit here and only default
    the CLI flags. ``spread``/``slippage`` are per-symbol in reality (0.30 on
    gold, ~2.0 on BTCUSDT); this default only covers a run that names none,
    it is not a substitute for setting the right value per pair.
    """

    model_config = SettingsConfigDict(env_prefix="QTE_ACCOUNT__", extra="ignore")

    #: Starting balance. Position size is a share of *this*, not of the equity
    #: as it moves — see :mod:`qte_shared.strategies.sizing`.
    capital: float = 1000.0
    #: Percent of :attr:`capital` put at risk on one entry when the mapping
    #: table names no ``risk_percent`` for the pair.
    risk_percent: float = 1.0
    #: Charged per unit on entry and on every partial exit, each side.
    commission_per_unit: float = 0.0
    #: Full bid/ask distance in price units; each side of a fill pays half.
    spread: float = 0.0
    #: Added on top of half the spread, on both entry and exit fills.
    slippage: float = 0.0
    #: Units per contract/lot. Scales both P&L and commission.
    contract_size: float = 1.0
    #: Hard ceiling on a sized entry. ``0`` means uncapped.
    max_quantity: float = 0.0
    #: Decimals a sized quantity is rounded to before it reaches the wire.
    quantity_precision: int = 4


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

    dsn: str = Field(default="postgresql+asyncpg://qte:qte@localhost:5432/qte_audit", repr=False)
    #: Compose supplies components when DSN is empty; URL.create escapes credentials.
    hostname: str = "localhost"
    port_number: int = Field(default=5432, ge=1, le=65535)
    username: str = "qte"
    password: str = Field(default="qte", repr=False)
    database: str = "qte_audit"
    pool_size: int = 5
    max_overflow: int = 5
    echo: bool = False
    enabled: bool = True

    @model_validator(mode="after")
    def _resolve_database_url(self) -> PostgresSettings:
        if not self.dsn:
            self.dsn = URL.create(
                "postgresql+asyncpg",
                username=self.username,
                password=self.password,
                host=self.hostname,
                port=self.port_number,
                database=self.database,
            ).render_as_string(hide_password=False)
        return self


class MarketStreamSettings(BaseSettings):
    """Feed configuration shared by ingestion and its consumers."""

    model_config = SettingsConfigDict(env_prefix="QTE_INGESTION__", extra="ignore")

    #: Markets for symbols absent from the plan; planned markets retain precedence.
    market_overrides: dict[str, str] = Field(default_factory=dict)
    #: Required when any runner strategy consumes ticks. Configure both services alike.
    publish_ticks: bool = False


class MarketDataSettings(BaseSettings):
    """Which market data vendor the process uses, and where its plan is.

    Only the *choice* lives here. A vendor's own endpoints sit with its
    provider (:mod:`qte_shared.providers.tiingo`) and its key comes from one
    generic ``QTE_DATA_PROVIDER_API_KEY``, so adding a second vendor never
    edits this file — see :mod:`qte_shared.interfaces.market_data`.
    """

    model_config = SettingsConfigDict(env_prefix="QTE_MARKET_DATA__", extra="ignore")

    provider: str = "tiingo"
    #: What the provider is asked to feed: symbols, timeframes and its own
    #: knobs (:mod:`qte_shared.market_data_plan`). Empty means the conventional path
    #: for whichever provider is on, so switching vendor switches plan with it.
    config_file: Path | None = None

    @property
    def plan_file(self) -> Path:
        """The plan this provider reads. Named after it, unless overridden."""
        return self.config_file or REPO_ROOT / "config" / f"{self.provider}.toml"


#: Resolved once, before :class:`Settings` is built: :class:`EngineSettings`
#: takes its defaults from the plan, so the plan has to be readable first.
market_data_settings = MarketDataSettings()


@lru_cache(maxsize=1)
def market_data_plan() -> MarketDataPlan:
    """The market-data plan, parsed once per process.

    Cached because it is read on every settings construction and its file does
    not change under a running service; a test that writes a plan calls
    ``market_data_plan.cache_clear()``.
    """
    return MarketDataPlan.load(market_data_settings.plan_file)


class EngineSettings(BaseSettings):
    """What the running engine trades and how much history it keeps warm.

    ``symbols`` and ``timeframes`` are the market-data plan's, not this block's:
    ``config/<provider>.toml`` states them per symbol, and these read whatever
    it says. The environment variables still exist and still win — a
    ``default_factory`` only runs when the variable is unset — so a one-off
    ``QTE_ENGINE__SYMBOLS='["EURUSD"]' make backtest`` overrides the file
    without editing it. With no plan on disk the defaults below apply, which is
    what a simulator dev stack runs on.

    ``signal_timeframe`` is *not* in the plan — it is one value for the whole
    engine, not a per-symbol one. It has to be a bar some symbol is resampled
    to, or the runner subscribes to a candle subject that never fires; the
    validator below refuses that outright rather than let it start quiet.

    ``weekend_flat_timezone`` is in this block and not in ``QTE_RUNNER__`` for
    the same reason: the live runner and the backtest replay both have to read a
    strategy's declared weekend window in the same zone, or a backtest stops
    predicting what the runner trades.
    """

    model_config = SettingsConfigDict(env_prefix="QTE_ENGINE__", extra="ignore")

    symbols: list[str] = Field(
        default_factory=lambda: market_data_plan().symbols if market_data_plan() else ["XAUUSD"]
    )
    timeframes: list[str] = Field(
        default_factory=lambda: market_data_plan().timeframes if market_data_plan() else ["M15"]
    )
    signal_timeframe: str = "M15"
    warmup_candles: int = 300
    strategies_dir: Path = REPO_ROOT / "__strategies__"
    #: Symbol → strategies table. Git-ignored, with a tracked template beside
    #: it; see :mod:`qte_shared.strategies.mapping`. Absent means every
    #: strategy keeps the symbols it declares on itself.
    mapping_file: Path = REPO_ROOT / "config" / "strategies_mapping.toml"
    #: Root of the history tree. Nothing is written here directly: each
    #: source owns a subdirectory (``tiingo/`` for a provider download,
    #: ``mt5/`` for a CSV import) so a file's path names its origin.
    parquet_dir: Path = REPO_ROOT / "data" / "parquet"
    reports_dir: Path = REPO_ROOT / "data" / "reports"
    #: The zone a strategy's declared weekend-flat window is read in — see
    #: :mod:`qte_shared.strategies.strategy_settings`. An IANA name; ``UTC``
    #: takes the declared times literally, which is what the mounted strategies
    #: state them in. The strategy owns *when* its market shuts, the operator
    #: owns *whose clock* that is.
    weekend_flat_timezone: str = "UTC"

    @property
    def market_zone(self) -> ZoneInfo:
        """:attr:`weekend_flat_timezone` as a tzinfo, validated on construction."""
        return ZoneInfo(self.weekend_flat_timezone)

    def resolve_subscriptions(
        self, market_plan: MarketDataPlan, market_overrides: dict[str, str]
    ) -> list[SymbolFeed]:
        """Apply explicit symbol/timeframe overrides without flattening plan defaults."""
        symbols = (
            self.symbols
            if "symbols" in self.model_fields_set or not market_plan
            else market_plan.symbols
        )
        timeframes = tuple(dict.fromkeys(normalize_timeframe(label) for label in self.timeframes))
        markets = {symbol.upper(): market for symbol, market in market_overrides.items()}
        markets.update(
            {symbol_feed.symbol: symbol_feed.market for symbol_feed in market_plan.feeds}
        )
        subscriptions = []
        for symbol in dict.fromkeys(symbol.upper() for symbol in symbols):
            planned_feed = market_plan.feed_for(symbol)
            resolved_frames = (
                planned_feed.timeframes
                if planned_feed is not None and "timeframes" not in self.model_fields_set
                else timeframes
            )
            if not resolved_frames:
                raise ValueError(f"No resampled timeframes configured for {symbol}")
            specification = build_specs([symbol], markets)[0]
            subscriptions.append(
                SymbolFeed(symbol=symbol, market=specification.market, timeframes=resolved_frames)
            )
        return subscriptions

    @model_validator(mode="after")
    def _signal_timeframe_is_resampled(self) -> EngineSettings:
        """The signal bar must be one the feed actually produces.

        ``QTE_ENGINE__SIGNAL_TIMEFRAME`` naming a bar no symbol is resampled to
        is not a runtime error anywhere — the runner just subscribes to
        ``QTE.candle.closed.<symbol>.<bar>`` and waits forever. Catch it here,
        where both values are resolved, instead.
        """
        try:
            signal = normalize_timeframe(self.signal_timeframe)
            resampled = {normalize_timeframe(label) for label in self.timeframes}
        except ValueError as exc:
            raise ValueError(f"QTE_ENGINE__ timeframe is not a valid label: {exc}") from exc
        if self.symbols and signal not in resampled:
            raise ValueError(
                f"QTE_ENGINE__SIGNAL_TIMEFRAME={self.signal_timeframe!r} is not one of the "
                f"resampled timeframes {sorted(resampled)} — nothing would ever close for it"
            )
        return self

    @model_validator(mode="after")
    def _weekend_flat_timezone_exists(self) -> EngineSettings:
        """Refuse an unknown zone name here, not on the first Friday.

        Left unchecked, a typo would raise inside the runner's candle handler
        every bar once a strategy declaring a weekend window was mounted — or,
        worse, be caught by the handler's own guard and simply never flatten.
        """
        try:
            ZoneInfo(self.weekend_flat_timezone)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise ValueError(
                f"QTE_ENGINE__WEEKEND_FLAT_TIMEZONE={self.weekend_flat_timezone!r} is not an "
                f"IANA timezone name ({exc}) — 'UTC' or, for example, 'Europe/Nicosia'"
            ) from exc
        return self


class StateSettings(BaseSettings):
    """Execution mode is explicit and independent of the hot delivery pause."""

    model_config = SettingsConfigDict(env_prefix="QTE_STATE__", extra="ignore")
    execution_mode: ExecutionMode = Field(default="shadow", validation_alias="QTE_STATE__MODE")


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
    state_config: StateSettings = Field(default_factory=StateSettings)

    nats: NatsSettings = Field(default_factory=NatsSettings)
    account: AccountSettings = Field(default_factory=AccountSettings)
    broker: BrokerSettings = Field(default_factory=BrokerSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    postgres: PostgresSettings = Field(default_factory=PostgresSettings)
    market_data: MarketDataSettings = Field(default_factory=MarketDataSettings)
    market_stream: MarketStreamSettings = Field(default_factory=MarketStreamSettings)
    engine: EngineSettings = Field(default_factory=EngineSettings)

    @property
    def state_scope(self) -> StateScope:
        return StateScope(
            self.env, self.state_config.execution_mode, self.market_data.provider.strip().lower()
        )

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
