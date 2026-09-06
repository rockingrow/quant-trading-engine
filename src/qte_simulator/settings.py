"""The server half of ``QTE_SIMULATOR__`` (the client half is the provider's).

Split that way because the two halves run in different processes: ingestion
reads ``url`` and never binds a port; the server binds a port and never dials a
URL. They share the prefix so ``.env`` holds one block, not two.

The warm-up defaults at the bottom are the CLI's, not the server's: they live
here so ``make warmup`` and ``qte-simulator replay`` read one source instead of
carrying a copy of every number in the Makefile.
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from qte_shared.config import settings
from qte_shared.providers.simulator.protocol import CONTROL_PATH, DEFAULT_PORT


class SimulatorServerSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="QTE_SIMULATOR__", extra="ignore")

    #: Binds to every interface by default. It is a dev-only server behind
    #: `require_dev_env`, and in compose the feed reaches it from another
    #: container, which loopback would refuse.
    host: str = "0.0.0.0"  # noqa: S104
    port: int = DEFAULT_PORT
    #: Where the CLI dials to send commands. Not derived from `host`, because
    #: 0.0.0.0 is an address to listen on and not one to connect to.
    control_url: str = f"ws://127.0.0.1:{DEFAULT_PORT}{CONTROL_PATH}"
    #: Log every tick the server sends. Off by default — a walk at 5/s fills a
    #: terminal in a minute — but it is the fastest way to answer "did the
    #: simulator send it, or did ingestion drop it?".
    log_ticks: bool = False

    # ── Warm-up defaults, read by the CLI ─────────────────────────────

    #: History `replay` plays when it is given neither `--file` nor
    #: `--generate`, so a dev stack can rehearse on prices that really printed
    #: without spending a request against a rate-limited plan. Set as
    #: ``QTE_SIMULATOR_PARQUET_FILE`` — one underscore, unlike the rest of this
    #: block. Empty means there is no cached history and the CLI asks for a
    #: source.
    parquet_file: str = Field(default="", validation_alias="QTE_SIMULATOR_PARQUET_FILE")
    #: Trailing bars taken from that file. Follows the Redis retention so one
    #: run fills exactly the window the runner will read back.
    cache_bars: int = Field(default_factory=lambda: settings.redis.candle_history)
    #: Bars `--generate` synthesises when no count is given. Follows the
    #: warm-up the strategies themselves wait for.
    generate_bars: int = Field(default_factory=lambda: settings.engine.warmup_candles)


simulator_settings = SimulatorServerSettings()
