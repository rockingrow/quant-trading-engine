"""The server half of ``QTE_SIMULATOR__`` (the client half is the provider's).

Split that way because the two halves run in different processes: ingestion
reads ``url`` and never binds a port; the server binds a port and never dials a
URL. They share the prefix so ``.env`` holds one block, not two.
"""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict
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


simulator_settings = SimulatorServerSettings()
