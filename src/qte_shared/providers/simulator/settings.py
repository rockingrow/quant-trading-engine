"""The simulator's configuration block (prefix ``QTE_SIMULATOR__``).

The client half lives here because that is what a provider owns: the URL
ingestion dials. The server half — bind address, port — sits with the server in
``qte_simulator.settings``. Both read the same prefix, so an operator fills in
one block in ``.env`` and both ends agree.
"""

from __future__ import annotations

from pydantic_settings import SettingsConfigDict

from qte_shared.interfaces.market_data import ProviderSettings
from qte_shared.providers.simulator.protocol import DEFAULT_PORT, STREAM_PATH


class SimulatorSettings(ProviderSettings):
    """Where the feed is, and how hard it tries to stay attached to it."""

    model_config = SettingsConfigDict(env_prefix="QTE_SIMULATOR__", extra="ignore")

    #: Defaults to the loopback address rather than a container name, because
    #: the common first run is `make sim` and `make ingestion` in two terminals.
    #: Compose overrides it with the service name in `.env`.
    url: str = f"ws://127.0.0.1:{DEFAULT_PORT}{STREAM_PATH}"
    #: Same unbounded-with-backoff policy as a real vendor feed: the simulator
    #: is usually started *after* ingestion, and a feed that gave up on the
    #: first refused connection would make that ordering matter.
    max_backoff_seconds: float = 5.0
    ping_interval: float = 20.0
