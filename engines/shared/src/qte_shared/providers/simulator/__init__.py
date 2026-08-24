"""The dev-only market data simulator. Reached as ``create_provider("simulator")``.

This package is the *client* half — the provider ingestion connects through,
plus the wire protocol both halves share. The server that answers it is the
``qte-simulator`` engine; see ``docs/simulator.md``.
"""

from __future__ import annotations

from qte_shared.providers.simulator.protocol import (
    CONTROL_PATH,
    DEFAULT_PORT,
    PROTOCOL_VERSION,
    STREAM_PATH,
)
from qte_shared.providers.simulator.provider import SimulatorProvider
from qte_shared.providers.simulator.settings import SimulatorSettings

__all__ = [
    "CONTROL_PATH",
    "DEFAULT_PORT",
    "PROTOCOL_VERSION",
    "STREAM_PATH",
    "SimulatorProvider",
    "SimulatorSettings",
]
