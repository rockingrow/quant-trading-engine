"""The dev-only market data simulator: a WebSocket feed you drive by hand.

The server half of ``qte_shared.providers.simulator``. It answers the socket
ingestion dials, so a rehearsal of the whole pipeline —

    qte-simulator → data-ingestion → Redis + NATS → strategy-runner → signal

— runs the real services with the real code path, and only the prices are
invented. See ``docs/simulator.md`` for the walkthrough.
"""

from __future__ import annotations

__all__ = ["__version__"]

__version__ = "0.1.0"
