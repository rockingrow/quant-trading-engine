"""Build an ``IngestionService`` for a test without any live collaborator.

Goes through the real constructor, so every attribute the service's methods
read exists, and swaps in what the test hands over. Nothing connects at
construction: ``NatsBus`` and ``RedisState`` dial only in ``start()``.
"""

from __future__ import annotations

from typing import Any

from qte_ingestion.service import IngestionService
from qte_shared.market_data_plan import SymbolFeed


def bare_ingestion_service(**collaborators: Any) -> IngestionService:
    """An ``IngestionService`` with no provider and one planned series by default."""
    collaborators.setdefault(
        "subscriptions", [SymbolFeed(symbol="XAUUSD", market="fx", timeframes=("M15",))]
    )
    collaborators.setdefault("providers", {})
    return IngestionService(**collaborators)
