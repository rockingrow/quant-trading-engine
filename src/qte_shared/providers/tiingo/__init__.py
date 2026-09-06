"""Tiingo market data. Reached as ``create_provider("tiingo")``."""

from __future__ import annotations

from qte_shared.providers.tiingo.provider import TiingoProvider
from qte_shared.providers.tiingo.settings import TiingoSettings

__all__ = ["TiingoProvider", "TiingoSettings"]
