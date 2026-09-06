"""Contracts QTE code programs against, so implementations stay swappable.

Everything in here is abstract: base classes, protocols and the value objects
they exchange. Concrete adapters live elsewhere — market-data vendors under
:mod:`qte_shared.providers`, for instance — and the rest of the engine imports
only what this package declares. Group future seams (brokers, storage backends,
notification sinks) here as their own module rather than letting a vendor name
appear in an engine again.
"""

from __future__ import annotations

from qte_shared.interfaces.market_data import (
    OHLCV_COLUMNS,
    Capability,
    HistoryRequest,
    HistorySource,
    LiveFeed,
    MarketDataProvider,
    ProviderError,
    ProviderNotConfigured,
    ProviderSettings,
    TickHandler,
    UnknownProvider,
    UnsupportedCapability,
    empty_ohlcv_frame,
    normalize_ohlcv,
)

__all__ = [
    "OHLCV_COLUMNS",
    "Capability",
    "HistoryRequest",
    "HistorySource",
    "LiveFeed",
    "MarketDataProvider",
    "ProviderError",
    "ProviderNotConfigured",
    "ProviderSettings",
    "TickHandler",
    "UnknownProvider",
    "UnsupportedCapability",
    "empty_ohlcv_frame",
    "normalize_ohlcv",
]
