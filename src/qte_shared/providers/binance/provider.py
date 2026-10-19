"""Binance spot market data: ``QTE_MARKET_DATA__PROVIDER=mt5,binance``. Not implemented.

A placeholder, so the name can already be listed beside another provider and
the multi-provider wiring has a second vendor to route crypto symbols to. It
declares what it will serve — live ticks and history, crypto only — but refuses
to be constructed: an ingestion started with it configured stops at start-up
with this message rather than running without the symbols of its plan.
"""

from __future__ import annotations

from typing import ClassVar

from qte_shared.interfaces.market_data import Capability, MarketDataProvider, ProviderError
from qte_shared.symbols import Market


class BinanceProvider(MarketDataProvider):
    """Binance spot klines and trades — to be implemented."""

    name: ClassVar[str] = "binance"
    capabilities: ClassVar[frozenset[Capability]] = frozenset({Capability.LIVE, Capability.HISTORY})
    markets: ClassVar[tuple[Market, ...]] = ("crypto",)

    def __init__(self) -> None:
        raise ProviderError(
            "The binance market data provider is not implemented yet; remove it from "
            "QTE_MARKET_DATA__PROVIDER"
        )
