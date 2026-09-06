"""Tiingo, as one factory class.

This is the only object in the codebase that says "Tiingo" out loud to an
engine. It owns the credentials and the ticker spelling, and hands back the two
vendor-neutral objects QTE actually consumes: a
:class:`~qte_shared.interfaces.market_data.HistorySource` for the backtester and
a list of :class:`~qte_shared.interfaces.market_data.LiveFeed` for ingestion.

The REST and WebSocket clients are imported inside the methods on purpose:
``httpx`` and ``websockets`` are then paid for by the image that asks for them,
and an engine that only needs history never imports the socket stack.
"""

from __future__ import annotations

from typing import ClassVar

from qte_shared.interfaces.market_data import (
    Capability,
    HistorySource,
    LiveFeed,
    MarketDataProvider,
    TickHandler,
)
from qte_shared.providers.tiingo.settings import TiingoSettings
from qte_shared.symbols import Market, SymbolSpec


class TiingoProvider(MarketDataProvider):
    """Tiingo: FX and crypto, history over REST and ticks over WebSocket."""

    name: ClassVar[str] = "tiingo"
    capabilities: ClassVar[frozenset[Capability]] = frozenset({Capability.HISTORY, Capability.LIVE})
    markets: ClassVar[tuple[Market, ...]] = ("fx", "crypto")

    def __init__(self, config: TiingoSettings | None = None) -> None:
        #: Read from the environment unless a caller injects a block -- which is
        #: how a test, or a second Tiingo account, gets its own credentials.
        self.config = config or TiingoSettings()

    def ticker_for(self, spec: SymbolSpec) -> str:
        """Tiingo speaks lowercase tickers on both sockets and both REST APIs."""
        return spec.symbol.lower()

    def history_source(self) -> HistorySource:
        from qte_shared.providers.tiingo.rest import TiingoHistorySource

        return TiingoHistorySource(self.config)

    def live_feeds(self, specs: list[SymbolSpec], on_tick: TickHandler) -> list[LiveFeed]:
        """One socket per market that actually has symbols on it."""
        from qte_shared.providers.tiingo.ws import TiingoLiveFeed

        feeds: list[LiveFeed] = []
        for market in self.markets:
            tickers = {
                self.ticker_for(spec): spec.symbol
                for spec in self.specs_for_markets(specs)
                if spec.market == market
            }
            if tickers:
                feeds.append(TiingoLiveFeed(market, tickers, on_tick, self.config))
        return feeds
