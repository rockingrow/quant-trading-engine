"""The simulator as a market data provider: ``QTE_MARKET_DATA__PROVIDER=simulator``.

Pointing ingestion at the simulator is a configuration change and nothing else
— no branch in the service, no "if testing" anywhere on the tick path. That is
what makes an end-to-end rehearsal worth running: the code under test is the
code that trades.

It serves :attr:`~qte_shared.interfaces.market_data.Capability.LIVE` only.
History deliberately stays with a real vendor: a backtest over invented bars
would produce an equity curve that means nothing, and the one thing worse than
no backtest is a convincing fake of one. Use ``make csv-import`` or a real
provider's ``download`` for history.
"""

from __future__ import annotations

from typing import ClassVar

from qte_shared.dev_only import require_dev_env
from qte_shared.interfaces.market_data import (
    Capability,
    LiveFeed,
    MarketDataProvider,
    TickHandler,
)
from qte_shared.providers.simulator.settings import SimulatorSettings
from qte_shared.symbols import Market, SymbolSpec


class SimulatorProvider(MarketDataProvider):
    """A dev-only feed served by ``qte-simulator``, over one socket."""

    name: ClassVar[str] = "simulator"
    capabilities: ClassVar[frozenset[Capability]] = frozenset({Capability.LIVE})
    #: Every market, because the simulator quotes whatever you tell it to.
    markets: ClassVar[tuple[Market, ...]] = ("fx", "crypto")

    def __init__(self, config: SimulatorSettings | None = None) -> None:
        # Before anything else: constructing this object is the moment a
        # process decides to take its prices from a fixture.
        require_dev_env("The market data simulator provider")
        self.config = config or SimulatorSettings()

    def ticker_for(self, spec: SymbolSpec) -> str:
        """The simulator speaks QTE's own symbols — there is no vendor spelling."""
        return spec.symbol.upper()

    def live_feeds(self, specs: list[SymbolSpec], on_tick: TickHandler) -> list[LiveFeed]:
        from qte_shared.providers.simulator.feed import SimulatorLiveFeed

        symbols = [self.ticker_for(spec) for spec in specs]
        if not symbols:
            return []
        return [SimulatorLiveFeed(symbols, on_tick, self.config)]
