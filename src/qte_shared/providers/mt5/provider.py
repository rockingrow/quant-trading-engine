"""MetaTrader 5, through ``algo-trading-ingester``: ``QTE_MARKET_DATA__PROVIDER=mt5``.

The MT5 terminal only runs on Windows and its Python API only polls, so QTE
does not talk to it. The ingester does: it sits beside the terminal, detects
each closed bar and publishes it to NATS. This provider is QTE's side of that
hand-off — a subscriber, not a client of the terminal.

It serves :attr:`~qte_shared.interfaces.market_data.Capability.LIVE_BARS` and
nothing else. There are no ticks to resample, because the ingester publishes
bars already closed; and there is no history endpoint, because the ingester's
JetStream stream *is* the history — a new durable consumer replays what the
stream still holds (``deliver_policy``), which warms Redis on the first start.
For backtests, export from MT5 and ``make csv-import``.
"""

from __future__ import annotations

from typing import ClassVar

from qte_shared.interfaces.market_data import (
    CandleHandler,
    Capability,
    LiveFeed,
    MarketDataProvider,
    WarmupBarHandler,
)
from qte_shared.market_data_plan import SymbolFeed
from qte_shared.providers.mt5.settings import Mt5Settings
from qte_shared.symbols import Market, SymbolSpec


class Mt5Provider(MarketDataProvider):
    """Closed MT5 bars, consumed off the ingester's NATS subjects."""

    name: ClassVar[str] = "mt5"
    capabilities: ClassVar[frozenset[Capability]] = frozenset({Capability.LIVE_BARS})
    #: A broker's book quotes both: gold and oil as FX-style CFDs, BTCUSD too.
    markets: ClassVar[tuple[Market, ...]] = ("fx", "crypto")

    def __init__(self, config: Mt5Settings | None = None) -> None:
        self.config = config or Mt5Settings()

    def ticker_for(self, spec: SymbolSpec) -> str:
        """The ingester publishes the bare symbol it was configured with.

        It resolves the broker's affix (Exness ``XAUUSDm``) itself and keeps the
        configured name in the payload, so QTE's symbol is the ingester's.
        """
        return spec.symbol.upper()

    def bar_feeds(
        self,
        subscriptions: list[SymbolFeed],
        on_bar: CandleHandler,
        on_warmup: WarmupBarHandler | None = None,
    ) -> list[LiveFeed]:
        """One subscription for every planned series — the feed filters by payload."""
        from qte_shared.providers.mt5.feed import IngesterBarFeed

        series = {
            self.ticker_for(feed.spec): feed.timeframes
            for feed in subscriptions
            if feed.market in self.markets
        }
        if not series:
            return []
        return [IngesterBarFeed(series, on_bar, self.config, on_warmup=on_warmup)]
