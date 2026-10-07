"""MetaTrader 5, through ``algo-trading-ingester``: ``QTE_MARKET_DATA__PROVIDER=mt5``.

The MT5 terminal only runs on Windows and its Python API only polls, so QTE
does not talk to it. The ingester does: it sits beside the terminal, detects
each closed bar and publishes it to NATS. This provider is QTE's side of that
hand-off — a subscriber, not a client of the terminal.

It serves :attr:`~qte_shared.interfaces.market_data.Capability.LIVE_BARS` —
there are no ticks to resample, because the ingester publishes bars already
closed — and :attr:`~qte_shared.interfaces.market_data.Capability.RECENT_BARS`:
the bars from before QTE was listening are asked of the ingester, one request
per planned series, which is how a cold Redis gets its indicator window
(:mod:`qte_shared.providers.mt5.history`). QTE decides which series and how
many bars (``history_bars``); the ingester pushes no warm-up of its own.

It does not serve ``HISTORY``. "The newest bars" is not an archive, so a
backtest download still refuses this provider: export from MT5 and
``make csv-import``.
"""

from __future__ import annotations

from typing import ClassVar

from qte_shared.interfaces.market_data import (
    CandleHandler,
    Capability,
    HistorySource,
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
    capabilities: ClassVar[frozenset[Capability]] = frozenset(
        {Capability.LIVE_BARS, Capability.RECENT_BARS}
    )
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

    def history_source(self) -> HistorySource:
        """The ingester, asked for the newest bars of a series over NATS."""
        from qte_shared.providers.mt5.history import IngesterHistorySource

        return IngesterHistorySource(self.config)

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
