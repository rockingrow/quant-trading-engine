"""Which market a symbol trades on.

QTE speaks broker symbols (``XAUUSD``, ``BTCUSDT``) because that is what ends
up in the signal a worker executes. Every vendor spells them differently, and
that translation belongs to the vendor: see
:meth:`~qte_shared.interfaces.market_data.MarketDataProvider.ticker_for`. What
stays here is the vendor-independent part — which market a symbol belongs to,
because that decides *which* feed or endpoint the provider reaches for.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

Market = Literal["fx", "crypto"]

#: Quote currencies that only ever appear on a crypto pair. ``USD`` is absent on
#: purpose — ``XAUUSD`` and ``BTCUSD`` both end in it, so it decides nothing.
_CRYPTO_QUOTES = ("USDT", "USDC", "BUSD", "DAI", "TUSD")
_CRYPTO_BASES = ("BTC", "ETH", "SOL", "XRP", "BNB", "ADA", "DOGE", "AVAX", "LTC", "LINK")


@dataclass(frozen=True, slots=True)
class SymbolSpec:
    """A symbol as QTE names it, and the market it trades on."""

    symbol: str
    market: Market


def infer_market(symbol: str) -> Market:
    """Best-effort market guess, overridable in configuration.

    ``BTCUSD`` is genuinely ambiguous (a CFD desk quotes it on FX, an exchange
    on crypto), so the base-asset check runs before any USD reasoning and
    ``QTE_INGESTION__MARKET_OVERRIDES`` exists for whatever is left.
    """
    upper = symbol.upper()
    if upper.endswith(_CRYPTO_QUOTES):
        return "crypto"
    if upper.startswith(_CRYPTO_BASES):
        return "crypto"
    return "fx"


def build_specs(symbols: list[str], overrides: dict[str, str] | None = None) -> list[SymbolSpec]:
    resolved = {key.upper(): value for key, value in (overrides or {}).items()}
    specs = []
    for symbol in symbols:
        upper = symbol.upper()
        market = resolved.get(upper) or infer_market(upper)
        if market not in ("fx", "crypto"):
            raise ValueError(f"Unknown market {market!r} for symbol {symbol!r}")
        specs.append(SymbolSpec(symbol=upper, market=market))  # type: ignore[arg-type]
    return specs
