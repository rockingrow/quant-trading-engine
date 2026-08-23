"""Which feed a symbol belongs to, and how Tiingo spells it.

QTE speaks broker symbols (``XAUUSD``, ``BTCUSDT``) because that is what ends
up in the signal a worker executes. Tiingo speaks lowercase tickers on two
different sockets. The translation is one-way and lives here so no other module
has to guess.
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
    """A symbol as QTE names it, as Tiingo names it, and where to find it."""

    symbol: str
    market: Market

    @property
    def tiingo_ticker(self) -> str:
        return self.symbol.lower()


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
