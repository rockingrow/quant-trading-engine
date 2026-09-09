"""Which market a symbol trades on.

QTE speaks broker symbols (``XAUUSD``, ``BTCUSDT``) because that is what ends
up in the signal a worker executes. Every vendor spells them differently, and
that translation belongs to the vendor: see
:meth:`~qte_shared.interfaces.market_data.MarketDataProvider.ticker_for`. What
stays here is the vendor-independent part — which market a symbol belongs to,
because that decides *which* feed or endpoint the provider reaches for.

The market is always stated, never guessed: ``BTCUSD`` is a crypto pair on an
exchange and an FX CFD on a broker's book, and only the operator knows which.
It comes from ``market`` beside the symbol in ``config/<provider>.toml``, or
from ``QTE_INGESTION__MARKET_OVERRIDES`` on the no-plan fallback path.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

Market = Literal["fx", "crypto"]


@dataclass(frozen=True, slots=True)
class SymbolSpec:
    """A symbol as QTE names it, and the market it trades on."""

    symbol: str
    market: Market


def build_specs(symbols: list[str], overrides: dict[str, str] | None = None) -> list[SymbolSpec]:
    """Pair each symbol with its market, which must be stated in *overrides*.

    This is the no-plan fallback path only: a market-data plan carries
    ``market`` beside every symbol. A symbol missing from *overrides* is an
    error rather than a guess, because ``BTCUSD`` alone cannot say whether it
    is the exchange pair or the FX CFD.
    """
    resolved = {key.upper(): value for key, value in (overrides or {}).items()}
    specs = []
    for symbol in symbols:
        upper = symbol.upper()
        market = resolved.get(upper)
        if market is None:
            raise ValueError(
                f"No market for {symbol!r}: name it in QTE_INGESTION__MARKET_OVERRIDES, "
                "or move the symbol into config/<provider>.toml where it sits beside one"
            )
        if market not in ("fx", "crypto"):
            raise ValueError(f"Unknown market {market!r} for symbol {symbol!r}")
        specs.append(SymbolSpec(symbol=upper, market=market))  # type: ignore[arg-type]
    return specs
