"""How big an entry is — decided by the engine, never by the strategy.

A strategy is not told the account balance (see
:class:`~qte_shared.strategy_base.StrategyContext`), which is exactly what lets
the same file run in a backtest and in production. Size is therefore the
runner's decision, and it is the same decision in both drivers because both go
through :class:`~qte_shared.signal_factory.SignalFactory`, which owns one of
these.

The rule is the one a risk-per-trade book uses::

    quantity = capital x risk_percent / 100 / |entry - stop| / contract_size

Read it as: *risk exactly this many currency units if the stop is hit.* The
stop distance is what converts a currency budget into instrument units, which
is why an entry with no stop cannot be sized here at all — :meth:`size` returns
``None`` and the caller falls back to its configured default rather than
guessing at a number that would mean nothing.

**``capital`` is fixed, not the running equity.** ``use_equity_sizing`` travels
on the payload for the broker to read, and it deliberately does not change the
figure below: compounding sizing would make a backtest's trade sequence depend
on its own P&L, so two runs differing by one early trade would be sized
differently for the rest of the file and could not be compared. Move to equity
sizing when the broker is the one holding the equity.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

from qte_shared.config import settings
from qte_shared.logging_setup import get_logger

log = get_logger(__name__)

#: Key the routing table and ``QTE_RUNNER__STRATEGY_PARAMS`` use to state a
#: pair's risk. Read off a strategy's params, which is where both land.
RISK_PERCENT_KEY = "risk_percent"

#: Params key mirrored onto the payload's ``position.use_equity_sizing``.
EQUITY_SIZING_KEY = "use_equity_sizing"


@dataclass(frozen=True, slots=True)
class PositionSizer:
    """Turns a stop distance into a quantity, against a fixed account.

    Immutable: one instance per (strategy, symbol) slot, built once from
    configuration, so a mid-run change of size is impossible by construction.
    """

    capital: float
    risk_percent: float
    contract_size: float = 1.0
    #: ``None`` (or ``0``) means no ceiling.
    max_quantity: float | None = None
    precision: int = 4

    @classmethod
    def from_settings(
        cls,
        params: dict[str, Any] | None = None,
        *,
        risk_percent: float | None = None,
    ) -> PositionSizer:
        """Build one from ``QTE_ACCOUNT__*`` and a pair's strategy params.

        *params* is what ``config/strategies_mapping.toml`` routed to this pair
        (merged over ``QTE_RUNNER__STRATEGY_PARAMS``), so a ``risk_percent``
        stated there wins over the account default. An explicit *risk_percent*
        argument wins over both — that is the caller saying it already resolved
        the question.
        """
        account = settings.account
        resolved = risk_percent
        if resolved is None:
            resolved = _as_float((params or {}).get(RISK_PERCENT_KEY))
        if resolved is None:
            resolved = account.risk_percent
        return cls(
            capital=account.capital,
            risk_percent=resolved,
            contract_size=account.contract_size,
            max_quantity=account.max_quantity or None,
            precision=account.quantity_precision,
        )

    # ── The rule ──────────────────────────────────────────────────────

    @property
    def risk_budget(self) -> float:
        """Currency put at risk on one entry."""
        return self.capital * self.risk_percent / 100.0

    def size(self, price: float | None, sl: float | None) -> float | None:
        """Quantity for an entry at *price* stopping at *sl*.

        ``None`` when the trade cannot be sized — no stop, a stop sitting on
        the entry, or a budget that does not buy one tick of the instrument.
        Every one of those is a caller's decision to make, and the honest
        answer here is "I cannot", not a zero the broker would reject.
        """
        if price is None or sl is None:
            return None
        stop_distance = abs(price - sl)
        if stop_distance <= 0 or self.contract_size <= 0 or self.risk_budget <= 0:
            return None

        quantity = self.risk_budget / (stop_distance * self.contract_size)
        if self.max_quantity:
            quantity = min(quantity, self.max_quantity)
        quantity = round(quantity, self.precision)
        if quantity <= 0:
            log.warning(
                "Risk budget %.4f over a stop %.5f away rounds to zero at %d dp — "
                "raise QTE_ACCOUNT__CAPITAL or the pair's risk_percent",
                self.risk_budget,
                stop_distance,
                self.precision,
            )
            return None
        return quantity

    def replace(self, **changes: Any) -> PositionSizer:
        """A copy with some fields overridden.

        The backtest is the caller that needs it: ``--equity`` sets the capital
        for one run without touching the environment every other process reads.
        """
        return replace(self, **changes)

    def rescale(self, quantity: float | None, factor: float) -> float | None:
        """Apply a cycle's entry scale to a strategy-supplied close quantity."""
        if quantity is None:
            return None
        return round(quantity * factor, self.precision)

    def describe(self) -> dict[str, Any]:
        return {
            "capital": self.capital,
            "risk_percent": self.risk_percent,
            "risk_budget": round(self.risk_budget, 6),
            "contract_size": self.contract_size,
            "max_quantity": self.max_quantity,
        }


def resolve_use_equity_sizing(params: dict[str, Any] | None) -> bool | None:
    """The pair's ``use_equity_sizing``, as the payload should carry it.

    ``None`` when nothing declared it, which is how the broker's schema spells
    "not stated" — distinct from an explicit ``false``.
    """
    value = (params or {}).get(EQUITY_SIZING_KEY)
    return None if value is None else bool(value)


def _as_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


__all__ = [
    "EQUITY_SIZING_KEY",
    "RISK_PERCENT_KEY",
    "PositionSizer",
    "resolve_use_equity_sizing",
]
