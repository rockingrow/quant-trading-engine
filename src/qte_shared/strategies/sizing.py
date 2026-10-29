"""How big an entry is — decided by the engine, never by the strategy.

A strategy is not told the account balance (see
:class:`~qte_shared.strategies.strategy_base.StrategyContext`), which is exactly what lets
the same file run in a backtest and in production. Size is therefore the
runner's decision, and it is the same decision in both drivers because both go
through :class:`~qte_shared.strategies.signal_factory.SignalFactory`, which owns one of
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

**``risk_percent`` is the pair's, unless the pair opts out of that.** By default
every entry risks the pair's own ``risk_percent`` and whatever a strategy states
on its intent is overruled. A pair whose mapping sets ``dynamic_risk = true``
lets its strategy state the risk of each entry — lower on a setup it trusts less,
higher on one it trusts more — and is sized at that, within a ceiling. The pair's
``max_risk_percent`` is that ceiling; left unset it is the pair's own
``risk_percent``, so turning the switch on lets a strategy *lower* its risk and
never raise it until the mapping says how far. ``QTE_ACCOUNT__MAX_RISK_PERCENT``
is a second, account-wide ceiling over every pair, dynamic or not. The strategy
still never sees the balance: it states a proportion, and the engine decides
what that proportion may be and what it buys.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import ROUND_DOWN, Decimal
from math import isfinite
from numbers import Real
from typing import Any

from qte_shared.config import settings
from qte_shared.logging_setup import get_logger

log = get_logger(__name__)

#: Key the mapping table uses to state a pair's risk — in a
#: ``[strategies.<name>]`` default or a ``[symbols.<symbol>.params.<name>]``
#: override. Read off a strategy's params, which is where both land.
RISK_PERCENT_KEY = "risk_percent"

#: Params key mirrored onto the payload's ``position.use_equity_sizing``.
EQUITY_SIZING_KEY = "use_equity_sizing"

#: Params key that opts a pair into sizing each entry at the ``risk_percent``
#: its strategy states on the intent, instead of the pair's own.
DYNAMIC_RISK_KEY = "dynamic_risk"

#: Params key for the highest ``risk_percent`` a dynamic pair's entry is sized
#: at. Unset, it is the pair's own ``risk_percent``: lower only.
MAX_RISK_PERCENT_KEY = "max_risk_percent"


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
    #: Highest ``risk_percent`` one entry is sized at. ``None`` means no ceiling.
    max_risk_percent: float | None = None
    #: Size each entry at the ``risk_percent`` its intent states, within
    #: :attr:`max_risk_percent`, rather than at :attr:`risk_percent`.
    dynamic_risk: bool = False

    def __post_init__(self) -> None:
        for setting_name in ("capital", "risk_percent", "contract_size"):
            setting_value = getattr(self, setting_name)
            if not isfinite(setting_value) or setting_value < 0:
                raise ValueError(f"{setting_name} must be finite and nonnegative")
        if self.contract_size == 0:
            raise ValueError("contract_size must be positive")
        if self.max_quantity is not None and (
            not isfinite(self.max_quantity) or self.max_quantity < 0
        ):
            raise ValueError("max_quantity must be finite and nonnegative")
        if self.max_risk_percent is not None and (
            not isfinite(self.max_risk_percent) or self.max_risk_percent <= 0
        ):
            raise ValueError("max_risk_percent must be finite and positive")
        if isinstance(self.precision, bool) or not isinstance(self.precision, int):
            raise ValueError("quantity precision must be an integer")
        if not 0 <= self.precision <= 12:
            raise ValueError("quantity precision must be between 0 and 12")

    @classmethod
    def from_settings(
        cls,
        params: dict[str, Any] | None = None,
        *,
        risk_percent: float | None = None,
    ) -> PositionSizer:
        """Build one from ``QTE_ACCOUNT__*`` and a pair's strategy params.

        *params* is what ``config/strategies_mapping.toml`` mapped to this pair
        (its ``[strategies.<name>]`` defaults merged under the pair's
        ``[symbols.<symbol>.params.<name>]`` overrides), so a ``risk_percent``
        stated there wins over the account default. An explicit *risk_percent*
        argument wins over both — that is the caller saying it already resolved
        the question.

        The same params say whether the pair sizes per entry and under which
        ceiling — see :func:`resolve_dynamic_risk` and :func:`resolve_risk_ceiling`.
        Unlike a junk ``risk_percent``, which falls back, a junk value for either
        of those raises: a typo in a switch that decides how much is risked
        must stop the pair, not trade it some other way.
        """
        account = settings.account
        pair_params = params or {}
        resolved = risk_percent
        if resolved is None:
            resolved = _as_float(pair_params.get(RISK_PERCENT_KEY))
        if resolved is None:
            resolved = account.risk_percent
        dynamic_risk = resolve_dynamic_risk(pair_params)
        return cls(
            capital=account.capital,
            risk_percent=resolved,
            contract_size=account.contract_size,
            max_quantity=account.max_quantity or None,
            precision=account.quantity_precision,
            max_risk_percent=resolve_risk_ceiling(
                pair_params,
                base_risk=resolved,
                dynamic_risk=dynamic_risk,
                account_ceiling=account.max_risk_percent or None,
            ),
            dynamic_risk=dynamic_risk,
        )

    # ── The rule ──────────────────────────────────────────────────────

    def budget_for(self, risk_percent: float) -> float:
        """Currency put at risk on one entry at *risk_percent*."""
        return self.capital * risk_percent / 100.0

    @property
    def risk_budget(self) -> float:
        """Currency put at risk on one entry at the pair's own ``risk_percent``."""
        return self.budget_for(self.risk_percent)

    def risk_for(self, requested: float | None) -> float:
        """The ``risk_percent`` an entry is sized at, given what its intent asked for.

        A pair that does not size per entry ignores *requested* and trades its
        own :attr:`risk_percent`. A dynamic pair takes *requested* when there is
        one. Either way :attr:`max_risk_percent` applies last, so no entry is
        ever sized above it.

        A request that is not a finite number is refused on every pair, and on
        a dynamic pair so is one that is not positive. Refused, not replaced: a
        strategy that asked for nonsense must not be traded at a default, and
        ``ValueError`` is what both drivers drop an intent on.
        """
        wanted = self.risk_percent
        if requested is not None:
            usable = (
                isinstance(requested, Real)
                and not isinstance(requested, bool)
                and isfinite(requested)
            )
            if self.dynamic_risk:
                if not usable or requested <= 0:
                    raise ValueError(
                        f"Entry risk_percent must be finite and positive, got {requested!r}"
                    )
                wanted = float(requested)
            elif not usable:
                raise ValueError(f"Entry risk_percent must be a finite number, got {requested!r}")
        if self.max_risk_percent is not None and wanted > self.max_risk_percent:
            return self.max_risk_percent
        return wanted

    def size(
        self, price: float | None, sl: float | None, *, risk_percent: float | None = None
    ) -> float | None:
        """Quantity for an entry at *price* stopping at *sl*.

        *risk_percent* is the entry's own, as :meth:`risk_for` settled it;
        omitted, the pair's :attr:`risk_percent`. Only missing price or stop
        returns ``None``. An invalid stop, zero budget or quantity smaller than
        one supported step raises ``ValueError``: those are rejected trades and
        must never select a fallback quantity.
        """
        entry_risk = self.risk_percent if risk_percent is None else risk_percent
        entry_budget = self.budget_for(entry_risk)
        if not isfinite(entry_budget) or entry_budget <= 0:
            raise ValueError("Risk budget must be finite and positive for an entry")
        if price is None or sl is None:
            return None
        if not isfinite(price) or not isfinite(sl):
            raise ValueError("Entry price and stop must be finite")
        stop_distance = abs(Decimal(str(price)) - Decimal(str(sl)))
        if stop_distance == 0:
            raise ValueError("Entry stop distance must be positive")
        risk_budget = Decimal(str(self.capital)) * Decimal(str(entry_risk)) / 100
        quantity = risk_budget / (stop_distance * Decimal(str(self.contract_size)))
        return self.limit_quantity(quantity)

    def limit_quantity(self, quantity: float | Decimal) -> float:
        """Enforce the account ceiling and round down, including fallback sizes."""
        if not isfinite(quantity) or quantity <= 0:
            raise ValueError("Entry quantity must be finite and positive")
        bounded = Decimal(str(quantity))
        if self.max_quantity:
            bounded = min(bounded, Decimal(str(self.max_quantity)))
        rounded = bounded.quantize(Decimal(1).scaleb(-self.precision), rounding=ROUND_DOWN)
        if rounded <= 0:
            raise ValueError("Entry quantity is below the supported quantity step")
        return float(rounded)

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
            "max_risk_percent": self.max_risk_percent,
            "dynamic_risk": self.dynamic_risk,
        }


def resolve_use_equity_sizing(params: dict[str, Any] | None) -> bool | None:
    """The pair's ``use_equity_sizing``, as the payload should carry it.

    ``None`` when nothing declared it, which is how the broker's schema spells
    "not stated" — distinct from an explicit ``false``.
    """
    value = (params or {}).get(EQUITY_SIZING_KEY)
    return None if value is None else bool(value)


def resolve_dynamic_risk(params: dict[str, Any] | None) -> bool:
    """Whether the pair sizes each entry at the risk its strategy states.

    Strict on purpose: ``bool("false")`` is ``True``, and a switch that decides
    how much an entry risks cannot be read that loosely.
    """
    switch_value = (params or {}).get(DYNAMIC_RISK_KEY)
    if switch_value is None:
        return False
    if isinstance(switch_value, bool):
        return switch_value
    raise ValueError(f"{DYNAMIC_RISK_KEY} must be true or false, not {switch_value!r}")


def resolve_risk_ceiling(
    params: dict[str, Any] | None,
    *,
    base_risk: float,
    dynamic_risk: bool,
    account_ceiling: float | None,
) -> float | None:
    """The highest ``risk_percent`` one of this pair's entries may be sized at.

    A dynamic pair's ceiling is its ``max_risk_percent``, or its *base_risk*
    when it states none. A static pair has no ceiling of its own, since it only
    ever trades *base_risk*. *account_ceiling* caps both, and the lower ceiling
    wins. ``None`` means nothing caps the pair.
    """
    stated = (params or {}).get(MAX_RISK_PERCENT_KEY)
    pair_ceiling: float | None = None
    if dynamic_risk:
        pair_ceiling = base_risk if stated is None else _as_float(stated)
        # Checked here, not only by the sizer: under an account ceiling an
        # infinite one would vanish into min() below instead of being refused.
        if pair_ceiling is None or not isfinite(pair_ceiling):
            raise ValueError(f"{MAX_RISK_PERCENT_KEY} must be a finite number, not {stated!r}")
        if pair_ceiling < base_risk:
            raise ValueError(
                f"{MAX_RISK_PERCENT_KEY} = {pair_ceiling} is below the pair's "
                f"{RISK_PERCENT_KEY} = {base_risk}"
            )
    elif stated is not None:
        log.warning(
            "%s = %r is ignored: the pair does not set %s = true",
            MAX_RISK_PERCENT_KEY,
            stated,
            DYNAMIC_RISK_KEY,
        )
    ceilings = [candidate for candidate in (pair_ceiling, account_ceiling) if candidate is not None]
    if not ceilings:
        return None
    ceiling = min(ceilings)
    if ceiling < base_risk:
        log.warning(
            "QTE_ACCOUNT__MAX_RISK_PERCENT = %s is below the pair's %s = %s; "
            "its entries are sized at the ceiling",
            ceiling,
            RISK_PERCENT_KEY,
            base_risk,
        )
    return ceiling


def _as_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


__all__ = [
    "DYNAMIC_RISK_KEY",
    "EQUITY_SIZING_KEY",
    "MAX_RISK_PERCENT_KEY",
    "RISK_PERCENT_KEY",
    "PositionSizer",
    "resolve_dynamic_risk",
    "resolve_risk_ceiling",
    "resolve_use_equity_sizing",
]
