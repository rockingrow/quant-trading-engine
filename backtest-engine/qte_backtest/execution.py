"""Fill simulation: what the market would actually have done to an intent.

The simulator is deliberately pessimistic, because a backtest's job is to
disprove a strategy, not to flatter it:

* Entries and exits cross the spread and pay slippage on top.
* When a bar's range covers both the stop and the target, the **stop** is taken.
  Without tick data there is no way to know which came first, and assuming the
  favourable one is how a losing strategy backtests profitably.
* A stop or target inside the bar fills at its own level, not at the close.
* A gap through a level fills at the open — the honest answer when price was
  never available at the level at all.

Sizes are in instrument units and P&L is ``(exit - entry) × quantity × direction``,
so ``quantity`` means lots only if you set :attr:`CostModel.contract_size` to the
lot size. Getting that wrong scales P&L, never the shape of the curve.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

import pandas as pd
from qte_shared.models import SignalAction


class ExitReason(str, Enum):
    TP1 = "TP1"
    TP2 = "TP2"
    SL = "SL"
    R_SL = "R_SL"
    FLAT = "FLAT"
    END_OF_DATA = "END_OF_DATA"


@dataclass(slots=True)
class CostModel:
    """Everything that stands between the quoted price and the fill.

    *spread* is the full bid/ask distance in price units (0.30 on gold, ~2.0 on
    BTCUSDT); each side pays half. *commission_per_unit* is charged on entry and
    on every partial exit.
    """

    spread: float = 0.0
    slippage: float = 0.0
    commission_per_unit: float = 0.0
    contract_size: float = 1.0

    def entry_fill(self, price: float, direction: int) -> float:
        return price + direction * (self.spread / 2.0 + self.slippage)

    def exit_fill(self, price: float, direction: int) -> float:
        return price - direction * (self.spread / 2.0 + self.slippage)

    def commission(self, quantity: float) -> float:
        return abs(quantity) * self.contract_size * self.commission_per_unit


@dataclass(slots=True)
class ClosedLeg:
    """One partial or full exit of a position."""

    closed_at: datetime
    price: float
    quantity: float
    reason: ExitReason
    gross_pnl: float
    fees: float

    @property
    def net_pnl(self) -> float:
        return self.gross_pnl - self.fees


@dataclass(slots=True)
class SimulatedPosition:
    """An open position and the legs already closed out of it."""

    symbol: str
    direction: int  # +1 long, -1 short
    opened_at: datetime
    entry_price: float
    quantity: float
    sl: float | None = None
    tp1: float | None = None
    tp2: float | None = None
    tp1_percent: float | None = None
    move_sl_to_be: bool = False
    signal_uxid: str | None = None
    entry_fees: float = 0.0
    remaining: float = 0.0
    legs: list[ClosedLeg] = field(default_factory=list)
    tp1_filled: bool = False

    def __post_init__(self) -> None:
        if not self.remaining:
            self.remaining = self.quantity

    @property
    def is_open(self) -> bool:
        return self.remaining > 1e-12

    @property
    def gross_pnl(self) -> float:
        return sum(leg.gross_pnl for leg in self.legs)

    @property
    def fees(self) -> float:
        return self.entry_fees + sum(leg.fees for leg in self.legs)

    @property
    def net_pnl(self) -> float:
        return self.gross_pnl - self.fees

    @property
    def closed_at(self) -> datetime | None:
        return self.legs[-1].closed_at if self.legs and not self.is_open else None

    @property
    def exit_price(self) -> float | None:
        """Size-weighted average of every closed leg."""
        if not self.legs:
            return None
        volume = sum(leg.quantity for leg in self.legs)
        return sum(leg.price * leg.quantity for leg in self.legs) / volume if volume else None

    @property
    def exit_reason(self) -> str | None:
        return self.legs[-1].reason.value if self.legs else None


class FillSimulator:
    """Opens positions from intents and walks them forward bar by bar."""

    def __init__(self, costs: CostModel | None = None) -> None:
        self.costs = costs or CostModel()

    # ── Opening ───────────────────────────────────────────────────────

    def open_position(
        self,
        *,
        symbol: str,
        action: SignalAction,
        bar_time: datetime,
        price: float,
        quantity: float,
        sl: float | None = None,
        tp1: float | None = None,
        tp2: float | None = None,
        tp1_percent: float | None = None,
        move_sl_to_be: bool = False,
        signal_uxid: str | None = None,
    ) -> SimulatedPosition:
        direction = 1 if action is SignalAction.LONG else -1
        fill = self.costs.entry_fill(price, direction)
        return SimulatedPosition(
            symbol=symbol,
            direction=direction,
            opened_at=bar_time,
            entry_price=fill,
            quantity=quantity,
            sl=sl,
            tp1=tp1,
            tp2=tp2,
            tp1_percent=tp1_percent,
            move_sl_to_be=move_sl_to_be,
            signal_uxid=signal_uxid,
            entry_fees=self.costs.commission(quantity),
        )

    # ── Walking forward ───────────────────────────────────────────────

    def process_bar(self, position: SimulatedPosition, bar: pd.Series, bar_time: datetime) -> None:
        """Apply one bar's range to *position*, filling any level it touched.

        Stop before target, always — see the module docstring.
        """
        if not position.is_open:
            return

        high, low = float(bar["high"]), float(bar["low"])
        open_price = float(bar["open"])

        if self._touched(position.sl, position.direction, high, low, is_stop=True):
            reason = (
                ExitReason.R_SL if position.tp1_filled and position.move_sl_to_be else ExitReason.SL
            )
            self._close(
                position,
                bar_time,
                self._fill_level(position.sl, open_price, position.direction, is_stop=True),
                position.remaining,
                reason,
            )
            return

        if (
            not position.tp1_filled
            and position.tp1 is not None
            and self._touched(position.tp1, position.direction, high, low, is_stop=False)
        ):
            share = (position.tp1_percent or 100.0) / 100.0
            quantity = min(position.remaining, position.quantity * share)
            self._close(
                position,
                bar_time,
                self._fill_level(position.tp1, open_price, position.direction, is_stop=False),
                quantity,
                ExitReason.TP1,
            )
            position.tp1_filled = True
            if position.move_sl_to_be:
                # Breakeven means the *entry fill*, not the signalled price —
                # the spread already paid on the way in does not come back.
                position.sl = position.entry_price
            if not position.is_open:
                return

        if position.tp2 is not None and self._touched(
            position.tp2, position.direction, high, low, is_stop=False
        ):
            self._close(
                position,
                bar_time,
                self._fill_level(position.tp2, open_price, position.direction, is_stop=False),
                position.remaining,
                ExitReason.TP2,
            )

    def close_at(
        self,
        position: SimulatedPosition,
        bar_time: datetime,
        price: float,
        reason: ExitReason = ExitReason.FLAT,
    ) -> None:
        """Discretionary close — a FLAT intent, or the end of the data."""
        if position.is_open:
            self._close(
                position,
                bar_time,
                self.costs.exit_fill(price, position.direction),
                position.remaining,
                reason,
            )

    # ── Internals ─────────────────────────────────────────────────────

    @staticmethod
    def _touched(
        level: float | None, direction: int, high: float, low: float, *, is_stop: bool
    ) -> bool:
        if level is None:
            return False
        # A long's stop sits below and its target above; a short is the mirror.
        below = (direction == 1) == is_stop
        return low <= level if below else high >= level

    def _fill_level(
        self, level: float | None, open_price: float, direction: int, *, is_stop: bool
    ) -> float:
        """Fill at the level, unless the bar gapped past it — then at the open.

        A stop that gapped fills worse than it was set, which is the real cost
        of a weekend gap and the thing a naive simulator hides.
        """
        assert level is not None
        gapped_through = open_price < level if ((direction == 1) == is_stop) else open_price > level
        raw = open_price if gapped_through else level
        return self.costs.exit_fill(raw, direction)

    def _close(
        self,
        position: SimulatedPosition,
        bar_time: datetime,
        price: float,
        quantity: float,
        reason: ExitReason,
    ) -> None:
        quantity = min(quantity, position.remaining)
        if quantity <= 0:
            return
        gross = (
            (price - position.entry_price)
            * position.direction
            * quantity
            * self.costs.contract_size
        )
        position.legs.append(
            ClosedLeg(
                closed_at=bar_time,
                price=price,
                quantity=quantity,
                reason=reason,
                gross_pnl=gross,
                fees=self.costs.commission(quantity),
            )
        )
        position.remaining -= quantity
