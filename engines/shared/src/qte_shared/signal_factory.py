"""Turning a strategy's :class:`SignalIntent` into a broker-shaped payload.

Both drivers use this — the backtest replay and the live runner — so a signal
recorded in a backtest report is byte-identical to the one a worker would have
executed. That is the whole point: reconciliation compares two rows, not two
implementations.

The factory owns three things a strategy is deliberately not allowed to:

* **Cycle ids.** An entry mints a ``signal_uxid``; every close reuses the one
  its entry got. Get this wrong and the broker renders an exit as a separate
  trade instead of closing the entry's broadcast.
* **The bracket.** SL/TP levels are attached here, from the intent when the
  strategy set them and from ATR/percentage defaults when it did not, so
  "never send a naked entry" is enforced in one place.
* **Timeframe spelling.** QTE says ``M15``; the broker's contract says ``"15"``.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from qte_shared.config import settings
from qte_shared.logging_setup import get_logger
from qte_shared.models import BrokerSignal, SignalAction, new_uxid
from qte_shared.strategy_base import SignalIntent
from qte_shared.timeframes import to_broker_timeframe

log = get_logger(__name__)


class BracketPolicy:
    """Fallback stop/target levels for an intent that arrived without them.

    A strategy is free to compute its own levels — most should. This exists so
    that an entry which somehow reaches the wire without a stop still carries
    one: an unbracketed position on a worker is an unbounded loss waiting for
    someone to notice.
    """

    def __init__(
        self,
        *,
        sl_pct: float = 0.5,
        tp1_r: float = 1.0,
        tp2_r: float = 2.0,
        tp1_percent: float = 50.0,
        move_sl_to_be: bool = True,
        risk_percent: float | None = None,
    ) -> None:
        self.sl_pct = sl_pct
        self.tp1_r = tp1_r
        self.tp2_r = tp2_r
        self.tp1_percent = tp1_percent
        self.move_sl_to_be = move_sl_to_be
        self.risk_percent = risk_percent

    def apply(self, intent: SignalIntent) -> SignalIntent:
        if not intent.action.is_entry or intent.price is None:
            return intent

        direction = 1 if intent.action is SignalAction.LONG else -1
        if intent.sl is None:
            intent.sl = intent.price * (1 - direction * self.sl_pct / 100.0)
            log.warning(
                "Entry for %s had no stop; applied the %.2f%% default at %.5f",
                intent.symbol,
                self.sl_pct,
                intent.sl,
            )

        risk = abs(intent.price - intent.sl)
        if intent.tp1 is None and risk > 0:
            intent.tp1 = intent.price + direction * risk * self.tp1_r
        if intent.tp2 is None and risk > 0:
            intent.tp2 = intent.price + direction * risk * self.tp2_r
        if intent.tp1_percent is None:
            intent.tp1_percent = self.tp1_percent
        if intent.move_sl_to_be is None:
            intent.move_sl_to_be = self.move_sl_to_be
        if intent.risk_percent is None and self.risk_percent is not None:
            intent.risk_percent = self.risk_percent
        return intent


class SignalFactory:
    """Builds :class:`BrokerSignal` objects and remembers open cycles."""

    def __init__(
        self,
        strategy_name: str,
        *,
        timeframe: str = "M15",
        token: str | None = None,
        bracket: BracketPolicy | None = None,
        inputs: dict[str, Any] | None = None,
    ) -> None:
        self.strategy_name = strategy_name
        self.timeframe = timeframe
        self.token = token if token is not None else settings.broker.token
        self.bracket = bracket or BracketPolicy()
        self.inputs = dict(inputs or {})
        #: symbol → cycle id of the position we believe is open on it.
        self._open_cycles: dict[str, str] = {}

    # ── Cycle bookkeeping ─────────────────────────────────────────────

    def open_cycle(self, symbol: str) -> str | None:
        return self._open_cycles.get(symbol)

    def restore_cycles(self, cycles: dict[str, str]) -> None:
        """Reload cycle ids from Redis after a runner restart."""
        self._open_cycles.update(cycles)

    def forget_cycle(self, symbol: str) -> None:
        self._open_cycles.pop(symbol, None)

    # ── Building ──────────────────────────────────────────────────────

    def build(
        self,
        intent: SignalIntent,
        *,
        symbol: str,
        moment: datetime,
        indicators: dict[str, Any] | None = None,
    ) -> BrokerSignal:
        """Materialise *intent* into the payload the broker ingests.

        Raises ``ValueError`` when the result would be a payload a worker
        cannot act on — an entry with no size, a close with no cycle to close.
        Failing here keeps a malformed signal off the wire entirely.
        """
        target_symbol = (intent.symbol or symbol).upper()
        if intent.action.is_entry:
            intent = self.bracket.apply(intent)
            uxid = intent.uxid or new_uxid()
            self._open_cycles[target_symbol] = uxid
        else:
            uxid = intent.uxid or self._open_cycles.get(target_symbol)
            if uxid is None:
                raise ValueError(
                    f"{intent.action.value} for {target_symbol} has no cycle to close — "
                    "the strategy emitted an exit with no matching entry"
                )
            if intent.action in (
                SignalAction.TP2,
                SignalAction.SL,
                SignalAction.R_SL,
                SignalAction.FLAT,
            ):
                self._open_cycles.pop(target_symbol, None)

        signal = BrokerSignal(
            strategy=self.strategy_name,
            symbol=target_symbol,
            timeframe=to_broker_timeframe(self.timeframe),
            timestamp=moment,
            signal_uxid=uxid,
            position=intent.to_position_block(),
            indicators={**(indicators or {}), **intent.indicators},
            inputs={**self.inputs, **intent.inputs},
            token=self.token,
        )
        signal.validate_shape()
        return signal
