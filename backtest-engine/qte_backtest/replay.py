"""The replay loop: parquet history in, filled trades and a report out.

Ordering inside one bar is what keeps a backtest honest, so it is fixed:

1. The bar's range is applied to whatever position is already open (stop and
   target checks).
2. The strategy sees history **up to and including** that bar and decides.
3. Its intents are filled at that bar's close, plus costs.

Step 2 never sees a future bar, and step 1 runs first so a position cannot be
closed by a decision made after the bar that would have stopped it out. Entries
fill at the signal bar's close rather than the next bar's open, which is the
common convention — it is slightly optimistic on a gap, and the gap handling in
:class:`~qte_backtest.execution.FillSimulator` is where that is paid back.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import pandas as pd
from qte_shared.logging_setup import get_logger
from qte_shared.models import BrokerSignal, SignalAction
from qte_shared.signal_factory import BracketPolicy, SignalFactory
from qte_shared.strategy_base import SignalIntent, StrategyBase, StrategyContext

from qte_backtest.execution import CostModel, ExitReason, FillSimulator, SimulatedPosition
from qte_backtest.metrics import BacktestMetrics, compute_metrics, format_report

log = get_logger(__name__)


@dataclass(slots=True)
class BacktestResult:
    strategy: str
    symbol: str
    timeframe: str
    metrics: BacktestMetrics
    positions: list[SimulatedPosition] = field(default_factory=list)
    signals: list[BrokerSignal] = field(default_factory=list)
    rejected: int = 0
    params: dict[str, Any] = field(default_factory=dict)

    def report(self) -> str:
        header = f"{self.strategy} — {self.symbol} {self.timeframe}"
        body = format_report(self.metrics, header)
        if self.rejected:
            body += f"\nRejected entries  {self.rejected} (position already open)"
        return body

    def trades_as_rows(self) -> list[dict[str, Any]]:
        """Shape the audit table's ``backtest_trades`` insert expects."""
        return [
            {
                "signal_uxid": position.signal_uxid,
                "symbol": position.symbol,
                "direction": "LONG" if position.direction == 1 else "SHORT",
                "opened_at": position.opened_at,
                "closed_at": position.closed_at,
                "entry_price": position.entry_price,
                "exit_price": position.exit_price,
                "quantity": position.quantity,
                "sl": position.sl,
                "tp1": position.tp1,
                "tp2": position.tp2,
                "exit_reason": position.exit_reason,
                "gross_pnl": position.gross_pnl,
                "fees": position.fees,
                "net_pnl": position.net_pnl,
            }
            for position in self.positions
            if position.legs
        ]


class BacktestEngine:
    """Drives one strategy over one symbol's history."""

    def __init__(
        self,
        strategy: StrategyBase,
        *,
        symbol: str,
        timeframe: str | None = None,
        costs: CostModel | None = None,
        starting_equity: float = 0.0,
        bracket: BracketPolicy | None = None,
        default_quantity: float = 1.0,
    ) -> None:
        self.strategy = strategy
        self.symbol = symbol.upper()
        self.timeframe = timeframe or strategy.timeframe
        self.simulator = FillSimulator(costs or CostModel())
        self.starting_equity = starting_equity
        self.default_quantity = default_quantity
        self.factory = SignalFactory(
            strategy.name,
            timeframe=self.timeframe,
            bracket=bracket,
            inputs=strategy.params,
        )
        self.positions: list[SimulatedPosition] = []
        self.signals: list[BrokerSignal] = []
        self._open: SimulatedPosition | None = None
        self._rejected = 0

    def run(self, frame: pd.DataFrame) -> BacktestResult:
        if frame.empty:
            raise ValueError(f"No history to replay for {self.symbol} {self.timeframe}")

        warmup = max(self.strategy.warmup, 1)
        if len(frame) <= warmup:
            raise ValueError(
                f"{len(frame)} bars is not enough for a strategy needing {warmup} of warm-up"
            )

        context = StrategyContext(
            symbol=self.symbol,
            timeframe=self.timeframe,
            now=_as_datetime(frame.index[warmup]),
            mode="backtest",
            params=self.strategy.params,
        )
        self.strategy.on_start(context)

        for position in range(warmup, len(frame)):
            bar_time = _as_datetime(frame.index[position])
            bar = frame.iloc[position]

            if self._open is not None and self._open.is_open:
                self.simulator.process_bar(self._open, bar, bar_time)
                if not self._open.is_open:
                    self.factory.forget_cycle(self.symbol)
                    self._open = None

            context.now = bar_time
            context.open_uxid = self.factory.open_cycle(self.symbol)
            window = frame.iloc[: position + 1]
            for intent in _as_intents(self.strategy.on_candle_closed(window, context)):
                self._apply(intent, bar_time, float(bar["close"]))

        # A position still open at the last bar is marked out at the final close
        # rather than dropped, so its unrealised P&L cannot silently flatter the
        # report by never being counted.
        if self._open is not None and self._open.is_open:
            self.simulator.close_at(
                self._open,
                _as_datetime(frame.index[-1]),
                float(frame.iloc[-1]["close"]),
                ExitReason.END_OF_DATA,
            )

        self.strategy.on_stop()
        return BacktestResult(
            strategy=self.strategy.name,
            symbol=self.symbol,
            timeframe=self.timeframe,
            metrics=compute_metrics(self.positions, self.starting_equity),
            positions=self.positions,
            signals=self.signals,
            rejected=self._rejected,
            params=dict(self.strategy.params),
        )

    # ── Intent handling ───────────────────────────────────────────────

    def _apply(self, intent: SignalIntent, bar_time: datetime, close: float) -> None:
        if intent.price is None:
            intent.price = close
        if intent.action.is_entry and intent.quantity is None:
            intent.quantity = self.default_quantity

        if intent.action.is_entry and self._open is not None and self._open.is_open:
            # The broker's workers refuse a second position on the same
            # symbol+strategy (they answer REJECTED), so a backtest that stacked
            # them would be scoring trades live trading will never take.
            self._rejected += 1
            log.debug("Rejected %s at %s — position already open", intent.action.value, bar_time)
            return

        try:
            signal = self.factory.build(intent, symbol=self.symbol, moment=bar_time)
        except ValueError as exc:
            log.warning("Dropped intent at %s: %s", bar_time, exc)
            return
        self.signals.append(signal)

        if intent.action.is_entry:
            self._open = self.simulator.open_position(
                symbol=self.symbol,
                action=intent.action,
                bar_time=bar_time,
                price=intent.price,
                quantity=intent.quantity or self.default_quantity,
                sl=intent.sl,
                tp1=intent.tp1,
                tp2=intent.tp2,
                tp1_percent=intent.tp1_percent,
                move_sl_to_be=bool(intent.move_sl_to_be),
                signal_uxid=signal.signal_uxid,
            )
            self.positions.append(self._open)
        elif self._open is not None and self._open.is_open:
            reason = _EXIT_REASONS.get(intent.action, ExitReason.FLAT)
            self.simulator.close_at(self._open, bar_time, intent.price, reason)
            if not self._open.is_open:
                self._open = None


_EXIT_REASONS = {
    SignalAction.TP1: ExitReason.TP1,
    SignalAction.TP2: ExitReason.TP2,
    SignalAction.SL: ExitReason.SL,
    SignalAction.R_SL: ExitReason.R_SL,
    SignalAction.FLAT: ExitReason.FLAT,
}


def _as_intents(result: Any) -> Sequence[SignalIntent]:
    if result is None:
        return ()
    if isinstance(result, SignalIntent):
        return (result,)
    return tuple(result)


def _as_datetime(value: Any) -> datetime:
    return pd.Timestamp(value).to_pydatetime()
