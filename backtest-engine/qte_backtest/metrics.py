"""Turning a list of closed trades into a report worth trusting.

Every figure here is computed from realised, net-of-cost P&L. Two of them are
easy to get wrong and are spelled out on purpose:

* **Max drawdown** is measured on the running equity curve of closed trades,
  not on the worst single loss.
* **Sharpe** is annualised from *per-trade* returns using the observed trade
  frequency, not from a daily series. It is a comparison aid between runs of
  the same strategy, not a number to quote at anyone.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any

from qte_backtest.execution import SimulatedPosition


@dataclass(slots=True)
class BacktestMetrics:
    trades: int = 0
    wins: int = 0
    losses: int = 0
    win_rate: float = 0.0
    gross_profit: float = 0.0
    gross_loss: float = 0.0
    total_fees: float = 0.0
    net_pnl: float = 0.0
    profit_factor: float | None = None
    expectancy: float = 0.0
    average_win: float = 0.0
    average_loss: float = 0.0
    largest_win: float = 0.0
    largest_loss: float = 0.0
    max_drawdown: float = 0.0
    max_drawdown_pct: float | None = None
    sharpe: float | None = None
    max_consecutive_losses: int = 0
    period_start: datetime | None = None
    period_end: datetime | None = None
    equity_curve: list[float] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["period_start"] = self.period_start.isoformat() if self.period_start else None
        data["period_end"] = self.period_end.isoformat() if self.period_end else None
        return data


def compute_metrics(
    positions: Sequence[SimulatedPosition], starting_equity: float = 0.0
) -> BacktestMetrics:
    closed = [position for position in positions if position.legs]
    metrics = BacktestMetrics()
    if not closed:
        return metrics

    pnls = [position.net_pnl for position in closed]
    wins = [value for value in pnls if value > 0]
    losses = [value for value in pnls if value < 0]

    metrics.trades = len(closed)
    metrics.wins = len(wins)
    metrics.losses = len(losses)
    metrics.win_rate = round(100.0 * len(wins) / len(closed), 4)
    metrics.gross_profit = round(sum(wins), 6)
    metrics.gross_loss = round(sum(losses), 6)
    metrics.total_fees = round(sum(position.fees for position in closed), 6)
    metrics.net_pnl = round(sum(pnls), 6)
    metrics.average_win = round(sum(wins) / len(wins), 6) if wins else 0.0
    metrics.average_loss = round(sum(losses) / len(losses), 6) if losses else 0.0
    metrics.expectancy = round(metrics.net_pnl / len(closed), 6)
    metrics.largest_win = round(max(wins), 6) if wins else 0.0
    metrics.largest_loss = round(min(losses), 6) if losses else 0.0
    # Profit factor is undefined without a losing trade — reporting it as
    # "infinite" reads as a great result when it usually means too few trades.
    metrics.profit_factor = (
        round(metrics.gross_profit / abs(metrics.gross_loss), 6) if losses else None
    )

    equity = starting_equity
    curve = [equity]
    peak = equity
    max_drawdown = 0.0
    max_drawdown_pct: float | None = None
    streak = 0
    worst_streak = 0
    for value in pnls:
        equity += value
        curve.append(equity)
        peak = max(peak, equity)
        drawdown = peak - equity
        if drawdown > max_drawdown:
            max_drawdown = drawdown
            if peak > 0:
                max_drawdown_pct = round(100.0 * drawdown / peak, 4)
        streak = streak + 1 if value < 0 else 0
        worst_streak = max(worst_streak, streak)

    metrics.equity_curve = [round(point, 6) for point in curve]
    metrics.max_drawdown = round(max_drawdown, 6)
    metrics.max_drawdown_pct = max_drawdown_pct
    metrics.max_consecutive_losses = worst_streak
    metrics.period_start = closed[0].opened_at
    metrics.period_end = closed[-1].closed_at or closed[-1].opened_at
    metrics.sharpe = _sharpe(pnls, metrics.period_start, metrics.period_end)
    return metrics


def _sharpe(pnls: Sequence[float], start: datetime | None, end: datetime | None) -> float | None:
    """Per-trade Sharpe, scaled by how many trades a year the run implies."""
    if len(pnls) < 2 or start is None or end is None:
        return None
    mean = sum(pnls) / len(pnls)
    variance = sum((value - mean) ** 2 for value in pnls) / (len(pnls) - 1)
    if variance <= 0:
        return None
    days = max((end - start).total_seconds() / 86400.0, 1.0)
    trades_per_year = len(pnls) * 365.0 / days
    return round((mean / math.sqrt(variance)) * math.sqrt(trades_per_year), 4)


def format_report(metrics: BacktestMetrics, header: str = "") -> str:
    """Human-readable summary for the CLI."""
    if metrics.trades == 0:
        return f"{header}\nNo trades were taken."
    profit_factor = (
        "n/a (no losing trade)" if metrics.profit_factor is None else f"{metrics.profit_factor:.3f}"
    )
    drawdown_pct = "n/a" if metrics.max_drawdown_pct is None else f"{metrics.max_drawdown_pct:.2f}%"
    sharpe = "n/a" if metrics.sharpe is None else f"{metrics.sharpe:.3f}"
    lines = [
        header,
        "─" * max(len(header), 46),
        f"Period            {metrics.period_start:%Y-%m-%d} → {metrics.period_end:%Y-%m-%d}",
        f"Trades            {metrics.trades}  (W {metrics.wins} / L {metrics.losses})",
        f"Win rate          {metrics.win_rate:.2f}%",
        f"Net PnL           {metrics.net_pnl:,.2f}   (fees {metrics.total_fees:,.2f})",
        f"Profit factor     {profit_factor}",
        f"Expectancy/trade  {metrics.expectancy:,.4f}",
        f"Avg win / loss    {metrics.average_win:,.2f} / {metrics.average_loss:,.2f}",
        f"Max drawdown      {metrics.max_drawdown:,.2f}  ({drawdown_pct})",
        f"Max losing streak {metrics.max_consecutive_losses}",
        f"Sharpe (per-trade){sharpe:>12}",
    ]
    return "\n".join(line for line in lines if line)
