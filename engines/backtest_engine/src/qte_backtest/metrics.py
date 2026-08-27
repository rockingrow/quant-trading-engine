"""Turning a list of closed trades into a report worth trusting.

Every figure here is computed from realised, net-of-cost P&L. Two of them are
easy to get wrong and are spelled out on purpose:

* **Max drawdown** is measured on the running equity curve of closed trades,
  not on the worst single loss. The curve starts at the account's capital
  (``QTE_ACCOUNT__CAPITAL``), so ``max_drawdown_pct`` and ``return_pct`` are
  percentages *of a real balance* rather than of whatever the first trade
  happened to make.
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

from qte_backtest.execution import CostModel, SimulatedPosition


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
    #: The account this was traded on. ``ending_equity`` is
    #: ``starting_equity + net_pnl``; ``return_pct`` is that as a percentage,
    #: which is the only figure comparable across two runs sized differently.
    starting_equity: float = 0.0
    ending_equity: float = 0.0
    return_pct: float | None = None
    max_consecutive_losses: int = 0
    max_consecutive_wins: int = 0
    period_start: datetime | None = None
    period_end: datetime | None = None
    equity_curve: list[float] = field(default_factory=list)

    # ── Risk-normalised view ──────────────────────────────────────────
    # R-multiples are P&L divided by the risk taken at entry, so they compare
    # across instruments and position sizes. An agent reading this report should
    # reason in R and treat the currency figures as scale.
    total_r: float | None = None
    expectancy_r: float | None = None
    average_win_r: float | None = None
    average_loss_r: float | None = None
    payoff_ratio: float | None = None
    trades_without_stop: int = 0

    # ── What the broker took ──────────────────────────────────────────
    # Commission lands in ``total_fees``. Spread and slippage do not: they are
    # paid inside the fill prices, so they never appear as a line item and a
    # zero-commission run reads as a zero-cost run. They are the larger number
    # in most retail backtests. Both are here because a strategy is accepted or
    # rejected on its gross edge against total friction — never on net P&L,
    # which has already had the answer subtracted out of it.
    spread_slippage_cost: float = 0.0
    total_cost: float = 0.0
    #: Net P&L with every transaction cost added back. Distinct from
    #: ``gross_profit``, which is the sum of the winning trades.
    pnl_before_costs: float = 0.0
    #: Total cost as a percentage of ``pnl_before_costs``. Over 100 means the
    #: entries had an edge and the broker took all of it.
    cost_share_pct: float | None = None
    #: The same split per trade, in R. ``expectancy_r_before_costs`` is what
    #: the entries are worth; ``friction_r`` is what it costs to hold them.
    friction_r: float | None = None
    expectancy_r_before_costs: float | None = None

    # ── Excursion ─────────────────────────────────────────────────────
    # What price did while the trade was open. The gap between MFE and realised
    # P&L is where exit logic leaks money; MAE on winners is where the stop is
    # tighter than the strategy needs.
    average_mae_r: float | None = None
    average_mfe_r: float | None = None
    average_mae_r_winners: float | None = None
    average_mfe_r_losers: float | None = None

    # ── Shape of the trading ──────────────────────────────────────────
    exit_reasons: dict[str, int] = field(default_factory=dict)
    long_trades: int = 0
    short_trades: int = 0
    long_net_pnl: float = 0.0
    short_net_pnl: float = 0.0
    average_bars_held: float | None = None
    max_bars_held: int = 0
    bars_in_market: int = 0
    exposure_pct: float | None = None
    best_trade_share_pct: float | None = None

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["period_start"] = self.period_start.isoformat() if self.period_start else None
        data["period_end"] = self.period_end.isoformat() if self.period_end else None
        return data


def compute_metrics(
    positions: Sequence[SimulatedPosition],
    starting_equity: float = 0.0,
    total_bars: int = 0,
    costs: CostModel | None = None,
) -> BacktestMetrics:
    closed = [position for position in positions if position.legs]
    metrics = BacktestMetrics(
        starting_equity=round(starting_equity, 6), ending_equity=round(starting_equity, 6)
    )
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
    metrics.ending_equity = round(equity, 6)
    if starting_equity > 0:
        metrics.return_pct = round(100.0 * metrics.net_pnl / starting_equity, 4)
    metrics.max_consecutive_wins = _longest_run(pnls, winning=True)
    metrics.max_drawdown = round(max_drawdown, 6)
    metrics.max_drawdown_pct = max_drawdown_pct
    metrics.max_consecutive_losses = worst_streak
    metrics.period_start = closed[0].opened_at
    metrics.period_end = closed[-1].closed_at or closed[-1].opened_at
    metrics.sharpe = _sharpe(pnls, metrics.period_start, metrics.period_end)
    _add_risk_normalised(metrics, closed)
    _add_cost_economics(metrics, closed, costs or CostModel())
    _add_excursion(metrics, closed)
    _add_shape(metrics, closed, total_bars)
    return metrics


def _add_risk_normalised(metrics: BacktestMetrics, closed: Sequence[SimulatedPosition]) -> None:
    """R-multiple statistics, over the trades that actually had a stop."""
    r_values = [position.r_multiple for position in closed]
    metrics.trades_without_stop = sum(1 for value in r_values if value is None)
    usable = [value for value in r_values if value is not None]
    if not usable:
        return

    metrics.total_r = round(sum(usable), 4)
    metrics.expectancy_r = round(sum(usable) / len(usable), 4)
    wins = [value for value in usable if value > 0]
    losses = [value for value in usable if value < 0]
    if wins:
        metrics.average_win_r = round(sum(wins) / len(wins), 4)
    if losses:
        metrics.average_loss_r = round(sum(losses) / len(losses), 4)
    if wins and losses:
        # How many times bigger the average win is than the average loss. Read
        # it against win rate: 30% at 3:1 and 60% at 0.8:1 are both viable, and
        # neither number means anything on its own.
        metrics.payoff_ratio = round(metrics.average_win_r / abs(metrics.average_loss_r), 4)


def _add_cost_economics(
    metrics: BacktestMetrics, closed: Sequence[SimulatedPosition], costs: CostModel
) -> None:
    """Split realised P&L into the edge and what the broker charged for it.

    Spread and slippage are charged inside the fill prices, so unlike
    commission they leave no trace in the P&L they reduce. Reconstructing them
    is arithmetic rather than estimation: every position crosses the spread
    once and pays slippage on each side, whatever the exit legs did, so the
    round turn per unit is ``spread + 2 x slippage`` and the position's share
    of it is that times its size.

    The R figures use the same denominator ``r_multiple`` does, which makes
    ``expectancy_r == expectancy_r_before_costs - friction_r`` hold exactly and
    lets the two be compared against each other directly.
    """
    round_trip = costs.spread + 2.0 * costs.slippage
    friction = sum(
        abs(position.quantity) * position.contract_size * round_trip for position in closed
    )
    metrics.spread_slippage_cost = round(friction, 6)
    metrics.total_cost = round(friction + metrics.total_fees, 6)
    metrics.pnl_before_costs = round(metrics.net_pnl + metrics.total_cost, 6)
    if metrics.pnl_before_costs > 0:
        metrics.cost_share_pct = round(100.0 * metrics.total_cost / metrics.pnl_before_costs, 4)

    if metrics.expectancy_r is None:
        return
    shares: list[float] = []
    for position in closed:
        risk = position.initial_risk
        if not risk or not position.quantity:
            continue
        exposure = risk * abs(position.quantity) * position.contract_size
        cost = abs(position.quantity) * position.contract_size * round_trip + position.fees
        shares.append(cost / exposure)
    if not shares:
        return
    metrics.friction_r = round(sum(shares) / len(shares), 4)
    metrics.expectancy_r_before_costs = round(metrics.expectancy_r + metrics.friction_r, 4)


def _add_excursion(metrics: BacktestMetrics, closed: Sequence[SimulatedPosition]) -> None:
    mae = [position.mae_r for position in closed if position.mae_r is not None]
    mfe = [position.mfe_r for position in closed if position.mfe_r is not None]
    if mae:
        metrics.average_mae_r = round(sum(mae) / len(mae), 4)
    if mfe:
        metrics.average_mfe_r = round(sum(mfe) / len(mfe), 4)

    winner_mae = [p.mae_r for p in closed if p.net_pnl > 0 and p.mae_r is not None]
    loser_mfe = [p.mfe_r for p in closed if p.net_pnl < 0 and p.mfe_r is not None]
    if winner_mae:
        metrics.average_mae_r_winners = round(sum(winner_mae) / len(winner_mae), 4)
    if loser_mfe:
        metrics.average_mfe_r_losers = round(sum(loser_mfe) / len(loser_mfe), 4)


def _add_shape(
    metrics: BacktestMetrics, closed: Sequence[SimulatedPosition], total_bars: int
) -> None:
    reasons: dict[str, int] = {}
    for position in closed:
        for leg in position.legs:
            reasons[leg.reason.value] = reasons.get(leg.reason.value, 0) + 1
    metrics.exit_reasons = dict(sorted(reasons.items()))

    longs = [position for position in closed if position.direction == 1]
    shorts = [position for position in closed if position.direction == -1]
    metrics.long_trades = len(longs)
    metrics.short_trades = len(shorts)
    metrics.long_net_pnl = round(sum(position.net_pnl for position in longs), 6)
    metrics.short_net_pnl = round(sum(position.net_pnl for position in shorts), 6)

    held = [position.bars_held for position in closed]
    if held:
        metrics.average_bars_held = round(sum(held) / len(held), 2)
        metrics.max_bars_held = max(held)
        metrics.bars_in_market = sum(held)
        if total_bars > 0:
            metrics.exposure_pct = round(100.0 * sum(held) / total_bars, 3)

    # How much of the result rests on the single best trade. A strategy whose
    # net P&L is one outlier has not been demonstrated, it has been sampled.
    if metrics.net_pnl > 0 and metrics.largest_win > 0:
        metrics.best_trade_share_pct = round(100.0 * metrics.largest_win / metrics.net_pnl, 2)


def _longest_run(pnls: Sequence[float], *, winning: bool) -> int:
    streak = 0
    longest = 0
    for value in pnls:
        hit = value > 0 if winning else value < 0
        streak = streak + 1 if hit else 0
        longest = max(longest, streak)
    return longest


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
    # Spread and slippage are invisible in net P&L, so a run that pays no
    # commission reads as costing nothing. Print the split whenever there was
    # one to print, above the ratios it explains.
    cost_line = ""
    if metrics.total_cost > 0:
        share = (
            "" if metrics.cost_share_pct is None else f"  ({metrics.cost_share_pct:.0f}% of gross)"
        )
        cost_line = (
            f"Cost / gross P&L  {metrics.total_cost:,.2f} / {metrics.pnl_before_costs:,.2f}{share}"
        )
    edge_line = ""
    if metrics.friction_r is not None:
        edge_line = (
            f"Edge vs friction  {metrics.expectancy_r_before_costs:+.4f}R gross"
            f"  −{metrics.friction_r:.4f}R cost"
        )
    lines = [
        header,
        "─" * max(len(header), 46),
        f"Period            {metrics.period_start:%Y-%m-%d} → {metrics.period_end:%Y-%m-%d}",
        f"Trades            {metrics.trades}  (W {metrics.wins} / L {metrics.losses})",
        f"Win rate          {metrics.win_rate:.2f}%",
        f"Capital           {metrics.starting_equity:,.2f} → {metrics.ending_equity:,.2f}"
        f"   ({_pct(metrics.return_pct)})",
        f"Net PnL           {metrics.net_pnl:,.2f}   (fees {metrics.total_fees:,.2f})",
        cost_line,
        edge_line,
        f"Profit factor     {profit_factor}",
        f"Expectancy/trade  {metrics.expectancy:,.4f}",
        f"Avg win / loss    {metrics.average_win:,.2f} / {metrics.average_loss:,.2f}",
        f"Max drawdown      {metrics.max_drawdown:,.2f}  ({drawdown_pct})",
        f"Max losing streak {metrics.max_consecutive_losses}",
        f"Sharpe (per-trade){sharpe:>12}",
    ]
    return "\n".join(line for line in lines if line)


def _pct(value: float | None) -> str:
    return "n/a" if value is None else f"{value:+.2f}%"
