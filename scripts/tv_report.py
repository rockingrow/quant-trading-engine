"""Render a QTE backtest report into TradingView's Strategy Tester layout.

TradingView's tester exports four tab-separated tables and a trade list. The
QTE report carries the same run in a different shape, so comparing a port
against the chart it came from means eyeballing two documents that agree on
nothing but the numbers. This renders the QTE JSON into TV's own row labels and
column order, and -- given the TV export beside it -- writes the side-by-side.

    uv run python scripts/tv_report.py data/reports/RUN.json \
        --out data/reports/qte/mt5_gold_m5/365days/tv_parity \
        --tv data/reports/tv/mt5_gold_m5/365days

Rows TradingView computes from an intrabar equity curve (its intrabar drawdown
and run-up, margin, liquidation) have no QTE equivalent: the replay only knows
closed-trade equity. Those are written as ``n/a`` rather than silently filled
with the close-to-close figure, because a drawdown that ignores open positions
is a smaller number and would read as an improvement.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from datetime import datetime
from pathlib import Path
from typing import Any

NA = "n/a"


def _f(value: float | None, digits: int = 2) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return NA
    return f"{value:.{digits}f}"


def _pct(part: float | None, whole: float) -> str:
    if part is None or not whole:
        return NA
    return f"{100.0 * part / whole:.2f}"


def _ts(raw: str | None) -> datetime | None:
    return datetime.fromisoformat(raw) if raw else None


class Run:
    """The QTE report, plus the derived figures TradingView reports and we do not."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.data: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
        self.metrics = self.data["metrics"]
        self.trades = self.data["trades"]
        self.capital = float(self.data["run"]["starting_equity"])

        self.pnls = [float(t["net_pnl"]) for t in self.trades]
        self.bars = [int(t["bars_held"]) for t in self.trades]
        self.bars_win = [int(t["bars_held"]) for t in self.trades if float(t["net_pnl"]) > 0]
        self.bars_loss = [int(t["bars_held"]) for t in self.trades if float(t["net_pnl"]) < 0]

    @property
    def max_qty(self) -> float:
        return max((float(t["quantity"]) for t in self.trades), default=0.0)

    @property
    def sortino(self) -> float | None:
        """Per-trade Sortino: mean P&L over the downside deviation.

        TradingView annualises a daily series; this is per-trade, like the QTE
        report's own Sharpe. Comparable between QTE runs, indicative against TV.
        """
        if not self.pnls:
            return None
        downside = [min(pnl, 0.0) for pnl in self.pnls]
        deviation = math.sqrt(sum(value * value for value in downside) / len(downside))
        if not deviation:
            return None
        return statistics.fmean(self.pnls) / deviation * math.sqrt(len(self.pnls))

    @property
    def outliers(self) -> tuple[int, float]:
        """Trades beyond three standard deviations of P&L -- TV's "Outliers" rows."""
        if len(self.pnls) < 2:
            return 0, 0.0
        mean, deviation = statistics.fmean(self.pnls), statistics.pstdev(self.pnls)
        if not deviation:
            return 0, 0.0
        picked = [pnl for pnl in self.pnls if abs(pnl - mean) > 3 * deviation]
        return len(picked), sum(picked)

    @property
    def max_runup(self) -> float:
        """Largest close-to-close climb off a trough -- the mirror of max drawdown."""
        equity, trough, best = self.capital, self.capital, 0.0
        for pnl in self.pnls:
            equity += pnl
            trough = min(trough, equity)
            best = max(best, equity - trough)
        return best


def performance_rows(run: Run) -> list[tuple[str, str, str]]:
    metrics, capital = run.metrics, run.capital
    gross_profit = float(metrics["gross_profit"])
    gross_loss = abs(float(metrics["gross_loss"]))
    net = float(metrics["net_pnl"])
    largest_win, largest_loss = float(metrics["largest_win"]), abs(float(metrics["largest_loss"]))
    drawdown = float(metrics["max_drawdown"])

    start, end = _ts(metrics["period_start"]), _ts(metrics["period_end"])
    days = (end - start).days if start and end else 0
    cagr = (
        ((capital + net) / capital) ** (365.0 / days) - 1.0
        if days > 0 and capital + net > 0
        else None
    )

    return [
        ("Initial capital", _f(capital), ""),
        ("Open PnL", "0.00", "0.00"),
        ("Net profit", _f(net), _pct(net, capital)),
        ("Gross profit", _f(gross_profit), _pct(gross_profit, capital)),
        ("Gross loss", _f(gross_loss), _pct(gross_loss, capital)),
        ("Expected payoff", _f(float(metrics["expectancy"])), ""),
        ("Commission paid", _f(float(metrics["total_fees"])), ""),
        ("Buy and hold PnL", NA, NA),
        ("Max contracts held", _f(run.max_qty, 4), ""),
        ("Annualized return (CAGR)", "", _f(cagr * 100.0) if cagr is not None else NA),
        ("Return on initial capital", "", _pct(net, capital)),
        ("Max run-up (close-to-close)", _f(run.max_runup), _pct(run.max_runup, capital)),
        ("Max run-up (intrabar)", NA, NA),
        ("Max drawdown (close-to-close)", _f(drawdown), _f(metrics["max_drawdown_pct"])),
        ("Max drawdown (intrabar)", NA, NA),
        ("Return of max drawdown", _f(net / drawdown) if drawdown else NA, ""),
        ("Net PnL as % of largest loss", "", _pct(net, largest_loss) if largest_loss else NA),
        ("Largest profit as % of gross profit", "", _pct(largest_win, gross_profit)),
        ("Largest loss as % of gross loss", "", _pct(largest_loss, gross_loss)),
    ]


def trade_analysis_rows(run: Run) -> list[tuple[str, str, str]]:
    metrics = run.metrics
    count, wins, losses = int(metrics["trades"]), int(metrics["wins"]), int(metrics["losses"])
    average_win = float(metrics["average_win"])
    average_loss = abs(float(metrics["average_loss"]))
    expectancy = float(metrics["expectancy"])
    outlier_count, outlier_pnl = run.outliers
    return [
        ("Total open trades", "0", ""),
        ("Total trades", str(count), ""),
        ("Total winners", str(wins), ""),
        ("Total losers", str(losses), ""),
        ("Even trades", str(count - wins - losses), ""),
        ("Percent profitable", "", _f(float(metrics["win_rate"]))),
        ("Average PnL", _f(expectancy), _pct(expectancy, run.capital)),
        ("Average profit", _f(average_win), _pct(average_win, run.capital)),
        ("Average loss", _f(average_loss), _pct(average_loss, run.capital)),
        (
            "Average profit / average loss",
            _f(average_win / average_loss, 3) if average_loss else NA,
            "",
        ),
        ("Largest profit", _f(float(metrics["largest_win"])), ""),
        ("Largest loss", _f(abs(float(metrics["largest_loss"]))), ""),
        ("Outliers", str(outlier_count), ""),
        ("Outliers P&L", _f(outlier_pnl), _pct(outlier_pnl, run.capital)),
        ("Average bars in trades", _f(statistics.fmean(run.bars), 0) if run.bars else NA, ""),
        (
            "Average bars in winners",
            _f(statistics.fmean(run.bars_win), 0) if run.bars_win else NA,
            "",
        ),
        (
            "Average bars in losers",
            _f(statistics.fmean(run.bars_loss), 0) if run.bars_loss else NA,
            "",
        ),
        ("Max consecutive losers", str(int(metrics["max_consecutive_losses"])), ""),
        ("Max consecutive winners", str(int(metrics["max_consecutive_wins"])), ""),
    ]


def risk_rows(run: Run) -> list[tuple[str, str, str]]:
    metrics = run.metrics
    return [
        ("Sharpe ratio", _f(metrics["sharpe"], 3), ""),
        ("Sortino ratio", _f(run.sortino, 3), ""),
        ("Profit factor", _f(metrics["profit_factor"], 3), ""),
        ("Margin calls", "0", ""),
    ]


def properties_rows(run: Run) -> list[tuple[str, str]]:
    """The run's inputs in TV's ``name<TAB>value`` shape, plus the cost model."""
    data, costs, metrics = run.data, run.data["costs"], run.metrics
    rows = [
        ("Trading range", f"{metrics['period_start']} - {metrics['period_end']}"),
        ("Backtesting range", f"{data['data']['first_bar']} - {data['data']['last_bar']}"),
        ("Symbol", data["run"]["symbol"]),
        ("Timeframe", data["run"]["timeframe"]),
        ("Bars", str(data["data"]["bars"])),
        ("Warm-up bars", str(data["run"]["warmup_bars"])),
        ("Initial capital", _f(run.capital)),
        ("Commission (per unit, each side)", _f(float(costs["commission_per_unit"]), 4)),
        ("Spread (price units, full)", _f(float(costs["spread"]), 4)),
        ("Slippage (price units, per side)", _f(float(costs["slippage"]), 4)),
        ("Contract size", _f(float(costs["contract_size"]), 4)),
        ("Script execution", "On bar close"),
        ("Pyramiding", "1 orders"),
    ]
    rows += [(key, str(value)) for key, value in sorted(data["run"]["params"].items())]
    return rows


def trades_rows(run: Run) -> list[list[str]]:
    """TV's trade list: an entry row per trade, then one row per exit leg."""
    header = [
        "Trade number",
        "Type",
        "Date and time",
        "Signal",
        "Price USD",
        "Size (qty)",
        "Size (value)",
        "Net PnL USD",
        "Return %",
        "Commission USD",
        "Favorable excursion USD",
        "Adverse excursion USD",
        "Cumulative PnL USD",
        "Cumulative PnL %",
        "Duration (bars)",
    ]
    rows = [header]
    cumulative = 0.0
    for trade in run.trades:
        cumulative += float(trade["net_pnl"])
        side = "long" if trade["direction"] == "LONG" else "short"
        notional = float(trade["entry_price"]) * float(trade["quantity"])
        rows.append(
            [
                str(trade["index"]),
                f"Entry {side}",
                str(trade["opened_at"]),
                f"{trade['exit_reason']}|entry {trade['entry_price']}|sl {trade['initial_sl']}"
                f"|tp1 {trade['tp1']}|tp2 {trade['tp2']}",
                _f(float(trade["entry_price"]), 3),
                _f(float(trade["quantity"]), 4),
                _f(notional),
                _f(float(trade["net_pnl"])),
                _pct(float(trade["net_pnl"]), notional),
                _f(float(trade["fees"])),
                _f(float(trade["mfe"] or 0.0)),
                _f(float(trade["mae"] or 0.0)),
                _f(cumulative),
                _pct(cumulative, run.capital),
                str(trade["bars_held"]),
            ]
        )
        for leg in trade["legs"]:
            rows.append(
                [
                    str(trade["index"]),
                    f"Exit {side}",
                    str(leg["closed_at"]),
                    f"{leg['reason']}|{leg['price']}",
                    _f(float(leg["price"]), 3),
                    _f(float(leg["quantity"]), 4),
                    "",
                    _f(float(leg["gross_pnl"]) - float(leg["fees"])),
                    "",
                    _f(float(leg["fees"])),
                    "",
                    "",
                    "",
                    "",
                    "",
                ]
            )
    return rows


def read_tv_table(path: Path) -> dict[str, str]:
    """First data column of a TV export, keyed by row label, decimal mark undone.

    The reference exports were saved with a comma decimal mark and a dot
    thousands separator, so both are normalised before anything subtracts them.
    """
    if not path.exists():
        return {}
    table: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        cells = [cell.strip() for cell in line.split("\t")]
        if len(cells) < 2 or not cells[0]:
            continue
        # Ratios and percentages live in the second column with the first left
        # blank (TV's "All %"), so falling through to it is what keeps rows like
        # "Percent profitable" from comparing empty against empty.
        raw = cells[1] or (cells[2] if len(cells) > 2 else "")
        table[cells[0]] = raw.replace(".", "").replace(",", ".")
    return table


def tv_positions(tv_dir: Path) -> list[dict[str, Any]]:
    """Fold TradingView's leg-level trade list back into whole positions.

    This is the correction that makes the comparison mean anything. A strategy
    with a TP1 partial closes as *two* TV trades sharing one entry — the partial
    and the runner — each with its own trade number, its own size and its own
    P&L. Taken at face value the export claims twice the trades QTE reports,
    half the average win, and a third of the holding time, and the port looks
    broken when it agrees. Grouping on the entry timestamp undoes the split.
    """
    path = tv_dir / "trades.csv"
    if not path.exists():
        return []

    def number(raw: str) -> float:
        raw = raw.strip()
        return float(raw.replace(".", "").replace(",", ".")) if raw else 0.0

    legs: dict[str, dict[str, Any]] = {}
    with path.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle, delimiter="\t"):
            leg = legs.setdefault(row["Trade number"], {})
            leg["exit" if row["Type"].startswith("Exit") else "entry"] = row

    grouped: dict[str, list[dict[str, Any]]] = {}
    for leg in legs.values():
        if "entry" in leg and "exit" in leg:
            grouped.setdefault(leg["entry"]["Date and time"], []).append(leg)

    positions = []
    for opened, members in grouped.items():
        exits = [member["exit"] for member in members]
        positions.append(
            {
                "opened_at": opened,
                "net_pnl": sum(number(exit_["Net PnL USD"]) for exit_ in exits),
                "bars_held": max(int(number(exit_["Duration (bars)"]) or 0) for exit_ in exits),
                "reasons": sorted({exit_["Signal"].split("|")[0] for exit_ in exits}),
            }
        )
    return positions


def tv_position_rows(positions: list[dict[str, Any]], capital: float) -> list[tuple[str, str]]:
    """The same figures QTE reports, recomputed off whole TV positions."""
    pnls = [position["net_pnl"] for position in positions]
    wins = [pnl for pnl in pnls if pnl > 0]
    losses = [pnl for pnl in pnls if pnl < 0]
    gross_profit, gross_loss = sum(wins), abs(sum(losses))
    average_win = gross_profit / len(wins) if wins else 0.0
    average_loss = gross_loss / len(losses) if losses else 0.0

    equity, peak, drawdown = capital, capital, 0.0
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)

    return [
        ("Total trades", str(len(pnls))),
        ("Total winners", str(len(wins))),
        ("Total losers", str(len(losses))),
        ("Percent profitable", _pct(len(wins), len(pnls)) if pnls else NA),
        ("Net profit", _f(sum(pnls))),
        ("Gross profit", _f(gross_profit)),
        ("Gross loss", _f(gross_loss)),
        ("Profit factor", _f(gross_profit / gross_loss, 3) if gross_loss else NA),
        ("Average PnL", _f(sum(pnls) / len(pnls)) if pnls else NA),
        ("Average profit", _f(average_win)),
        ("Average loss", _f(average_loss)),
        (
            "Average profit / average loss",
            _f(average_win / average_loss, 3) if average_loss else NA,
        ),
        ("Largest profit", _f(max(pnls)) if pnls else NA),
        ("Largest loss", _f(abs(min(pnls))) if pnls else NA),
        ("Max drawdown (close-to-close)", _f(drawdown)),
        (
            "Average bars in trades",
            _f(statistics.fmean(p["bars_held"] for p in positions), 0) if positions else NA,
        ),
    ]


def comparison(run: Run, tv_dir: Path) -> str:
    theirs = {
        **read_tv_table(tv_dir / "performance.txt"),
        **read_tv_table(tv_dir / "trade-analysis.txt"),
        **read_tv_table(tv_dir / "Risk-adjusted-performance.txt"),
    }
    rows = performance_rows(run) + trade_analysis_rows(run) + risk_rows(run)
    ours = {label: value or percent for label, value, percent in rows}
    percents = {label: percent for label, _, percent in rows if percent not in ("", NA)}

    lines = [
        f"# {run.data['run']['strategy']} - QTE replay vs TradingView",
        "",
        f"_QTE report: `{run.path.name}` - TV export: `{tv_dir.as_posix()}`_",
        "",
    ]

    positions = tv_positions(tv_dir)
    if positions:
        lines += [
            "## Position for position",
            "",
            "TradingView's export counts *legs*: a trade with a TP1 partial appears twice, "
            "once for the partial and once for the runner. QTE counts whole positions. These "
            "are TV's legs folded back into positions, which is the only comparison where the "
            "two sides are counting the same thing.",
            "",
            "| Metric | TradingView | QTE | delta |",
            "| --- | ---: | ---: | ---: |",
        ]
        for label, their in tv_position_rows(positions, run.capital):
            value = ours.get(label, NA)
            try:
                delta = f"{float(value) - float(their):+.2f}"
            except (TypeError, ValueError):
                delta = NA
            lines.append(f"| {label} | {their} | {value} | {delta} |")
        lines.append("")

    lines += [
        "## As TradingView exported it",
        "",
        "Rows counting trades are leg-level on the TV side and position-level on ours; read "
        "the table above for those. Totals (net profit, gross profit/loss) are unaffected by "
        "the split and compare directly here.",
        "",
        "| Metric | TradingView | QTE | delta |",
        "| --- | ---: | ---: | ---: |",
    ]
    for label, value in ours.items():
        their = theirs.get(label)
        if their is None:
            continue
        try:
            delta = f"{float(value) - float(their):+.2f}"
        except (TypeError, ValueError):
            delta = NA
        lines.append(f"| {label} | {their} | {value} | {delta} |")

    lines += ["", "QTE percent column (on initial capital):", ""]
    lines += [f"- {label}: {percent}%" for label, percent in percents.items()]
    return "\n".join(lines) + "\n"


def write_table(path: Path, rows: list[tuple[str, ...]], header: tuple[str, ...]) -> None:
    body = ["\t".join(header)] + ["\t".join(row) for row in rows]
    path.write_text("\n".join(body) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path, help="The qte-backtest JSON report")
    parser.add_argument("--out", type=Path, required=True, help="Directory to write into")
    parser.add_argument("--tv", type=Path, default=None, help="TV export to compare against")
    args = parser.parse_args()

    run = Run(args.report)
    args.out.mkdir(parents=True, exist_ok=True)

    write_table(args.out / "performance.txt", performance_rows(run), ("", "All USD", "All %"))
    write_table(args.out / "trade-analysis.txt", trade_analysis_rows(run), ("", "All USD", "All %"))
    write_table(
        args.out / "Risk-adjusted-performance.txt", risk_rows(run), ("", "All USD", "All %")
    )
    write_table(args.out / "properties.txt", properties_rows(run), ("name", "value"))

    with (args.out / "trades.csv").open("w", newline="", encoding="utf-8") as handle:
        csv.writer(handle, delimiter="\t").writerows(trades_rows(run))

    if args.tv is not None:
        (args.out / "comparison.md").write_text(comparison(run, args.tv), encoding="utf-8")

    print(f"Wrote TradingView-shaped report into {args.out}")


if __name__ == "__main__":
    main()
