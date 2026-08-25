"""Serialising a backtest into a file an AI agent can actually reason over.

The design goal is that an agent handed nothing but this file can answer three
questions without asking for anything else:

1. **Is this result trustworthy?** — the ``run`` and ``data`` blocks say what
   was tested, over what, at what cost, and how much of the history warm-up ate.
2. **How did it do?** — ``metrics``, in currency *and* in R-multiples, plus the
   per-trade table with excursion so the aggregate can be re-derived rather than
   taken on faith.
3. **What is wrong and what should change?** — ``diagnostics``, where every
   finding carries the threshold it tripped, the numbers that tripped it, and
   one concrete change.

Two format decisions worth stating:

* **JSON is the primary artefact.** It is a file on disk, not a context window,
  so it carries *every* trade rather than a sample. An agent can filter it; a
  truncated file cannot be un-truncated.
* **Markdown is a companion, not a subset with different numbers.** It renders
  the same report object, aggregates first, and quotes only the most
  instructive individual trades — for when a human, or a small context window,
  needs the gist.
* **HTML is the same object again, drawn.** ``qte_backtest.visualize`` turns the
  JSON into a self-contained dashboard laid out like a strategy tester. It is
  rendered *from* the report rather than from the engine, so anything the chart
  can show, a script reading the file can compute — and a report kept from a run
  months ago still draws.

``schema_version`` is at the top of the JSON on purpose: a consumer that has
learned this shape should be able to detect that it changed.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from qte_shared.logging_setup import get_logger

from qte_backtest.diagnostics import CRITICAL, INFO, WARNING, DiagnosticContext, Finding, diagnose
from qte_backtest.execution import SimulatedPosition
from qte_backtest.replay import BacktestResult
from qte_backtest.visualize import render_html

log = get_logger(__name__)

#: Bump the major part when a consumer that understood the old shape would
#: misread the new one.
SCHEMA_VERSION = "1.1"

#: A compact orientation for whoever reads the JSON cold. It costs a few
#: hundred bytes and saves an agent from inferring the conventions — or, worse,
#: inferring them wrongly and reporting confident nonsense about the strategy.
READING_GUIDE = {
    "r_multiple": (
        "Profit and loss divided by the risk taken at entry (|entry - initial stop| x "
        "quantity x contract_size). Reason in R; the currency figures are scale, not "
        "signal. A trade whose stop was missing has r_multiple: null and is excluded "
        "from every R statistic."
    ),
    "mae_r_mfe_r": (
        "Maximum adverse / favourable excursion in R: how far price went against and for "
        "the trade while it was open. High MAE on winners means the stop is near the "
        "noise floor; high MFE on losers means the exits give back what the entries find."
    ),
    "fill_assumptions": (
        "Entries and exits cross the spread and pay slippage. When one bar's range covers "
        "both stop and target the STOP is taken, because tick ordering is unknown. A gap "
        "through a level fills at the bar open. Entries fill at the close of the signal "
        "bar. Read a good result as an upper bound on a bad fill model, not a promise."
    ),
    "exit_reasons": (
        "TP1/TP2 targets, SL stop, R_SL stop after it moved to breakeven, FLAT a "
        "discretionary close, END_OF_DATA the replay running out of bars with the "
        "position still open — the last one is usually a bug, not a trade."
    ),
    "single_position": (
        "The engine holds at most one position per symbol, mirroring the live worker, "
        "which answers REJECTED rather than stacking. rejected_entries counts signals "
        "dropped for that reason."
    ),
    "market_and_benchmark": (
        "The market block is a downsampled OHLC window kept so the run can be *drawn*: "
        "each row aggregates bucket_bars consecutive bars (first open, highest high, "
        "lowest low, last close). Never compute a statistic from it — every metric in "
        "this report comes from the full series. buy_hold is the same instrument held "
        "at the strategy's default size from the first bar after warm-up to the last, "
        "paying no spread, no slippage and no commission: the floor a strategy has to "
        "beat, not a like-for-like trade."
    ),
    "how_to_use_diagnostics": (
        "Findings are sorted most severe first. Each states the threshold it tripped so "
        "you can disagree with the threshold rather than the finding. Address critical "
        "findings before reading any performance number as meaningful."
    ),
}


@dataclass(slots=True)
class BacktestReport:
    """The whole report, renderable as JSON or Markdown."""

    result: BacktestResult
    findings: list[Finding] = field(default_factory=list)
    generated_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    # ── Assembly ──────────────────────────────────────────────────────

    def to_dict(self, include_signals: bool = True) -> dict[str, Any]:
        result = self.result
        metrics = result.metrics
        return {
            "schema_version": SCHEMA_VERSION,
            "generated_at": self.generated_at.isoformat(),
            "reading_guide": READING_GUIDE,
            "run": {
                "strategy": result.strategy,
                "symbol": result.symbol,
                "timeframe": result.timeframe,
                "params": result.params,
                "warmup_bars": result.warmup,
                "default_quantity": result.quantity,
                "starting_equity": result.starting_equity,
                "strategy_meta": result.strategy_meta,
            },
            "data": {
                "bars": result.bars,
                "bars_after_warmup": max(result.bars - result.warmup, 0),
                "first_bar": _iso(result.data_start),
                "last_bar": _iso(result.data_end),
                "gaps": result.data_gaps,
            },
            "market": _market_to_dict(result),
            "costs": {
                "spread": result.costs.spread,
                "slippage": result.costs.slippage,
                "commission_per_unit": result.costs.commission_per_unit,
                "contract_size": result.costs.contract_size,
                "round_trip_cost": round(result.costs.spread + 2 * result.costs.slippage, 8),
            },
            "metrics": metrics.to_dict(),
            "diagnostics": {
                "counts": self.severity_counts(),
                "findings": [finding.to_dict() for finding in self.findings],
            },
            "activity": {
                "trades_taken": metrics.trades,
                "rejected_entries": result.rejected,
                "signals_emitted": len(result.signals),
            },
            "trades": [
                _trade_to_dict(index, position)
                for index, position in enumerate(_closed(result.positions), start=1)
            ],
            # The exact broker payloads this run would have published. Keeping
            # them here is what makes a backtest report comparable against the
            # live audit trail row by row.
            "signals": (
                [signal.model_dump(mode="json") for signal in result.signals]
                if include_signals
                else []
            ),
        }

    def severity_counts(self) -> dict[str, int]:
        counts = {CRITICAL: 0, WARNING: 0, INFO: 0}
        for finding in self.findings:
            counts[finding.severity] += 1
        return counts

    @property
    def is_trustworthy(self) -> bool:
        """False when anything critical fired — read the metrics with suspicion."""
        return self.severity_counts()[CRITICAL] == 0

    # ── Rendering ─────────────────────────────────────────────────────

    def to_json(self, indent: int = 2, include_signals: bool = True) -> str:
        return json.dumps(self.to_dict(include_signals=include_signals), indent=indent, default=str)

    def to_html(self, title: str | None = None) -> str:
        """The dashboard, as one self-contained page.

        Rendered without the signal payloads: the charts never read them, and
        carrying every emitted signal would double the size of a file whose
        whole point is that you can open it anywhere.
        """
        return render_html(self.to_dict(include_signals=False), title=title)

    def to_markdown(self, max_trades: int = 10) -> str:
        result = self.result
        metrics = result.metrics
        counts = self.severity_counts()

        lines = [
            f"# Backtest — {result.strategy} on {result.symbol} {result.timeframe}",
            "",
            f"_Generated {self.generated_at:%Y-%m-%d %H:%M} UTC · schema {SCHEMA_VERSION}_",
            "",
        ]

        if not self.is_trustworthy:
            lines += [
                f"> **{counts[CRITICAL]} critical finding(s).** Resolve them before reading "
                "the numbers below as meaningful.",
                "",
            ]

        lines += [
            "## Result",
            "",
            "| | |",
            "| --- | --- |",
            f"| Period | {_short(result.data_start)} → {_short(result.data_end)} "
            f"({result.bars} bars, {result.warmup} of warm-up) |",
            f"| Trades | {metrics.trades} — {metrics.wins}W / {metrics.losses}L "
            f"({metrics.win_rate:.1f}%) |",
            f"| Net P&L | {metrics.net_pnl:,.2f} (fees {metrics.total_fees:,.2f}) |",
            f"| Expectancy | {_fmt(metrics.expectancy_r, '{:+.3f}R')} per trade "
            f"({metrics.expectancy:,.4f} currency) |",
            f"| Payoff ratio | {_fmt(metrics.payoff_ratio, '{:.2f}')} "
            f"(avg win {_fmt(metrics.average_win_r, '{:+.2f}R')}, "
            f"avg loss {_fmt(metrics.average_loss_r, '{:+.2f}R')}) |",
            f"| Profit factor | {_fmt(metrics.profit_factor, '{:.3f}', 'n/a (no losing trade)')} |",
            f"| Max drawdown | {metrics.max_drawdown:,.2f} "
            f"({_fmt(metrics.max_drawdown_pct, '{:.2f}%')}) |",
            f"| Worst streak | {metrics.max_consecutive_losses} losses |",
            f"| Exposure | {_fmt(metrics.exposure_pct, '{:.1f}%')} of bars, "
            f"avg {_fmt(metrics.average_bars_held, '{:.1f}')} bars per trade |",
            f"| Direction | {metrics.long_trades} long ({metrics.long_net_pnl:,.2f}) / "
            f"{metrics.short_trades} short ({metrics.short_net_pnl:,.2f}) |",
            f"| Excursion | avg MAE {_fmt(metrics.average_mae_r, '{:.2f}R')}, "
            f"avg MFE {_fmt(metrics.average_mfe_r, '{:.2f}R')} |",
            f"| Exits | {_exit_summary(metrics.exit_reasons)} |",
            f"| Rejected entries | {result.rejected} |",
            "",
        ]

        lines += ["## Diagnostics", ""]
        if not self.findings:
            lines += ["No findings. Every rule passed on this run.", ""]
        for finding in self.findings:
            lines += [
                f"### {_BADGE[finding.severity]} {finding.title}",
                "",
                f"`{finding.code}`",
                "",
                finding.detail,
                "",
                f"**Do this:** {finding.suggestion}",
                "",
                f"<sub>{_compact(finding.evidence)}</sub>",
                "",
            ]

        instructive = _most_instructive(_closed(result.positions), max_trades)
        if instructive:
            lines += [
                f"## Most instructive trades ({len(instructive)} of {metrics.trades})",
                "",
                "Largest winners and losers, and the trades whose excursion says the most.",
                "",
                "| # | Dir | Opened | Bars | Entry | Exit | Why | R | MAE | MFE |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
            for index, position in instructive:
                lines.append(
                    f"| {index} | {'L' if position.direction == 1 else 'S'} "
                    f"| {position.opened_at:%Y-%m-%d %H:%M} | {position.bars_held} "
                    f"| {position.entry_price:.5f} | {_fmt(position.exit_price, '{:.5f}')} "
                    f"| {position.exit_reason or '—'} "
                    f"| {_fmt(position.r_multiple, '{:+.2f}')} "
                    f"| {_fmt(position.mae_r, '{:.2f}')} "
                    f"| {_fmt(position.mfe_r, '{:.2f}')} |"
                )
            lines += ["", "_The full trade list is in the JSON report._", ""]

        return "\n".join(lines)

    # ── Writing ───────────────────────────────────────────────────────

    def write(
        self,
        directory: Path | str,
        *,
        stem: str | None = None,
        formats: Sequence[str] = ("json", "md"),
        include_signals: bool = True,
    ) -> list[Path]:
        """Write the report and return the paths written.

        The stem carries strategy, symbol, timeframe and a UTC timestamp, so
        repeated runs accumulate side by side instead of overwriting each other
        — comparing this run against the last one is the common case.
        """
        target = Path(directory)
        target.mkdir(parents=True, exist_ok=True)
        name = stem or (
            f"{self.result.strategy}_{self.result.symbol}_{self.result.timeframe}"
            f"_{self.generated_at:%Y%m%dT%H%M%SZ}"
        )
        written: list[Path] = []
        for fmt in formats:
            if fmt == "json":
                path = target / f"{name}.json"
                path.write_text(self.to_json(include_signals=include_signals), encoding="utf-8")
            elif fmt == "md":
                path = target / f"{name}.md"
                path.write_text(self.to_markdown(), encoding="utf-8")
            elif fmt == "html":
                path = target / f"{name}.html"
                path.write_text(self.to_html(), encoding="utf-8")
            else:
                raise ValueError(f"Unknown report format {fmt!r}; expected 'json', 'md' or 'html'")
            written.append(path)
            log.info("Wrote %s", path)
        return written


def build_report(result: BacktestResult) -> BacktestReport:
    """Run the diagnostics over *result* and wrap both into a report."""
    findings = diagnose(
        DiagnosticContext(
            metrics=result.metrics,
            positions=result.positions,
            costs=result.costs,
            total_bars=result.bars,
            warmup=result.warmup,
            rejected_entries=result.rejected,
            data_gaps=result.data_gaps,
        )
    )
    return BacktestReport(result=result, findings=findings)


# ── Helpers ───────────────────────────────────────────────────────────

_BADGE = {CRITICAL: "🔴", WARNING: "🟠", INFO: "🔵"}


def _closed(positions: Sequence[SimulatedPosition]) -> list[SimulatedPosition]:
    return [position for position in positions if position.legs]


def _trade_to_dict(index: int, position: SimulatedPosition) -> dict[str, Any]:
    """One trade, with enough detail to re-derive any aggregate in the report."""
    return {
        "index": index,
        "signal_uxid": position.signal_uxid,
        "symbol": position.symbol,
        "direction": "LONG" if position.direction == 1 else "SHORT",
        "opened_at": _iso(position.opened_at),
        "closed_at": _iso(position.closed_at),
        "bars_held": position.bars_held,
        "entry_price": position.entry_price,
        "exit_price": position.exit_price,
        "quantity": position.quantity,
        "initial_sl": position.initial_sl,
        "final_sl": position.sl,
        "tp1": position.tp1,
        "tp2": position.tp2,
        "initial_risk": position.initial_risk,
        "exit_reason": position.exit_reason,
        "gross_pnl": round(position.gross_pnl, 8),
        "fees": round(position.fees, 8),
        "net_pnl": round(position.net_pnl, 8),
        "r_multiple": _round(position.r_multiple, 4),
        "mae": round(position.mae, 8),
        "mfe": round(position.mfe, 8),
        "mae_r": _round(position.mae_r, 4),
        "mfe_r": _round(position.mfe_r, 4),
        # Partial fills matter: a trade that took TP1 and then stopped at
        # breakeven is a different lesson from one that ran to TP2.
        "legs": [
            {
                "closed_at": _iso(leg.closed_at),
                "reason": leg.reason.value,
                "price": leg.price,
                "quantity": leg.quantity,
                "gross_pnl": round(leg.gross_pnl, 8),
                "fees": round(leg.fees, 8),
            }
            for leg in position.legs
        ],
    }


def _market_to_dict(result: BacktestResult) -> dict[str, Any] | None:
    """The drawable window, plus what holding the instrument would have returned.

    ``None`` when the result was assembled without a frame in hand — the report
    stays valid, and a consumer that wanted to draw a price chart learns that
    there is nothing to draw rather than plotting an empty one.
    """
    market = result.market
    if market is None or not market.rows:
        return None

    size = result.quantity * result.costs.contract_size
    hold_pnl = (market.last_close - market.benchmark_close) * size
    return {
        "bucket_bars": market.bucket_bars,
        "columns": list(market.columns),
        "rows": market.rows,
        "buy_hold": {
            "quantity": result.quantity,
            "contract_size": result.costs.contract_size,
            "from": _iso(market.benchmark_from),
            "entry_close": market.benchmark_close,
            "exit_close": market.last_close,
            "net_pnl": round(hold_pnl, 8),
            "return_pct": (
                round(100.0 * hold_pnl / result.starting_equity, 4)
                if result.starting_equity
                else None
            ),
        },
    }


def _most_instructive(
    positions: Sequence[SimulatedPosition], limit: int
) -> list[tuple[int, SimulatedPosition]]:
    """Pick the trades a reviewer learns most from, not simply the first N.

    Biggest winners and losers explain the P&L; the loser with the highest MFE
    and the winner with the deepest MAE explain the *exits*, which is where the
    fixable problems usually are.
    """
    if not positions:
        return []
    indexed = list(enumerate(positions, start=1))
    by_pnl = sorted(indexed, key=lambda item: item[1].net_pnl)
    half = max(limit // 2, 1)

    chosen: dict[int, SimulatedPosition] = {}
    for index, position in by_pnl[:half] + by_pnl[-half:]:
        chosen[index] = position

    losers = [item for item in indexed if item[1].net_pnl < 0 and item[1].mfe_r is not None]
    winners = [item for item in indexed if item[1].net_pnl > 0 and item[1].mae_r is not None]
    if losers:
        index, position = max(losers, key=lambda item: item[1].mfe_r or 0.0)
        chosen[index] = position
    if winners:
        index, position = max(winners, key=lambda item: item[1].mae_r or 0.0)
        chosen[index] = position

    return sorted(chosen.items())[:limit]


def _exit_summary(reasons: dict[str, int]) -> str:
    if not reasons:
        return "—"
    return ", ".join(f"{name} {count}" for name, count in reasons.items())


def _compact(evidence: dict[str, Any]) -> str:
    return " · ".join(f"{key}={value}" for key, value in evidence.items())


def _fmt(value: Any, spec: str, fallback: str = "n/a") -> str:
    return fallback if value is None else spec.format(value)


def _round(value: float | None, digits: int) -> float | None:
    return None if value is None else round(value, digits)


def _short(value: datetime | None) -> str:
    """Date-and-time for the prose rendering; the JSON keeps full ISO."""
    return "n/a" if value is None else f"{value:%Y-%m-%d %H:%M}"


def _iso(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()
