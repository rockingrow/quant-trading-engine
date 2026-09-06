"""Turning a backtest report into the numbers a dashboard draws.

Everything here is derived from the report JSON and nothing else. That is a
deliberate constraint: the dashboard has to be renderable from a file someone
kept from a run three months ago, on a machine with no parquet history and no
engine installed. If a panel needs a number the JSON does not carry, the fix is
to put it in the report — not to reach past the file.

The shape mirrors TradingView's Strategy Tester, because it is the layout every
discretionary trader already reads: four headline stats, one performance curve,
then two analysis blocks (performance, trades) that tab between views. Naming
follows theirs where the statistic is theirs, so a QTE report and a TV chart of
the same idea can be compared row by row rather than translated.

Two rules the derivations obey:

* **Nothing is invented.** A statistic that needs data the replay never had —
  intrabar equity, margin, liquidation — is absent, not approximated. TV's
  margin panel has no honest QTE equivalent, so there is no margin panel.
* **Every aggregate is re-derivable.** The trade list is the source; the panels
  are views over it. A reader who distrusts a number can recompute it from the
  same file.
"""

from __future__ import annotations

import math
import statistics
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

#: How many bins the returns histogram gets. Enough to show a shape, few enough
#: that a 40-trade run does not turn into a picket fence of single counts.
HISTOGRAM_BINS = 17

#: Trades this many inter-quartile ranges beyond the quartiles are outliers —
#: Tukey's fence, the same rule TradingView's "Outliers" row uses.
OUTLIER_FENCE = 1.5

#: The fallback fence, in median absolute deviations, for the degenerate case
#: where the quartiles coincide. Scaled by 1.4826 it is roughly three standard
#: deviations on a normal sample; P&L is not normal, which is the point of using
#: a median-based measure rather than a mean-based one.
MAD_FENCE = 3.0 * 1.4826


def build_view(report: dict[str, Any]) -> dict[str, Any]:
    """Derive every panel of the dashboard from *report*.

    The return value is JSON-serialisable on purpose: it is embedded verbatim in
    the HTML, so what the browser draws and what a script would compute from the
    report are the same object.
    """
    metrics = report.get("metrics") or {}
    run = report.get("run") or {}
    data = report.get("data") or {}
    costs = report.get("costs") or {}
    market = report.get("market")
    equity = float(run.get("starting_equity") or 0.0)
    contract = float(costs.get("contract_size") or 1.0)
    trades = [Trade(row, contract) for row in report.get("trades") or []]

    curve = _curve(trades, equity)
    benchmark = _benchmark(market, equity)

    return {
        "meta": _meta(report, run, data, metrics, trades),
        "headline": _headline(metrics, curve, equity),
        "curve": curve,
        "benchmark": benchmark,
        "market": _market(market, trades),
        "breakdown": _breakdown(metrics, trades),
        "periodical": _periodical(trades, metrics, curve, equity),
        "benchmarking": _benchmarking(curve, benchmark, market, equity, metrics),
        "growth": _growth(curve, equity),
        "distribution": _distribution(trades, metrics, equity),
        "streaks": _streaks(trades),
        "details": _details(trades),
        "excursion": _excursion(trades),
        "trades": [trade.row for trade in trades],
        "diagnostics": report.get("diagnostics") or {"counts": {}, "findings": []},
        "activity": report.get("activity") or {},
        "costs": costs,
        "reading_guide": report.get("reading_guide") or {},
    }


# ── The trade, as the panels need it ──────────────────────────────────


@dataclass(slots=True)
class Trade:
    """One row of ``report["trades"]``, with the parsing done once.

    ``contract_size`` comes from the run's cost model rather than the trade,
    because it is a property of the instrument. It is carried here so excursion
    can be shown in money: MAE and MFE are recorded in price units, and price
    units are not comparable across the panels that money is.
    """

    row: dict[str, Any]
    contract_size: float = 1.0

    @property
    def net(self) -> float:
        return float(self.row.get("net_pnl") or 0.0)

    @property
    def gross(self) -> float:
        return float(self.row.get("gross_pnl") or 0.0)

    @property
    def fees(self) -> float:
        return float(self.row.get("fees") or 0.0)

    @property
    def is_long(self) -> bool:
        return self.row.get("direction") == "LONG"

    @property
    def opened(self) -> datetime | None:
        return _parse(self.row.get("opened_at"))

    @property
    def closed(self) -> datetime | None:
        # A trade the replay never closed is still an event on the curve; it is
        # dated by its entry rather than dropped, which is what keeps the trade
        # count on the chart equal to the trade count in the table.
        return _parse(self.row.get("closed_at")) or self.opened

    @property
    def bars(self) -> int:
        return int(self.row.get("bars_held") or 0)

    @property
    def r(self) -> float | None:
        return _maybe_float(self.row.get("r_multiple"))

    @property
    def size(self) -> float:
        """Position size times contract size — what turns price into currency."""
        return float(self.row.get("quantity") or 0.0) * self.contract_size

    @property
    def mfe(self) -> float:
        """Best excursion in currency, always >= 0."""
        return abs(_maybe_float(self.row.get("mfe")) or 0.0) * self.size

    @property
    def mae(self) -> float:
        """Worst excursion in currency, always >= 0."""
        return abs(_maybe_float(self.row.get("mae")) or 0.0) * self.size


# ── Header, headline, curve ───────────────────────────────────────────


def _meta(
    report: dict[str, Any],
    run: dict[str, Any],
    data: dict[str, Any],
    metrics: dict[str, Any],
    trades: Sequence[Trade],
) -> dict[str, Any]:
    return {
        "strategy": run.get("strategy") or "unknown",
        "symbol": run.get("symbol") or "",
        "timeframe": run.get("timeframe") or "",
        "params": run.get("params") or {},
        "strategy_meta": run.get("strategy_meta") or {},
        "starting_equity": run.get("starting_equity"),
        "default_quantity": run.get("default_quantity"),
        "warmup_bars": run.get("warmup_bars"),
        "generated_at": report.get("generated_at"),
        "schema_version": report.get("schema_version"),
        "bars": data.get("bars"),
        "bars_after_warmup": data.get("bars_after_warmup"),
        "gaps": data.get("gaps"),
        "first_bar": data.get("first_bar"),
        "last_bar": data.get("last_bar"),
        "period_start": metrics.get("period_start"),
        "period_end": metrics.get("period_end"),
        "trading_days": _span_days(trades),
        "trustworthy": (report.get("diagnostics") or {}).get("counts", {}).get("critical", 0) == 0,
    }


def _headline(metrics: dict[str, Any], curve: dict[str, Any], equity: float) -> list[dict]:
    """The four figures TradingView puts above the fold, in its order.

    They are the four because between them they answer "did it make money, what
    did it cost me to hold, how often was it right, and how much does it win per
    unit lost". Any one of them alone is a way to be misled.
    """
    net = _maybe_float(metrics.get("net_pnl")) or 0.0
    drawdown = _maybe_float(metrics.get("max_drawdown")) or 0.0
    trades = int(metrics.get("trades") or 0)
    wins = int(metrics.get("wins") or 0)
    factor = _maybe_float(metrics.get("profit_factor"))
    return [
        {
            "label": "Total P&L",
            "value": net,
            "kind": "currency",
            "sub": _pct_of(net, equity),
            "tone": _tone(net),
        },
        {
            "label": "Max drawdown",
            "value": -abs(drawdown),
            "kind": "currency",
            "sub": _pct_of(-abs(drawdown), curve.get("peak_equity") or equity),
            "tone": "down" if drawdown else "flat",
        },
        {
            "label": "Profitable trades",
            "value": _maybe_float(metrics.get("win_rate")),
            "kind": "percent",
            "sub": f"{wins}/{trades}" if trades else None,
            "tone": "flat",
        },
        {
            "label": "Profit factor",
            "value": factor,
            "kind": "ratio",
            "sub": "no losing trade" if factor is None and trades else None,
            "tone": _tone((factor or 0.0) - 1.0) if factor is not None else "flat",
        },
    ]


def _curve(trades: Sequence[Trade], equity: float) -> dict[str, Any]:
    """The closed-trade equity curve, with per-point drawdown and excursion.

    This is the honest curve and it is worth being explicit about the limit:
    equity moves only when a trade closes. A position that was 3R underwater and
    recovered leaves no mark here, which is why the excursion series (MAE/MFE
    per trade, in currency) is drawn alongside it rather than instead of it.
    """
    points: list[dict[str, Any]] = []
    running = equity
    peak = equity
    trough = equity
    peak_equity = equity
    for index, trade in enumerate(trades, start=1):
        running += trade.net
        peak = max(peak, running)
        trough = min(trough, running)
        peak_equity = max(peak_equity, running)
        points.append(
            {
                "i": index,
                "t": _iso(trade.closed),
                "opened": _iso(trade.opened),
                "equity": round(running, 6),
                "pnl": round(trade.net, 6),
                "cum": round(running - equity, 6),
                "peak": round(peak, 6),
                "drawdown": round(running - peak, 6),
                "drawdown_pct": round(100.0 * (running - peak) / peak, 4) if peak else None,
                "runup": round(trade.mfe, 6),
                "adverse": round(-trade.mae, 6),
                "r": trade.r,
                "mae_r": _maybe_float(trade.row.get("mae_r")),
                "mfe_r": _maybe_float(trade.row.get("mfe_r")),
                "dir": "LONG" if trade.is_long else "SHORT",
                "reason": trade.row.get("exit_reason"),
                "bars": trade.bars,
            }
        )
    return {
        "start_equity": equity,
        "start_at": _iso(trades[0].opened) if trades else None,
        "points": points,
        "peak_equity": peak_equity,
        "trough_equity": trough,
        "final_equity": running,
    }


def _benchmark(market: dict[str, Any] | None, equity: float) -> dict[str, Any]:
    """Buy and hold, sampled at the market rows the report carries.

    The comparison a strategy has to win is not zero, it is the instrument. This
    line pays no spread and no commission, which makes it a floor rather than a
    fair fight — and that is the point of a floor.
    """
    if not market or not market.get("rows"):
        return {"available": False, "points": []}

    hold = market.get("buy_hold") or {}
    size = float(hold.get("quantity") or 0.0) * float(hold.get("contract_size") or 1.0)
    anchor = _maybe_float(hold.get("entry_close"))
    start = _parse(hold.get("from"))
    if anchor is None or not size:
        return {"available": False, "points": []}

    points = []
    for row in market["rows"]:
        moment = _parse(row[0])
        if start is not None and moment is not None and moment < start:
            continue
        close = float(row[4])
        points.append(
            {
                "t": row[0],
                "close": close,
                "equity": round(equity + (close - anchor) * size, 6),
                "pnl": round((close - anchor) * size, 6),
            }
        )
    return {
        "available": bool(points),
        "points": points,
        "net_pnl": _maybe_float(hold.get("net_pnl")),
        "return_pct": _maybe_float(hold.get("return_pct")),
        "size": size,
    }


def _market(market: dict[str, Any] | None, trades: Sequence[Trade]) -> dict[str, Any]:
    """The price window plus where each trade sat on it."""
    if not market or not market.get("rows"):
        return {"available": False, "rows": [], "trades": []}
    return {
        "available": True,
        "bucket_bars": market.get("bucket_bars"),
        "columns": market.get("columns"),
        "rows": market["rows"],
        "trades": [
            {
                "i": index,
                "dir": "LONG" if trade.is_long else "SHORT",
                "entry_t": _iso(trade.opened),
                "entry": _maybe_float(trade.row.get("entry_price")),
                "exit_t": _iso(trade.closed),
                "exit": _maybe_float(trade.row.get("exit_price")),
                "sl": _maybe_float(trade.row.get("initial_sl")),
                "tp1": _maybe_float(trade.row.get("tp1")),
                "tp2": _maybe_float(trade.row.get("tp2")),
                "pnl": round(trade.net, 6),
                "r": trade.r,
                "reason": trade.row.get("exit_reason"),
            }
            for index, trade in enumerate(trades, start=1)
        ],
    }


# ── Performance analysis ──────────────────────────────────────────────


def _breakdown(metrics: dict[str, Any], trades: Sequence[Trade]) -> dict[str, Any]:
    """Gross profit against gross loss, and where each side came from.

    TradingView groups its profit-and-loss bars by signal id. A QTE signal id is
    unique per trade, so grouping by it would draw one bar per trade and say
    nothing. The groupings that do carry information here are the exit that
    closed the trade, the direction it was taken in, and the month it happened
    in — each answers "which part of this strategy is the result".
    """
    gross_profit = _maybe_float(metrics.get("gross_profit")) or 0.0
    gross_loss = abs(_maybe_float(metrics.get("gross_loss")) or 0.0)
    fees = _maybe_float(metrics.get("total_fees")) or 0.0
    return {
        "gross_profit": gross_profit,
        "gross_loss": gross_loss,
        "profit_factor": _maybe_float(metrics.get("profit_factor")),
        "commission_load": round(100.0 * fees / gross_profit, 4) if gross_profit else None,
        "total_fees": fees,
        "net_pnl": _maybe_float(metrics.get("net_pnl")),
        "groups": {
            "By exit": _group(trades, lambda t: t.row.get("exit_reason") or "—"),
            "By direction": _group(trades, lambda t: "Long" if t.is_long else "Short"),
            "By month": _group(trades, lambda t: _key(t.closed, "month")),
        },
    }


def _group(trades: Sequence[Trade], key) -> list[dict[str, Any]]:
    buckets: dict[str, dict[str, Any]] = {}
    for trade in trades:
        name = str(key(trade))
        bucket = buckets.setdefault(
            name, {"name": name, "profit": 0.0, "loss": 0.0, "net": 0.0, "trades": 0, "wins": 0}
        )
        bucket["trades"] += 1
        bucket["net"] += trade.net
        if trade.net > 0:
            bucket["profit"] += trade.net
            bucket["wins"] += 1
        else:
            bucket["loss"] += abs(trade.net)
    rows = sorted(buckets.values(), key=lambda row: row["net"], reverse=True)
    for row in rows:
        for field in ("profit", "loss", "net"):
            row[field] = round(row[field], 6)
        row["win_rate"] = round(100.0 * row["wins"] / row["trades"], 2) if row["trades"] else None
    return rows


def _periodical(
    trades: Sequence[Trade],
    metrics: dict[str, Any],
    curve: dict[str, Any],
    equity: float,
) -> dict[str, Any]:
    """P&L bucketed by calendar period, plus the ratios that need a time span.

    Trades are dated by their **close**, because that is when the money moved.
    A trade opened in March and closed in April is April's result.
    """
    days = _span_days(trades) or 0
    final = curve.get("final_equity") or equity
    total_return = (100.0 * (final - equity) / equity) if equity else None
    cagr = None
    if equity > 0 and final > 0 and days >= 1:
        cagr = round(100.0 * ((final / equity) ** (365.0 / days) - 1.0), 4)

    return {
        "cagr": cagr,
        "total_return_pct": round(total_return, 4) if total_return is not None else None,
        "sharpe": _maybe_float(metrics.get("sharpe")),
        "sortino": _sortino([trade.net for trade in trades]),
        "buckets": {
            period: _bucket_pnl(trades, period) for period in ("day", "week", "month", "year")
        },
    }


def _bucket_pnl(trades: Sequence[Trade], period: str) -> list[dict[str, Any]]:
    buckets: dict[str, dict[str, Any]] = {}
    for trade in trades:
        name = _key(trade.closed, period)
        bucket = buckets.setdefault(
            name,
            {
                "t": name,
                "profit": 0.0,
                "loss": 0.0,
                "net": 0.0,
                "trades": 0,
                "mfe": 0.0,
                "mae": 0.0,
            },
        )
        bucket["trades"] += 1
        bucket["net"] += trade.net
        if trade.net >= 0:
            bucket["profit"] += trade.net
        else:
            bucket["loss"] += trade.net
        bucket["mfe"] += trade.mfe
        bucket["mae"] -= trade.mae
    rows = sorted(buckets.values(), key=lambda row: row["t"])
    for row in rows:
        for field in ("profit", "loss", "net", "mfe", "mae"):
            row[field] = round(row[field], 6)
    return rows


def _bucket_values(pairs: Iterable[tuple[str | None, float]], period: str) -> dict[str, float]:
    """Sum ``(timestamp, value)`` pairs into calendar buckets."""
    totals: dict[str, float] = {}
    for moment, value in pairs:
        key = _key(_parse(moment), period)
        totals[key] = totals.get(key, 0.0) + value
    return totals


def _benchmarking(
    curve: dict[str, Any],
    benchmark: dict[str, Any],
    market: dict[str, Any] | None,
    equity: float,
    metrics: dict[str, Any],
) -> dict[str, Any]:
    """Strategy against buy-and-hold, weekly, plus how alike the two are.

    Correlation is computed on weekly P&L, not on the equity levels: two rising
    curves correlate at 0.99 whatever they are made of, and the question this
    panel answers is whether the strategy is doing something other than being
    long the instrument.
    """
    if not benchmark.get("available"):
        return {"available": False}

    strategy_weeks = _bucket_values(
        ((point["t"], point["pnl"]) for point in curve["points"]), "week"
    )
    hold_weeks = _series_deltas(benchmark["points"], "week")

    weeks = sorted(set(strategy_weeks) | set(hold_weeks))
    series = [
        {
            "t": week,
            "strategy": round(strategy_weeks.get(week, 0.0), 6),
            "hold": round(hold_weeks.get(week, 0.0), 6),
        }
        for week in weeks
    ]

    strategy_return = (
        round(100.0 * ((curve.get("final_equity") or equity) - equity) / equity, 4)
        if equity
        else None
    )
    hold_return = benchmark.get("return_pct")
    return {
        "available": True,
        "strategy_return_pct": strategy_return,
        "hold_return_pct": hold_return,
        "outperformance_pct": (
            round(strategy_return - hold_return, 4)
            if strategy_return is not None and hold_return is not None
            else None
        ),
        "correlation": _correlation(
            [row["strategy"] for row in series], [row["hold"] for row in series]
        ),
        "series": series,
        "hold_net_pnl": benchmark.get("net_pnl"),
        "net_pnl": _maybe_float(metrics.get("net_pnl")),
    }


def _series_deltas(points: Sequence[dict[str, Any]], period: str) -> dict[str, float]:
    """Change in a sampled equity series, bucketed by calendar period."""
    deltas: dict[str, float] = {}
    previous: float | None = None
    for point in points:
        value = float(point["equity"])
        if previous is not None:
            key = _key(_parse(point["t"]), period)
            deltas[key] = deltas.get(key, 0.0) + (value - previous)
        previous = value
    return deltas


def _growth(curve: dict[str, Any], equity: float) -> dict[str, Any]:
    """Alternating run-ups and drawdowns, the way TradingView segments them.

    A drawdown episode opens at an equity peak and closes only when equity
    exceeds that peak again; the stretch of new highs between two episodes is a
    run-up. Segmenting on every reversal instead would produce one bar per trade
    and measure noise. The last segment is whatever is still in progress, which
    is why "current run-up" can equal "maximum run-up".
    """
    points = curve.get("points") or []
    if not points:
        return {"available": False, "segments": []}

    segments: list[dict[str, Any]] = []
    start_at = curve.get("start_at") or points[0]["t"]
    peak, peak_at = equity, start_at
    base, base_at = equity, start_at
    trough, trough_at = equity, start_at
    underwater = False

    def emit(kind: str, frm: str, to: str, amount: float, reference: float, ongoing: bool) -> None:
        segments.append(
            {
                "kind": kind,
                "from": frm,
                "to": to,
                "amount": round(amount, 6),
                "pct": round(100.0 * amount / reference, 4) if reference else None,
                "days": _days_between(frm, to),
                "ongoing": ongoing,
            }
        )

    for point in points:
        value = float(point["equity"])
        moment = point["t"]
        if value > peak:
            if underwater:
                emit("drawdown", peak_at, trough_at, peak - trough, peak, False)
                base, base_at = trough, trough_at
                underwater = False
            peak, peak_at = value, moment
        elif value < peak:
            if not underwater:
                emit("runup", base_at, peak_at, peak - base, base, False)
                underwater = True
                trough, trough_at = value, moment
            elif value < trough:
                trough, trough_at = value, moment

    last = points[-1]["t"]
    if underwater:
        emit("drawdown", peak_at, trough_at, peak - trough, peak, True)
    else:
        emit("runup", base_at, peak_at, peak - base, base, True)

    segments = [row for row in segments if row["amount"] > 0 or row["ongoing"]]
    runups = [row for row in segments if row["kind"] == "runup" and row["amount"] > 0]
    drawdowns = [row for row in segments if row["kind"] == "drawdown" and row["amount"] > 0]
    return {
        "available": True,
        "segments": segments,
        "last": last,
        "runup": _segment_stats(runups, segments),
        "drawdown": _segment_stats(drawdowns, segments),
    }


def _segment_stats(rows: Sequence[dict[str, Any]], all_rows: Sequence[dict]) -> dict[str, Any]:
    pcts = [row["pct"] for row in rows if row["pct"] is not None]
    days = [row["days"] for row in rows if row["days"] is not None]
    current = next((row for row in reversed(all_rows) if row["ongoing"]), None)
    kind = rows[0]["kind"] if rows else None
    return {
        "count": len(rows),
        "max_pct": round(max(pcts), 4) if pcts else None,
        "avg_pct": round(sum(pcts) / len(pcts), 4) if pcts else None,
        "max_amount": round(max(row["amount"] for row in rows), 6) if rows else None,
        "avg_days": round(sum(days) / len(days), 1) if days else None,
        "current_pct": current["pct"] if current and current["kind"] == kind else None,
    }


# ── Trades analysis ───────────────────────────────────────────────────


def _distribution(
    trades: Sequence[Trade], metrics: dict[str, Any], equity: float
) -> dict[str, Any]:
    """The histogram of per-trade results, and the three-way trade split.

    Returns are binned as a percentage of starting equity when there is one, so
    the axis reads the way a trader thinks about a trade ("that was a 0.4% day")
    rather than in an instrument's currency units.
    """
    pnls = [trade.net for trade in trades]
    scale = 100.0 / equity if equity else None
    values = [value * scale for value in pnls] if scale else list(pnls)

    bins: list[dict[str, Any]] = []
    if values:
        low, high = min(values), max(values)
        if math.isclose(low, high):
            low, high = low - 1.0, high + 1.0
        width = (high - low) / HISTOGRAM_BINS
        for index in range(HISTOGRAM_BINS):
            edge = low + index * width
            bins.append({"from": edge, "to": edge + width, "winners": 0, "losers": 0})
        for value, pnl in zip(values, pnls, strict=True):
            slot = min(int((value - low) / width), HISTOGRAM_BINS - 1)
            bins[slot]["winners" if pnl > 0 else "losers"] += 1

    wins = [value for value in pnls if value > 0]
    losses = [value for value in pnls if value < 0]
    outliers = _outliers(pnls)
    return {
        "unit": "percent" if scale else "currency",
        "bins": bins,
        "average_win": round(sum(wins) / len(wins) * (scale or 1.0), 6) if wins else None,
        "average_loss": round(sum(losses) / len(losses) * (scale or 1.0), 6) if losses else None,
        "expected_payoff": _maybe_float(metrics.get("expectancy")),
        "outlier_count": len(outliers),
        "outlier_pnl": round(sum(outliers), 6),
        "outlier_share_pct": (
            round(100.0 * sum(outliers) / sum(pnls), 2) if pnls and sum(pnls) else None
        ),
        "largest_profit": _maybe_float(metrics.get("largest_win")),
        "largest_loss": _maybe_float(metrics.get("largest_loss")),
        "split": {
            "winners": len(wins),
            "losers": len(losses),
            "breakeven": len(pnls) - len(wins) - len(losses),
            "total": len(pnls),
        },
    }


def _outliers(pnls: Sequence[float]) -> list[float]:
    """Trades outside Tukey's fence — the ones the average is not describing.

    Tukey alone has a blind spot that a strategy tester walks straight into: a
    fixed-bracket strategy whose trades all land on the same few values has an
    inter-quartile range of zero, and a zero-width fence reports the one trade
    that was ten times the size as perfectly ordinary. When the quartiles
    coincide the fence falls back to the median absolute deviation, and when
    that is zero too — every trade identical but a handful — anything away from
    the median is the outlier, because by then it is the only thing there is.
    """
    if len(pnls) < 4:
        return []
    ordered = sorted(pnls)
    first, third = _quantile(ordered, 0.25), _quantile(ordered, 0.75)
    spread = third - first
    if spread > 0:
        low, high = first - OUTLIER_FENCE * spread, third + OUTLIER_FENCE * spread
        return [value for value in pnls if value < low or value > high]

    middle = _quantile(ordered, 0.5)
    deviation = _quantile(sorted(abs(value - middle) for value in pnls), 0.5)
    if deviation > 0:
        reach = MAD_FENCE * deviation
        return [value for value in pnls if abs(value - middle) > reach]
    return [value for value in pnls if value != middle]


def _quantile(ordered: Sequence[float], fraction: float) -> float:
    position = (len(ordered) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[int(position)]
    return ordered[lower] * (upper - position) + ordered[upper] * (position - lower)


def _streaks(trades: Sequence[Trade]) -> dict[str, Any]:
    """Runs of winners and losers, in count and in money.

    The length of the worst run is what decides whether a strategy is
    survivable: expectancy says what it earns, the streak says what it asks you
    to sit through first.
    """
    runs: list[dict[str, Any]] = []
    for trade in trades:
        kind = "win" if trade.net > 0 else "loss"
        if runs and runs[-1]["kind"] == kind:
            runs[-1]["count"] += 1
            runs[-1]["amount"] += trade.net
            runs[-1]["to"] = _iso(trade.closed)
        else:
            runs.append(
                {
                    "kind": kind,
                    "count": 1,
                    "amount": trade.net,
                    "from": _iso(trade.closed),
                    "to": _iso(trade.closed),
                }
            )
    for run in runs:
        run["amount"] = round(run["amount"], 6)

    wins = [run["count"] for run in runs if run["kind"] == "win"]
    losses = [run["count"] for run in runs if run["kind"] == "loss"]
    return {
        "runs": runs,
        "longest_win": max(wins, default=0),
        "longest_loss": max(losses, default=0),
        "average_win": round(sum(wins) / len(wins), 1) if wins else None,
        "average_loss": round(sum(losses) / len(losses), 1) if losses else None,
    }


#: The rows of TradingView's "Trades analysis details" table, in its order. Each
#: is a callable over a subset of trades so All/Long/Short share one definition.
def _details(trades: Sequence[Trade]) -> dict[str, Any]:
    subsets = {
        "all": list(trades),
        "long": [trade for trade in trades if trade.is_long],
        "short": [trade for trade in trades if not trade.is_long],
    }
    gross_profit = sum(trade.net for trade in trades if trade.net > 0)
    gross_loss = abs(sum(trade.net for trade in trades if trade.net < 0))
    rows = [
        ("Total trades", "count", lambda t: len(t)),
        ("Total winners", "count", lambda t: sum(1 for x in t if x.net > 0)),
        ("Total losers", "count", lambda t: sum(1 for x in t if x.net < 0)),
        ("Percent profitable", "percent", _percent_profitable),
        ("Average P&L", "currency", lambda t: _mean([x.net for x in t])),
        ("Average profit", "currency", lambda t: _mean([x.net for x in t if x.net > 0])),
        ("Average loss", "currency", lambda t: _mean([x.net for x in t if x.net < 0])),
        ("Average profit / average loss", "ratio", _payoff),
        ("Largest profit", "currency", lambda t: max((x.net for x in t), default=None)),
        ("Largest loss", "currency", lambda t: min((x.net for x in t), default=None)),
        (
            "Largest profit as % of gross profit",
            "percent",
            lambda t: _share(max((x.net for x in t), default=0.0), gross_profit),
        ),
        (
            "Largest loss as % of gross loss",
            "percent",
            lambda t: _share(abs(min((x.net for x in t), default=0.0)), gross_loss),
        ),
        ("Outliers", "count", lambda t: len(_outliers([x.net for x in t]))),
        ("Outliers P&L", "currency", lambda t: sum(_outliers([x.net for x in t]))),
        ("Average bars in trades", "count", lambda t: _mean([x.bars for x in t])),
        ("Average bars in winners", "count", lambda t: _mean([x.bars for x in t if x.net > 0])),
        ("Average bars in losers", "count", lambda t: _mean([x.bars for x in t if x.net < 0])),
        ("Average R-multiple", "r", lambda t: _mean([x.r for x in t if x.r is not None])),
        ("Average MAE", "r", lambda t: _mean(_column(t, "mae_r"))),
        ("Average MFE", "r", lambda t: _mean(_column(t, "mfe_r"))),
        ("Total fees", "currency", lambda t: sum(x.fees for x in t)),
    ]
    return {
        "columns": ["Metric", "All", "Long", "Short"],
        "rows": [
            {
                "metric": name,
                "kind": kind,
                "all": _clean(fn(subsets["all"])),
                "long": _clean(fn(subsets["long"])),
                "short": _clean(fn(subsets["short"])),
            }
            for name, kind, fn in rows
        ],
    }


def _excursion(trades: Sequence[Trade]) -> dict[str, Any]:
    """What the excursion panel needs beyond the curve points it plots.

    The scatter and the R sequence are drawn from ``curve.points`` — they
    already carry ``r``, ``mae_r`` and ``mfe_r``. What the curve cannot say is
    how many trades have no R at all, which is the one number that decides
    whether the panel is describing the run or a subset of it.
    """
    return {"without_stop": sum(1 for trade in trades if trade.r is None)}


# ── Small numeric helpers ─────────────────────────────────────────────


def _percent_profitable(trades: Sequence[Trade]) -> float | None:
    if not trades:
        return None
    return 100.0 * sum(1 for trade in trades if trade.net > 0) / len(trades)


def _payoff(trades: Sequence[Trade]) -> float | None:
    wins = _mean([trade.net for trade in trades if trade.net > 0])
    losses = _mean([trade.net for trade in trades if trade.net < 0])
    if wins is None or losses is None or losses == 0:
        return None
    return wins / abs(losses)


def _share(part: float, whole: float) -> float | None:
    return 100.0 * part / whole if whole else None


def _column(trades: Sequence[Trade], field: str) -> list[float]:
    return [
        value
        for value in (_maybe_float(trade.row.get(field)) for trade in trades)
        if value is not None
    ]


def _mean(values: Sequence[float]) -> float | None:
    return sum(values) / len(values) if values else None


def _sortino(pnls: Sequence[float]) -> float | None:
    """Per-trade Sortino: mean P&L over the downside deviation.

    Per-trade like the report's Sharpe, and for the same reason — the replay
    knows trades, not a daily series. Comparable between QTE runs; indicative,
    not equivalent, against a platform that annualises a daily series.
    """
    if len(pnls) < 2:
        return None
    downside = [min(value, 0.0) for value in pnls]
    deviation = math.sqrt(sum(value * value for value in downside) / len(downside))
    if not deviation:
        return None
    return round(statistics.fmean(pnls) / deviation, 4)


def _correlation(left: Sequence[float], right: Sequence[float]) -> float | None:
    if len(left) < 3 or len(left) != len(right):
        return None
    try:
        return round(statistics.correlation(left, right), 4)
    except statistics.StatisticsError:
        # Constant series — a flat week against a flat week has no correlation
        # to report, and 0.0 would read as "unrelated" rather than "undefined".
        return None


def _tone(value: float | None) -> str:
    if value is None or value == 0:
        return "flat"
    return "up" if value > 0 else "down"


def _pct_of(part: float | None, whole: float | None) -> str | None:
    if part is None or not whole:
        return None
    return f"{100.0 * part / whole:.2f}%"


def _clean(value: Any) -> Any:
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return round(value, 6)
    return value


def _maybe_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(number) else number


def _parse(raw: Any) -> datetime | None:
    if not isinstance(raw, str):
        return None
    try:
        moment = datetime.fromisoformat(raw)
    except ValueError:
        return None
    return moment if moment.tzinfo else moment.replace(tzinfo=UTC)


def _iso(moment: datetime | None) -> str | None:
    return moment.isoformat() if moment else None


def _key(moment: datetime | None, period: str) -> str:
    """The label a period bucket is filed and sorted under.

    Sortable as a string on purpose: the dashboard orders buckets without
    re-parsing dates, and an ISO week label sorts correctly where "week 9 of
    2026" does not.
    """
    if moment is None:
        return "unknown"
    if period == "day":
        return moment.date().isoformat()
    if period == "week":
        monday = moment.date() - timedelta(days=moment.weekday())
        return monday.isoformat()
    if period == "month":
        return f"{moment.year:04d}-{moment.month:02d}"
    return f"{moment.year:04d}"


def _days_between(start: str | None, end: str | None) -> float | None:
    first, last = _parse(start), _parse(end)
    if first is None or last is None:
        return None
    return round(max((last - first).total_seconds(), 0.0) / 86400.0, 2)


def _span_days(trades: Sequence[Trade]) -> float | None:
    moments = [moment for moment in (trade.closed for trade in trades) if moment]
    opens = [moment for moment in (trade.opened for trade in trades) if moment]
    if not moments or not opens:
        return None
    return round(max((max(moments) - min(opens)).total_seconds(), 0.0) / 86400.0, 2)
