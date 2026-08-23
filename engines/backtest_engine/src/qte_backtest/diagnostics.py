"""Rules that read a finished backtest and say what is wrong with it.

The metrics answer "how did it do". These answer "should you believe it, and
what would you change" — which is what an agent reviewing a strategy actually
needs, and what a table of numbers does not supply on its own.

Every rule follows the same discipline:

* it fires on a **threshold stated in the finding**, so the reader can disagree
  with the threshold rather than having to reverse-engineer it;
* it carries the **numbers that triggered it** in ``evidence``, so the claim is
  checkable without re-running anything;
* it proposes **one concrete change**, not a direction to think about.

A rule that cannot meet those three is not a diagnostic, it is a vibe, and it
does not belong here. Severity means: ``critical`` — the result is not
trustworthy or the strategy is broken; ``warning`` — real, act on it;
``info`` — worth knowing, not necessarily wrong.
"""

from __future__ import annotations

import statistics
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from typing import Any

from qte_backtest.execution import CostModel, SimulatedPosition
from qte_backtest.metrics import BacktestMetrics

CRITICAL = "critical"
WARNING = "warning"
INFO = "info"

#: Below this many trades, none of the ratios mean anything. It is the usual
#: rule-of-thumb floor for a sample, not a guarantee at or above it.
MIN_TRADES_FOR_STATISTICS = 30


@dataclass(slots=True)
class Finding:
    """One diagnosed problem, with the evidence that produced it."""

    code: str
    severity: str
    title: str
    detail: str
    suggestion: str
    evidence: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class DiagnosticContext:
    """Everything the rules are allowed to look at."""

    metrics: BacktestMetrics
    positions: Sequence[SimulatedPosition]
    costs: CostModel
    total_bars: int
    warmup: int
    rejected_entries: int
    data_gaps: int = 0


def diagnose(context: DiagnosticContext) -> list[Finding]:
    """Run every rule and return the findings, most severe first."""
    findings: list[Finding] = []
    for rule in _RULES:
        finding = rule(context)
        if finding is not None:
            findings.append(finding)
    order = {CRITICAL: 0, WARNING: 1, INFO: 2}
    return sorted(findings, key=lambda item: (order[item.severity], item.code))


# ── Can this result be trusted at all? ────────────────────────────────


def _no_trades(context: DiagnosticContext) -> Finding | None:
    if context.metrics.trades > 0:
        return None
    return Finding(
        code="NO_TRADES",
        severity=CRITICAL,
        title="The strategy never opened a position",
        detail=(
            f"{context.total_bars} bars were replayed and no entry was taken. Either the "
            "entry condition never became true on this data, or an indicator stayed NaN "
            "and the guard clause returned early on every bar."
        ),
        suggestion=(
            "Log the entry condition's components on the last bar and check none of them "
            "is NaN. If the strategy needs N bars of warm-up, confirm the history is "
            "longer than N by a wide margin."
        ),
        evidence={"bars": context.total_bars, "warmup": context.warmup},
    )


def _sample_too_small(context: DiagnosticContext) -> Finding | None:
    trades = context.metrics.trades
    if trades == 0 or trades >= MIN_TRADES_FOR_STATISTICS:
        return None
    return Finding(
        code="SAMPLE_TOO_SMALL",
        severity=CRITICAL,
        title=f"{trades} trades is too few to conclude anything",
        detail=(
            f"Win rate, profit factor and expectancy are all computed over {trades} "
            f"trades, below the {MIN_TRADES_FOR_STATISTICS}-trade floor where the "
            "figures start to carry signal. Any of them could move sharply on one more "
            "trade."
        ),
        suggestion=(
            "Extend the history, loosen the entry filter, or test on more symbols before "
            "reading anything into these numbers. Do not tune parameters against this run."
        ),
        evidence={"trades": trades, "minimum": MIN_TRADES_FOR_STATISTICS},
    )


def _warmup_dominates(context: DiagnosticContext) -> Finding | None:
    if context.total_bars <= 0 or context.warmup <= 0:
        return None
    share = 100.0 * context.warmup / context.total_bars
    if share < 25.0:
        return None
    return Finding(
        code="WARMUP_DOMINATES",
        severity=WARNING if share < 50 else CRITICAL,
        title=f"Warm-up consumes {share:.0f}% of the available history",
        detail=(
            f"{context.warmup} of {context.total_bars} bars pass before the strategy is "
            "allowed to trade, so the effective test window is much shorter than the file "
            "suggests."
        ),
        suggestion=(
            "Download more history, or shorten the slowest indicator. A 200-period EMA on "
            "M15 needs roughly a month of data before its first valid value."
        ),
        evidence={
            "warmup_bars": context.warmup,
            "total_bars": context.total_bars,
            "share_pct": round(share, 2),
        },
    )


def _trades_without_stop(context: DiagnosticContext) -> Finding | None:
    missing = context.metrics.trades_without_stop
    if missing == 0:
        return None
    return Finding(
        code="TRADES_WITHOUT_STOP",
        severity=CRITICAL,
        title=f"{missing} trade(s) carried no usable stop",
        detail=(
            "Risk at entry was undefined for these trades, so their R-multiples are "
            "missing and every risk-normalised figure in this report is computed over a "
            "subset. Live, a worker would hold an unbounded position."
        ),
        suggestion=(
            "Make the strategy set `sl` on every entry. If it is meant to rely on the "
            "runner's BracketPolicy fallback, confirm that policy is configured — the "
            "backtest applies it too, so a missing stop here means it was skipped."
        ),
        evidence={"trades_without_stop": missing, "trades": context.metrics.trades},
    )


def _data_gaps(context: DiagnosticContext) -> Finding | None:
    if context.data_gaps <= 0:
        return None
    return Finding(
        code="DATA_GAPS",
        severity=INFO,
        title=f"{context.data_gaps} gap(s) in the bar series",
        detail=(
            "Consecutive bars were not one timeframe apart. Weekends and session breaks "
            "are expected on FX; anything else means missing history, and an indicator "
            "reading across a gap is comparing prices further apart than it thinks."
        ),
        suggestion=(
            "Check the gaps against the instrument's trading calendar. If they are not "
            "session breaks, re-download that period."
        ),
        evidence={"gaps": context.data_gaps, "bars": context.total_bars},
    )


# ── Wiring bugs ───────────────────────────────────────────────────────


def _exits_never_trigger(context: DiagnosticContext) -> Finding | None:
    reasons = context.metrics.exit_reasons
    total = sum(reasons.values())
    end_of_data = reasons.get("END_OF_DATA", 0)
    if total == 0 or end_of_data / total < 0.5:
        return None
    return Finding(
        code="EXITS_NEVER_TRIGGER",
        severity=CRITICAL,
        title=f"{end_of_data}/{total} exits were forced by the end of the data",
        detail=(
            "Most positions were still open when the replay ran out of bars, which means "
            "neither the stop nor the targets were ever reached. Levels that far from "
            "price are usually a units bug — an ATR multiple applied to a price, a "
            "percentage used as a fraction, or a stop set on the wrong side."
        ),
        suggestion=(
            "Print entry, sl, tp1 and tp2 for the first trade and check the distances "
            "against the instrument's typical bar range. A stop should be a small "
            "multiple of ATR, not a multiple of price."
        ),
        evidence={"end_of_data_exits": end_of_data, "total_exits": total},
    )


def _all_exits_are_stops(context: DiagnosticContext) -> Finding | None:
    reasons = context.metrics.exit_reasons
    total = sum(reasons.values())
    stops = reasons.get("SL", 0) + reasons.get("R_SL", 0)
    if total < 10 or stops / total < 0.9:
        return None
    return Finding(
        code="ALL_EXITS_ARE_STOPS",
        severity=WARNING,
        title=f"{stops}/{total} exits were stop-outs",
        detail=(
            "Targets are almost never reached, so the reward side of the plan is "
            "theoretical. Either the targets sit beyond what the instrument moves in the "
            "holding period, or the stop is inside normal noise and gets hit first."
        ),
        suggestion=(
            "Compare the target distance to the average MFE in this report. If MFE rarely "
            "reaches 1R, the targets are unreachable rather than the entries being wrong."
        ),
        evidence={"stop_exits": stops, "total_exits": total, "exit_reasons": reasons},
    )


def _tp2_never_reached(context: DiagnosticContext) -> Finding | None:
    with_tp2 = [position for position in context.positions if position.tp2 is not None]
    if len(with_tp2) < 10 or context.metrics.exit_reasons.get("TP2", 0) > 0:
        return None
    return Finding(
        code="TP2_NEVER_REACHED",
        severity=WARNING,
        title="The second target was never hit",
        detail=(
            f"{len(with_tp2)} trades carried a tp2 and none of them reached it. The runner "
            "portion of every trade ended some other way, so tp2 is currently decoration."
        ),
        suggestion=(
            "Either pull tp2 in to something the average MFE actually reaches, or drop it "
            "and trail the runner instead."
        ),
        evidence={"trades_with_tp2": len(with_tp2), "tp2_hits": 0},
    )


def _entries_rejected(context: DiagnosticContext) -> Finding | None:
    rejected = context.rejected_entries
    attempted = context.metrics.trades + rejected
    if attempted == 0 or rejected / attempted < 0.3:
        return None
    return Finding(
        code="ENTRIES_REJECTED",
        severity=WARNING,
        title=f"{rejected} entry signals were dropped because a position was already open",
        detail=(
            f"{100.0 * rejected / attempted:.0f}% of the strategy's entry signals could not "
            "be taken. The live worker behaves the same way — it answers REJECTED rather "
            "than stacking — so these signals are noise on the wire, not missed trades."
        ),
        suggestion=(
            "Gate the entry on `context.open_uxid is None` so the strategy stops signalling "
            "while it is in a trade. If the intent was to add to the position, that needs "
            "an explicit scale-in (`is_scale_position`), which this engine does not "
            "simulate."
        ),
        evidence={"rejected": rejected, "taken": context.metrics.trades},
    )


def _one_sided(context: DiagnosticContext) -> Finding | None:
    metrics = context.metrics
    if metrics.trades < MIN_TRADES_FOR_STATISTICS:
        return None
    if metrics.long_trades > 0 and metrics.short_trades > 0:
        return None
    side = "long" if metrics.long_trades else "short"
    return Finding(
        code="ONE_SIDED",
        severity=WARNING,
        title=f"Every trade was {side}",
        detail=(
            f"Across {metrics.trades} trades the strategy never took the other side. That "
            "is correct for a deliberately long-only strategy and a bug otherwise — most "
            "often a trend filter that can only be satisfied in one direction, or a "
            "crossunder branch that is never reached."
        ),
        suggestion=(
            "If both directions are intended, assert the short condition in isolation on a "
            "period where price fell. If long-only is intended, say so in the docstring so "
            "this finding can be ignored on sight."
        ),
        evidence={"long_trades": metrics.long_trades, "short_trades": metrics.short_trades},
    )


def _trades_last_one_bar(context: DiagnosticContext) -> Finding | None:
    average = context.metrics.average_bars_held
    if context.metrics.trades < 10 or average is None or average > 1.5:
        return None
    return Finding(
        code="TRADES_LAST_ONE_BAR",
        severity=WARNING,
        title=f"Positions last {average:.1f} bars on average",
        detail=(
            "Trades open and close within a bar or two, so the stop sits inside a single "
            "bar's range. At that scale the result is decided by intrabar path, which this "
            "simulator cannot see — it assumes the stop is hit before the target, and that "
            "assumption dominates the outcome."
        ),
        suggestion=(
            "Widen the stop relative to ATR, or move to a lower timeframe where the same "
            "distance spans several bars. Treat the current P&L as unreliable rather than "
            "pessimistic."
        ),
        evidence={"average_bars_held": average, "max_bars_held": context.metrics.max_bars_held},
    )


def _stop_inside_costs(context: DiagnosticContext) -> Finding | None:
    risks = [position.initial_risk for position in context.positions]
    usable = [risk for risk in risks if risk]
    round_trip = context.costs.spread + 2 * context.costs.slippage
    if not usable or round_trip <= 0:
        return None
    median_risk = statistics.median(usable)
    ratio = median_risk / round_trip
    if ratio >= 3.0:
        return None
    return Finding(
        code="STOP_INSIDE_COSTS",
        severity=CRITICAL if ratio < 1.5 else WARNING,
        title=f"The stop is only {ratio:.1f}× the round-trip cost",
        detail=(
            f"Median risk per trade is {median_risk:.5f} while crossing the spread and "
            f"paying slippage costs {round_trip:.5f} per round trip. A large share of every "
            "winner is handed straight back to costs, and the edge has to be enormous to "
            "survive that."
        ),
        suggestion=(
            "Widen the stop, trade a larger timeframe, or check that the --spread you "
            "passed matches your broker. Below about 3× the round trip, cost modelling "
            "errors swamp the strategy's own signal."
        ),
        evidence={
            "median_initial_risk": round(median_risk, 6),
            "round_trip_cost": round(round_trip, 6),
            "ratio": round(ratio, 2),
        },
    )


# ── Economics and where the edge leaks ────────────────────────────────


def _costs_exceed_edge(context: DiagnosticContext) -> Finding | None:
    metrics = context.metrics
    if metrics.trades == 0 or metrics.total_fees <= 0:
        return None
    if metrics.total_fees < metrics.gross_profit:
        return None
    return Finding(
        code="COSTS_EXCEED_EDGE",
        severity=WARNING,
        title="Fees alone exceed the gross profit",
        detail=(
            f"Gross profit was {metrics.gross_profit:,.2f} and fees {metrics.total_fees:,.2f}. "
            "Whatever edge the entries have is being consumed by transaction costs before "
            "the losing trades are even counted."
        ),
        suggestion=(
            "Trade less often (tighter filter), hold longer per trade, or move to an "
            "instrument with lower per-unit commission. Parameter tuning will not fix a "
            "cost problem."
        ),
        evidence={
            "gross_profit": metrics.gross_profit,
            "total_fees": metrics.total_fees,
            "trades": metrics.trades,
        },
    )


def _negative_expectancy(context: DiagnosticContext) -> Finding | None:
    metrics = context.metrics
    if metrics.trades < MIN_TRADES_FOR_STATISTICS or metrics.expectancy_r is None:
        return None
    if metrics.expectancy_r >= 0:
        return None
    return Finding(
        code="NEGATIVE_EXPECTANCY",
        severity=WARNING,
        title=f"Expectancy is {metrics.expectancy_r:+.3f}R per trade",
        detail=(
            f"Over {metrics.trades} trades the strategy loses "
            f"{abs(metrics.expectancy_r):.3f}R on average. Win rate "
            f"{metrics.win_rate:.1f}% and payoff ratio "
            f"{metrics.payoff_ratio if metrics.payoff_ratio is not None else float('nan'):.2f} "
            "do not combine into a positive edge."
        ),
        suggestion=(
            "Raise the payoff ratio or the win rate — the two figures above say which one "
            "is short. Widening targets lifts payoff and lowers win rate; a stricter entry "
            "filter usually does the opposite."
        ),
        evidence={
            "expectancy_r": metrics.expectancy_r,
            "win_rate": metrics.win_rate,
            "payoff_ratio": metrics.payoff_ratio,
        },
    )


def _result_concentrated(context: DiagnosticContext) -> Finding | None:
    share = context.metrics.best_trade_share_pct
    if share is None or share < 50.0 or context.metrics.trades < 10:
        return None
    return Finding(
        code="RESULT_CONCENTRATED",
        severity=WARNING,
        title=f"One trade produced {share:.0f}% of the net result",
        detail=(
            "The strategy's profitability rests on a single outcome. Remove that trade and "
            "the run is roughly flat or negative, which means this has not been "
            "demonstrated — it has been sampled once."
        ),
        suggestion=(
            "Re-run on a different period and check whether the result survives without "
            "its outlier. If the strategy is deliberately built around rare large winners, "
            "it needs far more trades before any conclusion holds."
        ),
        evidence={
            "largest_win": context.metrics.largest_win,
            "net_pnl": context.metrics.net_pnl,
            "share_pct": share,
        },
    )


def _drawdown_exceeds_profit(context: DiagnosticContext) -> Finding | None:
    metrics = context.metrics
    if metrics.trades == 0 or metrics.net_pnl <= 0:
        return None
    if metrics.max_drawdown <= metrics.net_pnl:
        return None
    return Finding(
        code="DRAWDOWN_EXCEEDS_PROFIT",
        severity=WARNING,
        title="Peak drawdown is larger than the whole net profit",
        detail=(
            f"The equity curve gave back {metrics.max_drawdown:,.2f} at its worst while "
            f"ending {metrics.net_pnl:,.2f} ahead. The strategy is profitable on paper and "
            "hard to hold through in practice."
        ),
        suggestion=(
            "Look at the losing streak of "
            f"{metrics.max_consecutive_losses} trades: cutting risk per trade does not "
            "change this ratio, only its scale. A filter that avoids the regime driving "
            "that streak does."
        ),
        evidence={
            "max_drawdown": metrics.max_drawdown,
            "net_pnl": metrics.net_pnl,
            "max_consecutive_losses": metrics.max_consecutive_losses,
        },
    )


def _stop_too_tight(context: DiagnosticContext) -> Finding | None:
    winners_mae = context.metrics.average_mae_r_winners
    if winners_mae is None or context.metrics.wins < 10 or winners_mae < 0.6:
        return None
    return Finding(
        code="STOP_TOO_TIGHT",
        severity=INFO,
        title=f"Winning trades go {winners_mae:.2f}R against you before working",
        detail=(
            "Trades that eventually won spent most of their risk budget under water first. "
            "That means the entries are early rather than wrong, and that losers sitting "
            "just past the stop are likely the same trade with slightly worse timing."
        ),
        suggestion=(
            f"Test a stop {1.0 / max(winners_mae, 0.01):.1f}× wider with position size "
            "reduced to keep risk per trade constant. If win rate rises more than payoff "
            "falls, the stop was the binding constraint."
        ),
        evidence={"average_mae_r_winners": winners_mae, "wins": context.metrics.wins},
    )


def _losers_were_winners(context: DiagnosticContext) -> Finding | None:
    losers_mfe = context.metrics.average_mfe_r_losers
    if losers_mfe is None or context.metrics.losses < 10 or losers_mfe < 1.0:
        return None
    return Finding(
        code="LOSERS_WERE_WINNERS",
        severity=WARNING,
        title=f"Losing trades reached {losers_mfe:.2f}R in profit before losing",
        detail=(
            "On average the losers were more than one full R in profit at some point and "
            "gave all of it back. This is an exit problem, not an entry problem — the "
            "entries were right and the plan did not bank anything."
        ),
        suggestion=(
            "Move the stop to breakeven once price reaches 1R (`move_sl_to_be=True` with a "
            "tp1 at 1R), or take a partial there. Both convert a share of these losses to "
            "scratches without touching the entry logic."
        ),
        evidence={
            "average_mfe_r_losers": losers_mfe,
            "losses": context.metrics.losses,
            "move_sl_to_be_used": any(position.move_sl_to_be for position in context.positions),
        },
    )


def _exits_leave_money(context: DiagnosticContext) -> Finding | None:
    metrics = context.metrics
    if metrics.average_mfe_r is None or metrics.expectancy_r is None:
        return None
    if metrics.wins < 10 or metrics.average_win_r is None:
        return None
    if metrics.average_mfe_r < metrics.average_win_r * 2.0:
        return None
    return Finding(
        code="EXITS_LEAVE_MONEY",
        severity=INFO,
        title=(
            f"Average favourable excursion is {metrics.average_mfe_r:.2f}R but the average "
            f"win banks {metrics.average_win_r:.2f}R"
        ),
        detail=(
            "Trades routinely reach more than twice what they are eventually closed for. "
            "The entries find moves the exits do not capture."
        ),
        suggestion=(
            "Push tp2 further out and let the runner ride, or trail the stop behind "
            "structure once tp1 fills. Compare the two on the same data before choosing."
        ),
        evidence={
            "average_mfe_r": metrics.average_mfe_r,
            "average_win_r": metrics.average_win_r,
        },
    )


def _always_in_market(context: DiagnosticContext) -> Finding | None:
    exposure = context.metrics.exposure_pct
    if exposure is None or exposure < 90.0:
        return None
    return Finding(
        code="ALWAYS_IN_MARKET",
        severity=INFO,
        title=f"The strategy holds a position {exposure:.0f}% of the time",
        detail=(
            "There is effectively no flat state, so any entry filter in the strategy is not "
            "binding — the result is close to a buy-and-hold of whatever direction it "
            "favours."
        ),
        suggestion=(
            "Compare against holding the instrument outright over the same period. If the "
            "strategy does not beat that, the filters are the part to work on."
        ),
        evidence={"exposure_pct": exposure, "bars_in_market": context.metrics.bars_in_market},
    )


_RULES = (
    _no_trades,
    _sample_too_small,
    _warmup_dominates,
    _trades_without_stop,
    _data_gaps,
    _exits_never_trigger,
    _all_exits_are_stops,
    _tp2_never_reached,
    _entries_rejected,
    _one_sided,
    _trades_last_one_bar,
    _stop_inside_costs,
    _costs_exceed_edge,
    _negative_expectancy,
    _result_concentrated,
    _drawdown_exceeds_profit,
    _stop_too_tight,
    _losers_were_winners,
    _exits_leave_money,
    _always_in_market,
)
