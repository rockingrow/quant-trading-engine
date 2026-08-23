"""Each rule must fire on the fault it names, and stay quiet on a clean run."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from qte_backtest.diagnostics import (
    CRITICAL,
    MIN_TRADES_FOR_STATISTICS,
    DiagnosticContext,
    diagnose,
)
from qte_backtest.execution import ClosedLeg, CostModel, ExitReason, SimulatedPosition
from qte_backtest.metrics import compute_metrics

START = datetime(2026, 1, 1, tzinfo=UTC)


def _position(
    *,
    pnl: float,
    index: int = 0,
    direction: int = 1,
    reason: ExitReason = ExitReason.TP2,
    sl_distance: float | None = 10.0,
    mae: float = 0.0,
    mfe: float = 0.0,
    bars: int = 20,
    entry: float = 2000.0,
) -> SimulatedPosition:
    """A closed trade with the excursion and risk the rule under test needs."""
    opened = START + timedelta(hours=index)
    stop = None if sl_distance is None else entry - direction * sl_distance
    position = SimulatedPosition(
        symbol="XAUUSD",
        direction=direction,
        opened_at=opened,
        entry_price=entry,
        quantity=1.0,
        sl=stop,
        tp1=entry + direction * 15.0,
        tp2=entry + direction * 30.0,
        initial_sl=stop,
        bars_held=bars,
    )
    position.remaining = 0.0
    position.worst_price = entry - direction * mae
    position.best_price = entry + direction * mfe
    position.legs = [
        ClosedLeg(
            closed_at=opened + timedelta(hours=1),
            price=entry + direction * pnl,
            quantity=1.0,
            reason=reason,
            gross_pnl=pnl,
            fees=0.0,
        )
    ]
    return position


def _context(positions, *, total_bars=3000, warmup=100, rejected=0, costs=None, gaps=0):
    return DiagnosticContext(
        metrics=compute_metrics(positions, 10_000.0, total_bars=total_bars),
        positions=positions,
        costs=costs or CostModel(spread=0.3),
        total_bars=total_bars,
        warmup=warmup,
        rejected_entries=rejected,
        data_gaps=gaps,
    )


def _codes(positions, **kwargs) -> set[str]:
    return {finding.code for finding in diagnose(_context(positions, **kwargs))}


def _healthy_positions(count: int = 60) -> list[SimulatedPosition]:
    """A believable winning run: both directions, mixed exits, sane excursion."""
    positions = []
    for index in range(count):
        winner = index % 5 < 3
        positions.append(
            _position(
                pnl=12.0 if winner else -10.0,
                index=index,
                direction=1 if index % 2 else -1,
                reason=ExitReason.TP2 if winner else ExitReason.SL,
                mae=3.0 if winner else 10.0,
                mfe=13.0 if winner else 4.0,
            )
        )
    return positions


def test_a_healthy_run_produces_no_critical_findings():
    findings = diagnose(_context(_healthy_positions()))
    assert [f for f in findings if f.severity == CRITICAL] == []


def test_no_trades_is_critical():
    assert "NO_TRADES" in _codes([])


def test_a_thin_sample_is_flagged_as_unusable():
    codes = _codes([_position(pnl=5.0, index=i) for i in range(MIN_TRADES_FOR_STATISTICS - 1)])
    assert "SAMPLE_TOO_SMALL" in codes


def test_warmup_eating_the_history_is_flagged():
    codes = _codes(_healthy_positions(), total_bars=1000, warmup=600)
    assert "WARMUP_DOMINATES" in codes


def test_a_trade_with_no_stop_is_critical():
    positions = _healthy_positions()
    positions.append(_position(pnl=5.0, index=99, sl_distance=None))
    assert "TRADES_WITHOUT_STOP" in _codes(positions)


def test_positions_never_closing_by_themselves_is_critical():
    # The classic units bug: levels so far away nothing ever reaches them.
    positions = [_position(pnl=1.0, index=i, reason=ExitReason.END_OF_DATA) for i in range(40)]
    assert "EXITS_NEVER_TRIGGER" in _codes(positions)


def test_every_exit_being_a_stop_is_flagged():
    positions = [
        _position(pnl=-10.0, index=i, reason=ExitReason.SL, mae=10.0, mfe=1.0) for i in range(40)
    ]
    assert "ALL_EXITS_ARE_STOPS" in _codes(positions)


def test_a_tp2_that_is_never_reached_is_flagged():
    positions = [_position(pnl=-10.0, index=i, reason=ExitReason.SL, mae=10.0) for i in range(40)]
    assert "TP2_NEVER_REACHED" in _codes(positions)


def test_heavy_rejection_points_at_the_missing_open_position_guard():
    codes = _codes(_healthy_positions(), rejected=200)
    assert "ENTRIES_REJECTED" in codes


def test_trading_only_one_direction_is_flagged():
    positions = [_position(pnl=5.0, index=i, direction=1) for i in range(40)]
    assert "ONE_SIDED" in _codes(positions)


def test_one_sided_is_not_flagged_on_a_sample_too_small_to_judge():
    positions = [_position(pnl=5.0, index=i, direction=1) for i in range(5)]
    assert "ONE_SIDED" not in _codes(positions)


def test_trades_lasting_a_single_bar_are_flagged():
    positions = [_position(pnl=5.0, index=i, bars=1) for i in range(40)]
    assert "TRADES_LAST_ONE_BAR" in _codes(positions)


def test_a_stop_inside_the_round_trip_cost_is_flagged():
    positions = [_position(pnl=0.5, index=i, sl_distance=0.2) for i in range(40)]
    codes = _codes(positions, costs=CostModel(spread=0.3, slippage=0.05))
    assert "STOP_INSIDE_COSTS" in codes


def test_a_wide_stop_relative_to_costs_is_not_flagged():
    codes = _codes(_healthy_positions(), costs=CostModel(spread=0.3))
    assert "STOP_INSIDE_COSTS" not in codes


def test_negative_expectancy_is_flagged():
    positions = [
        _position(pnl=-10.0 if i % 2 else 5.0, index=i, direction=1 if i % 2 else -1)
        for i in range(40)
    ]
    assert "NEGATIVE_EXPECTANCY" in _codes(positions)


def test_a_result_resting_on_one_trade_is_flagged():
    positions = [_position(pnl=1.0, index=i) for i in range(20)]
    positions.append(_position(pnl=500.0, index=99))
    assert "RESULT_CONCENTRATED" in _codes(positions)


def test_losers_that_were_deeply_profitable_first_point_at_the_exits():
    positions = []
    for index in range(40):
        loser = index % 2 == 0
        positions.append(
            _position(
                pnl=-10.0 if loser else 12.0,
                index=index,
                direction=1 if index % 3 else -1,
                reason=ExitReason.SL if loser else ExitReason.TP2,
                # Every loser reached 2R in profit before dying.
                mfe=20.0 if loser else 13.0,
                mae=10.0 if loser else 2.0,
            )
        )
    assert "LOSERS_WERE_WINNERS" in _codes(positions)


def test_winners_that_nearly_stopped_out_suggest_a_wider_stop():
    positions = []
    for index in range(40):
        winner = index % 2 == 0
        positions.append(
            _position(
                pnl=12.0 if winner else -10.0,
                index=index,
                direction=1 if index % 3 else -1,
                reason=ExitReason.TP2 if winner else ExitReason.SL,
                mae=9.0 if winner else 10.0,
                mfe=13.0 if winner else 2.0,
            )
        )
    assert "STOP_TOO_TIGHT" in _codes(positions)


def test_being_always_in_the_market_is_reported():
    positions = [
        _position(pnl=5.0, index=i, bars=90, direction=1 if i % 2 else -1) for i in range(40)
    ]
    assert "ALWAYS_IN_MARKET" in _codes(positions, total_bars=3600)


def test_data_gaps_are_reported_as_information_not_a_defect():
    findings = diagnose(_context(_healthy_positions(), gaps=12))
    gap_finding = next(f for f in findings if f.code == "DATA_GAPS")
    assert gap_finding.severity == "info"
    assert gap_finding.evidence["gaps"] == 12


def test_every_finding_carries_evidence_and_one_concrete_action():
    # The contract that makes a finding actionable rather than a vibe.
    for finding in diagnose(_context([_position(pnl=-10.0, index=i, mfe=25.0) for i in range(40)])):
        assert finding.evidence, f"{finding.code} has no evidence"
        assert finding.suggestion.strip(), f"{finding.code} suggests nothing"
        assert finding.severity in ("critical", "warning", "info")


def test_findings_are_ordered_most_severe_first():
    findings = diagnose(_context([], total_bars=100, warmup=90))
    severities = [finding.severity for finding in findings]
    assert severities == sorted(severities, key={"critical": 0, "warning": 1, "info": 2}.get)


@pytest.mark.parametrize("count", [0, 1, 5, 40])
def test_diagnose_never_raises_whatever_the_run_looked_like(count):
    diagnose(_context([_position(pnl=1.0, index=i) for i in range(count)]))
