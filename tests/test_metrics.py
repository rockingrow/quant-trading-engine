from datetime import UTC, datetime, timedelta

import pytest
from qte_backtest.execution import ClosedLeg, ExitReason, SimulatedPosition
from qte_backtest.metrics import compute_metrics, format_report

START = datetime(2026, 1, 1, tzinfo=UTC)


def _position(pnl: float, day: int) -> SimulatedPosition:
    opened = START + timedelta(days=day)
    position = SimulatedPosition(
        symbol="XAUUSD", direction=1, opened_at=opened, entry_price=2000.0, quantity=1.0
    )
    position.remaining = 0.0
    position.legs = [
        ClosedLeg(
            closed_at=opened + timedelta(hours=4),
            price=2000.0 + pnl,
            quantity=1.0,
            reason=ExitReason.TP2 if pnl > 0 else ExitReason.SL,
            gross_pnl=pnl,
            fees=0.0,
        )
    ]
    return position


def test_no_trades_is_an_empty_report_not_a_crash():
    metrics = compute_metrics([])
    assert metrics.trades == 0
    assert "No trades" in format_report(metrics, "empty")


def test_headline_figures():
    positions = [_position(10, 1), _position(-5, 2), _position(20, 3), _position(-5, 4)]
    metrics = compute_metrics(positions, starting_equity=1000)

    assert metrics.trades == 4
    assert (metrics.wins, metrics.losses) == (2, 2)
    assert metrics.win_rate == 50.0
    assert metrics.net_pnl == pytest.approx(20.0)
    assert metrics.profit_factor == pytest.approx(3.0)
    assert metrics.expectancy == pytest.approx(5.0)


def test_drawdown_is_measured_on_the_equity_curve_not_the_worst_trade():
    positions = [_position(100, 1), _position(-30, 2), _position(-40, 3), _position(50, 4)]
    metrics = compute_metrics(positions, starting_equity=0)
    assert metrics.max_drawdown == pytest.approx(70.0)  # 100 → 30, not the -40 leg
    assert metrics.max_consecutive_losses == 2


def test_profit_factor_is_none_rather_than_infinite_without_a_loss():
    metrics = compute_metrics([_position(10, 1), _position(5, 2)])
    assert metrics.profit_factor is None
    assert "n/a" in format_report(metrics, "all wins")


def test_equity_curve_starts_at_the_opening_balance():
    metrics = compute_metrics([_position(10, 1)], starting_equity=500)
    assert metrics.equity_curve == [500.0, 510.0]


# ── The account the run was traded on ────────────────────────────────────
#
# Starting capital is QTE_ACCOUNT__CAPITAL. It is what makes a percentage mean
# anything: without it, "max drawdown 200" is a number with no denominator and
# two runs sized differently cannot be compared at all.


def test_the_equity_curve_starts_at_the_capital_and_ends_where_the_pnl_leaves_it():
    positions = [_position(10, 1), _position(-5, 2), _position(20, 3), _position(-5, 4)]
    metrics = compute_metrics(positions, starting_equity=1000)

    assert metrics.starting_equity == pytest.approx(1000.0)
    assert metrics.ending_equity == pytest.approx(1020.0)
    assert metrics.equity_curve[0] == pytest.approx(1000.0)
    assert metrics.equity_curve[-1] == pytest.approx(metrics.ending_equity)


def test_return_pct_is_the_net_pnl_against_the_capital():
    metrics = compute_metrics([_position(30, 1)], starting_equity=1000)
    assert metrics.return_pct == pytest.approx(3.0)


def test_a_losing_run_returns_a_negative_percentage():
    metrics = compute_metrics([_position(-50, 1)], starting_equity=1000)
    assert metrics.return_pct == pytest.approx(-5.0)
    assert metrics.ending_equity == pytest.approx(950.0)


def test_drawdown_is_a_percentage_of_the_peak_balance_not_of_the_profit():
    # Up to 1100, back to 1050: 50 off a 1100 peak is 4.55%, not 50% of the
    # 100 that had been made.
    metrics = compute_metrics([_position(100, 1), _position(-50, 2)], starting_equity=1000)
    assert metrics.max_drawdown == pytest.approx(50.0)
    assert metrics.max_drawdown_pct == pytest.approx(4.5455, abs=1e-3)


def test_the_same_trades_on_a_bigger_account_are_a_smaller_return():
    """The P&L is unchanged; only what it is a share of moves."""
    trades = [_position(30, 1)]
    small = compute_metrics(trades, starting_equity=1000)
    large = compute_metrics(trades, starting_equity=10_000)

    assert small.net_pnl == large.net_pnl == pytest.approx(30.0)
    assert small.return_pct == pytest.approx(3.0)
    assert large.return_pct == pytest.approx(0.3)


def test_a_run_with_no_capital_reports_no_return_rather_than_dividing_by_zero():
    metrics = compute_metrics([_position(30, 1)], starting_equity=0.0)
    assert metrics.return_pct is None


def test_an_empty_run_still_says_what_the_account_started_at():
    metrics = compute_metrics([], starting_equity=1000)
    assert metrics.starting_equity == pytest.approx(1000.0)
    assert metrics.ending_equity == pytest.approx(1000.0)


def test_the_cli_summary_shows_the_balance_it_started_and_finished_with():
    metrics = compute_metrics([_position(30, 1), _position(-5, 2)], starting_equity=1000)
    report = format_report(metrics, "MY_EDGE — XAUUSD M15")
    assert "Capital" in report
    assert "1,000.00" in report and "1,025.00" in report
    assert "+2.50%" in report
