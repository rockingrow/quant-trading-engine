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
