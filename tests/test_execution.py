from datetime import UTC, datetime

import pandas as pd
import pytest
from qte_backtest.execution import CostModel, ExitReason, FillSimulator
from qte_shared.models import SignalAction

NOW = datetime(2026, 5, 1, tzinfo=UTC)


def _bar(open_, high, low, close):
    return pd.Series({"open": open_, "high": high, "low": low, "close": close, "volume": 0.0})


def _long(simulator, **overrides):
    kwargs = dict(
        symbol="XAUUSD",
        action=SignalAction.LONG,
        bar_time=NOW,
        price=2000.0,
        quantity=1.0,
        sl=1990.0,
        tp1=2010.0,
        tp2=2020.0,
    )
    kwargs.update(overrides)
    return simulator.open_position(**kwargs)


def test_entry_crosses_the_spread():
    simulator = FillSimulator(CostModel(spread=2.0))
    position = _long(simulator)
    assert position.entry_price == 2001.0  # a buy pays the ask


def test_target_fills_at_its_level_net_of_the_spread():
    simulator = FillSimulator(CostModel(spread=2.0))
    position = _long(simulator, tp1=2010.0, tp1_percent=100.0)
    simulator.process_bar(position, _bar(2001, 2011, 2000, 2010), NOW)
    assert position.legs[0].reason is ExitReason.TP1
    assert position.legs[0].price == 2009.0  # sold at the bid


def test_a_bar_covering_both_levels_takes_the_stop():
    # No tick data means no ordering, and assuming the good one flatters the run.
    simulator = FillSimulator(CostModel())
    position = _long(simulator)
    simulator.process_bar(position, _bar(2000, 2015, 1985, 2005), NOW)
    assert [leg.reason for leg in position.legs] == [ExitReason.SL]
    assert not position.is_open


def test_a_gap_through_the_stop_fills_at_the_open():
    simulator = FillSimulator(CostModel())
    position = _long(simulator)
    simulator.process_bar(position, _bar(1980, 1985, 1975, 1982), NOW)
    assert position.legs[0].price == 1980.0  # worse than the 1990 stop, as in life
    assert position.net_pnl == pytest.approx(-20.0)


def test_tp1_closes_a_share_and_leaves_the_runner():
    simulator = FillSimulator(CostModel())
    position = _long(simulator, quantity=1.0, tp1_percent=40.0)
    simulator.process_bar(position, _bar(2000, 2011, 1999, 2010), NOW)
    assert position.legs[0].quantity == pytest.approx(0.4)
    assert position.remaining == pytest.approx(0.6)
    assert position.is_open


def test_move_sl_to_be_uses_the_entry_fill_not_the_signalled_price():
    # The spread paid on the way in does not come back at breakeven.
    simulator = FillSimulator(CostModel(spread=2.0))
    position = _long(simulator, tp1_percent=50.0)
    simulator.process_bar(position, _bar(2001, 2011, 2000, 2010), NOW)
    position.move_sl_to_be = True
    simulator.process_bar(position, _bar(2010, 2012, 2009, 2011), NOW)
    assert position.tp1_filled


def test_a_short_makes_money_when_price_falls():
    simulator = FillSimulator(CostModel())
    position = simulator.open_position(
        symbol="XAUUSD",
        action=SignalAction.SHORT,
        bar_time=NOW,
        price=2000.0,
        quantity=1.0,
        sl=2010.0,
        tp1=1990.0,
        tp1_percent=100.0,
    )
    simulator.process_bar(position, _bar(2000, 2001, 1989, 1990), NOW)
    assert position.legs[0].reason is ExitReason.TP1
    assert position.net_pnl == pytest.approx(10.0)


def test_commission_is_charged_on_entry_and_on_each_exit():
    simulator = FillSimulator(CostModel(commission_per_unit=3.0))
    position = _long(simulator, quantity=2.0, tp1_percent=100.0)
    simulator.process_bar(position, _bar(2000, 2011, 1999, 2010), NOW)
    assert position.fees == pytest.approx(12.0)  # 2 units × 3 × two sides
    assert position.net_pnl == pytest.approx(20.0 - 12.0)


def test_contract_size_scales_pnl():
    simulator = FillSimulator(CostModel(contract_size=100.0))
    position = _long(simulator, quantity=1.0, tp1_percent=100.0)
    simulator.process_bar(position, _bar(2000, 2011, 1999, 2010), NOW)
    assert position.net_pnl == pytest.approx(1000.0)
