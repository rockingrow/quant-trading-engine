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


def test_excursion_records_the_worst_and_best_price_touched():
    # MAE/MFE is what separates "entry was wrong" from "exit gave it back".
    simulator = FillSimulator(CostModel())
    position = _long(simulator, sl=1950.0, tp1=2100.0, tp2=2200.0)
    simulator.process_bar(position, _bar(2000, 2030, 1980, 2010), NOW)
    simulator.process_bar(position, _bar(2010, 2050, 1995, 2040), NOW)

    assert position.mae == pytest.approx(20.0)  # low of 1980, 20 below entry
    assert position.mfe == pytest.approx(50.0)  # high of 2050
    assert position.bars_held == 2


def test_excursion_includes_the_bar_that_closes_the_trade():
    # Skipping it would understate MAE on exactly the trades it matters for.
    simulator = FillSimulator(CostModel())
    position = _long(simulator, sl=1990.0)
    simulator.process_bar(position, _bar(2000, 2005, 1985, 1988), NOW)
    assert not position.is_open
    assert position.mae == pytest.approx(15.0)
    assert position.bars_held == 1


def test_a_shorts_excursion_is_mirrored():
    simulator = FillSimulator(CostModel())
    position = simulator.open_position(
        symbol="XAUUSD",
        action=SignalAction.SHORT,
        bar_time=NOW,
        price=2000.0,
        quantity=1.0,
        sl=2100.0,
        tp1=1900.0,
    )
    simulator.process_bar(position, _bar(2000, 2030, 1960, 1970), NOW)
    assert position.mae == pytest.approx(30.0)  # price rose against the short
    assert position.mfe == pytest.approx(40.0)


def test_r_multiple_is_measured_against_the_risk_taken_at_entry():
    simulator = FillSimulator(CostModel())
    position = _long(simulator, sl=1990.0, tp1=2010.0, tp1_percent=100.0)
    simulator.process_bar(position, _bar(2000, 2011, 1999, 2010), NOW)
    # Risked 10 to make 10.
    assert position.initial_risk == pytest.approx(10.0)
    assert position.r_multiple == pytest.approx(1.0)


def test_moving_the_stop_to_breakeven_does_not_shrink_the_recorded_risk():
    # initial_sl exists precisely so a trade's risk cannot appear to change
    # after the fact and inflate every R-multiple that follows.
    simulator = FillSimulator(CostModel())
    position = _long(simulator, sl=1990.0, tp1=2010.0, tp1_percent=50.0, move_sl_to_be=True)
    simulator.process_bar(position, _bar(2000, 2011, 1999, 2010), NOW)

    assert position.sl == pytest.approx(2000.0)  # moved to breakeven
    assert position.initial_sl == pytest.approx(1990.0)
    assert position.initial_risk == pytest.approx(10.0)


def test_a_trade_with_no_stop_has_no_r_multiple_rather_than_a_wrong_one():
    simulator = FillSimulator(CostModel())
    position = _long(simulator, sl=None, tp1=2010.0, tp1_percent=100.0)
    simulator.process_bar(position, _bar(2000, 2011, 1999, 2010), NOW)
    assert position.initial_risk is None
    assert position.r_multiple is None
    assert position.mae_r is None


def test_r_multiple_is_unaffected_by_contract_size():
    # It must compare across instruments, so scaling P&L must not scale R.
    for contract_size in (1.0, 100.0):
        simulator = FillSimulator(CostModel(contract_size=contract_size))
        position = _long(simulator, sl=1990.0, tp1=2010.0, tp1_percent=100.0)
        simulator.process_bar(position, _bar(2000, 2011, 1999, 2010), NOW)
        assert position.r_multiple == pytest.approx(1.0)
