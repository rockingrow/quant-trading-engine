"""Risk sizing: the engine's answer to "how big", and where it gets its inputs.

The worked example below is
``examples/algo-trading-broker/entry.long.json`` — a $1,000 account risking 3%
with a $5 stop buys 6 units. That file is what the broker actually receives, so
if this arithmetic drifts the payloads stop matching the contract they document.
"""

from __future__ import annotations

import pytest
from qte_shared.config import settings
from qte_shared.sizing import PositionSizer, resolve_use_equity_sizing


def test_the_worked_example_from_the_broker_contract():
    sizer = PositionSizer(capital=1000.0, risk_percent=3.0)
    assert sizer.risk_budget == pytest.approx(30.0)
    assert sizer.size(price=2334.50, sl=2329.50) == pytest.approx(6.0)


def test_a_wider_stop_buys_less_of_the_same_risk():
    # The budget is what stays fixed; the size is what absorbs the distance.
    sizer = PositionSizer(capital=1000.0, risk_percent=3.0)
    assert sizer.size(price=2334.50, sl=2324.50) == pytest.approx(3.0)


def test_direction_does_not_change_the_size():
    sizer = PositionSizer(capital=1000.0, risk_percent=2.0)
    long = sizer.size(price=100.0, sl=95.0)
    short = sizer.size(price=100.0, sl=105.0)
    assert long == short == pytest.approx(4.0)


def test_contract_size_divides_the_quantity():
    """`quantity` is in lots when a lot is not one unit — the risk is the same."""
    sizer = PositionSizer(capital=1000.0, risk_percent=3.0, contract_size=100.0)
    assert sizer.size(price=2334.50, sl=2329.50) == pytest.approx(0.06)


def test_an_entry_with_no_stop_cannot_be_sized():
    # Refusing is the honest answer: the stop distance is the whole denominator,
    # and a number invented without one would mean nothing.
    sizer = PositionSizer(capital=1000.0, risk_percent=3.0)
    assert sizer.size(price=2334.50, sl=None) is None
    assert sizer.size(price=None, sl=2329.50) is None


def test_a_stop_sitting_on_the_entry_cannot_be_sized():
    sizer = PositionSizer(capital=1000.0, risk_percent=3.0)
    assert sizer.size(price=2334.50, sl=2334.50) is None


def test_the_cap_is_a_ceiling_not_a_target():
    sizer = PositionSizer(capital=1000.0, risk_percent=3.0, max_quantity=2.0)
    assert sizer.size(price=2334.50, sl=2329.50) == pytest.approx(2.0)
    assert sizer.size(price=2334.50, sl=2314.50) == pytest.approx(1.5)


def test_a_size_that_rounds_away_is_refused_rather_than_sent_as_zero():
    # The broker rejects a zero-quantity entry; catching it here says why.
    sizer = PositionSizer(capital=10.0, risk_percent=0.001, precision=4)
    assert sizer.size(price=2334.50, sl=2329.50) is None


def test_precision_is_where_the_size_is_rounded():
    coarse = PositionSizer(capital=1000.0, risk_percent=1.0, precision=2)
    fine = PositionSizer(capital=1000.0, risk_percent=1.0, precision=6)
    assert coarse.size(price=100.0, sl=97.0) == pytest.approx(3.33)
    assert fine.size(price=100.0, sl=97.0) == pytest.approx(3.333333)


# ── Where the numbers come from ──────────────────────────────────────────


def test_the_pairs_risk_percent_beats_the_account_default():
    # `risk_percent = 3.0` under [symbols.XAUUSD.params.…] is how an operator
    # states a pair's risk; it has to win over the process-wide fallback.
    sizer = PositionSizer.from_settings({"risk_percent": 3.0})
    assert sizer.risk_percent == pytest.approx(3.0)
    assert sizer.capital == pytest.approx(settings.account.capital)


def test_a_pair_that_states_nothing_falls_back_to_the_account():
    sizer = PositionSizer.from_settings({})
    assert sizer.risk_percent == pytest.approx(settings.account.risk_percent)


def test_an_explicit_argument_beats_both():
    sizer = PositionSizer.from_settings({"risk_percent": 3.0}, risk_percent=0.5)
    assert sizer.risk_percent == pytest.approx(0.5)


@pytest.mark.parametrize("junk", [None, "", "not-a-number", True, False])
def test_an_unusable_risk_percent_in_the_table_falls_back_rather_than_crashing(junk):
    # A routing table is hand-edited. Refusing to start over one bad cell would
    # stop the whole book from trading; the fallback is the safer failure.
    sizer = PositionSizer.from_settings({"risk_percent": junk})
    assert sizer.risk_percent == pytest.approx(settings.account.risk_percent)


def test_the_capital_can_be_replaced_for_one_run():
    """`qte-backtest run --equity` must not need the environment changing."""
    sizer = PositionSizer.from_settings({"risk_percent": 3.0}).replace(capital=10_000.0)
    assert sizer.capital == pytest.approx(10_000.0)
    assert sizer.risk_percent == pytest.approx(3.0)
    assert sizer.size(price=2334.50, sl=2329.50) == pytest.approx(60.0)


# ── use_equity_sizing ────────────────────────────────────────────────────


def test_equity_sizing_is_read_off_the_pairs_params():
    assert resolve_use_equity_sizing({"use_equity_sizing": True}) is True
    assert resolve_use_equity_sizing({"use_equity_sizing": False}) is False


def test_an_undeclared_equity_sizing_stays_absent_rather_than_becoming_false():
    # The broker's schema distinguishes "not stated" from an explicit false.
    assert resolve_use_equity_sizing({}) is None
    assert resolve_use_equity_sizing(None) is None


def test_equity_sizing_does_not_change_the_size():
    """It is reported to the broker, never obeyed here — see qte_shared.sizing.

    Compounding would make a run's later sizes depend on its own earlier P&L,
    and two backtests differing by one early trade would stop being comparable.
    """
    on = PositionSizer.from_settings({"risk_percent": 3.0, "use_equity_sizing": True})
    off = PositionSizer.from_settings({"risk_percent": 3.0, "use_equity_sizing": False})
    assert on.size(price=2334.50, sl=2329.50) == off.size(price=2334.50, sl=2329.50)
