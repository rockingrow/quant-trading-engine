"""Risk sizing: the engine's answer to "how big", and where it gets its inputs.

The worked example below is
``examples/algo-trading-broker/entry.long.json`` — a $1,000 account risking 3%
with a $5 stop buys 6 units. That file is what the broker actually receives, so
if this arithmetic drifts the payloads stop matching the contract they document.
"""

from __future__ import annotations

import pytest

from qte_shared.config import settings
from qte_shared.strategies.sizing import (
    PositionSizer,
    resolve_dynamic_risk,
    resolve_use_equity_sizing,
)


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
    with pytest.raises(ValueError, match="stop distance"):
        sizer.size(price=2334.50, sl=2334.50)


def test_the_cap_is_a_ceiling_not_a_target():
    sizer = PositionSizer(capital=1000.0, risk_percent=3.0, max_quantity=2.0)
    assert sizer.size(price=2334.50, sl=2329.50) == pytest.approx(2.0)
    assert sizer.size(price=2334.50, sl=2314.50) == pytest.approx(1.5)


def test_a_size_that_rounds_away_is_refused_rather_than_sent_as_zero():
    # The broker rejects a zero-quantity entry; catching it here says why.
    sizer = PositionSizer(capital=10.0, risk_percent=0.001, precision=4)
    with pytest.raises(ValueError, match="quantity step"):
        sizer.size(price=2334.50, sl=2329.50)


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
    # A mapping table is hand-edited. Refusing to start over one bad cell would
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
    """It is reported to the broker, never obeyed here — see qte_shared.strategies.sizing.

    Compounding would make a run's later sizes depend on its own earlier P&L,
    and two backtests differing by one early trade would stop being comparable.
    """
    on = PositionSizer.from_settings({"risk_percent": 3.0, "use_equity_sizing": True})
    off = PositionSizer.from_settings({"risk_percent": 3.0, "use_equity_sizing": False})
    assert on.size(price=2334.50, sl=2329.50) == off.size(price=2334.50, sl=2329.50)


# ── Risk per entry: dynamic_risk and the ceilings ────────────────────────
#
# By default an entry risks the pair's own risk_percent, whatever the strategy
# states. `dynamic_risk = true` sizes it at the strategy's value instead, under
# the pair's `max_risk_percent` (unset: the base, so lower only) and the
# account's QTE_ACCOUNT__MAX_RISK_PERCENT.


@pytest.fixture
def no_account_ceiling(monkeypatch):
    monkeypatch.setattr(settings.account, "max_risk_percent", 0.0)


def test_a_stated_risk_buys_its_own_budget():
    position_sizer = PositionSizer(capital=1000.0, risk_percent=3.0)
    assert position_sizer.size(price=2334.50, sl=2329.50, risk_percent=5.0) == pytest.approx(10.0)
    assert position_sizer.size(price=2334.50, sl=2329.50, risk_percent=2.0) == pytest.approx(4.0)


@pytest.mark.parametrize("unusable", [0.0, -1.0, float("nan"), float("inf")])
def test_an_unusable_stated_risk_is_refused_by_the_position_sizer(unusable):
    position_sizer = PositionSizer(capital=1000.0, risk_percent=3.0)
    with pytest.raises(ValueError, match="Risk budget"):
        position_sizer.size(price=2334.50, sl=2329.50, risk_percent=unusable)


def test_a_static_pair_ignores_what_the_strategy_asks_for():
    position_sizer = PositionSizer(capital=1000.0, risk_percent=3.0)
    assert position_sizer.risk_for(5.0) == pytest.approx(3.0)
    assert position_sizer.risk_for(1.0) == pytest.approx(3.0)
    assert position_sizer.risk_for(None) == pytest.approx(3.0)


def test_a_dynamic_pair_takes_the_request_within_its_ceiling():
    position_sizer = PositionSizer(
        capital=1000.0, risk_percent=3.0, dynamic_risk=True, max_risk_percent=5.0
    )
    assert position_sizer.risk_for(2.0) == pytest.approx(2.0)
    assert position_sizer.risk_for(4.5) == pytest.approx(4.5)
    assert position_sizer.risk_for(9.0) == pytest.approx(5.0)
    assert position_sizer.risk_for(None) == pytest.approx(3.0)


@pytest.mark.parametrize("unusable", [0.0, -1.0, float("nan"), float("inf")])
def test_a_dynamic_pair_refuses_an_unusable_request_rather_than_defaulting(unusable):
    # inf would otherwise be capped into a perfectly ordinary-looking trade.
    position_sizer = PositionSizer(
        capital=1000.0, risk_percent=3.0, dynamic_risk=True, max_risk_percent=5.0
    )
    with pytest.raises(ValueError, match="finite and positive"):
        position_sizer.risk_for(unusable)


@pytest.mark.parametrize("unusable", [float("nan"), float("inf"), "3", True])
def test_a_static_pair_still_refuses_a_request_no_payload_could_carry(unusable):
    # Ignored or not, a NaN used to be refused by the payload's own validation;
    # overruling it with the base would trade a broken intent silently.
    position_sizer = PositionSizer(capital=1000.0, risk_percent=3.0)
    with pytest.raises(ValueError, match="finite number"):
        position_sizer.risk_for(unusable)


def test_a_static_pair_still_ignores_a_zero_request():
    # Zero is how upstream spells "no opinion" to the worker; it is not broken.
    position_sizer = PositionSizer(capital=1000.0, risk_percent=3.0)
    assert position_sizer.risk_for(0.0) == pytest.approx(3.0)


def test_the_account_ceiling_caps_a_static_pair_too():
    position_sizer = PositionSizer(capital=1000.0, risk_percent=3.0, max_risk_percent=2.0)
    assert position_sizer.risk_for(None) == pytest.approx(2.0)


@pytest.mark.parametrize("unusable", [0.0, -1.0, float("nan"), float("inf")])
def test_an_unusable_ceiling_is_refused(unusable):
    with pytest.raises(ValueError, match="max_risk_percent"):
        PositionSizer(capital=1000.0, risk_percent=3.0, max_risk_percent=unusable)


@pytest.mark.usefixtures("no_account_ceiling")
def test_a_pair_is_static_unless_its_mapping_says_otherwise():
    position_sizer = PositionSizer.from_settings({"risk_percent": 3.0})
    assert position_sizer.dynamic_risk is False
    assert position_sizer.max_risk_percent is None


@pytest.mark.usefixtures("no_account_ceiling")
def test_a_dynamic_pair_with_no_ceiling_of_its_own_can_only_lower_its_risk():
    position_sizer = PositionSizer.from_settings({"risk_percent": 3.0, "dynamic_risk": True})
    assert position_sizer.max_risk_percent == pytest.approx(3.0)
    assert position_sizer.risk_for(2.0) == pytest.approx(2.0)
    assert position_sizer.risk_for(5.0) == pytest.approx(3.0)


@pytest.mark.usefixtures("no_account_ceiling")
def test_the_pairs_ceiling_is_read_off_its_params():
    position_sizer = PositionSizer.from_settings(
        {"risk_percent": 3.0, "dynamic_risk": True, "max_risk_percent": 5.0}
    )
    assert position_sizer.max_risk_percent == pytest.approx(5.0)
    assert position_sizer.risk_for(5.0) == pytest.approx(5.0)


def test_the_account_ceiling_beats_a_higher_pair_ceiling(monkeypatch):
    monkeypatch.setattr(settings.account, "max_risk_percent", 4.0)
    position_sizer = PositionSizer.from_settings(
        {"risk_percent": 3.0, "dynamic_risk": True, "max_risk_percent": 5.0}
    )
    assert position_sizer.max_risk_percent == pytest.approx(4.0)
    assert position_sizer.risk_for(5.0) == pytest.approx(4.0)


def test_the_account_ceiling_caps_a_static_pairs_base(monkeypatch):
    monkeypatch.setattr(settings.account, "max_risk_percent", 2.0)
    position_sizer = PositionSizer.from_settings({"risk_percent": 3.0})
    assert position_sizer.max_risk_percent == pytest.approx(2.0)
    assert position_sizer.risk_for(None) == pytest.approx(2.0)


@pytest.mark.usefixtures("no_account_ceiling")
def test_a_static_pair_ignores_a_ceiling_of_its_own():
    # Without the switch the pair only ever trades its base; the stray key
    # is logged, not obeyed, and caps nothing.
    position_sizer = PositionSizer.from_settings({"risk_percent": 3.0, "max_risk_percent": 1.0})
    assert position_sizer.max_risk_percent is None
    assert position_sizer.risk_for(None) == pytest.approx(3.0)


@pytest.mark.usefixtures("no_account_ceiling")
def test_a_pair_ceiling_below_its_base_is_refused():
    with pytest.raises(ValueError, match="below the pair's risk_percent"):
        PositionSizer.from_settings(
            {"risk_percent": 3.0, "dynamic_risk": True, "max_risk_percent": 2.0}
        )


@pytest.mark.usefixtures("no_account_ceiling")
@pytest.mark.parametrize("unusable", ["", "five", True, "inf", float("nan")])
def test_an_unusable_pair_ceiling_is_refused(unusable):
    with pytest.raises(ValueError, match="max_risk_percent"):
        PositionSizer.from_settings(
            {"risk_percent": 3.0, "dynamic_risk": True, "max_risk_percent": unusable}
        )


def test_an_infinite_pair_ceiling_is_refused_under_an_account_ceiling_too(monkeypatch):
    # min() with the account's 4 would otherwise swallow it without a word.
    monkeypatch.setattr(settings.account, "max_risk_percent", 4.0)
    with pytest.raises(ValueError, match="max_risk_percent must be a finite number"):
        PositionSizer.from_settings(
            {"risk_percent": 3.0, "dynamic_risk": True, "max_risk_percent": float("inf")}
        )


@pytest.mark.parametrize("unusable", ["true", "yes", 1, 0.0])
def test_the_switch_must_be_a_real_boolean(unusable):
    # `bool("false")` is True; a switch on how much is risked is read strictly.
    with pytest.raises(ValueError, match="dynamic_risk must be true or false"):
        resolve_dynamic_risk({"dynamic_risk": unusable})


def test_the_switch_reads_a_boolean_and_defaults_off():
    assert resolve_dynamic_risk({"dynamic_risk": True}) is True
    assert resolve_dynamic_risk({"dynamic_risk": False}) is False
    assert resolve_dynamic_risk({}) is False
    assert resolve_dynamic_risk(None) is False
