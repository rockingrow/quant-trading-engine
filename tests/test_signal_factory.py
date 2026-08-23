from datetime import UTC, datetime

import pytest
from qte_shared.models import SignalAction, is_valid_uxid
from qte_shared.signal_factory import BracketPolicy, SignalFactory
from qte_shared.strategy_base import SignalIntent

NOW = datetime(2026, 5, 1, 10, 0, tzinfo=UTC)


def _factory(**kwargs) -> SignalFactory:
    return SignalFactory("MT5_GOLD_M5_V1", timeframe="M15", token="tok", **kwargs)


def test_entry_mints_a_cycle_and_the_close_reuses_it():
    # Getting this wrong makes the broker render an exit as a separate trade.
    factory = _factory()
    entry = factory.build(
        SignalIntent(action=SignalAction.LONG, price=2000.0, quantity=1.0, sl=1990.0),
        symbol="XAUUSD",
        moment=NOW,
    )
    close = factory.build(
        SignalIntent(action=SignalAction.TP1, price=2010.0, quantity=0.5),
        symbol="XAUUSD",
        moment=NOW,
    )
    assert is_valid_uxid(entry.signal_uxid)
    assert close.signal_uxid == entry.signal_uxid


def test_a_terminal_close_releases_the_cycle():
    factory = _factory()
    factory.build(
        SignalIntent(action=SignalAction.LONG, price=2000.0, quantity=1.0, sl=1990.0),
        symbol="XAUUSD",
        moment=NOW,
    )
    factory.build(
        SignalIntent(action=SignalAction.TP2, price=2020.0, quantity=1.0),
        symbol="XAUUSD",
        moment=NOW,
    )
    assert factory.open_cycle("XAUUSD") is None


def test_tp1_keeps_the_cycle_open_because_part_of_the_position_remains():
    factory = _factory()
    factory.build(
        SignalIntent(action=SignalAction.LONG, price=2000.0, quantity=1.0, sl=1990.0),
        symbol="XAUUSD",
        moment=NOW,
    )
    factory.build(
        SignalIntent(action=SignalAction.TP1, price=2010.0, quantity=0.5),
        symbol="XAUUSD",
        moment=NOW,
    )
    assert factory.open_cycle("XAUUSD") is not None


def test_a_close_with_no_entry_is_refused():
    factory = _factory()
    with pytest.raises(ValueError, match="no cycle to close"):
        factory.build(SignalIntent(action=SignalAction.FLAT), symbol="XAUUSD", moment=NOW)


def test_restore_cycles_lets_a_restarted_runner_close_its_open_trade():
    factory = _factory()
    factory.restore_cycles({"XAUUSD": "9F2C4B7E18A3D605"})
    close = factory.build(
        SignalIntent(action=SignalAction.SL, price=1990.0, quantity=1.0),
        symbol="XAUUSD",
        moment=NOW,
    )
    assert close.signal_uxid == "9F2C4B7E18A3D605"


def test_an_entry_with_no_stop_gets_the_default_bracket():
    # Never let a naked entry reach a worker.
    factory = _factory(bracket=BracketPolicy(sl_pct=1.0, tp1_r=1.0, tp2_r=2.0))
    signal = factory.build(
        SignalIntent(action=SignalAction.LONG, price=2000.0, quantity=1.0),
        symbol="XAUUSD",
        moment=NOW,
    )
    assert signal.position.sl == pytest.approx(1980.0)
    assert signal.position.tp1 == pytest.approx(2020.0)
    assert signal.position.tp2 == pytest.approx(2040.0)


def test_the_strategys_own_levels_are_never_overwritten():
    factory = _factory(bracket=BracketPolicy(sl_pct=1.0))
    signal = factory.build(
        SignalIntent(
            action=SignalAction.LONG, price=2000.0, quantity=1.0, sl=1995.0, tp1=2007.0, tp2=2015.0
        ),
        symbol="XAUUSD",
        moment=NOW,
    )
    assert (signal.position.sl, signal.position.tp1, signal.position.tp2) == (
        1995.0,
        2007.0,
        2015.0,
    )


def test_timeframe_is_rendered_the_way_the_broker_stores_it():
    signal = _factory().build(
        SignalIntent(action=SignalAction.LONG, price=2000.0, quantity=1.0, sl=1990.0),
        symbol="XAUUSD",
        moment=NOW,
    )
    assert signal.timeframe == "15"


def test_strategy_params_ride_along_as_inputs_for_the_audit_trail():
    factory = SignalFactory("s", timeframe="M15", token="", inputs={"atr_len": 14})
    signal = factory.build(
        SignalIntent(
            action=SignalAction.LONG,
            price=1.0,
            quantity=1.0,
            sl=0.9,
            inputs={"risk_percent": 2.0},
            indicators={"atr_val": 3.2},
        ),
        symbol="XAUUSD",
        moment=NOW,
    )
    assert signal.inputs == {"atr_len": 14, "risk_percent": 2.0}
    assert signal.indicators == {"atr_val": 3.2}
