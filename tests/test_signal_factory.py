from datetime import UTC, datetime

import pytest
from qte_shared.models import SignalAction, is_valid_uxid
from qte_shared.signal_factory import BracketPolicy, SignalFactory
from qte_shared.sizing import PositionSizer
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


def test_deferred_build_does_not_claim_a_cycle_until_delivery_is_committed():
    factory = _factory()
    signal = factory.build(
        SignalIntent(action=SignalAction.LONG, price=2000.0, quantity=1.0, sl=1990.0),
        symbol="XAUUSD",
        moment=NOW,
        commit=False,
    )

    assert factory.open_cycle("XAUUSD") is None
    factory.commit(signal)
    assert factory.open_cycle("XAUUSD") == signal.signal_uxid


def test_invalid_entry_does_not_leave_a_ghost_open_cycle():
    factory = _factory()

    with pytest.raises(ValueError, match="quantity must be positive"):
        factory.build(
            SignalIntent(action=SignalAction.LONG, price=2000.0, quantity=0.0, sl=1990.0),
            symbol="XAUUSD",
            moment=NOW,
        )

    assert factory.open_cycle("XAUUSD") is None


def test_a_second_entry_cannot_orphan_the_open_cycle():
    factory = _factory()
    first = factory.build(
        SignalIntent(action=SignalAction.LONG, price=2000.0, quantity=1.0, sl=1990.0),
        symbol="XAUUSD",
        moment=NOW,
    )

    with pytest.raises(ValueError, match="close the current position"):
        factory.build(
            SignalIntent(action=SignalAction.LONG, price=2001.0, quantity=1.0, sl=1991.0),
            symbol="XAUUSD",
            moment=NOW,
        )

    assert factory.open_cycle("xauusd") == first.signal_uxid


# ── Sizing ───────────────────────────────────────────────────────────────
#
# `quantity` on the wire is the engine's number, not the strategy's: risk-sized
# against QTE_ACCOUNT__CAPITAL at the pair's risk_percent. The examples in
# `examples/algo-trading-broker/` are what the broker receives, and
# entry.long.json is this arithmetic — $1,000 at 3% over a $5 stop is 6 units.


def _priced(**kwargs) -> SignalFactory:
    """A factory sizing against a $1,000 account, as the contract example does."""
    return SignalFactory(
        "MT5_GOLD_M5_V1",
        timeframe="M5",
        token="tok",
        sizer=PositionSizer(capital=1000.0, risk_percent=3.0),
        **kwargs,
    )


def test_the_entry_is_risk_sized_against_the_account():
    signal = _priced().build(
        SignalIntent(action=SignalAction.LONG, price=2334.50, sl=2329.50),
        symbol="XAUUSD",
        moment=NOW,
    )
    assert signal.position.quantity == pytest.approx(6.0)


def test_the_strategys_own_size_is_replaced_not_honoured():
    # A strategy sizes against a notional capital of its own; the book the
    # operator configured is the one that trades.
    signal = _priced().build(
        SignalIntent(action=SignalAction.LONG, price=2334.50, quantity=99.0, sl=2329.50),
        symbol="XAUUSD",
        moment=NOW,
    )
    assert signal.position.quantity == pytest.approx(6.0)


def test_an_entry_that_cannot_be_sized_falls_back_to_the_configured_default():
    # No stop reaches the sizer only when the bracket cannot supply one either,
    # which is why the fallback exists at all.
    factory = SignalFactory(
        "S",
        timeframe="M5",
        token="",
        sizer=PositionSizer(capital=1000.0, risk_percent=3.0),
        default_quantity=0.01,
    )
    intent = SignalIntent(action=SignalAction.LONG, price=2334.50, sl=2334.50)
    signal = factory.build(intent, symbol="XAUUSD", moment=NOW)
    assert signal.position.quantity == pytest.approx(0.01)


def test_a_strategy_declining_the_trade_with_a_zero_size_is_not_sized_into_one():
    with pytest.raises(ValueError, match="quantity must be positive"):
        _priced().build(
            SignalIntent(action=SignalAction.LONG, price=2334.50, quantity=0.0, sl=2329.50),
            symbol="XAUUSD",
            moment=NOW,
        )


def test_the_risk_percent_that_sized_the_entry_rides_along_on_the_payload():
    signal = _priced().build(
        SignalIntent(action=SignalAction.LONG, price=2334.50, sl=2329.50),
        symbol="XAUUSD",
        moment=NOW,
    )
    assert signal.position.risk_percent == pytest.approx(3.0)


def test_use_equity_sizing_is_mirrored_from_the_pairs_params():
    factory = SignalFactory(
        "S", timeframe="M5", token="", inputs={"use_equity_sizing": False, "risk_percent": 3.0}
    )
    signal = factory.build(
        SignalIntent(action=SignalAction.LONG, price=2334.50, sl=2329.50),
        symbol="XAUUSD",
        moment=NOW,
    )
    assert signal.position.use_equity_sizing is False
    assert signal.position.risk_percent == pytest.approx(3.0)


def test_use_equity_sizing_does_not_change_the_size():
    sized = {
        state: _priced(inputs={"use_equity_sizing": state})
        .build(
            SignalIntent(action=SignalAction.LONG, price=2334.50, sl=2329.50),
            symbol="XAUUSD",
            moment=NOW,
        )
        .position.quantity
        for state in (True, False)
    }
    assert sized[True] == sized[False] == pytest.approx(6.0)


# ── Closes are expressed in the size QTE actually holds ──────────────────


def _open(factory: SignalFactory, **kwargs):
    return factory.build(
        SignalIntent(
            action=SignalAction.LONG, price=2334.50, sl=2329.50, tp1_percent=30.0, **kwargs
        ),
        symbol="XAUUSD",
        moment=NOW,
    )


def test_a_partial_close_is_rescaled_from_the_strategys_units_into_ours():
    # The strategy sized itself at 3 and banks 0.9 of it (30%); QTE opened 6,
    # so the same 30% is 1.8 — exactly close.tp1.json against entry.long.json.
    factory = _priced()
    _open(factory, quantity=3.0)
    close = factory.build(
        SignalIntent(action=SignalAction.TP1, price=2345.0, quantity=0.9),
        symbol="XAUUSD",
        moment=NOW,
    )
    assert close.position.quantity == pytest.approx(1.8)


def test_a_close_can_never_claim_more_than_the_position_has_left():
    factory = _priced()
    _open(factory)
    factory.build(
        SignalIntent(action=SignalAction.TP1, price=2345.0, quantity=1.8),
        symbol="XAUUSD",
        moment=NOW,
    )
    close = factory.build(
        SignalIntent(action=SignalAction.TP2, price=2350.0, quantity=99.0),
        symbol="XAUUSD",
        moment=NOW,
    )
    assert close.position.quantity == pytest.approx(4.2)


def test_a_tp1_naming_no_size_takes_its_configured_share_of_the_entry():
    factory = _priced()
    _open(factory)
    close = factory.build(
        SignalIntent(action=SignalAction.TP1, price=2345.0), symbol="XAUUSD", moment=NOW
    )
    assert close.position.quantity == pytest.approx(1.8)
    assert close.position.tp1_percent == pytest.approx(30.0)


def test_a_stop_naming_no_size_closes_whatever_is_left():
    factory = _priced()
    _open(factory)
    factory.build(SignalIntent(action=SignalAction.TP1, price=2345.0), symbol="XAUUSD", moment=NOW)
    close = factory.build(
        SignalIntent(action=SignalAction.SL, price=2334.50), symbol="XAUUSD", moment=NOW
    )
    assert close.position.quantity == pytest.approx(4.2)


def test_a_flat_stays_the_bare_close_everything_directive():
    # close.flat.json carries an action and nothing else; inventing a size here
    # would narrow a directive that means "close the lot".
    factory = _priced()
    _open(factory)
    close = factory.build(SignalIntent(action=SignalAction.FLAT), symbol="XAUUSD", moment=NOW)
    assert close.position.quantity is None
    assert factory.open_cycle("XAUUSD") is None


# ── The full life cycle ──────────────────────────────────────────────────


def test_a_tp1_that_takes_the_whole_entry_ends_the_cycle():
    # The rule the exit definition turns on: same action, different size, and
    # this one is an exit because nothing is left to hold.
    factory = _priced()
    _open(factory)
    factory.build(
        SignalIntent(action=SignalAction.TP1, price=2345.0, quantity=6.0),
        symbol="XAUUSD",
        moment=NOW,
    )
    assert factory.open_position("XAUUSD") is None


def test_the_cycle_survives_a_partial_and_dies_when_the_runner_is_taken_off():
    factory = _priced()
    entry = _open(factory)
    factory.build(SignalIntent(action=SignalAction.TP1, price=2345.0), symbol="XAUUSD", moment=NOW)
    held = factory.open_position("XAUUSD")
    assert held is not None
    assert held.remaining == pytest.approx(4.2)

    tp2 = factory.build(
        SignalIntent(action=SignalAction.TP2, price=2350.0), symbol="XAUUSD", moment=NOW
    )
    assert tp2.signal_uxid == entry.signal_uxid
    assert factory.open_position("XAUUSD") is None


def test_a_restarted_runner_closes_what_it_was_holding_at_the_right_size():
    """The whole reason the cycle record carries more than an id."""
    live = _priced()
    entry = _open(live)
    live.build(SignalIntent(action=SignalAction.TP1, price=2345.0), symbol="XAUUSD", moment=NOW)
    saved = live.open_position("XAUUSD")

    restarted = _priced()
    restarted.restore_positions({"XAUUSD": saved})
    close = restarted.build(
        SignalIntent(action=SignalAction.TP2, price=2350.0), symbol="XAUUSD", moment=NOW
    )
    assert close.signal_uxid == entry.signal_uxid
    assert close.position.quantity == pytest.approx(4.2)
    assert restarted.open_position("XAUUSD") is None


def test_a_close_for_a_cycle_we_no_longer_hold_does_not_disturb_the_current_one():
    factory = _priced()
    stale = factory.build(
        SignalIntent(action=SignalAction.SL, price=2300.0, signal_uxid="AAAAAAAAAAAAAAAA"),
        symbol="XAUUSD",
        moment=NOW,
    )
    assert stale.signal_uxid == "AAAAAAAAAAAAAAAA"

    current = _open(factory)
    factory.commit(stale)
    assert factory.open_cycle("XAUUSD") == current.signal_uxid


def test_a_rejected_entry_does_not_leave_its_scale_behind_for_the_next_one():
    # The scale is stashed at build time and only claimed on commit; a build
    # that never lands must not rescale the cycle that follows it.
    factory = _priced()
    factory.build(
        SignalIntent(action=SignalAction.LONG, price=2334.50, quantity=3.0, sl=2329.50),
        symbol="XAUUSD",
        moment=NOW,
        commit=False,
    )
    signal = factory.build(
        SignalIntent(action=SignalAction.LONG, price=2334.50, sl=2329.50),
        symbol="XAUUSD",
        moment=NOW,
    )
    factory.commit(signal)
    assert factory.open_position("XAUUSD").scale == pytest.approx(1.0)
