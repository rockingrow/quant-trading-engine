"""What counts as an open position, and what ends one.

The rule under test is the awkward one: a cycle ends on TP2/SL/R_SL/FLAT, and
*also* on a TP1 that happens to take the entry's whole quantity. Getting it
wrong in either direction is expensive and quiet — a cycle left open means the
next entry is refused for a position that no longer exists, and a cycle closed
early means the runner mints a fresh id for a position the broker still holds
and nobody ever closes the first one.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from qte_shared.cache.redis_state import _decode_position
from qte_shared.models import TERMINAL_ACTIONS, OpenPosition, SignalAction

NOW = datetime(2026, 5, 1, 10, 0, tzinfo=UTC)


def _position(**kwargs) -> OpenPosition:
    defaults = {
        "signal_uxid": "9F2C4B7E18A3D605",
        "strategy": "MT5_GOLD_M5_V1",
        "symbol": "XAUUSD",
        "action": SignalAction.LONG,
        "opened_at": NOW,
        "price": 2334.5,
        "quantity": 6.0,
        "remaining": 6.0,
        "tp1_percent": 30.0,
    }
    return OpenPosition(**{**defaults, **kwargs})


# ── The exit rule ────────────────────────────────────────────────────────


@pytest.mark.parametrize("action", sorted(TERMINAL_ACTIONS, key=lambda a: a.value))
def test_a_terminal_action_ends_the_cycle_whatever_size_it_names(action):
    position = _position()
    assert position.apply_close(action, 0.1) is True


def test_a_partial_tp1_leaves_the_cycle_open():
    position = _position()
    assert position.apply_close(SignalAction.TP1, 1.8) is False
    assert position.remaining == pytest.approx(4.2)
    assert position.tp1_filled is True


def test_a_tp1_taking_the_whole_entry_is_an_exit():
    # "TP1 exit với đúng quantity của entry thì vẫn tính là đã exit."
    position = _position()
    assert position.apply_close(SignalAction.TP1, 6.0) is True
    assert position.is_flat


def test_two_partials_that_add_up_to_the_entry_end_it_on_the_second():
    position = _position()
    assert position.apply_close(SignalAction.TP1, 3.0) is False
    assert position.apply_close(SignalAction.TP1, 3.0) is True


def test_a_close_can_never_drive_the_remaining_size_negative():
    # A residual below zero would never satisfy `is_flat` and would strand the
    # cycle open for the life of the process.
    position = _position()
    position.apply_close(SignalAction.TP1, 99.0)
    assert position.remaining == pytest.approx(0.0)
    assert position.is_flat


def test_a_cycle_of_unknown_size_is_not_treated_as_flat():
    """A record restored from a bare uxid knows no size. That is not zero."""
    position = OpenPosition(signal_uxid="9F2C4B7E18A3D605", symbol="XAUUSD")
    assert position.remaining is None
    assert not position.is_flat
    assert position.apply_close(SignalAction.TP1, 6.0) is False
    assert position.apply_close(SignalAction.SL, None) is True


# ── Partial shares ───────────────────────────────────────────────────────


def test_a_share_is_taken_off_the_entry_not_off_what_is_left():
    """Two 30% partials take 30% of the entry twice, not 30% then 21%."""
    position = _position()
    assert position.share(30.0) == pytest.approx(1.8)
    position.apply_close(SignalAction.TP1, 1.8)
    assert position.share(30.0) == pytest.approx(1.8)


def test_a_share_larger_than_the_remainder_is_clamped_to_it():
    position = _position(remaining=1.0)
    assert position.share(50.0) == pytest.approx(1.0)


def test_a_share_of_nothing_stated_is_nothing():
    assert _position().share(None) is None


# ── Persistence ──────────────────────────────────────────────────────────


def test_a_position_survives_a_json_round_trip():
    position = _position(remaining=4.2, scale=0.5, tp1_filled=True)
    restored = OpenPosition.model_validate_json(position.model_dump_json())
    assert restored == position


def test_redis_reads_back_what_it_wrote():
    position = _position()
    decoded = _decode_position(
        position.model_dump_json(), strategy="MT5_GOLD_M5_V1", symbol="XAUUSD"
    )
    assert decoded == position


def test_a_cycle_stored_before_this_record_existed_is_still_readable():
    """Redis holds bare uxid strings from older runners. Discarding one would
    orphan a live position at exactly the moment the runner is upgraded."""
    decoded = _decode_position("9F2C4B7E18A3D605", strategy="MT5_GOLD_M5_V1", symbol="XAUUSD")
    assert decoded is not None
    assert decoded.signal_uxid == "9F2C4B7E18A3D605"
    assert decoded.strategy == "MT5_GOLD_M5_V1"
    assert decoded.remaining is None


@pytest.mark.parametrize("empty", [None, "", "   "])
def test_nothing_stored_means_flat(empty):
    assert _decode_position(empty, strategy="s", symbol="XAUUSD") is None


def test_an_unparseable_record_is_reported_as_flat_rather_than_crashing_the_boot():
    assert _decode_position('{"nope": 1}', strategy="s", symbol="XAUUSD") is None
