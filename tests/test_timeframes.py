from datetime import UTC, datetime

import pytest
from qte_shared.timeframes import (
    floor_to_bucket,
    next_bucket,
    normalize_timeframe,
    timeframe_seconds,
    to_broker_timeframe,
)


@pytest.mark.parametrize(
    "raw,expected",
    [("M15", "M15"), ("15", "M15"), ("15m", "M15"), ("1h", "H1"), ("60", "H1"), ("1d", "D1")],
)
def test_normalize_accepts_every_spelling(raw, expected):
    assert normalize_timeframe(raw) == expected


def test_normalize_rejects_a_timeframe_we_cannot_bucket():
    with pytest.raises(ValueError):
        normalize_timeframe("7m")


def test_broker_timeframe_is_bare_minutes():
    # The broker's contract stores TradingView's spelling, not ours.
    assert to_broker_timeframe("M15") == "15"
    assert to_broker_timeframe("H1") == "60"


def test_floor_lands_on_the_bucket_open():
    moment = datetime(2026, 5, 1, 10, 37, 42, tzinfo=UTC)
    assert floor_to_bucket(moment, "M15") == datetime(2026, 5, 1, 10, 30, tzinfo=UTC)
    assert next_bucket(moment, "M15") == datetime(2026, 5, 1, 10, 45, tzinfo=UTC)


def test_naive_datetimes_are_treated_as_utc():
    naive = datetime(2026, 5, 1, 10, 37, 42)
    assert floor_to_bucket(naive, "M15").tzinfo is UTC


def test_seconds_lookup():
    assert timeframe_seconds("M15") == 900
