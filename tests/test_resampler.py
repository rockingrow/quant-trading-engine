from datetime import UTC, datetime, timedelta

from qte_ingestion.resampler import Resampler
from qte_shared.models import Tick

START = datetime(2026, 5, 1, 10, 0, tzinfo=UTC)


def _tick(offset_seconds: int, price: float) -> Tick:
    return Tick(
        symbol="XAUUSD", ts=START + timedelta(seconds=offset_seconds), last=price, volume=1.0
    )


def test_bar_stays_open_within_its_bucket():
    resampler = Resampler("XAUUSD", ["M1"])
    assert resampler.add_tick(_tick(0, 2000.0)) == []
    assert resampler.add_tick(_tick(30, 2005.0)) == []
    open_bar = resampler.open_candle("M1")
    assert open_bar is not None and open_bar.is_closed is False
    assert (open_bar.open, open_bar.high, open_bar.close) == (2000.0, 2005.0, 2005.0)


def test_a_tick_in_the_next_bucket_closes_the_previous_bar():
    resampler = Resampler("XAUUSD", ["M1"])
    resampler.add_tick(_tick(0, 2000.0))
    resampler.add_tick(_tick(30, 2010.0))
    resampler.add_tick(_tick(20, 1995.0))
    closed = resampler.add_tick(_tick(61, 2001.0))

    assert len(closed) == 1
    bar = closed[0]
    assert bar.open_time == START
    assert (bar.open, bar.high, bar.low, bar.close) == (2000.0, 2010.0, 1995.0, 1995.0)
    assert bar.tick_count == 3
    assert bar.is_closed


def test_one_tick_feeds_every_configured_timeframe():
    resampler = Resampler("XAUUSD", ["M1", "M15"])
    resampler.add_tick(_tick(0, 2000.0))
    assert {candle.timeframe for candle in resampler.open_candles()} == {"M1", "M15"}


def test_flush_closes_a_bar_even_though_no_tick_arrived():
    # The reason flush exists: a quiet market must still produce a candle on time.
    resampler = Resampler("XAUUSD", ["M1"])
    resampler.add_tick(_tick(10, 2000.0))
    assert resampler.flush(START + timedelta(seconds=30)) == []

    closed = resampler.flush(START + timedelta(seconds=61))
    assert len(closed) == 1 and closed[0].open_time == START
    assert resampler.open_candle("M1") is None


def test_a_late_tick_cannot_repaint_a_published_bar():
    resampler = Resampler("XAUUSD", ["M1"])
    resampler.add_tick(_tick(10, 2000.0))
    resampler.add_tick(_tick(70, 2010.0))
    before = resampler.open_candle("M1")

    resampler.add_tick(_tick(20, 9999.0))  # replayed from a reconnect

    after = resampler.open_candle("M1")
    assert after is not None and before is not None
    assert after.high == before.high  # the outlier was dropped, not folded in


def test_restore_resumes_a_bar_recovered_from_redis():
    resampler = Resampler("XAUUSD", ["M1"])
    resampler.add_tick(_tick(0, 2000.0))
    resampler.add_tick(_tick(10, 2020.0))
    saved = resampler.open_candle("M1")
    assert saved is not None

    revived = Resampler("XAUUSD", ["M1"])
    revived.restore(saved)
    revived.add_tick(_tick(20, 1990.0))

    bar = revived.open_candle("M1")
    assert bar is not None
    assert bar.open == 2000.0 and bar.high == 2020.0 and bar.low == 1990.0


# ── A bucket already published stays published ────────────────────────────
#
# `flush` deletes the builder, so the "is this behind the open bar?" check has
# nothing left to compare against once it has run. These pin the watermark that
# replaces it: a bucket that has gone out as a closed candle is never rebuilt,
# whether the flush timer closed it or a later tick did.


def test_a_late_tick_cannot_reopen_a_bucket_the_flush_timer_closed():
    resampler = Resampler("XAUUSD", ["M1"])
    resampler.add_tick(_tick(0, 2000.0))
    resampler.add_tick(_tick(20, 2010.0))

    first = resampler.flush(START + timedelta(minutes=1))
    assert [bar.open_time for bar in first] == [START]

    # The rest of that same bar turns up after the flush — feed lag, or a
    # vendor replaying its buffer after a reconnect.
    assert resampler.add_tick(_tick(40, 1995.0)) == []
    assert resampler.add_tick(_tick(59, 2008.0)) == []
    assert resampler.open_candle("M1") is None
    assert resampler.flush(START + timedelta(minutes=1)) == []


def test_a_late_tick_cannot_reopen_a_bucket_a_later_tick_closed():
    resampler = Resampler("XAUUSD", ["M1"])
    resampler.add_tick(_tick(0, 2000.0))
    closed = resampler.add_tick(_tick(61, 2020.0))
    assert [bar.open_time for bar in closed] == [START]

    assert resampler.add_tick(_tick(30, 1990.0)) == []
    open_bar = resampler.open_candle("M1")
    assert open_bar is not None
    assert open_bar.open_time == START + timedelta(minutes=1)
    assert open_bar.low == 2020.0  # the late 1990 never touched it


def test_the_flushed_bar_is_the_whole_bar_not_the_half_before_the_flush():
    """The repaint this guards against published *two* candles for one bucket,
    and the second held only the ticks that arrived after the flush."""
    resampler = Resampler("XAUUSD", ["M1"])
    for offset, price in ((0, 2000.0), (20, 2010.0)):
        resampler.add_tick(_tick(offset, price))
    published = list(resampler.flush(START + timedelta(minutes=1)))

    for offset, price in ((40, 1995.0), (59, 2008.0)):
        resampler.add_tick(_tick(offset, price))
    published += resampler.flush(START + timedelta(minutes=2))

    assert len(published) == 1, "one bucket must produce exactly one closed candle"
    assert published[0].open == 2000.0


def test_restore_refuses_a_bar_this_process_has_already_closed():
    resampler = Resampler("XAUUSD", ["M1"])
    resampler.add_tick(_tick(0, 2000.0))
    stale = resampler.flush(START + timedelta(minutes=1))[0]

    resampler.restore(stale)
    assert resampler.open_candle("M1") is None


def test_restore_still_resumes_a_bar_that_was_genuinely_open():
    resampler = Resampler("XAUUSD", ["M1"])
    resampler.add_tick(_tick(0, 2000.0))
    in_progress = resampler.open_candle("M1")
    assert in_progress is not None

    fresh = Resampler("XAUUSD", ["M1"])
    fresh.restore(in_progress)
    resumed = fresh.open_candle("M1")
    assert resumed is not None and resumed.open_time == START
