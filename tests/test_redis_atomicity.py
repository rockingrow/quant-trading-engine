"""Run the Redis Lua paths against an isolated in-memory Redis/Lua interpreter."""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from redis.exceptions import ResponseError

from qte_shared.cache.redis_state import MERGE_HISTORY_CANDLES, RedisState
from qte_shared.config import settings
from qte_shared.models import Candle


@pytest.fixture
async def candle_state():
    redis_state = RedisState(prefix="atomicity-test")
    redis_state._client = FakeRedis(decode_responses=True)
    try:
        yield redis_state
    finally:
        await redis_state.close()


def closed_candle(offset=0):
    return Candle(
        origin=settings.state_scope.origin(),
        symbol="XAUUSD",
        timeframe="M15",
        open_time=datetime(2026, 9, 14, tzinfo=UTC) + timedelta(minutes=offset),
        open=2000,
        high=2001,
        low=1999,
        close=2000,
    )


async def test_staging_is_idempotent_after_a_lost_redis_response(candle_state, monkeypatch):
    candle = closed_candle()
    await candle_state.set_open_candle(candle.model_copy(update={"is_closed": False}))
    original_eval = candle_state.client.eval
    lose_response = True

    async def lose_first_reply(script, number_keys, *arguments):
        nonlocal lose_response
        result = await original_eval(script, number_keys, *arguments)
        if lose_response:
            lose_response = False
            raise ConnectionError("Redis committed but its response was lost")
        return result

    monkeypatch.setattr(candle_state.client, "eval", lose_first_reply)
    with pytest.raises(ConnectionError):
        await candle_state.stage_closed_candle(candle)
    await candle_state.stage_closed_candle(candle)
    assert await candle_state.get_candles("XAUUSD", "M15") == [candle]
    assert await candle_state.pending_candles() == [candle]
    assert await candle_state.get_open_candle("XAUUSD", "M15") is None


async def test_staging_an_older_retirement_preserves_the_new_open_builder(candle_state):
    retired = closed_candle()
    current = closed_candle(15).model_copy(update={"is_closed": False})
    await candle_state.set_open_candle(current)
    await candle_state.stage_closed_candle(retired)
    await candle_state.stage_closed_candle(retired)
    assert await candle_state.get_open_candle("XAUUSD", "M15") == current
    assert await candle_state.pending_candles() == [retired]


async def test_staging_concurrent_duplicates_keeps_one_history_and_event(candle_state):
    candle = closed_candle()
    await asyncio.gather(*(candle_state.stage_closed_candle(candle) for _ in range(10)))
    assert await candle_state.get_candles("XAUUSD", "M15") == [candle]
    assert await candle_state.pending_candles() == [candle]


async def test_staging_retains_the_history_limit_without_duplicate_retries(candle_state):
    candles = [closed_candle(offset * 15) for offset in range(5)]
    for candle in candles:
        await candle_state.stage_closed_candle(candle, max_len=3)
    await candle_state.stage_closed_candle(candles[1], max_len=3)
    assert await candle_state.get_candles("XAUUSD", "M15") == candles[-3:]
    assert await candle_state.pending_candles() == candles


@pytest.mark.parametrize("corruption", ["outbox_type", "open_json"])
async def test_invalid_redis_state_fails_before_any_partial_stage(candle_state, corruption):
    if corruption == "outbox_type":
        await candle_state.client.set(candle_state.key("outbox", "candles"), "invalid")
    else:
        await candle_state.client.set(candle_state.key("open_candle", "XAUUSD", "M15"), "invalid")
    with pytest.raises(ResponseError):
        await candle_state.stage_closed_candle(closed_candle())
    assert await candle_state.get_candles("XAUUSD", "M15") == []
    assert await candle_state.client.get(candle_state.key("staged", "XAUUSD", "M15")) is None


async def test_provider_discard_resets_the_staging_watermark(candle_state):
    await candle_state.stage_closed_candle(closed_candle(15))
    await candle_state.discard_candle_state("XAUUSD", "M15")
    await candle_state.discard_candle_outbox()
    earlier = closed_candle()
    await candle_state.stage_closed_candle(earlier)
    assert await candle_state.get_candles("XAUUSD", "M15") == [earlier]
    assert await candle_state.pending_candles() == [earlier]


async def test_runner_claim_is_exclusive_nonexpiring_and_owner_checked():
    redis_server = FakeServer()
    first_state = RedisState(prefix="ownership-test")
    second_state = RedisState(prefix="ownership-test")
    first_state._client = FakeRedis(server=redis_server, decode_responses=True)
    second_state._client = FakeRedis(server=redis_server, decode_responses=True)
    try:
        claimed = await asyncio.gather(
            first_state.claim_runner("first-owner"), second_state.claim_runner("second-owner")
        )
        assert sum(claimed) == 1
        selected = "first-owner" if claimed[0] else "second-owner"
        rejected = "second-owner" if claimed[0] else "first-owner"
        assert await first_state.client.ttl(first_state.key("runner", "owner")) == -1
        assert not await first_state.release_runner(rejected)
        assert await second_state.owns_runner(selected)
        await first_state.close()
        assert not await second_state.claim_runner("replacement-owner")
        assert await second_state.release_runner(selected)
        assert await second_state.claim_runner("replacement-owner")
    finally:
        await first_state.close()
        await second_state.close()


async def test_only_the_same_container_instance_reclaims_a_held_claim(candle_state):
    owner_key = candle_state.key("runner", "owner")
    assert await candle_state.runner_owner() is None
    # Nothing held: reclaiming is not a way to claim.
    assert not await candle_state.reclaim_runner("instance-one", "instance-one:second")
    assert await candle_state.claim_runner("instance-one:first")

    assert not await candle_state.reclaim_runner("instance-two", "instance-two:first")
    # A token that is only a prefix of the holder's token is another container.
    assert not await candle_state.reclaim_runner("instance", "instance:first")
    assert await candle_state.runner_owner() == "instance-one:first"

    assert await candle_state.reclaim_runner("instance-one", "instance-one:second")
    assert await candle_state.owns_runner("instance-one:second")
    assert not await candle_state.owns_runner("instance-one:first")
    assert await candle_state.client.ttl(owner_key) == -1


async def test_a_claim_without_an_instance_token_is_never_reclaimed(candle_state):
    assert await candle_state.claim_runner("9d0c5c1e-plain-host-process")
    assert not await candle_state.reclaim_runner("9d0c5c1e", "9d0c5c1e:replacement")
    assert await candle_state.runner_owner() == "9d0c5c1e-plain-host-process"


# ── Warm-up history: the one write allowed behind the watermark ───────────
#
# `stage_closed_candle` reads any bar at or below the staging watermark as a
# duplicate replay, which is correct for a forward-only feed and wrong for a
# vendor replaying the history a half-filled window is missing — those bars are
# precisely the old ones. `merge_history_candles` is that second path.


def history_candle(index, close=2000.0):
    """A bar on its M15 bucket; *index* may be negative for older history."""
    return Candle(
        origin=settings.state_scope.origin(),
        symbol="XAUUSD",
        timeframe="M15",
        open_time=datetime(2026, 9, 28, 10, 0, tzinfo=UTC) + timedelta(minutes=15 * index),
        open=2000,
        high=2001,
        low=1999,
        close=close,
    )


async def staged_watermark(candle_state):
    return await candle_state.client.get(candle_state.key("staged", "XAUUSD", "M15"))


async def test_bars_older_than_the_watermark_merge_although_staging_refuses_them(candle_state):
    """The regression this path exists for: a window stuck part-filled."""
    for index in range(35):
        await candle_state.stage_closed_candle(history_candle(index))
    assert await candle_state.count_candles("XAUUSD", "M15") == 35
    missing = [history_candle(index) for index in range(-115, 0)]

    # The live path cannot take them: to it they are a redelivery.
    await candle_state.stage_closed_candle(missing[-1])
    assert await candle_state.count_candles("XAUUSD", "M15") == 35

    assert await candle_state.merge_history_candles("XAUUSD", "M15", missing) == 150
    stored = await candle_state.get_candles("XAUUSD", "M15")
    assert len(stored) == 150
    assert stored[0].open_time == history_candle(-115).open_time
    assert stored[-1].open_time == history_candle(34).open_time
    assert all(
        earlier.open_time < later.open_time
        for earlier, later in zip(stored, stored[1:], strict=False)
    )


async def test_the_watermark_rises_for_newer_history_and_never_falls(candle_state):
    """Lowering it would reopen the duplicate window it exists to close."""
    await candle_state.stage_closed_candle(history_candle(10))
    ahead = await staged_watermark(candle_state)

    await candle_state.merge_history_candles(
        "XAUUSD", "M15", [history_candle(index) for index in range(5)]
    )
    assert await staged_watermark(candle_state) == ahead

    await candle_state.merge_history_candles("XAUUSD", "M15", [history_candle(20)])
    assert float(await staged_watermark(candle_state)) == history_candle(20).open_time.timestamp()


async def test_a_close_staged_mid_merge_is_not_lost(candle_state):
    """The window and the watermark are read together, and the swap re-checks it."""
    for index in range(3):
        await candle_state.stage_closed_candle(history_candle(index))
    original_eval = candle_state._client.eval
    raced = []

    async def stage_a_close_between_the_read_and_the_swap(script, number_keys, *arguments):
        if script is MERGE_HISTORY_CANDLES and not raced:
            raced.append(True)
            await candle_state.stage_closed_candle(history_candle(3))
        return await original_eval(script, number_keys, *arguments)

    candle_state._client.eval = stage_a_close_between_the_read_and_the_swap
    written = await candle_state.merge_history_candles(
        "XAUUSD", "M15", [history_candle(-2), history_candle(-1)]
    )
    assert raced, "the race this guards against never happened"
    assert written == 6
    open_times = [candle.open_time for candle in await candle_state.get_candles("XAUUSD", "M15")]
    assert history_candle(3).open_time in open_times


async def test_a_warmup_bar_wins_the_same_open_time(candle_state):
    """A vendor replaying its own history is the completer record of that bucket."""
    await candle_state.stage_closed_candle(history_candle(0, close=1111.0))
    await candle_state.merge_history_candles("XAUUSD", "M15", [history_candle(0, close=2222.0)])
    stored = await candle_state.get_candles("XAUUSD", "M15")
    assert len(stored) == 1
    assert stored[0].close == 2222.0


async def test_a_batch_larger_than_the_window_keeps_its_newest_bars(candle_state):
    batch = [history_candle(index) for index in range(10)]
    written = await candle_state.merge_history_candles("XAUUSD", "M15", batch, max_len=4)
    assert written == 4
    stored = await candle_state.get_candles("XAUUSD", "M15")
    assert [candle.open_time for candle in stored] == [
        history_candle(index).open_time for index in range(6, 10)
    ]


async def test_an_empty_batch_touches_nothing(candle_state):
    await candle_state.stage_closed_candle(history_candle(0))
    assert await candle_state.merge_history_candles("XAUUSD", "M15", []) == 0
    assert await candle_state.count_candles("XAUUSD", "M15") == 1


async def test_a_window_that_keeps_changing_is_left_alone_rather_than_half_written(
    candle_state, monkeypatch, caplog
):
    """Losing the race every time is reported, not retried for ever."""
    await candle_state.stage_closed_candle(history_candle(0))

    async def always_lose(script, number_keys, *arguments):
        return -1

    monkeypatch.setattr(candle_state._client, "eval", always_lose)
    with caplog.at_level("WARNING"):
        assert await candle_state.merge_history_candles("XAUUSD", "M15", [history_candle(-1)]) == 0
    assert "Gave up merging" in caplog.text
    assert await candle_state.count_candles("XAUUSD", "M15") == 1
