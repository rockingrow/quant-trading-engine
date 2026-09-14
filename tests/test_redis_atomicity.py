"""Run the Redis Lua paths against an isolated in-memory Redis/Lua interpreter."""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from redis.exceptions import ResponseError

from qte_shared.cache.redis_state import RedisState
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
