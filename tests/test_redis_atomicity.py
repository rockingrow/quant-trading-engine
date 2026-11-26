"""Run the Redis Lua paths against an isolated in-memory Redis/Lua interpreter."""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from redis.exceptions import ResponseError

from qte_shared.cache import redis_state as redis_state_module
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


async def test_each_service_claims_its_own_namespace_independently(candle_state):
    # Ingestion claims one too: two of them share the candle outbox. The two
    # claims must not block or reclaim each other — one host runs both.
    assert await candle_state.claim_service("data-ingestion", "ingestion-one")
    assert await candle_state.claim_runner("runner-one")
    assert not await candle_state.claim_service("data-ingestion", "ingestion-two")

    assert await candle_state.service_owner("data-ingestion") == "ingestion-one"
    assert await candle_state.runner_owner() == "runner-one"
    assert await candle_state.client.ttl(candle_state.key("data-ingestion", "owner")) == -1

    assert not await candle_state.release_service("data-ingestion", "runner-one")
    assert await candle_state.release_service("data-ingestion", "ingestion-one")
    assert await candle_state.runner_owner() == "runner-one"
    assert await candle_state.claim_service("data-ingestion", "ingestion-two")


# ── The candle outbox: acknowledge only what was published ────────────────


async def test_acknowledging_a_batch_removes_exactly_that_batch(candle_state):
    for offset in range(3):
        await candle_state.stage_closed_candle(closed_candle(offset * 15))
    batch = await candle_state.pending_candle_batch(2)

    assert await candle_state.ack_pending_candles(batch)
    remaining = await candle_state.pending_candle_batch(10)
    assert [entry.candle.open_time for entry in remaining] == [closed_candle(30).open_time]


async def test_a_batch_that_is_no_longer_the_head_is_not_acknowledged(candle_state):
    # Two ingestion processes on one namespace: both read the same batch, both
    # publish it, and the second one must not trim the batch behind the first.
    # Trimming by count would acknowledge closes nobody ever published, and the
    # staging watermark means they could never be staged again.
    for offset in range(4):
        await candle_state.stage_closed_candle(closed_candle(offset * 15))
    first_drain = await candle_state.pending_candle_batch(2)
    second_drain = await candle_state.pending_candle_batch(2)

    assert await candle_state.ack_pending_candles(first_drain)
    assert not await candle_state.ack_pending_candles(second_drain)

    survivors = await candle_state.pending_candle_batch(10)
    assert [entry.candle.open_time for entry in survivors] == [
        closed_candle(30).open_time,
        closed_candle(45).open_time,
    ]


async def test_acknowledging_more_than_the_outbox_holds_removes_nothing(candle_state):
    await candle_state.stage_closed_candle(closed_candle())
    batch = await candle_state.pending_candle_batch(1)
    await candle_state.ack_pending_candle()

    assert not await candle_state.ack_pending_candles(batch)
    assert await candle_state.pending_candle_batch(10) == []


async def test_an_empty_acknowledgement_is_a_no_op(candle_state):
    await candle_state.stage_closed_candle(closed_candle())
    assert await candle_state.ack_pending_candles([])
    assert len(await candle_state.pending_candle_batch(10)) == 1


async def test_quarantine_moves_the_head_and_caps_the_dead_letters(candle_state, monkeypatch):
    monkeypatch.setattr(redis_state_module, "DEAD_LETTER_LIMIT", 2)
    for offset in range(3):
        await candle_state.stage_closed_candle(closed_candle(offset * 15))

    for _ in range(3):
        await candle_state.quarantine_pending_candle()

    assert await candle_state.pending_candle_batch(10) == []
    dead_letters = await candle_state.client.lrange(
        candle_state.key("outbox", "candles", "dead"), 0, -1
    )
    # One move and one trim in a single script, so the cap holds even though
    # three entries passed through.
    assert len(dead_letters) == 2
    # An empty outbox is not an error, and moves nothing.
    await candle_state.quarantine_pending_candle()
    assert (
        len(await candle_state.client.lrange(candle_state.key("outbox", "candles", "dead"), 0, -1))
        == 2
    )


# ── The decided-bar watermark ─────────────────────────────────────────────


async def test_the_decided_bar_mark_never_moves_backwards(candle_state):
    newest = datetime(2026, 9, 14, 12, 0, tzinfo=UTC)
    await candle_state.set_decided_open_time("QTE_X", "XAUUSD", "M15", newest)
    # An older bar reaching this after the newer one would reopen a window the
    # runner has already decided on, and the catch-up would trade it again.
    await candle_state.set_decided_open_time(
        "QTE_X", "XAUUSD", "M15", newest - timedelta(minutes=15)
    )
    assert await candle_state.get_decided_open_time("QTE_X", "XAUUSD", "M15") == newest

    later = newest + timedelta(minutes=15)
    await candle_state.set_decided_open_time("QTE_X", "XAUUSD", "M15", later)
    assert await candle_state.get_decided_open_time("QTE_X", "XAUUSD", "M15") == later


async def test_an_unreadable_decided_bar_mark_is_replaced(candle_state, caplog):
    key = candle_state.key("decided", "QTE_X", "XAUUSD", "M15")
    await candle_state.client.set(key, "not-a-timestamp")
    moment = datetime(2026, 9, 14, 12, 0, tzinfo=UTC)

    with caplog.at_level("WARNING"):
        await candle_state.set_decided_open_time("QTE_X", "XAUUSD", "M15", moment)

    assert "unreadable decided-bar mark" in caplog.text
    assert await candle_state.get_decided_open_time("QTE_X", "XAUUSD", "M15") == moment


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


async def test_connect_declares_the_connection_health_options(monkeypatch):
    """A dropped pooled connection must be detected, not waited out.

    redis-py's defaults leave a connection the container network killed in the
    pool until a command blocks on it for the whole read deadline. These options
    are the difference between a reconnect and a ``TimeoutError`` against a
    server that answers in under a millisecond, so they are asserted rather
    than left to the client's defaults — which changed under a `>=5.0` floor.
    """
    captured = {}

    class ProbeClient:
        async def ping(self):
            return True

    def record_from_url(url, **options):
        captured["url"] = url
        captured.update(options)
        return ProbeClient()

    monkeypatch.setattr(redis_state_module.redis, "from_url", record_from_url)
    storage = RedisState(url="redis://redis-cache:6379/0")
    await storage.connect()

    assert captured["socket_keepalive"] is True
    assert captured["retry_on_timeout"] is True
    assert captured["decode_responses"] is True
    assert captured["socket_timeout"] > 0
    assert captured["socket_connect_timeout"] > 0
    assert captured["health_check_interval"] > 0
    # RESP3 pairs with maintenance notifications, which the OSS server rejects.
    assert captured["protocol"] == 2


async def test_a_decided_bar_mark_dated_after_now_is_repaired(candle_state):
    # A synthetic feed anchors its bars ahead of the clock, so it leaves a mark
    # nothing can be newer than. Nothing deletes this key — `discard_candle_state`
    # clears the window, not the mark — so refusing the write because the
    # poisoned value is "newer" would make it permanent.
    poisoned = datetime.now(UTC) + timedelta(days=400)
    await candle_state.set_decided_open_time("QTE_X", "XAUUSD", "M15", poisoned)
    assert await candle_state.get_decided_open_time("QTE_X", "XAUUSD", "M15") == poisoned

    real_bar = datetime.now(UTC) - timedelta(minutes=15)
    await candle_state.set_decided_open_time("QTE_X", "XAUUSD", "M15", real_bar)
    assert await candle_state.get_decided_open_time("QTE_X", "XAUUSD", "M15") == real_bar
