"""The candle outbox pops a close only once the NATS server has it.

nats-py buffers a core publish while it reconnects and returns as if the
message had gone. An outbox that popped its row on that return lost every
close staged during an outage to a restart before the connection came back —
the case the outbox exists for. These tests pin the three guards: no draining
without a connection, a server-confirmed publish before the pop, and an
unpublishable head moved aside instead of blocking everything behind it.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

import pytest
from fakeredis.aioredis import FakeRedis
from ingestion_support import bare_ingestion_service
from nats.js import errors as js_errors

from qte_ingestion.resampler import Resampler
from qte_shared.bus import NatsBus, Subjects
from qte_shared.cache.redis_state import RedisState
from qte_shared.config import settings
from qte_shared.models import Tick
from qte_shared.state_scope import StateScope, stamp_market_data

MOMENT = datetime(2026, 5, 1, 10, 0, tzinfo=UTC)


class ConfirmingBus:
    """A bus whose connection, and whose flush, the test controls."""

    def __init__(self, *, connected: bool, flush_fails: bool = False) -> None:
        self.is_connected = connected
        self.flush_fails = flush_fails
        self.buffered: list[str] = []
        self.confirmed: list[str] = []
        self.flushes = 0

    async def publish(self, subject, payload) -> None:
        self.buffered.append(subject)

    async def confirm_published(self, *, timeout: float = 5.0) -> None:
        self.flushes += 1
        if self.flush_fails:
            raise TimeoutError("no PONG")
        self.confirmed.extend(self.buffered)
        self.buffered.clear()


def closed_candle(symbol: str = "XAUUSD", *, scope: StateScope | None = None):
    resampler = Resampler(symbol, ["M1"])
    resampler.add_tick(Tick(symbol=symbol, ts=MOMENT, last=2400.0))
    candle = resampler.flush(MOMENT + timedelta(minutes=1))[0]
    return stamp_market_data(candle, (scope or settings.state_scope).origin())


def outbox_service(bus: ConfirmingBus):
    state = RedisState(prefix="candle-outbox-test")
    state._client = FakeRedis(decode_responses=True)
    service = bare_ingestion_service(bus=bus, state=state)
    service.subjects = Subjects()
    return service


async def test_nothing_is_popped_while_nats_is_disconnected(caplog):
    bus = ConfirmingBus(connected=False)
    service = outbox_service(bus)
    await service.state.stage_closed_candle(closed_candle())

    with caplog.at_level(logging.WARNING, logger="qte_ingestion.service"):
        await service._drain_candle_outbox()
        await service._drain_candle_outbox()

    assert await service.state.peek_pending_candle() is not None
    assert bus.confirmed == []
    outage_lines = [record for record in caplog.records if "stay in the outbox" in record.message]
    assert len(outage_lines) == 1, "an outage is logged once, not on every flush interval"

    bus.is_connected = True
    await service._drain_candle_outbox()

    assert await service.state.peek_pending_candle() is None
    assert len(bus.confirmed) == 1


async def test_an_undecodable_head_is_moved_aside_and_the_rest_go_out(caplog):
    bus = ConfirmingBus(connected=True)
    service = outbox_service(bus)
    outbox_key = service.state.key("outbox", "candles")
    await service.state.client.rpush(outbox_key, "{not a candle")
    await service.state.stage_closed_candle(closed_candle())

    with caplog.at_level(logging.ERROR, logger="qte_ingestion.service"):
        await service._drain_candle_outbox()

    assert len(bus.confirmed) == 1
    assert await service.state.peek_pending_candle() is None
    dead_letters = await service.state.client.lrange(
        service.state.key("outbox", "candles", "dead"), 0, -1
    )
    assert dead_letters == ["{not a candle"]
    assert any("dead-letter" in record.message for record in caplog.records)


async def test_a_foreign_scope_head_is_moved_aside_rather_than_blocking():
    bus = ConfirmingBus(connected=True)
    service = outbox_service(bus)
    foreign = closed_candle(scope=StateScope("prod", "live", "tiingo"))
    outbox_key = service.state.key("outbox", "candles")
    await service.state.client.rpush(outbox_key, foreign.model_dump_json())
    await service.state.stage_closed_candle(closed_candle("BTCUSDT"))

    await service._drain_candle_outbox()

    assert len(bus.confirmed) == 1
    assert "BTCUSDT" in bus.confirmed[0]
    assert await service.state.peek_pending_candle() is None


# ── NatsBus ───────────────────────────────────────────────────────────


class RecordingClient:
    def __init__(self, *, connected: bool = True) -> None:
        self.is_connected = connected
        self.calls: list[str] = []

    async def publish(self, subject, data) -> None:
        self.calls.append(f"publish:{subject}")

    async def flush(self, timeout) -> None:
        self.calls.append("flush")


def bus_with(client: RecordingClient) -> NatsBus:
    bus = NatsBus(url="nats://engine-node:4222", token="secret")
    bus._nc = client
    return bus


async def test_confirming_a_batch_is_one_flush_behind_its_publishes():
    client = RecordingClient()
    bus = bus_with(client)
    await bus.publish("QTE.candle.a", {"close": 1.0})
    await bus.publish("QTE.candle.b", {"close": 2.0})
    await bus.confirm_published()
    assert client.calls == ["publish:QTE.candle.a", "publish:QTE.candle.b", "flush"]


async def test_confirming_refuses_without_a_connection():
    client = RecordingClient(connected=False)
    with pytest.raises(ConnectionError, match="not connected"):
        await bus_with(client).confirm_published()
    assert client.calls == []


async def test_a_batch_of_closes_is_confirmed_with_one_round_trip():
    bus = ConfirmingBus(connected=True)
    service = outbox_service(bus)
    for symbol in ("XAUUSD", "BTCUSDT", "EURUSD"):
        await service.state.stage_closed_candle(closed_candle(symbol))

    await service._drain_candle_outbox()

    assert len(bus.confirmed) == 3
    assert bus.flushes == 1
    assert await service.state.peek_pending_candle() is None


async def test_a_flush_that_times_out_leaves_the_batch_staged_without_raising():
    # At boot or on any drain: a slow PONG must neither crash ingestion nor
    # remove closes the server may not have.
    bus = ConfirmingBus(connected=True, flush_fails=True)
    service = outbox_service(bus)
    await service.state.stage_closed_candle(closed_candle())

    await service._drain_candle_outbox()

    assert await service.state.peek_pending_candle() is not None
    bus.flush_fails = False
    await service._drain_candle_outbox()
    assert await service.state.peek_pending_candle() is None


async def test_an_outage_with_nothing_staged_does_not_mute_the_next_one(caplog):
    bus = ConfirmingBus(connected=False)
    service = outbox_service(bus)
    await service._drain_candle_outbox()  # nothing staged: nothing to say
    bus.is_connected = True
    await service._drain_candle_outbox()
    bus.is_connected = False
    await service.state.stage_closed_candle(closed_candle())

    with caplog.at_level(logging.WARNING, logger="qte_ingestion.service"):
        await service._drain_candle_outbox()

    assert any("stay in the outbox" in record.message for record in caplog.records)


class StreamContext:
    def __init__(self, lookup_error: Exception) -> None:
        self.lookup_error = lookup_error
        self.created: list[str] = []

    async def stream_info(self, name):
        raise self.lookup_error

    async def add_stream(self, config):
        self.created.append(config.name)


async def test_a_missing_stream_is_created():
    bus = bus_with(RecordingClient())
    bus._js = StreamContext(js_errors.NotFoundError())
    await bus.ensure_stream("QTE_TEST", ["QTE.test.>"])
    assert bus._js.created == ["QTE_TEST"]


async def test_a_lookup_that_times_out_is_not_read_as_missing():
    bus = bus_with(RecordingClient())
    bus._js = StreamContext(TimeoutError("no answer"))
    with pytest.raises(TimeoutError):
        await bus.ensure_stream("QTE_TEST", ["QTE.test.>"])
    assert bus._js.created == []


async def test_wait_closed_returns_once_the_client_gives_up():
    bus = NatsBus(url="nats://engine-node:4222", token="secret")
    await bus._on_closed()
    await bus.wait_closed()


async def test_a_held_outbox_keeps_reporting_itself(caplog):
    """One line per outage is not enough when the hold never ends.

    A connection that answers but never confirms fails every drain the same
    way; logging only the first would leave an engine delivering no candles at
    all looking healthy.
    """
    from qte_ingestion.service import OUTBOX_HOLD_LOG_EVERY

    bus = ConfirmingBus(connected=True, flush_fails=True)
    service = outbox_service(bus)
    await service.state.stage_closed_candle(closed_candle())

    with caplog.at_level(logging.WARNING, logger="qte_ingestion.service"):
        for _ in range(OUTBOX_HOLD_LOG_EVERY + 1):
            await service._drain_candle_outbox()

    held = [record for record in caplog.records if "stay in the outbox" in record.message]
    assert len(held) == 2, "the first hold and one reminder a minute later"
    assert "TimeoutError" in held[0].getMessage()


async def test_a_hold_for_a_new_reason_is_reported_at_once(caplog):
    bus = ConfirmingBus(connected=True, flush_fails=True)
    service = outbox_service(bus)
    await service.state.stage_closed_candle(closed_candle())

    with caplog.at_level(logging.WARNING, logger="qte_ingestion.service"):
        await service._drain_candle_outbox()
        bus.is_connected = False
        await service._drain_candle_outbox()

    held = [
        record.getMessage() for record in caplog.records if "stay in the outbox" in record.message
    ]
    assert len(held) == 2
    assert "TimeoutError" in held[0] and "not connected" in held[1]


async def test_a_batch_limit_of_zero_reads_nothing():
    # LRANGE 0 -1 is Redis for "everything"; a limit of zero must not drain
    # the whole outbox into one batch.
    bus = ConfirmingBus(connected=True)
    service = outbox_service(bus)
    for symbol in ("XAUUSD", "BTCUSDT"):
        await service.state.stage_closed_candle(closed_candle(symbol))

    assert await service.state.pending_candle_batch(0) == []
    assert len(await service.state.pending_candle_batch(2)) == 2
