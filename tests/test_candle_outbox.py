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
    """A bus whose connection the test switches off and on."""

    def __init__(self, *, connected: bool) -> None:
        self.is_connected = connected
        self.confirmed: list[str] = []

    async def publish(self, subject, payload) -> None:
        raise AssertionError("the outbox must not use the unconfirmed publish")

    async def publish_confirmed(self, subject, payload, *, timeout: float = 5.0) -> None:
        self.confirmed.append(subject)


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
    outage_lines = [record for record in caplog.records if "not connected" in record.message]
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


async def test_a_confirmed_publish_flushes_after_publishing():
    client = RecordingClient()
    await bus_with(client).publish_confirmed("QTE.candle", {"close": 1.0})
    assert client.calls == ["publish:QTE.candle", "flush"]


async def test_a_confirmed_publish_refuses_without_a_connection():
    client = RecordingClient(connected=False)
    with pytest.raises(ConnectionError, match="not connected"):
        await bus_with(client).publish_confirmed("QTE.candle", {"close": 1.0})
    assert client.calls == []


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
