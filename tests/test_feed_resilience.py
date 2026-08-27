"""A consumer's failures must not cost the feed its socket.

`_on_tick` runs inside the same `try` that wraps the connect and the receive
loop. Before these tests, a handler that raised was caught by the reconnect
handler: it tore down a healthy connection, lost every tick that arrived
during the backoff, and logged a Redis or NATS failure as the *feed* dropping
— pointing whoever read that log at the wrong service entirely.

The ingestion flush loop has the same shape of problem from the other end: it
is the only thing that closes a bar in a market too quiet to push the bucket
over with a tick, and nothing awaits it until shutdown, so an exception
escaping it would silently end wall-clock closing for the life of the process.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta

import pytest
from qte_ingestion.resampler import Resampler
from qte_ingestion.service import IngestionService
from qte_shared.models import Tick
from qte_shared.providers.simulator.feed import SimulatorLiveFeed
from qte_shared.providers.simulator.protocol import encode_tick
from qte_shared.providers.tiingo.settings import TiingoSettings
from qte_shared.providers.tiingo.ws import TiingoLiveFeed

MOMENT = datetime(2026, 5, 1, 10, 0, tzinfo=UTC)


class LifecycleResource:
    def __init__(self, name: str, events: list[str], *, fail_connect: bool = False) -> None:
        self.name = name
        self.events = events
        self.fail_connect = fail_connect

    async def connect(self) -> None:
        self.events.append(f"connect:{self.name}")
        if self.fail_connect:
            raise RuntimeError(f"{self.name} unavailable")

    async def close(self) -> None:
        self.events.append(f"close:{self.name}")


def _frame(price: float) -> str:
    tick = Tick(symbol="XAUUSD", ts=MOMENT, last=price, volume=1.0)
    return json.dumps(encode_tick(tick, seq=1))


async def test_partial_ingestion_startup_closes_resources_already_acquired():
    events: list[str] = []
    service = object.__new__(IngestionService)
    service.bus = LifecycleResource("bus", events)
    service.state = LifecycleResource("state", events, fail_connect=True)
    service._feeds = []
    service._flush_task = None
    service._stopping = asyncio.Event()

    with pytest.raises(RuntimeError, match="state unavailable"):
        await service.start()
    await service.stop()

    assert events == ["connect:bus", "connect:state", "close:state", "close:bus"]


# ── The simulator feed ────────────────────────────────────────────────────


async def test_a_raising_handler_does_not_stop_the_simulator_feed():
    seen: list[float] = []

    async def handler(tick: Tick) -> None:
        seen.append(tick.price)
        raise ZeroDivisionError("a bug in the consumer, not in the feed")

    feed = SimulatorLiveFeed(["XAUUSD"], handler)
    for price in (2400.0, 2401.0, 2402.0):
        await feed._handle_raw(_frame(price))

    assert seen == [2400.0, 2401.0, 2402.0]
    assert feed.ticks_received == 3


async def test_a_cancelled_handler_still_propagates():
    """Cancellation is shutdown, not a consumer bug — it must not be logged
    and swallowed like one, or `stop()` would never unwind."""

    async def handler(_tick: Tick) -> None:
        raise asyncio.CancelledError

    feed = SimulatorLiveFeed(["XAUUSD"], handler)
    with pytest.raises(asyncio.CancelledError):
        await feed._handle_raw(_frame(2400.0))


# ── The vendor feed, which has the identical shape ────────────────────────


async def test_a_raising_handler_does_not_stop_the_tiingo_feed():
    seen: list[float] = []

    async def handler(tick: Tick) -> None:
        seen.append(tick.price)
        raise ConnectionError("Redis went away mid-tick")

    feed = TiingoLiveFeed(
        "crypto",
        {"btcusdt": "BTCUSDT"},
        handler,
        TiingoSettings(api_key="test-key"),
    )
    row = ["T", "btcusdt", MOMENT.isoformat(), "binance", 0.5, 60000.0]
    for _ in range(3):
        await feed._handle_raw(json.dumps({"messageType": "A", "data": row}))

    assert len(seen) == 3


# ── The ingestion flush loop ──────────────────────────────────────────────


async def test_a_failed_publish_costs_one_cycle_not_the_flush_loop(monkeypatch):
    service = object.__new__(IngestionService)  # no Redis, NATS or provider
    service._stopping = asyncio.Event()
    service._resamplers = {"XAUUSD": Resampler("XAUUSD", ["M1"])}

    emitted: list[object] = []
    failing = True

    async def emit(candles: list[object]) -> None:
        if failing:
            raise ConnectionError("NATS publish failed")
        emitted.extend(candles)

    async def drain() -> None:
        return None

    service._emit_candles = emit
    service._drain_candle_outbox = drain

    import qte_ingestion.service as service_module

    monkeypatch.setattr(service_module.ingestion_settings, "flush_interval", 0.01)
    loop_task = asyncio.create_task(service._flush_loop())
    try:
        # A bar whose bucket is already over, so the next flush closes it.
        service._resamplers["XAUUSD"].add_tick(
            Tick(symbol="XAUUSD", ts=datetime.now(UTC) - timedelta(minutes=5), last=2400.0)
        )
        await asyncio.sleep(0.05)
        assert not loop_task.done(), "the loop died on a failed publish"

        failing = False
        service._resamplers["XAUUSD"].add_tick(
            Tick(symbol="XAUUSD", ts=datetime.now(UTC) - timedelta(minutes=2), last=2410.0)
        )
        await asyncio.sleep(0.05)
        assert emitted, "the loop stopped closing bars after the failure"
    finally:
        service._stopping.set()
        loop_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await loop_task


class CandleOutbox:
    def __init__(self) -> None:
        self.pending = []
        self.staged = []

    async def stage_closed_candle(self, candle) -> None:
        self.staged.append(candle)
        self.pending.append(candle)

    async def peek_pending_candle(self):
        return self.pending[0] if self.pending else None

    async def ack_pending_candle(self) -> None:
        self.pending.pop(0)


class FlakyCandleBus:
    def __init__(self) -> None:
        self.fail = True
        self.published = []

    async def publish(self, subject, payload) -> None:
        if self.fail:
            raise ConnectionError("core NATS is unavailable")
        self.published.append((subject, payload))


class YieldingCandleBus(FlakyCandleBus):
    def __init__(self) -> None:
        super().__init__()
        self.fail = False

    async def publish(self, subject, payload) -> None:
        await asyncio.sleep(0)
        self.published.append((subject, payload))


async def test_a_failed_candle_publish_remains_in_the_durable_outbox():
    service = object.__new__(IngestionService)
    service.state = CandleOutbox()
    service.bus = FlakyCandleBus()
    subject = staticmethod(lambda symbol, timeframe: f"{symbol}.{timeframe}")
    service.subjects = type("Subjects", (), {"candle_closed": subject})()
    candle = Resampler("XAUUSD", ["M1"])
    candle.add_tick(Tick(symbol="XAUUSD", ts=MOMENT, last=2400.0))
    closed = candle.flush(MOMENT + timedelta(minutes=1))[0]

    with pytest.raises(ConnectionError, match="unavailable"):
        await service._emit_candle(closed)

    assert service.state.pending == [closed]
    assert service.state.staged == [closed]

    service.bus.fail = False
    await service._drain_candle_outbox()

    assert service.state.pending == []
    assert len(service.bus.published) == 1
    assert (
        service.bus.published[0][1]["candle"]["open_time"]
        == closed.model_dump(mode="json")["open_time"]
    )


async def test_concurrent_feeds_cannot_ack_a_candle_another_feed_has_not_published():
    service = object.__new__(IngestionService)
    service.state = CandleOutbox()
    service.bus = YieldingCandleBus()
    subject = staticmethod(lambda symbol, timeframe: f"{symbol}.{timeframe}")
    service.subjects = type("Subjects", (), {"candle_closed": subject})()
    first = Resampler("XAUUSD", ["M1"])
    second = Resampler("BTCUSDT", ["M1"])
    first.add_tick(Tick(symbol="XAUUSD", ts=MOMENT, last=2400.0))
    second.add_tick(Tick(symbol="BTCUSDT", ts=MOMENT, last=60000.0))
    service.state.pending.extend(
        [
            first.flush(MOMENT + timedelta(minutes=1))[0],
            second.flush(MOMENT + timedelta(minutes=1))[0],
        ]
    )

    await asyncio.gather(service._drain_candle_outbox(), service._drain_candle_outbox())

    assert service.state.pending == []
    assert [payload["symbol"] for _, payload in service.bus.published] == [
        "XAUUSD",
        "BTCUSDT",
    ]
