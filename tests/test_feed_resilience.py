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
    service._resamplers = {}
    service._repairer = None
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


class RecordingRepairer:
    """Stands in for vendor history: records what it completed, and what had gone out."""

    def __init__(self, bus) -> None:
        self.bus = bus
        self.repaired: list[object] = []
        self.published_before_repair: list[int] = []

    async def repair(self, candle, **repair_options):
        self.repaired.append(candle)
        self.published_before_repair.append(len(self.bus.published))
        return candle.model_copy(update={"open": 1111.0})


async def test_a_partial_bar_is_repaired_after_whole_bars_have_gone_out():
    service = object.__new__(IngestionService)
    service.state = CandleOutbox()
    service.bus = YieldingCandleBus()
    subject = staticmethod(lambda symbol, timeframe: f"{symbol}.{timeframe}")
    service.subjects = type("Subjects", (), {"candle_closed": subject})()
    joined_late = Resampler("XAUUSD", ["M1"])
    joined_late.mark_joined(MOMENT + timedelta(seconds=20))
    listening = Resampler("BTCUSDT", ["M1"])
    service._resamplers = {"XAUUSD": joined_late, "BTCUSDT": listening}
    service._repairer = RecordingRepairer(service.bus)

    joined_late.add_tick(Tick(symbol="XAUUSD", ts=MOMENT + timedelta(seconds=30), last=2400.0))
    listening.add_tick(Tick(symbol="BTCUSDT", ts=MOMENT + timedelta(seconds=5), last=60000.0))
    bucket_end = MOMENT + timedelta(minutes=1)
    closes = joined_late.flush(bucket_end) + listening.flush(bucket_end)
    await service._emit_candles(closes)

    assert [candle.symbol for candle in service.state.staged] == ["BTCUSDT", "XAUUSD"]
    assert service.state.staged[1].open == 1111.0, "the partial bar went out repaired"
    assert service._repairer.published_before_repair == [1], "the whole bar did not wait"


class FlakyStaging(CandleOutbox):
    """Fail at a chosen batch item without changing the accepted history."""

    def __init__(self, failure_index):
        super().__init__()
        self.failure_index = failure_index
        self.attempts = 0
        self.available = True

    async def stage_closed_candle(self, candle):
        self.attempts += 1
        if not self.available or self.attempts == self.failure_index:
            raise ConnectionError("Redis staging unavailable")
        await super().stage_closed_candle(candle)


def staging_service(failure_index):
    service = object.__new__(IngestionService)
    service.state = FlakyStaging(failure_index)
    service.bus = YieldingCandleBus()
    service._resamplers = {"XAUUSD": Resampler("XAUUSD", ["M1", "M5", "M15"])}
    service._repairer = None
    from qte_shared.bus import Subjects

    service.subjects = Subjects()
    return service


@pytest.mark.parametrize("failure_index", [1, 2, 3])
async def test_a_failed_stage_retains_every_unwritten_bar_in_the_batch(failure_index):
    service = staging_service(failure_index)
    resampler = service._resamplers["XAUUSD"]
    resampler.add_tick(Tick(symbol="XAUUSD", ts=MOMENT, last=2400))
    retired = resampler.flush(MOMENT + timedelta(minutes=15))
    with pytest.raises(ConnectionError, match="staging unavailable"):
        await service._emit_candles(retired)
    assert resampler.flush(MOMENT + timedelta(minutes=15)) == []
    assert len(service._retired_candles) == 4 - failure_index
    await service._emit_candles([])
    assert service.state.staged == retired
    assert service._retired_candles == {}
    assert len(service.bus.published) == 3


async def test_a_partial_flag_survives_failure_on_an_earlier_whole_bar():
    service = staging_service(1)
    partial = Resampler("EURUSD", ["M15"])
    partial.mark_joined(MOMENT + timedelta(seconds=20))
    partial.add_tick(Tick(symbol="EURUSD", ts=MOMENT + timedelta(seconds=30), last=1.2))
    whole_resampler = service._resamplers["XAUUSD"]
    whole_resampler.add_tick(Tick(symbol="XAUUSD", ts=MOMENT, last=2400))
    service._resamplers["EURUSD"] = partial
    service._repairer = RecordingRepairer(service.bus)
    closes = whole_resampler.flush(MOMENT + timedelta(minutes=15)) + partial.flush(
        MOMENT + timedelta(minutes=15)
    )
    with pytest.raises(ConnectionError):
        await service._emit_candles(closes, close_is_current=False)
    assert len(service._retired_candles) == 4
    await service._emit_candles([])
    assert len(service._repairer.repaired) == 1
    assert service.state.staged[-1].symbol == "EURUSD"
    assert service.state.staged[-1].open == 1111
    assert len(service.bus.published) == 4


async def test_a_repaired_bar_is_retained_without_repeating_vendor_repair():
    service = staging_service(1)
    resampler = service._resamplers["XAUUSD"]
    resampler.mark_joined(MOMENT + timedelta(seconds=20))
    resampler.add_tick(Tick(symbol="XAUUSD", ts=MOMENT + timedelta(seconds=30), last=2400))
    service._repairer = RecordingRepairer(service.bus)
    closes = resampler.flush(MOMENT + timedelta(minutes=1))
    with pytest.raises(ConnectionError):
        await service._emit_candles(closes)
    await service._emit_candles([])
    assert len(service._repairer.repaired) == 1
    assert service.state.staged[0].open == 1111
    assert service._retired_candles == {}


async def test_staging_backlog_prevents_unbounded_retirement_on_new_ticks():
    service = staging_service(1)
    resampler = service._resamplers["XAUUSD"]
    resampler.add_tick(Tick(symbol="XAUUSD", ts=MOMENT, last=2400))
    retired = resampler.flush(MOMENT + timedelta(minutes=15))
    with pytest.raises(ConnectionError):
        await service._emit_candles(retired)
    service.state.available = False
    for offset in range(20):
        with pytest.raises(ConnectionError):
            await service._handle_tick(
                Tick(symbol="XAUUSD", ts=MOMENT + timedelta(minutes=30 + offset), last=2401)
            )
        assert len(service._retired_candles) == 3
        assert resampler.open_candles() == []
    service.state.available = True
    await service._emit_candles([])
    assert service.state.staged == retired


async def test_wall_clock_loop_retries_retired_bars_without_another_tick(monkeypatch):
    service = staging_service(1)
    service._stopping = asyncio.Event()
    service._resamplers["XAUUSD"].add_tick(Tick(symbol="XAUUSD", ts=MOMENT, last=2400))
    monkeypatch.setattr("qte_ingestion.service.ingestion_settings.flush_interval", 0.001)
    original_publish = service.bus.publish

    async def stop_after_publish(subject, payload):
        await original_publish(subject, payload)
        if len(service.bus.published) == 3:
            service._stopping.set()

    service.bus.publish = stop_after_publish
    await asyncio.wait_for(service._flush_loop(), timeout=1)
    assert len(service.state.staged) == 3
    assert service._retired_candles == {}
