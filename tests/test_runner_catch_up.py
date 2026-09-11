"""A close published while the runner was not listening still reaches its strategy.

Pinned from the 2026-09-10 production audit: ingestion and the runner restarted
together, ingestion published the bar it had restored twelve seconds before the
runner subscribed, and that bar entered the window without the strategy ever
deciding on it. Core NATS does not replay, so the runner reads what it missed
back out of Redis — and decides on it only while it is still fresh.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from qte_shared.config import settings
from qte_shared.models import Candle, CandleClosedEvent
from qte_shared.strategies.signal_factory import SignalFactory
from qte_shared.strategies.strategy_base import StrategyBase
from qte_shared.timeframes import floor_to_bucket
from qte_strategy_engine.runner import StrategyRunner, StrategySlot

BAR_LENGTH = timedelta(minutes=15)


class DecisionRecorder(StrategyBase):
    name = "CATCH_UP_PROBE"
    timeframe = "M15"
    warmup = 1
    max_history = 50

    def __init__(self, params=None) -> None:
        super().__init__(params)
        self.decided_on: list[datetime] = []

    def on_candle_closed(self, candles_frame, context):
        # The bar being decided on is the context's moment, whichever way its
        # candle reached the runner (Redis history or a live NATS close).
        self.decided_on.append(context.now)
        return None


def bar_at(open_time: datetime, *, tick_count: int = 5) -> Candle:
    return Candle(
        symbol="XAUUSD",
        timeframe="M15",
        open_time=open_time,
        open=1.0,
        high=1.0,
        low=1.0,
        close=1.0,
        tick_count=tick_count,
    )


def completed_bars(bar_count: int) -> list[Candle]:
    """*bar_count* consecutive bars ending on the last completed bucket."""
    last_completed = floor_to_bucket(datetime.now(UTC), "M15") - BAR_LENGTH
    return [
        bar_at(last_completed - BAR_LENGTH * (bar_count - 1 - offset))
        for offset in range(bar_count)
    ]


class CandleStore:
    """The Redis surface the runner reads for history, cycles and the watermark."""

    def __init__(self, candles: list[Candle], decided: datetime | None = None) -> None:
        self.candles = list(candles)
        self.decided = decided
        self.recorded: list[datetime] = []

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def get_candles(self, symbol, timeframe, count=0):
        return self.candles[-count:] if count else list(self.candles)

    async def get_decided_open_time(self, strategy, symbol, timeframe):
        return self.decided

    async def set_decided_open_time(self, strategy, symbol, timeframe, open_time):
        self.decided = open_time
        self.recorded.append(open_time)

    async def get_open_position(self, strategy, symbol):
        return None


class SubscribingBus:
    def __init__(self) -> None:
        self.handlers: dict[str, object] = {}

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def subscribe(self, subject, handler, queue=""):
        self.handlers[subject] = handler

    async def publish(self, subject, payload) -> None:
        return None


class QuietSink:
    shadow_mode = True
    transport = "nats"

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None


class NoPositions:
    async def get(self, strategy, symbol):
        return None


class NoPendingSignals:
    async def pending_deliveries(self, *, statuses=("pending", "unknown")):
        return []


class SilentEvents:
    async def record_event(self, **event_fields) -> None:
        return None


def build_runner(monkeypatch, candle_store: CandleStore, *, provider: str = "tiingo"):
    monkeypatch.setattr("qte_strategy_engine.runner.run_preflight_audit", lambda: None)
    monkeypatch.setattr(settings.market_data, "provider", provider)
    runner = StrategyRunner(sink=QuietSink())
    runner.bus = SubscribingBus()
    runner.state = candle_store
    runner.positions = NoPositions()
    runner.signals = NoPendingSignals()
    runner.events = SilentEvents()
    runner._by_subject = defaultdict(list)
    strategy = DecisionRecorder()
    strategy_slot = StrategySlot(
        strategy, "XAUUSD", SignalFactory(strategy.name, timeframe="M15", token="test")
    )

    def build_slots() -> None:
        runner.slots.append(strategy_slot)
        runner._by_subject[("XAUUSD", "M15")].append(strategy_slot)

    monkeypatch.setattr(runner, "_build_slots", build_slots)
    return runner, strategy_slot, strategy


async def deliver_live(runner: StrategyRunner, candle: Candle) -> None:
    handler = runner.bus.handlers[runner.subjects.candle_closed("XAUUSD", "M15")]
    close_event = CandleClosedEvent(symbol="XAUUSD", timeframe="M15", candle=candle)
    await handler(SimpleNamespace(data=close_event.model_dump_json().encode()))


def allow_catch_up_within(monkeypatch, seconds: float) -> None:
    monkeypatch.setattr(
        "qte_strategy_engine.runner.runner_settings.catch_up_max_age", seconds, raising=False
    )


def logged_messages(caplog) -> list[str]:
    return [record.getMessage() for record in caplog.records]


async def test_a_first_start_warms_without_deciding_on_history(monkeypatch):
    allow_catch_up_within(monkeypatch, 86_400)
    history = completed_bars(5)
    runner, strategy_slot, strategy = build_runner(monkeypatch, CandleStore(history))

    await runner.start()
    try:
        assert strategy.decided_on == [], "no watermark means nothing was ever missed"
        assert list(strategy_slot.buffer) == history
    finally:
        await runner.stop()


async def test_a_close_newer_than_the_watermark_is_decided_exactly_once(monkeypatch):
    allow_catch_up_within(monkeypatch, 86_400)
    history = completed_bars(5)
    candle_store = CandleStore(history, decided=history[3].open_time)
    runner, _, strategy = build_runner(monkeypatch, candle_store)

    await runner.start()
    try:
        assert strategy.decided_on == [history[4].open_time]
        assert candle_store.decided == history[4].open_time
        # The same close arriving live afterwards is a duplicate, not a second bar.
        await deliver_live(runner, history[4])
        assert strategy.decided_on == [history[4].open_time]
    finally:
        await runner.stop()


async def test_an_old_missed_close_joins_the_window_without_a_decision(monkeypatch, caplog):
    allow_catch_up_within(monkeypatch, 1800)
    last_completed = completed_bars(1)[0].open_time
    stale_bar = bar_at(last_completed - timedelta(hours=4))
    recent_bar = bar_at(last_completed)
    history = [bar_at(last_completed - timedelta(hours=5)), stale_bar, recent_bar]
    candle_store = CandleStore(history, decided=history[0].open_time)
    runner, strategy_slot, strategy = build_runner(monkeypatch, candle_store)

    with caplog.at_level("WARNING"):
        await runner.start()
    try:
        assert strategy.decided_on == [recent_bar.open_time]
        assert list(strategy_slot.buffer) == history
        assert any("Skipped the decision" in message for message in logged_messages(caplog))
    finally:
        await runner.stop()


async def test_a_mark_dated_after_now_gives_way_to_the_catch_up_age(monkeypatch, caplog):
    """A synthetic feed ran ahead of the clock and wrote the mark, and no bar of the
    current feed has been fed since, so every close newer than the age limit is new."""
    allow_catch_up_within(monkeypatch, 1800)
    last_completed = completed_bars(1)[0].open_time
    older_bars = [bar_at(last_completed - timedelta(hours=offset)) for offset in (5, 4)]
    recent_bar = bar_at(last_completed)
    candle_store = CandleStore(
        [*older_bars, recent_bar], decided=last_completed + timedelta(days=3)
    )
    runner, strategy_slot, strategy = build_runner(monkeypatch, candle_store)

    with caplog.at_level("WARNING"):
        await runner.start()
    try:
        assert strategy.decided_on == [recent_bar.open_time]
        assert list(strategy_slot.buffer) == [*older_bars, recent_bar]
        assert any(
            "Ignoring the decided-bar mark" in message for message in logged_messages(caplog)
        )
    finally:
        await runner.stop()


async def test_restored_history_drops_duplicates_and_future_bars(monkeypatch):
    allow_catch_up_within(monkeypatch, 86_400)
    first_bar, second_bar = completed_bars(2)
    live_copy = second_bar.model_copy(update={"tick_count": 9})
    future_bar = bar_at(first_bar.open_time + timedelta(days=3))
    candle_store = CandleStore([first_bar, second_bar, live_copy, future_bar])
    runner, strategy_slot, _ = build_runner(monkeypatch, candle_store)

    await runner.start()
    try:
        assert list(strategy_slot.buffer) == [first_bar, live_copy]
    finally:
        await runner.stop()


async def test_a_synthetic_feed_keeps_its_forward_anchored_bars(monkeypatch):
    allow_catch_up_within(monkeypatch, 86_400)
    now_bucket = floor_to_bucket(datetime.now(UTC), "M15")
    forward = [bar_at(now_bucket + BAR_LENGTH * offset) for offset in range(3)]
    runner, strategy_slot, _ = build_runner(monkeypatch, CandleStore(forward), provider="simulator")

    await runner.start()
    try:
        assert list(strategy_slot.buffer) == forward
    finally:
        await runner.stop()


async def test_the_live_path_records_the_bar_it_fed(monkeypatch):
    allow_catch_up_within(monkeypatch, 86_400)
    history = completed_bars(3)
    candle_store = CandleStore(history[:2])
    runner, _, strategy = build_runner(monkeypatch, candle_store)

    await runner.start()
    try:
        await deliver_live(runner, history[2])
        assert strategy.decided_on == [history[2].open_time]
        assert candle_store.recorded == [history[2].open_time]
    finally:
        await runner.stop()
