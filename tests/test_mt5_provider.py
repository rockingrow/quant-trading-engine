"""The MT5 provider: closed bars from algo-trading-ingester, consumed off NATS.

Three promises are pinned here. The ingester's payload is read into QTE's own
candle, and anything this code would misread is refused rather than half-used.
The ingester is never kept waiting: a message is acknowledged — a JetStream ack,
or ``{"code": 200}`` to a core-NATS request — the moment it is held, before the
engine does any work on it. And ingestion stages those bars without resampling
them, refusing one whose bucket has not ended yet.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import nats.errors
import pytest
from nats.js.errors import NotFoundError

from qte_ingestion.service import IngestionService
from qte_shared.config import settings
from qte_shared.interfaces.market_data import Capability, ProviderError, UnsupportedCapability
from qte_shared.market_data_plan import MarketDataPlan, SymbolFeed
from qte_shared.models import Candle
from qte_shared.providers import create_provider, get_provider_class
from qte_shared.providers.mt5 import Mt5Provider, Mt5Settings
from qte_shared.providers.mt5.feed import ACCEPTED_REPLY, BUSY_REPLY, IngesterBarFeed
from qte_shared.providers.mt5.protocol import IngesterPayloadError, decode_bar_closed

EXAMPLE_PLAN = "config/data_providers/mt5.example.toml"


@pytest.fixture(autouse=True)
def no_plan_on_disk(monkeypatch):
    """Settings built here must not read whatever config/data_providers/<provider>.toml exists."""
    monkeypatch.setattr("qte_shared.config.market_data_plan", lambda: MarketDataPlan())


def bar_payload(
    *,
    symbol: str = "XAUUSD",
    timeframe: str = "M15",
    open_time: datetime = datetime(2026, 9, 28, 10, 0, tzinfo=UTC),
    **overrides,
) -> dict:
    """The ingester's ``examples/nats/bar.closed.mt5.json``, one field at a time."""
    minutes = {"M1": 1, "M15": 15, "H1": 60}[timeframe]
    close_time = open_time + timedelta(minutes=minutes)
    payload = {
        "schema_version": "2.0",
        "event_id": f"mt5:{symbol}:{timeframe}:{int(open_time.timestamp())}",
        "event_type": "bar.closed",
        "source": {
            "gateway": "mt5",
            "market": "forex",
            "ingester_id": "vps-mt5-01",
            "venue": "Broker-Server-Demo",
        },
        "emitted_at": (close_time + timedelta(milliseconds=412)).isoformat(),
        "symbol": symbol,
        "timeframe": timeframe,
        "bar": {
            "open_time": open_time.isoformat().replace("+00:00", "Z"),
            "close_time": close_time.isoformat().replace("+00:00", "Z"),
            "open": 2651.12,
            "high": 2654.8,
            "low": 2649.95,
            "close": 2653.4,
            "volume": 1843.0,
            "tick_count": 1843,
            "quote_volume": None,
            "spread": 12.0,
        },
    }
    payload.update(overrides)
    return payload


def encoded(payload: dict) -> bytes:
    return json.dumps(payload).encode()


# ── Registry ──────────────────────────────────────────────────────────────


def test_mt5_is_a_builtin_that_serves_closed_bars_only():
    assert get_provider_class("mt5") is Mt5Provider
    provider = create_provider("mt5")
    assert provider.supports(Capability.LIVE_BARS)
    assert not provider.supports(Capability.LIVE)
    assert not provider.supports(Capability.HISTORY)


def test_ingestion_accepts_a_provider_that_streams_bars_instead_of_ticks():
    provider = create_provider("mt5", capability=(Capability.LIVE, Capability.LIVE_BARS))
    assert isinstance(provider, Mt5Provider)


def test_a_backtest_download_is_refused_by_name():
    with pytest.raises(UnsupportedCapability, match="'mt5' does not serve 'history'"):
        create_provider("mt5", capability=Capability.HISTORY)


# ── Settings ──────────────────────────────────────────────────────────────


def test_defaults_match_the_ingesters_own_env():
    config = Mt5Settings()
    assert config.subject_filter == "INGESTER.bar.closed.mt5.>"
    assert config.jetstream is True
    assert config.stream == "INGESTER"
    assert config.deliver_policy == "all"


def test_a_blank_server_falls_back_to_qtes_own_bus(monkeypatch):
    monkeypatch.setattr(settings.nats, "url", "nats://qte-bus:4222")
    monkeypatch.setattr(settings.nats, "token", "qte-token")
    config = Mt5Settings()
    assert (config.server_url, config.server_token) == ("nats://qte-bus:4222", "qte-token")

    own = Mt5Settings(nats_url="nats://ingester:4222", nats_token="ingester-token")
    assert (own.server_url, own.server_token) == ("nats://ingester:4222", "ingester-token")


def test_the_token_never_shows_in_a_repr():
    assert "ingester-token" not in repr(Mt5Settings(nats_token="ingester-token"))


def test_the_consumer_is_named_per_state_namespace_and_safe_for_jetstream():
    derived = Mt5Settings().consumer_name
    assert derived.startswith("qte-ingestion-")
    assert settings.state_scope.namespace.replace(":", "-") in derived
    assert Mt5Settings(durable_name="qte.live *x>").consumer_name == "qte-live--x-"


def test_a_token_written_into_the_plan_is_dropped(tmp_path, caplog):
    path = tmp_path / "mt5.toml"
    path.write_text(
        '[provider]\nnats_token = "leaked"\ntoken = "leaked"\nstream = "BARS"\n',
        encoding="utf-8",
    )
    plan = MarketDataPlan.load(path)
    assert plan.options == {"stream": "BARS"}
    assert "nats_token" in caplog.text


def test_the_example_plan_parses_and_names_only_known_settings():
    plan = MarketDataPlan.load(EXAMPLE_PLAN)
    assert [feed.symbol for feed in plan.feeds] == ["XAUUSD", "USOIL"]
    assert set(plan.options) <= set(Mt5Settings.model_fields)
    Mt5Settings(**plan.options)


# ── Protocol ──────────────────────────────────────────────────────────────


def test_the_ingesters_bar_becomes_a_closed_candle():
    bar = decode_bar_closed(encoded(bar_payload()))
    assert bar.event_id == "mt5:XAUUSD:M15:1790589600"
    assert bar.candle == Candle(
        symbol="XAUUSD",
        timeframe="M15",
        open_time=datetime(2026, 9, 28, 10, 0, tzinfo=UTC),
        open=2651.12,
        high=2654.8,
        low=2649.95,
        close=2653.4,
        volume=1843.0,
        tick_count=1843,
        is_closed=True,
    )


def test_unknown_counts_are_zero_and_symbols_are_upper_case():
    payload = bar_payload(symbol="xauusd")
    payload["bar"].update(volume=None, tick_count=None)
    candle = decode_bar_closed(encoded(payload)).candle
    assert (candle.symbol, candle.volume, candle.tick_count) == ("XAUUSD", 0.0, 0)


def test_an_added_field_under_the_same_major_is_ignored():
    payload = bar_payload(schema_version="2.1", something_new={"a": 1})
    assert decode_bar_closed(encoded(payload)).candle.symbol == "XAUUSD"


@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        (lambda payload: payload.update(schema_version="3.0"), "schema_version"),
        (lambda payload: payload.update(event_type="tick"), "event_type"),
        (lambda payload: payload.pop("symbol"), "symbol"),
        (lambda payload: payload.update(timeframe="M7"), "M7"),
        (lambda payload: payload["bar"].update(open_time="2026-09-28T10:00:00"), "timezone"),
        (lambda payload: payload["bar"].update(close_time="2026-09-28T11:00:00Z"), "bucket"),
        (lambda payload: payload["bar"].update(close=None), "unusable bar"),
    ],
)
def test_a_payload_this_code_would_misread_is_refused(mutate, reason):
    payload = bar_payload()
    mutate(payload)
    with pytest.raises(IngesterPayloadError, match=reason):
        decode_bar_closed(encoded(payload))


def test_something_that_is_not_json_is_refused():
    with pytest.raises(IngesterPayloadError, match="not JSON"):
        decode_bar_closed(b"\xff not json")


# ── Provider ──────────────────────────────────────────────────────────────


def test_one_feed_covers_every_planned_series():
    subscriptions = [
        SymbolFeed(symbol="XAUUSD", market="fx", timeframes=("M15", "H1")),
        SymbolFeed(symbol="USOIL", market="fx", timeframes=("M15",)),
    ]

    async def on_bar(candle: Candle) -> None:
        return None

    feeds = Mt5Provider(Mt5Settings()).bar_feeds(subscriptions, on_bar)
    assert len(feeds) == 1
    assert feeds[0].symbols == ("USOIL", "XAUUSD")
    assert Mt5Provider(Mt5Settings()).bar_feeds([], on_bar) == []


# ── Feed: acknowledged on receipt, processed afterwards ──────────────────


class FakeMessage:
    def __init__(self, data: bytes, *, subject: str = "INGESTER.bar.closed.mt5.XAUUSD.M15"):
        self.data = data
        self.subject = subject
        self.reply = ""
        self.acked = False
        self.replies: list[bytes] = []

    async def ack(self) -> None:
        self.acked = True

    async def respond(self, data: bytes) -> None:
        self.replies.append(data)


class FakePullSubscription:
    def __init__(self, messages: list[FakeMessage]) -> None:
        self.pending = list(messages)
        self.batches: list[int] = []

    async def fetch(self, batch: int = 1, timeout: float | None = 5) -> list[FakeMessage]:
        self.batches.append(batch)
        if not self.pending:
            await asyncio.sleep(0.01)
            raise nats.errors.TimeoutError
        taken, self.pending = self.pending[:batch], self.pending[batch:]
        return taken


class FakeJetStream:
    def __init__(self, subscription: FakePullSubscription, *, stream_exists: bool = True):
        self.subscription = subscription
        self.stream_exists = stream_exists
        self.consumer: dict = {}

    async def stream_info(self, name: str) -> object:
        if not self.stream_exists:
            raise NotFoundError
        return object()

    async def pull_subscribe(self, subject: str, **options) -> FakePullSubscription:
        self.consumer = {"subject": subject, **options}
        return self.subscription


class FakeBus:
    def __init__(self, jetstream: FakeJetStream | None = None) -> None:
        self.js = jetstream
        self.handlers: dict[str, Callable] = {}
        self.closed = False

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        self.closed = True

    async def subscribe(self, subject: str, handler) -> None:
        self.handlers[subject] = handler

    @property
    def nc(self):
        return self

    @property
    def is_closed(self) -> bool:
        return self.closed


async def wait_until(condition: Callable[[], bool], timeout: float = 2.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not condition():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition never became true")
        await asyncio.sleep(0.005)


def bars(count: int, **payload_fields) -> list[FakeMessage]:
    first = datetime(2026, 9, 28, 10, 0, tzinfo=UTC)
    return [
        FakeMessage(
            encoded(bar_payload(open_time=first + timedelta(minutes=15 * i), **payload_fields))
        )
        for i in range(count)
    ]


def feed_for(bus: FakeBus, on_bar, **config) -> IngesterBarFeed:
    return IngesterBarFeed(
        {"XAUUSD": ("M15",)}, on_bar, Mt5Settings(**config), bus_factory=lambda: bus
    )


async def test_stream_bars_are_acknowledged_before_the_engine_touches_them():
    release = asyncio.Event()
    handled: list[Candle] = []

    async def slow_engine(candle: Candle) -> None:
        handled.append(candle)
        await release.wait()

    messages = bars(3)
    jetstream = FakeJetStream(FakePullSubscription(messages))
    feed = feed_for(FakeBus(jetstream), slow_engine)
    feed.start()
    try:
        await wait_until(lambda: all(message.acked for message in messages))
        # Every bar is acknowledged while the engine is still stuck on the first.
        assert len(handled) == 1
        release.set()
        await wait_until(lambda: feed.bars_delivered == 3)
    finally:
        await feed.stop()
    assert [candle.open_time.minute for candle in handled] == [0, 15, 30]


async def test_the_durable_consumer_reads_the_ingesters_subjects_from_the_start():
    jetstream = FakeJetStream(FakePullSubscription([]))
    feed = feed_for(FakeBus(jetstream), lambda candle: asyncio.sleep(0))
    feed.start()
    try:
        await wait_until(lambda: bool(jetstream.consumer))
    finally:
        await feed.stop()
    assert jetstream.consumer["subject"] == "INGESTER.bar.closed.mt5.>"
    assert jetstream.consumer["stream"] == "INGESTER"
    assert jetstream.consumer["durable"] == Mt5Settings().consumer_name
    assert jetstream.consumer["config"].deliver_policy.value == "all"
    assert jetstream.consumer["config"].ack_policy.value == "explicit"


async def test_a_full_queue_stops_the_pull_instead_of_refusing_what_it_took():
    release = asyncio.Event()

    async def stuck_engine(candle: Candle) -> None:
        await release.wait()

    messages = bars(5)
    subscription = FakePullSubscription(messages)
    feed = feed_for(FakeBus(FakeJetStream(subscription)), stuck_engine, max_pending_bars=2)
    feed.start()
    try:
        await wait_until(lambda: sum(message.acked for message in messages) >= 3)
        await asyncio.sleep(0.05)
        # One bar in the engine, two held: nothing more is pulled, nothing refused.
        assert sum(message.acked for message in messages) == 3
        assert all(batch <= 2 for batch in subscription.batches)
        release.set()
        await wait_until(lambda: feed.bars_delivered == 5)
    finally:
        await feed.stop()


async def test_a_missing_stream_is_reported_not_created():
    feed = feed_for(FakeBus(FakeJetStream(FakePullSubscription([]), stream_exists=False)), None)
    with pytest.raises(ProviderError, match="NATS_JETSTREAM_ENABLED"):
        await feed._consume_stream(feed._bus_factory())


async def test_a_core_request_gets_its_200_before_the_engine_runs():
    release = asyncio.Event()
    handled: list[Candle] = []

    async def slow_engine(candle: Candle) -> None:
        handled.append(candle)
        await release.wait()

    bus = FakeBus()
    feed = feed_for(bus, slow_engine, jetstream=False)
    feed.start()
    try:
        await wait_until(lambda: "INGESTER.bar.closed.mt5.>" in bus.handlers)
        first, second = bars(2)
        first.reply = second.reply = "_INBOX.reply"
        await bus.handlers["INGESTER.bar.closed.mt5.>"](first)
        await bus.handlers["INGESTER.bar.closed.mt5.>"](second)
        assert first.replies == [ACCEPTED_REPLY]
        assert second.replies == [ACCEPTED_REPLY]
        await wait_until(lambda: len(handled) == 1)
        release.set()
        await wait_until(lambda: feed.bars_delivered == 2)
    finally:
        await feed.stop()
    assert json.loads(ACCEPTED_REPLY)["code"] == 200


async def test_a_core_request_is_told_when_the_queue_is_full():
    release = asyncio.Event()

    async def stuck_engine(candle: Candle) -> None:
        await release.wait()

    bus = FakeBus()
    feed = feed_for(bus, stuck_engine, jetstream=False, max_pending_bars=1)
    feed.start()
    try:
        await wait_until(lambda: bool(bus.handlers))
        handler = bus.handlers["INGESTER.bar.closed.mt5.>"]
        first, second, third = bars(3)
        for message in (first, second, third):
            message.reply = "_INBOX.reply"
        await handler(first)
        await wait_until(lambda: feed._inbox.empty())
        await handler(second)
        await handler(third)
        assert third.replies == [BUSY_REPLY]
        release.set()
    finally:
        await feed.stop()


async def test_bad_payloads_unplanned_series_and_a_failing_engine_cost_one_bar_each():
    delivered: list[Candle] = []

    async def engine(candle: Candle) -> None:
        if candle.open_time.minute == 15:
            raise RuntimeError("redis is down")
        delivered.append(candle)

    messages = [
        FakeMessage(b"{not json"),
        *bars(1, symbol="EURUSD"),
        *bars(1, timeframe="H1"),
        *bars(3),
    ]
    feed = feed_for(FakeBus(FakeJetStream(FakePullSubscription(messages))), engine)
    feed.start()
    try:
        await wait_until(lambda: len(delivered) == 2)
    finally:
        await feed.stop()
    assert all(message.acked for message in messages)
    assert [candle.open_time.minute for candle in delivered] == [0, 30]


# ── Ingestion: bars skip the resampler ────────────────────────────────────


class BarProvider:
    name = "mt5"
    synthetic = False

    def __init__(self) -> None:
        self.asked: list = []

    def supports(self, capability: Capability) -> bool:
        return capability is Capability.LIVE_BARS

    def bar_feeds(self, subscriptions, on_bar):
        self.asked.append((subscriptions, on_bar))
        return ["bar-feed"]

    def live_feeds(self, specs, on_tick):  # pragma: no cover - must not be reached
        raise AssertionError("a bar provider was asked for ticks")


def bare_service() -> IngestionService:
    service = object.__new__(IngestionService)
    service.subscriptions = [SymbolFeed(symbol="XAUUSD", market="fx", timeframes=("M15",))]
    service.specs = [feed.spec for feed in service.subscriptions]
    service.providers = {"mt5": BarProvider()}
    service._symbol_providers = {"XAUUSD": "mt5"}
    service._processing_lock = asyncio.Lock()
    return service


def test_a_bar_provider_is_asked_for_bar_feeds_with_whole_subscriptions():
    service = bare_service()
    assert service._open_feeds() == [("mt5", "bar-feed")]
    subscriptions, on_bar = service.providers["mt5"].asked[0]
    assert subscriptions == service.subscriptions
    assert on_bar == service._handle_closed_bar


async def test_a_closed_bar_is_staged_as_it_arrives(monkeypatch):
    service = bare_service()
    emitted: list[list[Candle]] = []

    async def emit(candles, **options):
        emitted.append(candles)

    monkeypatch.setattr(service, "_emit_candles", emit)
    candle = decode_bar_closed(encoded(bar_payload())).candle
    await service._handle_closed_bar(candle)
    assert emitted == [[candle]]


async def test_a_bar_whose_bucket_has_not_ended_is_dropped(monkeypatch, caplog):
    service = bare_service()
    emitted: list[list[Candle]] = []

    async def emit(candles, **options):
        emitted.append(candles)

    monkeypatch.setattr(service, "_emit_candles", emit)
    opened = datetime.now(UTC).replace(second=0, microsecond=0) + timedelta(minutes=1)
    candle = decode_bar_closed(encoded(bar_payload(open_time=opened))).candle
    await service._handle_closed_bar(candle)
    assert emitted == []
    assert "MT5_SERVER_TIMEZONE" in caplog.text
