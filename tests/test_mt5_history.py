"""History asked of the MT5 ingester: QTE decides what is warmed, and by how much.

The ingester publishes a bar when it closes and pushes nothing else. What is
pinned here is QTE's side of the request that fills an indicator window — the
subject it asks on, the reply it accepts, what it does when nobody answers —
and how ingestion uses it: merged behind the staging watermark, asked for again
when the ingester was not up yet, and fetched ahead of a live bar that arrives
with a hole before it.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import nats.errors
import pytest
from fakeredis.aioredis import FakeRedis

from qte_ingestion.backfill import HistoryBackfiller
from qte_ingestion.service import IngestionService
from qte_shared.cache.redis_state import RedisState
from qte_shared.config import settings
from qte_shared.interfaces.market_data import (
    Capability,
    HistoryNotServed,
    HistoryOffline,
    HistoryRequest,
    ProviderError,
    UnsupportedCapability,
)
from qte_shared.market_data_plan import MarketDataPlan, SymbolFeed
from qte_shared.models import Candle
from qte_shared.providers import create_provider
from qte_shared.providers.mt5 import Mt5Provider, Mt5Settings
from qte_shared.providers.mt5.history import IngesterHistorySource
from qte_shared.providers.mt5.protocol import (
    IngesterPayloadError,
    decode_history_reply,
    encode_history_request,
)

REPO_ROOT = Path(__file__).resolve().parents[1]

#: A quarter-hour boundary comfortably in the past.
FIRST_BAR = datetime(2026, 9, 28, 10, 0, tzinfo=UTC)
STEP = timedelta(minutes=15)


@pytest.fixture(autouse=True)
def no_plan_on_disk(monkeypatch):
    """Settings built here must not read whatever config/data_providers/<provider>.toml exists."""
    monkeypatch.setattr("qte_shared.config.market_data_plan", lambda: MarketDataPlan())
    monkeypatch.setattr(
        "qte_shared.config.provider_market_data_plan", lambda provider_name: MarketDataPlan()
    )


def wire_bar(index: int, close: float = 2653.4) -> dict:
    open_time = FIRST_BAR + STEP * index
    return {
        "open_time": open_time.isoformat().replace("+00:00", "Z"),
        "close_time": (open_time + STEP).isoformat().replace("+00:00", "Z"),
        "open": 2651.12,
        "high": 2654.8,
        "low": 2649.95,
        "close": close,
        "volume": 1843.0,
        "tick_count": 1843,
        "quote_volume": None,
        "spread": 12.0,
    }


def reply_payload(indexes, **overrides) -> dict:
    payload = {
        "schema_version": "1.0.0",
        "request_id": "qte-test",
        "status": "ok",
        "served_at": "2026-09-30T00:00:00Z",
        "source": {"gateway": "mt5", "market": "forex", "ingester_id": "vps-mt5-01"},
        "symbol": "XAUUSD",
        "timeframe": "M15",
        "requested": 150,
        "truncated": False,
        "bars": [wire_bar(index) for index in indexes],
        "error": None,
    }
    payload.update(overrides)
    return payload


def refusal(code: str, message: str = "no") -> dict:
    return reply_payload([], status="error", error={"code": code, "message": message})


def encoded(payload: dict) -> bytes:
    return json.dumps(payload).encode()


# ── Settings: where the request goes ──────────────────────────────────────


def test_the_request_prefix_is_derived_the_way_the_ingester_derives_it():
    assert Mt5Settings().request_prefix == "INGESTER_RPC"
    assert Mt5Settings(subject_prefix="DESK.MT5").request_prefix == "DESK_RPC"
    assert Mt5Settings(rpc_prefix="ASK.").request_prefix == "ASK"


def test_the_history_subject_names_the_gateway_and_the_series():
    config = Mt5Settings()
    assert config.history_subject("XAUUSD", "M15") == "INGESTER_RPC.history.mt5.XAUUSD.M15"
    # A broker suffix must not add a subject token.
    assert config.history_subject("XAUUSD.m", "M15") == "INGESTER_RPC.history.mt5.XAUUSD_m.M15"


@pytest.mark.parametrize("rpc_prefix", ["INGESTER", "INGESTER.rpc"])
def test_a_request_prefix_inside_the_bar_stream_is_refused(rpc_prefix):
    # The stream listens on INGESTER.> — it would store the request as market
    # data and answer it with its own acknowledgement before the ingester does.
    with pytest.raises(ValueError, match="sits under"):
        Mt5Settings(rpc_prefix=rpc_prefix)


def test_requests_are_never_addressed_under_the_subject_the_feed_consumes():
    config = Mt5Settings()
    stream_tree = config.subject_filter.removesuffix("bar.closed.mt5.>")
    assert not config.history_subject("XAUUSD", "M15").startswith(stream_tree)


# ── Capability: a window, not an archive ──────────────────────────────────


def test_mt5_serves_recent_bars_and_still_refuses_a_backtest_download():
    provider = create_provider("mt5")
    assert provider.supports(Capability.RECENT_BARS)
    assert not provider.supports(Capability.HISTORY)
    with pytest.raises(UnsupportedCapability, match="'mt5' does not serve 'history'"):
        create_provider("mt5", capability=Capability.HISTORY)


def test_the_backfill_accepts_a_provider_that_only_serves_recent_bars():
    provider = create_provider("mt5", capability=(Capability.HISTORY, Capability.RECENT_BARS))
    source = provider.history_source()
    assert isinstance(provider, Mt5Provider)
    assert isinstance(source, IngesterHistorySource)
    # Never written to parquet: it would shadow the archive a backtest reads.
    assert source.cacheable is False
    assert source.max_bars == Mt5Settings().history_bars


# ── Protocol ──────────────────────────────────────────────────────────────


def test_the_request_names_the_series_and_the_count():
    body = json.loads(encode_history_request("xauusd", "m15", 150, "qte-1"))
    assert body == {
        "schema_version": "1.0.0",
        "request_id": "qte-1",
        "symbol": "XAUUSD",
        "timeframe": "M15",
        "count": 150,
    }


def test_a_reply_becomes_closed_candles_oldest_first():
    answer = decode_history_reply(encoded(reply_payload([2, 0, 1])), "XAUUSD", "M15")
    assert [candle.open_time for candle in answer.candles] == [
        FIRST_BAR,
        FIRST_BAR + STEP,
        FIRST_BAR + STEP * 2,
    ]
    assert all(candle.is_closed and candle.symbol == "XAUUSD" for candle in answer.candles)
    assert answer.candles[0].tick_count == 1843
    assert answer.truncated is False
    assert answer.ingester_id == "vps-mt5-01"


def test_a_repeated_bar_in_a_reply_collapses():
    answer = decode_history_reply(encoded(reply_payload([0, 1, 1])), "XAUUSD", "M15")
    assert len(answer.candles) == 2


def test_the_ingesters_own_samples_are_read():
    request = json.loads((REPO_ROOT / "examples/nats/history.request.mt5.json").read_bytes())
    assert (
        json.loads(
            encode_history_request(
                request["symbol"], request["timeframe"], request["count"], request["request_id"]
            )
        )
        == request
    )

    answer = decode_history_reply(
        (REPO_ROOT / "examples/nats/history.reply.mt5.json").read_bytes(), "XAUUSD", "M15"
    )
    assert [candle.open_time.minute for candle in answer.candles] == [45, 0]

    with pytest.raises(HistoryNotServed, match="unknown_symbol"):
        decode_history_reply(
            (REPO_ROOT / "examples/nats/history.reply.error.json").read_bytes(), "EURUSD", "M15"
        )


@pytest.mark.parametrize("code", ["unknown_symbol", "bad_request"])
def test_a_final_refusal_is_not_worth_asking_again(code):
    with pytest.raises(HistoryNotServed, match=code):
        decode_history_reply(encoded(refusal(code)), "XAUUSD", "M15")


@pytest.mark.parametrize("code", ["unavailable", "timeout", "internal"])
def test_a_refusal_for_now_is_an_ordinary_provider_error(code):
    with pytest.raises(ProviderError, match=code) as raised:
        decode_history_reply(encoded(refusal(code)), "XAUUSD", "M15")
    assert not isinstance(raised.value, HistoryNotServed)


@pytest.mark.parametrize(
    ("payload", "reason"),
    [
        (reply_payload([0], symbol="USOIL"), "not the XAUUSD M15"),
        (reply_payload([0], timeframe="M5"), "not the XAUUSD M15"),
        (reply_payload([0], schema_version="2.0.0"), "not accepted"),
        (reply_payload([0], status="maybe"), "neither ok nor error"),
        (reply_payload([0], bars=None), "no bars list"),
        (reply_payload([0], bars=[{"open_time": "2026-09-28T10:00:00Z"}]), "close_time"),
    ],
)
def test_a_reply_that_cannot_be_trusted_is_refused(payload, reason):
    # A reply for another series, merged, would put one instrument's bars in
    # another's window.
    with pytest.raises(IngesterPayloadError, match=reason):
        decode_history_reply(encoded(payload), "XAUUSD", "M15")


def test_a_reply_that_is_not_json_is_refused():
    with pytest.raises(IngesterPayloadError, match="not JSON"):
        decode_history_reply(b"+ACK", "XAUUSD", "M15")


# ── The source: one request, one reply ────────────────────────────────────


class FakeReply:
    def __init__(self, data: bytes) -> None:
        self.data = data


class FakeNats:
    """Scripted answers to ``request``; an Exception in the script is raised."""

    def __init__(self, script: list) -> None:
        self.script = list(script)
        self.requests: list[tuple[str, dict, float]] = []

    async def request(self, subject: str, body: bytes, timeout: float):
        self.requests.append((subject, json.loads(body), timeout))
        answer = self.script.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return FakeReply(encoded(answer))


class FakeBus:
    def __init__(self, script: list, connect_error: Exception | None = None) -> None:
        self.nc = FakeNats(script)
        self.connect_error = connect_error
        self.connected = False
        self.closed = False

    async def connect(self) -> None:
        if self.connect_error is not None:
            raise self.connect_error
        self.connected = True

    async def close(self) -> None:
        self.closed = True


def make_source(script: list, *, now: datetime | None = None, **config):
    bus = FakeBus(script)
    source = IngesterHistorySource(
        Mt5Settings(**config),
        bus_factory=lambda: bus,
        utc_clock=lambda: now or FIRST_BAR + STEP * 4,
    )
    return source, bus


async def test_candles_are_asked_for_on_the_series_subject_and_the_connection_closed():
    source, bus = make_source([reply_payload([0, 1, 2])])

    candles = await source.fetch_candles("XAUUSD", "M15", 150)

    ((subject, body, timeout),) = bus.nc.requests
    assert subject == "INGESTER_RPC.history.mt5.XAUUSD.M15"
    assert (body["symbol"], body["timeframe"], body["count"]) == ("XAUUSD", "M15", 150)
    assert body["request_id"].startswith("qte-")
    assert timeout == Mt5Settings().history_timeout
    assert len(candles) == 3
    assert bus.closed is True


async def test_a_wide_range_asks_for_history_bars_and_no_more():
    source, bus = make_source([reply_payload([0, 1])], history_bars=200)
    request = HistoryRequest(
        symbol="XAUUSD", timeframe="M15", start=date(2026, 8, 1), end=date(2026, 9, 28)
    )

    frame = await source.fetch(request)

    assert bus.nc.requests[0][1]["count"] == 200
    assert list(frame.index) == [FIRST_BAR, FIRST_BAR + STEP]
    assert list(frame.columns) == ["open", "high", "low", "close", "volume"]


async def test_a_top_up_from_this_morning_asks_only_for_the_buckets_since():
    # 10:00 + 4 bars = 11:00 UTC, so a range starting today spans 44 buckets.
    source, bus = make_source([reply_payload([0])])
    request = HistoryRequest(
        symbol="XAUUSD", timeframe="M15", start=date(2026, 9, 28), end=date(2026, 9, 28)
    )

    await source.fetch(request)

    assert bus.nc.requests[0][1]["count"] == 44


async def test_bars_outside_the_requested_dates_are_left_out():
    # The ingester counts back from the newest bar, so a reply can reach
    # further back than the range that was asked for.
    source, _ = make_source([reply_payload([-45, -1, 0, 1])])
    request = HistoryRequest(
        symbol="XAUUSD", timeframe="M15", start=date(2026, 9, 28), end=date(2026, 9, 28)
    )

    frame = await source.fetch(request)

    assert list(frame.index) == [FIRST_BAR - STEP, FIRST_BAR, FIRST_BAR + STEP]


async def test_no_bars_is_an_empty_frame_not_an_error():
    source, _ = make_source([reply_payload([])])
    request = HistoryRequest(
        symbol="XAUUSD", timeframe="M15", start=date(2026, 9, 28), end=date(2026, 9, 28)
    )
    assert (await source.fetch(request)).empty


async def test_nobody_listening_is_reported_at_once_and_names_both_settings():
    source, bus = make_source([nats.errors.NoRespondersError()], history_attempts=3)

    with pytest.raises(HistoryOffline, match="no ingester is answering") as raised:
        await source.fetch_candles("XAUUSD", "M15", 10)

    # Not retried, in the fetch or on a timer: nobody is subscribed. The
    # ingester announces itself when it is.
    assert len(bus.nc.requests) == 1
    assert "NATS_RPC_SUBJECT_PREFIX" in str(raised.value)
    assert bus.closed is True


async def test_a_timeout_is_asked_again_up_to_the_configured_attempts():
    source, bus = make_source([nats.errors.TimeoutError(), reply_payload([0])], history_attempts=2)
    assert len(await source.fetch_candles("XAUUSD", "M15", 10)) == 1
    assert len(bus.nc.requests) == 2

    source, bus = make_source(
        [nats.errors.TimeoutError(), nats.errors.TimeoutError()], history_attempts=2
    )
    with pytest.raises(ProviderError, match="no reply"):
        await source.fetch_candles("XAUUSD", "M15", 10)
    assert bus.closed is True


async def test_a_final_refusal_passes_through_as_history_not_served():
    source, bus = make_source([refusal("unknown_symbol", "mt5 is not configured for 'BTCUSD'")])
    with pytest.raises(HistoryNotServed, match="BTCUSD"):
        await source.fetch_candles("BTCUSD", "M15", 10)
    assert bus.closed is True


async def test_an_unusable_reply_is_a_provider_error_not_a_crash():
    source, _ = make_source([reply_payload([0], symbol="USOIL")])
    with pytest.raises(ProviderError, match="unusable history reply"):
        await source.fetch_candles("XAUUSD", "M15", 10)


async def test_an_unreachable_server_is_a_provider_error():
    bus = FakeBus([], connect_error=ConnectionError("refused"))
    source = IngesterHistorySource(Mt5Settings(), bus_factory=lambda: bus)
    with pytest.raises(ProviderError, match="cannot reach NATS"):
        await source.fetch_candles("XAUUSD", "M15", 10)


# ── Ingestion: the window is filled by asking ─────────────────────────────


def candle(index: int, *, close: float = 2653.4, ticks: int = 1843) -> Candle:
    return Candle(
        symbol="XAUUSD",
        timeframe="M15",
        open_time=FIRST_BAR + STEP * index,
        open=2651.12,
        high=2654.8,
        low=2649.95,
        close=close,
        volume=1843.0,
        tick_count=ticks,
        is_closed=True,
    )


class ScriptedSource(IngesterHistorySource):
    """The real source over a bus that answers with whatever bars are scripted."""

    def __init__(self, indexes, *, history_bars: int = 150, now: datetime | None = None) -> None:
        self.indexes = list(indexes)
        self.failure: Exception | None = None
        self.counts: list[int] = []
        self._now = now or FIRST_BAR + STEP * 200
        super().__init__(
            Mt5Settings(history_bars=history_bars),
            bus_factory=self._bus,
            utc_clock=lambda: self._now,
        )

    def _bus(self) -> FakeBus:
        if self.failure is not None:
            return FakeBus([self.failure])
        return FakeBus([reply_payload(self.indexes)])

    async def fetch_candles(self, symbol: str, timeframe: str, count: int) -> list[Candle]:
        self.counts.append(count)
        return await super().fetch_candles(symbol, timeframe, count)


class ExplodingCache:
    """Any use of the parquet cache is the bug."""

    def load(self, *arguments, **keywords):
        raise AssertionError("an ingester window must never be read from the cache")

    def store(self, *arguments, **keywords):
        raise AssertionError("an ingester window must never be written to the cache")


def redis_state() -> RedisState:
    state = RedisState(prefix="mt5-history-test")
    state._client = FakeRedis(decode_responses=True)
    return state


def make_backfiller(state, source, *, now: datetime | None = None) -> HistoryBackfiller:
    backfiller = HistoryBackfiller(
        state,
        [SymbolFeed(symbol="XAUUSD", market="fx", timeframes=("M15",))],
        cache=ExplodingCache(),
        utc_clock=lambda: now or FIRST_BAR + STEP * 200,
    )
    backfiller._source = source
    return backfiller


async def stored_opens(state) -> list[datetime]:
    return [bar.open_time for bar in await state.get_candles("XAUUSD", "M15")]


async def test_a_cold_window_is_filled_from_one_reply_without_touching_the_cache():
    state = redis_state()
    source = ScriptedSource(range(150))
    backfiller = make_backfiller(state, source)

    assert await backfiller.run() == []

    assert source.counts == [150]
    assert await stored_opens(state) == [FIRST_BAR + STEP * index for index in range(150)]


async def test_the_window_is_full_at_what_one_request_returns_not_at_the_redis_history():
    # QTE_REDIS__CANDLE_HISTORY is thousands of bars; the ingester is asked for
    # history_bars. Measured against the former, the window would be "short"
    # on every boot and re-requested forever.
    state = redis_state()
    for index in range(150):
        await state.stage_closed_candle(candle(index))
    source = ScriptedSource(range(150))
    now = FIRST_BAR + STEP * 150 + timedelta(minutes=4)

    assert await make_backfiller(state, source, now=now).run() == []

    assert source.counts == []
    assert settings.redis.candle_history > 150


async def test_history_goes_in_behind_the_watermark_so_a_replayed_bar_is_a_duplicate():
    # The feed's durable consumer replays what the stream still holds. Without
    # the watermark those bars would be appended after the history they belong
    # inside, and published as closes.
    state = redis_state()
    await make_backfiller(state, ScriptedSource(range(10))).run()

    await state.stage_closed_candle(candle(3))
    await state.stage_closed_candle(candle(9))

    assert await stored_opens(state) == [FIRST_BAR + STEP * index for index in range(10)]
    assert await state.peek_pending_candle() is None

    await state.stage_closed_candle(candle(10))
    assert (await stored_opens(state))[-1] == FIRST_BAR + STEP * 10


async def test_a_half_filled_window_keeps_the_bars_ingestion_already_staged():
    state = redis_state()
    for index in (7, 8, 9):
        await state.stage_closed_candle(candle(index, close=1111.0))
    source = ScriptedSource(range(10))

    await make_backfiller(state, source).run()

    held = await state.get_candles("XAUUSD", "M15")
    assert [bar.open_time for bar in held] == [FIRST_BAR + STEP * index for index in range(10)]
    # A bar this engine staged from the live feed is what the strategy acted on.
    assert [bar.close for bar in held[-3:]] == [1111.0] * 3


async def test_an_ingester_that_is_not_up_yet_leaves_the_series_owed_not_failed():
    state = redis_state()
    source = ScriptedSource(range(10))
    source.failure = nats.errors.NoRespondersError()
    backfiller = make_backfiller(state, source)

    owed = await backfiller.run()

    assert [(spec.symbol, timeframe) for spec, timeframe in owed] == [("XAUUSD", "M15")]
    assert backfiller.offline is True
    assert await state.count_candles("XAUUSD", "M15") == 0


async def test_once_nobody_answers_the_other_series_are_owed_without_being_asked():
    state = redis_state()
    source = ScriptedSource(range(10))
    source.failure = nats.errors.NoRespondersError()
    backfiller = make_backfiller(state, source)
    backfiller.subscriptions = [
        SymbolFeed(symbol="XAUUSD", market="fx", timeframes=("M15",)),
        SymbolFeed(symbol="USOIL", market="fx", timeframes=("M15",)),
    ]

    owed = await backfiller.run()

    assert [spec.symbol for spec, _ in owed] == ["XAUUSD", "USOIL"]
    assert len(source.counts) == 1


async def test_a_symbol_the_ingester_does_not_carry_is_not_asked_for_again(caplog):
    state = redis_state()
    source = ScriptedSource([])
    source.failure = None
    source._bus = lambda: FakeBus([refusal("unknown_symbol", "mt5 is not configured for it")])
    source._bus_factory = source._bus

    assert await make_backfiller(state, source).run() == []
    assert "fills from live closes only" in caplog.text


async def test_before_keeps_the_live_bar_itself_out_of_the_merge():
    state = redis_state()
    source = ScriptedSource(range(10))
    backfiller = make_backfiller(state, source)
    spec = backfiller.subscriptions[0].spec

    await backfiller.backfill_series(spec, "M15", before=FIRST_BAR + STEP * 9)

    assert (await stored_opens(state))[-1] == FIRST_BAR + STEP * 8
    # So the close itself is still new to the staging path, and is published.
    await state.stage_closed_candle(candle(9))
    assert (await state.peek_pending_candle()).open_time == FIRST_BAR + STEP * 9


# ── Ingestion service: asking again, and filling a hole ───────────────────


def history_service(state, source) -> IngestionService:
    service = object.__new__(IngestionService)
    service.subscriptions = [SymbolFeed(symbol="XAUUSD", market="fx", timeframes=("M15",))]
    service._symbol_providers = {"XAUUSD": "mt5"}
    service._origins = {"mt5": settings.state_scope.origin()}
    service._processing_lock = asyncio.Lock()
    service.state = state
    service._backfillers = {"mt5": make_backfiller(state, source)}
    service.providers = {"mt5": object()}
    service._history_owed = {}
    service._history_wake = asyncio.Event()
    service._vendor_offline = False
    service._gap_asked_at = {}
    return service


def capture_emits(service, monkeypatch) -> list[Candle]:
    emitted: list[Candle] = []

    async def emit(candles, **keywords):
        for bar in candles:
            await service.state.stage_closed_candle(bar)
            emitted.append(bar)

    monkeypatch.setattr(service, "_emit_candles", emit)
    return emitted


async def run_history_loop(service):
    task = asyncio.create_task(service._history_loop())

    async def stop() -> None:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    return stop


async def settle(predicate, attempts: int = 300) -> None:
    for _ in range(attempts):
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not met in time")


async def test_with_the_ingester_offline_nothing_is_asked_on_a_timer(monkeypatch):
    # QTE started first. It carries on with what Redis holds and does not poll:
    # there is nobody to ask until the ingester says it has connected.
    monkeypatch.setattr("qte_ingestion.service.ingestion_settings.history_retry_interval", 0.01)
    monkeypatch.setattr(
        "qte_ingestion.service.ingestion_settings.history_offline_probe_interval", 0.0
    )
    state = redis_state()
    source = ScriptedSource(range(20))
    source.failure = nats.errors.NoRespondersError()
    service = history_service(state, source)
    service._history_owed[("XAUUSD", "M15")] = service.subscriptions[0].spec
    service._vendor_offline = True

    stop = await run_history_loop(service)
    try:
        await asyncio.sleep(0.15)
    finally:
        await stop()

    assert source.counts == []
    assert await state.count_candles("XAUUSD", "M15") == 0


async def test_the_ingester_announcing_itself_is_what_gets_a_short_window_requested(monkeypatch):
    monkeypatch.setattr(
        "qte_ingestion.service.ingestion_settings.history_offline_probe_interval", 0.0
    )
    state = redis_state()
    # Half a window, from an earlier run.
    for index in range(12, 20):
        await state.stage_closed_candle(candle(index))
    source = ScriptedSource(range(20))
    source.failure = nats.errors.NoRespondersError()
    service = history_service(state, source)
    service._history_owed[("XAUUSD", "M15")] = service.subscriptions[0].spec
    service._vendor_offline = True

    stop = await run_history_loop(service)
    try:
        await asyncio.sleep(0.05)
        assert source.counts == []

        source.failure = None  # the ingester connects…
        await service._handle_vendor_online("mt5")  # …and says so
        await settle(lambda: not service._history_owed)
    finally:
        await stop()

    assert len(source.counts) == 1
    assert service._vendor_offline is False
    assert await stored_opens(state) == [FIRST_BAR + STEP * index for index in range(20)]


async def test_an_announcement_checks_every_window_and_requests_only_the_short_ones(monkeypatch):
    # The ingester reconnects while QTE holds a full, current window: Redis is
    # read, nothing is requested.
    state = redis_state()
    for index in range(150):
        await state.stage_closed_candle(candle(index))
    now = FIRST_BAR + STEP * 150 + timedelta(minutes=4)
    source = ScriptedSource(range(150), now=now)
    service = history_service(state, source)
    service._backfillers["mt5"].utc_clock = lambda: now

    stop = await run_history_loop(service)
    try:
        await service._handle_vendor_online("mt5")
        await settle(lambda: not service._history_owed)
    finally:
        await stop()

    assert source.counts == []


async def test_an_ingester_that_is_connected_but_not_ready_is_asked_again_shortly(monkeypatch):
    # It answered "unavailable": the terminal is still logging in. That is a
    # responder, so this one is retried — unlike silence.
    monkeypatch.setattr("qte_ingestion.service.ingestion_settings.history_retry_interval", 0.01)
    state = redis_state()
    source = ScriptedSource(range(20))
    source._bus_factory = lambda: FakeBus([refusal("unavailable", "MT5 terminal is not connected")])
    service = history_service(state, source)
    service._history_owed[("XAUUSD", "M15")] = service.subscriptions[0].spec
    service._history_wake.set()

    stop = await run_history_loop(service)
    try:
        await settle(lambda: len(source.counts) >= 2)
        assert service._vendor_offline is False
        source._bus_factory = source._bus  # the terminal is up
        await settle(lambda: not service._history_owed)
    finally:
        await stop()

    assert await state.count_candles("XAUUSD", "M15") == 20


async def test_the_watch_calls_back_on_the_online_subject_and_closes_its_connection():
    class WatchBus(FakeBus):
        def __init__(self) -> None:
            super().__init__([])
            self.subscribed: dict = {}
            self.reconnect_hooks: list = []

        async def subscribe(self, subject, handler, queue=""):
            self.subscribed[subject] = handler

        def on_reconnect(self, hook) -> None:
            self.reconnect_hooks.append(hook)

    bus = WatchBus()
    source = IngesterHistorySource(Mt5Settings(), bus_factory=lambda: bus)
    heard: list[int] = []

    async def on_online() -> None:
        heard.append(1)

    assert await source.watch_online(on_online) is True
    (subject,) = bus.subscribed
    assert subject == "INGESTER_RPC.online.mt5"

    class Announcement:
        subject = "INGESTER_RPC.online.mt5"
        data = b'{"event_type":"ingester.online"}'

    await bus.subscribed[subject](Announcement())
    assert heard == [1]

    # An announcement made while the watch was disconnected was never
    # delivered, so coming back checks once — the only probe there is.
    (reconnected,) = bus.reconnect_hooks
    await reconnected()
    assert heard == [1, 1]

    await source.close()
    assert bus.closed is True


def test_no_probe_timer_runs_by_default():
    # Asking a vendor that is not connected, on a schedule, is exactly what
    # the announcement replaces.
    from qte_ingestion.settings import IngestionSettings

    assert IngestionSettings().history_offline_probe_interval == 0


async def test_a_bar_that_follows_the_newest_stored_one_asks_for_nothing(monkeypatch):
    state = redis_state()
    for index in range(5):
        await state.stage_closed_candle(candle(index))
    source = ScriptedSource(range(5))
    service = history_service(state, source)
    capture_emits(service, monkeypatch)

    await service._handle_closed_bar(candle(5))

    assert source.counts == []


async def test_a_bar_with_a_hole_before_it_has_the_hole_filled_before_it_is_staged(monkeypatch):
    # The ingester restarted: it primed on the bar it found and published
    # nothing it had missed, so bar 9 arrives with 5..8 never having been sent.
    state = redis_state()
    for index in range(5):
        await state.stage_closed_candle(candle(index))
    while await state.peek_pending_candle():
        await state.ack_pending_candle()
    source = ScriptedSource(range(10))
    service = history_service(state, source)
    windows_at_emit: list[list[datetime]] = []

    async def emit(candles, **keywords):
        windows_at_emit.append(await stored_opens(state))
        for bar in candles:
            await state.stage_closed_candle(bar)

    monkeypatch.setattr(service, "_emit_candles", emit)

    await service._handle_closed_bar(candle(9))

    # Whole up to bar 8 at the moment the live bar is handed over…
    assert windows_at_emit == [[FIRST_BAR + STEP * index for index in range(9)]]
    # …and the live bar is still staged and queued for publishing as a close.
    assert await stored_opens(state) == [FIRST_BAR + STEP * index for index in range(10)]
    assert (await state.peek_pending_candle()).open_time == FIRST_BAR + STEP * 9


async def test_a_hole_that_cannot_be_filled_never_holds_the_bar_back(monkeypatch):
    state = redis_state()
    for index in range(5):
        await state.stage_closed_candle(candle(index))
    source = ScriptedSource(range(10))
    source.failure = nats.errors.NoRespondersError()
    service = history_service(state, source)
    emitted = capture_emits(service, monkeypatch)

    await service._handle_closed_bar(candle(9))

    assert [bar.open_time for bar in emitted] == [FIRST_BAR + STEP * 9]
    # Owed, not forgotten — and with nobody to ask, not retried on a timer:
    # the ingester's announcement is what gets it requested.
    assert ("XAUUSD", "M15") in service._history_owed
    assert service._vendor_offline is True
    assert not service._history_wake.is_set()


async def test_a_session_break_costs_one_request_not_one_per_bar(monkeypatch):
    monkeypatch.setattr("qte_ingestion.service.ingestion_settings.history_gap_cooldown", 60.0)
    state = redis_state()
    await state.stage_closed_candle(candle(0))
    # The market was shut: the ingester has nothing between bar 0 and bar 8.
    source = ScriptedSource([0])
    service = history_service(state, source)
    capture_emits(service, monkeypatch)

    await service._handle_closed_bar(candle(8))
    await service._handle_closed_bar(candle(12))

    assert len(source.counts) == 1


async def test_a_provider_with_no_history_to_ask_is_left_exactly_as_it_was(monkeypatch):
    state = redis_state()
    service = history_service(state, ScriptedSource([]))
    service._backfillers = {}
    emitted = capture_emits(service, monkeypatch)

    await service._handle_closed_bar(candle(9))

    assert len(emitted) == 1
