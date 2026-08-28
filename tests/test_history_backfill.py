"""Boot-time warm-up: Redis is topped up from the vendor, or deliberately not.

The runner reads its whole indicator window out of Redis, so what this fills is
what a restarted engine can compute. The rules under test are the ones a live
deploy depends on: fetch only when short, never overwrite a bar the engine
built itself, never take the service down over a vendor, and never call a
vendor at all when the configured provider is the dev simulator.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest
from qte_ingestion.backfill import (
    HistoryBackfiller,
    _frame_to_candles,
    _merge_candles,
)
from qte_shared.cache.redis_state import RedisState
from qte_shared.config import settings
from qte_shared.history_cache import fetch_history
from qte_shared.interfaces import HistoryRequest, HistorySource, UnsupportedCapability
from qte_shared.models import Candle
from qte_shared.symbols import build_specs

START = datetime(2026, 1, 1, tzinfo=UTC)


def bars(count: int, start: datetime = START, close: float = 2000.0) -> pd.DataFrame:
    index = pd.DatetimeIndex(
        [start + timedelta(minutes=15 * step) for step in range(count)], name="open_time"
    )
    return pd.DataFrame(
        {"open": close, "high": close + 1, "low": close - 1, "close": close, "volume": 0.0},
        index=index,
    )


def candle_at(offset: int, close: float = 1.0) -> Candle:
    return Candle(
        symbol="XAUUSD",
        timeframe="M15",
        open_time=START + timedelta(minutes=15 * offset),
        open=close,
        high=close,
        low=close,
        close=close,
        volume=0.0,
        tick_count=7,
    )


class FakeState:
    """Just the three Redis methods the backfiller touches."""

    def __init__(self, held: list[Candle] | None = None) -> None:
        self.held = held or []
        self.written: list[Candle] = []

    async def count_candles(self, symbol: str, timeframe: str) -> int:
        return len(self.held)

    async def get_candles(self, symbol: str, timeframe: str, count: int = 0) -> list[Candle]:
        return list(self.held)

    async def replace_candles(self, symbol, timeframe, candles, max_len=None) -> int:
        self.written = candles[-(max_len or len(candles)) :]
        return len(self.written)


class FakeSource(HistorySource):
    def __init__(self, frame: pd.DataFrame | None = None, error: Exception | None = None) -> None:
        self.frame = frame if frame is not None else bars(10)
        self.error = error
        self.calls: list[HistoryRequest] = []

    async def fetch(self, request: HistoryRequest) -> pd.DataFrame:
        self.calls.append(request)
        if self.error is not None:
            raise self.error
        return self.frame


def backfiller(state, target: int = 100, cache=None) -> HistoryBackfiller:
    # The cache is always injected: a default one would write parquet into the
    # repository's own data/ directory as a side effect of running the suite.
    instance = HistoryBackfiller(
        state, build_specs(["XAUUSD"], {}), ["M15"], cache=cache or NullCache()
    )
    instance.target = target
    return instance


class NullCache:
    """A cache that remembers nothing and touches no disk."""

    def load(self, symbol, timeframe, start=None, end=None):
        return pd.DataFrame()

    def store(self, frame, symbol, timeframe):
        return None


def install_source(monkeypatch, source: FakeSource | None, *, unsupported: bool = False) -> None:
    def create_provider(*args, **kwargs):
        if unsupported:
            raise UnsupportedCapability("provider 'simulator' does not serve 'history'")

        class Provider:
            name = "fake"

            def history_source(self):
                return source

        return Provider()

    monkeypatch.setattr("qte_ingestion.backfill.create_provider", create_provider)


# ── When it fetches, and when it does not ─────────────────────────────────


async def test_full_cache_is_left_alone(monkeypatch):
    source = FakeSource()
    install_source(monkeypatch, source)
    state = FakeState([candle_at(index) for index in range(100)])

    await backfiller(state, target=100).run()

    assert source.calls == [], "a warm cache must not cost a vendor request"
    assert state.written == []


async def test_short_cache_is_filled(monkeypatch):
    source = FakeSource(bars(40))
    install_source(monkeypatch, source)
    state = FakeState([candle_at(index) for index in range(5)])

    await backfiller(state, target=100).run()

    assert len(source.calls) == 1
    assert len(state.written) == 40
    assert state.written == sorted(state.written, key=lambda candle: candle.open_time)


async def test_simulator_provider_never_calls_a_vendor(monkeypatch):
    install_source(monkeypatch, None, unsupported=True)
    state = FakeState()

    await backfiller(state).run()

    assert state.written == [], "the dev fixture must warm by hand, not over the network"


async def test_vendor_failure_does_not_stop_the_service(monkeypatch):
    source = FakeSource(error=RuntimeError("Tiingo is down"))
    install_source(monkeypatch, source)
    state = FakeState()

    await backfiller(state).run()  # must not raise

    assert state.written == []


async def test_disabled_by_configuration(monkeypatch):
    source = FakeSource()
    install_source(monkeypatch, source)
    monkeypatch.setattr("qte_ingestion.backfill.ingestion_settings.backfill_history", False)

    await backfiller(FakeState()).run()

    assert source.calls == []


async def test_requested_span_is_wide_enough_for_the_target(monkeypatch):
    source = FakeSource(bars(10))
    install_source(monkeypatch, source)

    await backfiller(FakeState(), target=6000).run()

    request = source.calls[0]
    days = (request.end - request.start).days
    # 6000 M15 bars is ~62 trading days; a calendar window has to be wider.
    assert days >= 87, f"span of {days} days cannot yield 6000 M15 bars on a five-day week"


# ── Merging ───────────────────────────────────────────────────────────────


def test_locally_built_bars_win_over_the_vendors():
    """A bar the engine resampled is what the strategy already acted on."""
    existing = [candle_at(3, close=111.0)]
    fetched = _frame_to_candles(bars(10), "XAUUSD", "M15")
    merged = _merge_candles(existing, fetched)

    assert len(merged) == 10
    assert merged[3].close == 111.0
    assert merged[3].tick_count == 7
    assert [candle.open_time for candle in merged] == sorted(candle.open_time for candle in merged)


def test_vendor_bars_carry_no_invented_tick_count():
    converted = _frame_to_candles(bars(3), "XAUUSD", "M15")
    assert [candle.tick_count for candle in converted] == [0, 0, 0]
    assert all(candle.is_closed for candle in converted)
    assert converted[0].open_time == START


def test_merge_fills_around_a_held_island():
    existing = [candle_at(index) for index in (0, 9)]
    fetched = _frame_to_candles(bars(10), "XAUUSD", "M15")
    merged = _merge_candles(existing, fetched)
    assert len(merged) == 10
    assert merged[0].tick_count == 7
    assert merged[9].tick_count == 7
    assert merged[5].tick_count == 0


# ── Redis rewrite ─────────────────────────────────────────────────────────


class FakePipeline:
    def __init__(self, store: dict) -> None:
        self.store = store
        self.operations: list[tuple] = []

    def delete(self, key):
        self.operations.append(("delete", key))

    def rpush(self, key, *values):
        self.operations.append(("rpush", key, values))

    def expire(self, key, ttl):
        self.operations.append(("expire", key, ttl))

    async def execute(self):
        for operation in self.operations:
            if operation[0] == "delete":
                self.store.pop(operation[1], None)
            elif operation[0] == "rpush":
                self.store.setdefault(operation[1], []).extend(operation[2])
        return []


class FakeRedis:
    def __init__(self) -> None:
        self.store: dict[str, list] = {}

    def pipeline(self):
        return FakePipeline(self.store)

    async def llen(self, key):
        return len(self.store.get(key, []))


async def test_replace_candles_writes_oldest_first_and_trims(monkeypatch):
    state = RedisState()
    fake = FakeRedis()
    monkeypatch.setattr(type(state), "client", property(lambda self: fake))

    written = await state.replace_candles(
        "XAUUSD", "M15", [candle_at(index) for index in range(10)], max_len=4
    )

    assert written == 4
    key = state.key("candles", "XAUUSD", "M15")
    stored = [Candle.model_validate_json(item) for item in fake.store[key]]
    assert [candle.open_time for candle in stored] == [
        START + timedelta(minutes=15 * index) for index in (6, 7, 8, 9)
    ], "the tail must survive, in order — the runner reads it as its newest window"


async def test_replace_candles_ignores_an_empty_list(monkeypatch):
    state = RedisState()
    fake = FakeRedis()
    monkeypatch.setattr(type(state), "client", property(lambda self: fake))

    assert await state.replace_candles("XAUUSD", "M15", []) == 0
    assert fake.store == {}, "an empty fetch must not wipe the history that is there"


# ── Cache policy ──────────────────────────────────────────────────────────


class RecordingCache:
    def __init__(self, frame: pd.DataFrame) -> None:
        self.frame = frame
        self.stored: list[pd.DataFrame] = []

    def load(self, symbol, timeframe, start=None, end=None):
        return self.frame

    def store(self, frame, symbol, timeframe):
        self.stored.append(frame)
        return None


def request_for(days: int) -> HistoryRequest:
    end = START.date()
    return HistoryRequest(
        symbol="XAUUSD", timeframe="M15", start=end - timedelta(days=days), end=end
    ).normalized()


async def test_dev_reads_the_cache_instead_of_the_vendor():
    request = request_for(days=5)
    covering = bars(500, start=datetime(2025, 12, 27, tzinfo=UTC))
    source = FakeSource()

    frame = await fetch_history(source, request, cache=RecordingCache(covering), use_cache=True)

    assert source.calls == []
    assert len(frame) == len(covering)


async def test_production_always_asks_the_vendor(monkeypatch):
    """A stale file must never stand in for the market on a live deploy."""
    monkeypatch.setattr(settings, "env", "prod")
    request = request_for(days=5)
    cache = RecordingCache(bars(500, start=datetime(2025, 12, 27, tzinfo=UTC)))
    source = FakeSource(bars(20))

    await fetch_history(source, request, cache=cache)

    assert len(source.calls) == 1, "QTE_ENV=prod must bypass the cache"
    assert cache.stored, "a production fetch still refreshes the cache for dev"


async def test_a_cache_miss_falls_through_and_is_written():
    request = request_for(days=365)
    cache = RecordingCache(bars(10))  # nowhere near covering a year
    source = FakeSource(bars(40))

    frame = await fetch_history(source, request, cache=cache, use_cache=True)

    assert len(source.calls) == 1
    assert len(cache.stored) == 1
    assert len(frame) == 40


@pytest.mark.parametrize("environment", ["dev", "staging"])
async def test_non_production_environments_may_use_the_cache(monkeypatch, environment):
    monkeypatch.setattr(settings, "env", environment)
    request = request_for(days=5)
    source = FakeSource()

    await fetch_history(
        source, request, cache=RecordingCache(bars(500, start=datetime(2025, 12, 27, tzinfo=UTC)))
    )

    assert source.calls == []


# ── Regressions from the first review ─────────────────────────────────────


async def test_an_explicit_download_always_reaches_the_vendor(tmp_path, monkeypatch):
    """`make download` must download, even with a covering cache in dev.

    Serving it from disk made the command a silent no-op that returned bars up
    to COVERAGE_TOLERANCE_DAYS old.
    """
    from qte_backtest.downloader import DownloadRequest, HistoryDownloader

    monkeypatch.setattr(settings, "env", "dev")
    source = FakeSource(bars(40))

    class Provider:
        name = "fake"

        def history_source(self):
            return source

    downloader = HistoryDownloader(provider=Provider(), parquet_dir=tmp_path)
    covering = bars(5000, start=datetime(2020, 1, 1, tzinfo=UTC))
    downloader._cache.store(covering, "XAUUSD", "M15")

    await downloader.download(DownloadRequest(symbol="XAUUSD", timeframe="M15"))

    assert len(source.calls) == 1, "an explicit download must not be served from the cache"


async def test_misconfigured_provider_warns_rather_than_whispers(monkeypatch, caplog):
    """A missing API key leaves Redis cold; it must not read like the dev skip."""
    from qte_shared.interfaces import ProviderNotConfigured

    def create_provider(*args, **kwargs):
        raise ProviderNotConfigured("QTE_TIINGO__API_KEY is not set")

    monkeypatch.setattr("qte_ingestion.backfill.create_provider", create_provider)

    with caplog.at_level("WARNING"):
        await backfiller(FakeState()).run()

    assert any(record.levelname == "WARNING" for record in caplog.records)


async def test_the_deliberate_simulator_skip_stays_quiet(monkeypatch, caplog):
    install_source(monkeypatch, None, unsupported=True)

    with caplog.at_level("WARNING"):
        await backfiller(FakeState()).run()

    assert not [record for record in caplog.records if record.levelname == "WARNING"]
