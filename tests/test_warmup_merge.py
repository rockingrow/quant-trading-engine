"""A replayed warm-up batch fills the indicator window; a close still does not.

The MT5 ingester cannot serve history, so the only way it can fill a window it
finds part-filled is to replay the bars into it. Those bars arrive marked
``warmup_bar`` and are *not* closes: ingestion buffers the batch, merges it into
the stored window in one transaction, and never stages, publishes or decides on
any of it. What is pinned here is that split — the live path keeps behaving
exactly as it did, and the merge is what gets the old bars in.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta

from fakeredis.aioredis import FakeRedis

from qte_ingestion.service import IngestionService
from qte_shared.cache.redis_state import RedisState
from qte_shared.config import settings
from qte_shared.interfaces.market_data import WarmupBatch
from qte_shared.market_data_plan import SymbolFeed
from qte_shared.providers.mt5.protocol import decode_bar_closed

#: Comfortably in the past, so the unfinished-bucket guard never fires.
FIRST_BAR = datetime(2026, 9, 28, 10, 0, tzinfo=UTC)


def warmup_service() -> IngestionService:
    """An ingestion service with just enough wired to route a bar."""
    service = object.__new__(IngestionService)
    service.subscriptions = [
        SymbolFeed(symbol="XAUUSD", market="fx", timeframes=("M15",)),
        SymbolFeed(symbol="BTCUSDT", market="crypto", timeframes=("M15",)),
    ]
    service._symbol_providers = {"XAUUSD": "mt5", "BTCUSDT": "mt5"}
    service._origins = {"mt5": settings.state_scope.origin()}
    service._processing_lock = asyncio.Lock()
    service._warmup_batches = {}
    service.state = RedisState(prefix="warmup-merge-test")
    service.state._client = FakeRedis(decode_responses=True)
    return service


def bar_message(index: int, *, symbol: str = "XAUUSD", warmup: tuple[int, int] | None = None):
    """One ingester payload, decoded — a live close unless *warmup* is given."""
    open_time = FIRST_BAR + timedelta(minutes=15 * index)
    payload = {
        "schema_version": "1.0.0",
        "event_id": f"mt5:{symbol}:M15:{int(open_time.timestamp())}",
        "event_type": "bar.closed",
        "source": {"gateway": "mt5", "market": "forex"},
        "emitted_at": (open_time + timedelta(minutes=15)).isoformat(),
        "symbol": symbol,
        "timeframe": "M15",
        "bar": {
            "open_time": open_time.isoformat().replace("+00:00", "Z"),
            "close_time": (open_time + timedelta(minutes=15)).isoformat().replace("+00:00", "Z"),
            "open": 2651.12,
            "high": 2654.8,
            "low": 2649.95,
            "close": 2653.4,
            "volume": 1843.0,
            "tick_count": 1843,
        },
    }
    if warmup is not None:
        position, total = warmup
        payload.update(warmup_bar=True, warmup_index=position, warmup_total=total)
    return decode_bar_closed(json.dumps(payload))


async def replay_batch(service, indexes, *, symbol="XAUUSD", total=None):
    """Hand *indexes* over as one warm-up batch, oldest first."""
    total = total or len(indexes)
    for position, index in enumerate(indexes, start=1):
        bar = bar_message(index, symbol=symbol, warmup=(position, total))
        await service._handle_warmup_bar(bar.candle, bar.warmup)


async def test_a_batch_is_held_until_it_is_whole_then_written_once(monkeypatch):
    """A merge rewrites the whole window, so 150 of them is the thing to avoid."""
    service = warmup_service()
    merges: list[int] = []
    original = service.state.merge_history_candles

    async def counted(symbol, timeframe, candles, **options):
        merges.append(len(candles))
        return await original(symbol, timeframe, candles, **options)

    monkeypatch.setattr(service.state, "merge_history_candles", counted)
    await replay_batch(service, range(150))
    assert merges == [150]
    assert await service.state.count_candles("XAUUSD", "M15") == 150
    # Nothing is left holding memory once the batch has been written.
    assert service._warmup_batches == {}


async def test_a_replayed_bar_is_never_staged_or_published():
    """It carries no decision, so it must not reach the outbox the runner drains."""
    service = warmup_service()
    await replay_batch(service, range(10))
    assert await service.state.count_candles("XAUUSD", "M15") == 10
    assert await service.state.pending_candles() == []
    assert await service.state.peek_pending_candle() is None


async def test_a_half_filled_window_is_completed_by_the_bars_it_is_missing():
    """The scenario this exists for: 35 of 150 bars, and the other 115 are older."""
    service = warmup_service()
    for index in range(35):
        await service.state.stage_closed_candle(bar_message(index).candle)
    assert await service.state.count_candles("XAUUSD", "M15") == 35

    await replay_batch(service, range(-115, 35), total=150)

    stored = await service.state.get_candles("XAUUSD", "M15")
    assert len(stored) == 150
    assert stored[0].open_time == FIRST_BAR - timedelta(minutes=15 * 115)
    assert stored[-1].open_time == FIRST_BAR + timedelta(minutes=15 * 34)


async def test_a_live_bar_newer_than_the_batch_survives_the_merge():
    """A replay a moment behind the live stream must not cost the newest close."""
    service = warmup_service()
    newest = bar_message(200).candle
    await service.state.stage_closed_candle(newest)

    await replay_batch(service, range(150))

    stored = await service.state.get_candles("XAUUSD", "M15")
    assert len(stored) == 151
    assert stored[-1].open_time == newest.open_time


async def test_a_close_still_takes_the_live_path(monkeypatch):
    """The split is the point: an unmarked bar behaves exactly as it always did."""
    service = warmup_service()
    emitted: list[list] = []

    async def emit(candles, **options):
        emitted.append(candles)

    monkeypatch.setattr(service, "_emit_candles", emit)
    bar = bar_message(0)
    assert bar.warmup is None
    await service._handle_closed_bar(bar.candle)
    assert emitted == [[bar.candle]]
    # And it was staged by that path, not merged by this one.
    assert service._warmup_batches == {}


async def test_two_series_replaying_at_once_do_not_mix():
    service = warmup_service()
    for position, index in enumerate(range(5), start=1):
        gold = bar_message(index, warmup=(position, 5))
        coin = bar_message(index, symbol="BTCUSDT", warmup=(position, 5))
        await service._handle_warmup_bar(gold.candle, gold.warmup)
        await service._handle_warmup_bar(coin.candle, coin.warmup)
    assert await service.state.count_candles("XAUUSD", "M15") == 5
    assert await service.state.count_candles("BTCUSDT", "M15") == 5
    assert service._warmup_batches == {}


async def test_a_redelivered_warmup_bar_collapses_instead_of_doubling():
    """JetStream may redeliver; the batch is keyed by open time, so it is harmless."""
    service = warmup_service()
    first = bar_message(0, warmup=(1, 3))
    await service._handle_warmup_bar(first.candle, first.warmup)
    await service._handle_warmup_bar(first.candle, WarmupBatch(index=2, total=3))
    last = bar_message(1, warmup=(3, 3))
    await service._handle_warmup_bar(last.candle, last.warmup)
    assert await service.state.count_candles("XAUUSD", "M15") == 2


async def test_a_warmup_bar_whose_bucket_has_not_ended_is_dropped(caplog):
    """A vendor clock that runs ahead must not plant a future bar in the window."""
    service = warmup_service()
    ahead = int((datetime.now(UTC) - FIRST_BAR).total_seconds() // 900) + 8
    bar = bar_message(ahead, warmup=(1, 1))
    await service._handle_warmup_bar(bar.candle, bar.warmup)
    assert await service.state.count_candles("XAUUSD", "M15") == 0
    assert "MT5_SERVER_TIMEZONE" in caplog.text


async def test_a_batch_that_never_ends_is_flushed_at_the_window_size(monkeypatch, caplog):
    """Without the cap, an ingester that loses its last bar buffers without bound."""
    service = warmup_service()
    monkeypatch.setattr(settings.redis, "candle_history", 4)
    with caplog.at_level("WARNING"):
        # Ten bars claiming to be part of a far larger batch: none is the last.
        await replay_batch(service, range(10), total=999)
    assert "the window size is the cap" in caplog.text
    assert await service.state.count_candles("XAUUSD", "M15") == 4
