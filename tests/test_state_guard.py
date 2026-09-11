"""Candle state another feed wrote is discarded before ingestion reads it.

Pinned from the 2026-09-10 production audit: a Redis volume a simulator
rehearsal left behind held bars dated days ahead, and a Tiingo feed started on
it dropped every real tick as late while the runner warmed on invented prices.
"""

from __future__ import annotations

from datetime import UTC, datetime

from qte_ingestion.state_guard import discard_foreign_candle_state
from qte_shared.cache.redis_state import RedisState
from qte_shared.market_data_plan import SymbolFeed
from qte_shared.models import Candle

MOMENT = datetime(2026, 9, 10, 9, 47, tzinfo=UTC)
SUBSCRIPTIONS = [SymbolFeed(symbol="XAUUSD", market="fx", timeframes=("M15",))]


def candle_at(open_time: datetime, *, closed: bool = True) -> Candle:
    return Candle(
        symbol="XAUUSD",
        timeframe="M15",
        open_time=open_time,
        open=1.0,
        high=1.0,
        low=1.0,
        close=1.0,
        is_closed=closed,
    )


class RecordingState:
    """The candle-state surface the guard reads and writes, and nothing else.

    It has no cycle methods at all, so a guard that reached for open positions
    would fail here rather than quietly delete one.
    """

    def __init__(self, *, provider=None, candles=None, open_bar=None, outbox=None) -> None:
        self.provider = provider
        self.candles = list(candles or [])
        self.open_bar = open_bar
        self.outbox = list(outbox or [])
        self.operations: list[str] = []

    async def get_history_provider(self):
        return self.provider

    async def set_history_provider(self, provider_name):
        self.operations.append(f"set_provider:{provider_name}")
        self.provider = provider_name

    async def get_candles(self, symbol, timeframe, count=0):
        return self.candles[-count:] if count else list(self.candles)

    async def get_open_candle(self, symbol, timeframe):
        return self.open_bar

    async def pending_candles(self):
        return list(self.outbox)

    async def discard_candle_state(self, symbol, timeframe):
        self.operations.append(f"discard:{symbol}:{timeframe}")
        self.candles, self.open_bar = [], None

    async def discard_candle_outbox(self):
        self.operations.append("discard_outbox")
        self.outbox = []


async def guard(candle_state, *, provider_name="tiingo", synthetic=False) -> bool:
    return await discard_foreign_candle_state(
        candle_state,
        SUBSCRIPTIONS,
        provider_name=provider_name,
        synthetic=synthetic,
        moment=MOMENT,
    )


async def test_state_written_by_another_provider_is_discarded():
    candle_state = RecordingState(
        provider="simulator", candles=[candle_at(datetime(2026, 9, 10, 8, 0, tzinfo=UTC))]
    )

    assert await guard(candle_state) is True
    assert candle_state.operations == [
        "discard:XAUUSD:M15",
        "discard_outbox",
        "set_provider:tiingo",
    ]


async def test_unmarked_state_holding_future_bars_is_discarded():
    """The audit's scenario B, exactly: no marker, bars and open bar three days out."""
    candle_state = RecordingState(
        candles=[candle_at(datetime(2026, 9, 13, 19, 45, tzinfo=UTC))],
        open_bar=candle_at(datetime(2026, 9, 13, 20, 0, tzinfo=UTC), closed=False),
    )

    assert await guard(candle_state) is True
    assert candle_state.candles == [] and candle_state.open_bar is None


async def test_unmarked_state_is_discarded_once_then_kept():
    """Nothing says who wrote it: a simulator replay on past buckets looks like vendor
    history, so state from before the marker existed is rebuilt rather than trusted."""
    past_bar = candle_at(datetime(2026, 9, 10, 9, 30, tzinfo=UTC))
    candle_state = RecordingState(candles=[past_bar])

    assert await guard(candle_state) is True
    assert candle_state.operations == [
        "discard:XAUUSD:M15",
        "discard_outbox",
        "set_provider:tiingo",
    ]

    candle_state.candles = [past_bar]
    candle_state.operations.clear()
    assert await guard(candle_state) is False, "once marked, this provider's state is kept"
    assert candle_state.operations == ["set_provider:tiingo"]
    assert candle_state.candles == [past_bar]


async def test_an_empty_unmarked_cache_is_only_marked():
    candle_state = RecordingState()

    assert await guard(candle_state) is False
    assert candle_state.operations == ["set_provider:tiingo"]


async def test_a_closed_bar_whose_bucket_has_not_ended_is_not_history():
    """What the old backfill stored: the 09:45 bucket as closed, read at 09:47."""
    candle_state = RecordingState(
        provider="tiingo", candles=[candle_at(datetime(2026, 9, 10, 9, 45, tzinfo=UTC))]
    )

    assert await guard(candle_state) is True


async def test_a_future_close_waiting_in_the_outbox_is_discarded():
    candle_state = RecordingState(
        provider="tiingo", outbox=[candle_at(datetime(2026, 9, 13, 20, 0, tzinfo=UTC))]
    )

    assert await guard(candle_state) is True
    assert "discard_outbox" in candle_state.operations


async def test_a_synthetic_provider_keeps_its_own_forward_anchored_bars():
    candle_state = RecordingState(
        provider="simulator",
        candles=[candle_at(datetime(2026, 9, 13, 19, 45, tzinfo=UTC))],
        open_bar=candle_at(datetime(2026, 9, 13, 20, 0, tzinfo=UTC), closed=False),
    )

    assert await guard(candle_state, provider_name="simulator", synthetic=True) is False
    assert candle_state.operations == ["set_provider:simulator"]


class DeletingRedis:
    def __init__(self) -> None:
        self.deleted: list[str] = []

    async def delete(self, *redis_keys: str) -> None:
        self.deleted.extend(redis_keys)


async def test_discarding_removes_candles_open_bar_and_outbox_but_no_cycle(monkeypatch):
    redis_state = RedisState()
    fake_client = DeletingRedis()
    monkeypatch.setattr(type(redis_state), "client", property(lambda self: fake_client))

    await redis_state.discard_candle_state("XAUUSD", "M15")
    await redis_state.discard_candle_outbox()

    assert fake_client.deleted == [
        redis_state.key("candles", "XAUUSD", "M15"),
        redis_state.key("open_candle", "XAUUSD", "M15"),
        redis_state.key("outbox", "candles"),
    ]
    assert not [deleted_key for deleted_key in fake_client.deleted if ":cycle:" in deleted_key]
