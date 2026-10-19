"""Executable spec for ``/warmup`` and ``/flush``, which ingestion serves.

They are ingestion's and not the bot's for one reason, asserted here: the bars
in Redis, the staging watermark that decides whether a replayed bar is a
duplicate, and the resampler's half-built bucket are three parts of one state.
A flush that dropped the bars from outside would leave the other two pointing
at bars that no longer exist.

The vendor and Redis are doubles; what is under test is the handler's contract
— what it asks for, what it clears, and what it says about a symbol nothing
feeds.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

from qte_ingestion.service import SERVICE_NAME, IngestionService
from qte_shared.config import settings
from qte_shared.market_data_plan import SymbolFeed
from qte_shared.models import Candle


class ControlMessage:
    def __init__(self, payload: dict, *, reply: str = "reply.subject") -> None:
        self.data = json.dumps(payload).encode()
        self.reply = reply


class ReplyBus:
    def __init__(self) -> None:
        self.replies: list[dict] = []
        self.nc = self

    async def publish(self, subject, payload=None):
        self.replies.append(json.loads(payload))


class WindowStore:
    """The candle-window surface these three actions touch."""

    def __init__(self, bars: dict[tuple[str, str], int] | None = None) -> None:
        self.bars = dict(bars or {})
        self.discarded: list[tuple[str, str]] = []

    async def count_candles(self, symbol, timeframe):
        return self.bars.get((symbol, timeframe), 0)

    async def get_candles(self, symbol, timeframe, count=0):
        held = self.bars.get((symbol, timeframe), 0)
        if not held:
            return []
        bar = Candle(
            origin=settings.state_scope.origin(),
            symbol=symbol,
            timeframe=timeframe,
            open_time=datetime(2026, 10, 1, tzinfo=UTC),
            open=1.0,
            high=1.0,
            low=1.0,
            close=1.0,
            tick_count=1,
        )
        return [bar]

    async def discard_candle_state(self, symbol, timeframe):
        self.discarded.append((symbol, timeframe))
        self.bars.pop((symbol, timeframe), None)


class RecordingBackfiller:
    """Stands in for the vendor request, and records how it was asked."""

    def __init__(self, *, failure: str | None = None) -> None:
        self.asked: list[tuple[str, str, bool]] = []
        self.failure = failure

    async def backfill_series(self, spec, timeframe, *, before=None, force=False):
        self.asked.append((spec.symbol, timeframe, force))
        if self.failure:
            raise RuntimeError(self.failure)


class RecordingResampler:
    def __init__(self) -> None:
        self.joined: list[datetime] = []

    def mark_joined(self, moment) -> None:
        self.joined.append(moment)


def _service(*, bars=None, backfiller=None, symbols=("XAUUSD", "EURUSD")):
    """A service built field by field: start() needs a vendor socket, this does not."""
    service = object.__new__(IngestionService)
    service._scope = settings.state_scope
    service.subscriptions = [
        SymbolFeed(symbol=symbol, market="fx", timeframes=("M15",)) for symbol in symbols
    ]
    service.specs = [feed.spec for feed in service.subscriptions]
    service.timeframes = ["M15"]
    service.providers = {"mt5": object()}
    service.state = WindowStore(bars)
    service.bus = ReplyBus()
    service._history_owed = {}
    service._gap_asked_at = {}
    service._vendor_offline = False
    service._resamplers = {symbol: RecordingResampler() for symbol in symbols}
    service._backfiller = backfiller or RecordingBackfiller()
    service._backfiller_for = lambda symbol: service._backfiller
    return service


async def _ask(service, payload: dict) -> dict | None:
    await service._on_control_message(ControlMessage(payload))
    return service.bus.replies[-1] if service.bus.replies else None


# ── Status ────────────────────────────────────────────────────────────────


async def test_status_reports_every_subscribed_series_and_its_window():
    service = _service(bars={("XAUUSD", "M15"): 420})

    answer = await _ask(service, {"action": "status"})

    assert answer["service"] == SERVICE_NAME
    assert answer["namespace"] == settings.state_scope.namespace
    windows = {(row["symbol"], row["timeframe"]): row for row in answer["series"]}
    assert windows[("XAUUSD", "M15")]["bars"] == 420
    assert windows[("EURUSD", "M15")]["bars"] == 0
    assert windows[("XAUUSD", "M15")]["newest_bar"] is not None


# ── Warm-up ───────────────────────────────────────────────────────────────


async def test_warmup_forces_the_request_even_when_the_window_looks_full():
    """An operator asking by hand is usually checking whether it is *right*."""
    backfiller = RecordingBackfiller()
    service = _service(bars={("XAUUSD", "M15"): 500}, backfiller=backfiller)

    answer = await _ask(service, {"action": "warmup", "symbols": ["XAUUSD"]})

    assert backfiller.asked == [("XAUUSD", "M15", True)]
    [warmed] = answer["warmed"]
    assert warmed["symbol"] == "XAUUSD" and warmed["bars_before"] == 500


async def test_warmup_without_symbols_asks_for_every_planned_series():
    backfiller = RecordingBackfiller()
    service = _service(backfiller=backfiller)

    answer = await _ask(service, {"action": "warmup", "symbols": None})

    assert [symbol for symbol, _, _ in backfiller.asked] == ["XAUUSD", "EURUSD"]
    assert len(answer["warmed"]) == 2


async def test_warmup_reports_a_symbol_nothing_here_feeds():
    service = _service()

    answer = await _ask(service, {"action": "warmup", "symbols": ["BTCUSDT"]})

    assert answer["warmed"] == []
    assert answer["unknown"] == ["BTCUSDT"]


async def test_a_failed_warmup_is_reported_per_series_not_raised():
    service = _service(backfiller=RecordingBackfiller(failure="the vendor is not connected"))

    answer = await _ask(service, {"action": "warmup", "symbols": ["XAUUSD"]})

    [warmed] = answer["warmed"]
    assert warmed["error"] == "the vendor is not connected"


# ── Flush ─────────────────────────────────────────────────────────────────


async def test_flush_drops_the_window_and_owes_it_again():
    service = _service(bars={("XAUUSD", "M15"): 420})

    answer = await _ask(service, {"action": "flush", "symbols": ["XAUUSD"]})

    assert service.state.discarded == [("XAUUSD", "M15")]
    [flushed] = answer["flushed"]
    assert flushed == {"symbol": "XAUUSD", "timeframe": "M15", "dropped": 420}
    # Owed again, so the history loop asks for it without being told twice.
    assert ("XAUUSD", "M15") in service._history_owed


async def test_flush_re_marks_the_resampler_so_no_half_built_bar_survives():
    """The bucket under way was built on bars that no longer exist."""
    service = _service(bars={("XAUUSD", "M15"): 10})

    await _ask(service, {"action": "flush", "symbols": ["XAUUSD"]})

    assert service._resamplers["XAUUSD"].joined, "the join point was not moved"
    assert not service._resamplers["EURUSD"].joined, "a symbol out of scope was touched"


async def test_flush_all_clears_every_planned_series():
    service = _service(bars={("XAUUSD", "M15"): 5, ("EURUSD", "M15"): 7})

    answer = await _ask(service, {"action": "flush", "symbols": None})

    assert sorted(service.state.discarded) == [("EURUSD", "M15"), ("XAUUSD", "M15")]
    assert sum(row["dropped"] for row in answer["flushed"]) == 12


async def test_flush_leaves_a_symbol_it_does_not_feed_alone():
    service = _service(bars={("XAUUSD", "M15"): 5})

    answer = await _ask(service, {"action": "flush", "symbols": ["BTCUSDT"]})

    assert service.state.discarded == []
    assert answer == {"flushed": [], "unknown": ["BTCUSDT"]}


async def test_an_unknown_action_is_ignored():
    service = _service()
    assert await _ask(service, {"action": "drop_everything"}) is None
