"""Ingestion's start-up order is what keeps foreign and half-built bars out of Redis.

Each step depends on the one before it. The guard has to discard another feed's
state before the outbox is drained or an open bar is restored from it. A restored
bar whose bucket ended during the downtime has to close before backfill reads the
list, or the top-up lands beside it. And the join time has to be recorded before
the first tick, or the bucket already under way is published as if whole.
"""

from __future__ import annotations

import asyncio

from qte_ingestion.service import IngestionService
from qte_shared.market_data_plan import SymbolFeed


class JournalledResource:
    def __init__(self, resource_name: str, journal: list[str]) -> None:
        self.resource_name = resource_name
        self.journal = journal

    async def connect(self) -> None:
        self.journal.append(f"connect:{self.resource_name}")

    async def close(self) -> None:
        self.journal.append(f"close:{self.resource_name}")


class JournalledResampler:
    def __init__(self, journal: list[str]) -> None:
        self.journal = journal

    def mark_joined(self, moment) -> None:
        self.journal.append("mark_joined")

    def flush(self, moment) -> list:
        return []


class JournalledFeed:
    def __init__(self, journal: list[str]) -> None:
        self.journal = journal

    def start(self) -> object:
        self.journal.append("feed_start")
        return object()

    async def stop(self) -> None:
        return None


class JournalledProvider:
    name = "tiingo"
    synthetic = False

    def __init__(self, journal: list[str]) -> None:
        self.journal = journal

    def live_feeds(self, specs, on_tick) -> list[JournalledFeed]:
        return [JournalledFeed(self.journal)]


class SilentEvents:
    async def record_event(self, **event_fields) -> None:
        return None


async def test_start_up_guards_restores_closes_backfills_then_listens(monkeypatch):
    journal: list[str] = []
    service = object.__new__(IngestionService)
    service.subscriptions = [SymbolFeed(symbol="XAUUSD", market="fx", timeframes=("M15",))]
    service.specs = [symbol_feed.spec for symbol_feed in service.subscriptions]
    service.timeframes = ["M15"]
    service.bus = JournalledResource("bus", journal)
    service.state = JournalledResource("state", journal)
    service.events = SilentEvents()
    service.provider = JournalledProvider(journal)
    service._resamplers = {"XAUUSD": JournalledResampler(journal)}
    service._feeds = []
    service._repairer = None
    service._flush_task = None
    service._stopping = asyncio.Event()
    service._outbox_lock = asyncio.Lock()
    service._cleaned = False

    async def record_guard(*guard_arguments, **guard_keywords) -> bool:
        journal.append("guard")
        return False

    async def record_drain() -> None:
        journal.append("drain")

    async def record_restore() -> None:
        journal.append("restore")

    async def record_close(candles, **close_options) -> None:
        journal.append(f"close_ended:current={close_options.get('close_is_current', True)}")

    def record_repairer() -> None:
        journal.append("repairer")

    class JournalledBackfiller:
        def __init__(self, candle_state, subscriptions) -> None:
            self.subscriptions = subscriptions

        async def run(self) -> None:
            journal.append("backfill")

    monkeypatch.setattr("qte_ingestion.service.discard_foreign_candle_state", record_guard)
    monkeypatch.setattr("qte_ingestion.service.HistoryBackfiller", JournalledBackfiller)
    monkeypatch.setattr("qte_ingestion.service.ingestion_settings.flush_interval", 60.0)
    service._drain_candle_outbox = record_drain
    service._restore_open_candles = record_restore
    service._emit_candles = record_close
    service._build_repairer = record_repairer

    await service.start()
    try:
        assert journal == [
            "connect:bus",
            "connect:state",
            "guard",
            "drain",
            "repairer",
            "restore",
            "close_ended:current=False",
            "backfill",
            "mark_joined",
            "feed_start",
        ]
    finally:
        await service.stop()
