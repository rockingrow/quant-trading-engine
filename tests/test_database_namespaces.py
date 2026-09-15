"""Execute repository isolation against an in-memory relational database.

SQLite exercises row visibility, uniqueness and mutation boundaries here.
PostgreSQL migrations and dialect differences still require Alembic checks.
"""

from contextlib import asynccontextmanager

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Session
from test_broker_sink import _signal
from test_runner_delivery import _held
from test_state_isolation import select_scope

from qte_backtest.db.repository import BacktestRepository
from qte_shared.db.base import Base
from qte_shared.db.models import EngineEvent
from qte_shared.db.repository import EventRepository
from qte_strategy_engine.db.models import OpenPositionRow
from qte_strategy_engine.db.repository import OpenPositionRepository, SignalRepository


@compiles(JSONB, "sqlite")
def compile_json_storage(element, compiler, **keywords):
    return "JSON"


class SessionAdapter:
    def __init__(self, transaction):
        self.transaction = transaction

    def add(self, record):
        self.transaction.add(record)

    async def flush(self):
        self.transaction.flush()

    async def execute(self, statement):
        return self.transaction.execute(statement)


class MemoryDatabase:
    def __init__(self):
        self.engine = create_engine("sqlite://")
        Base.metadata.create_all(self.engine)

    @asynccontextmanager
    async def session(self):
        with Session(self.engine, expire_on_commit=False) as transaction:
            with transaction.begin():
                yield SessionAdapter(transaction)


@pytest.fixture
def database():
    database = MemoryDatabase()
    try:
        yield database
    finally:
        database.engine.dispose()


async def test_positions_with_the_same_pair_can_coexist_and_clear_independently(
    monkeypatch, database
):
    repositories = []
    for execution_mode in ("shadow", "live"):
        select_scope(monkeypatch, execution_mode=execution_mode)
        repository = OpenPositionRepository(database)
        repositories.append(repository)
        assert await repository.get("DELIVERY_PROBE", "XAUUSD") is None
        assert await repository.upsert(_held())
        assert await repository.upsert(_held(remaining=2))
        assert len(await repository.list_open()) == 1
    paper_book, broker_book = repositories
    assert await broker_book.clear("DELIVERY_PROBE", "XAUUSD")
    assert await broker_book.list_open() == []
    assert (await paper_book.get("DELIVERY_PROBE", "XAUUSD")).remaining == 2
    with Session(database.engine) as transaction:
        assert len(transaction.scalars(select(OpenPositionRow)).all()) == 1


async def test_foreign_outbox_ids_cannot_be_read_recovered_or_updated(monkeypatch, database):
    select_scope(monkeypatch, execution_mode="shadow")
    paper_book = SignalRepository(database)
    signal = _signal()
    delivery_id = await paper_book.stage_signal(signal, transport="nats", shadow=True)
    assert delivery_id is not None
    select_scope(monkeypatch, execution_mode="live")
    broker_book = SignalRepository(database)
    assert await broker_book.get_delivery(delivery_id) is None
    assert await broker_book.pending_deliveries(include_ids=[delivery_id]) == []
    assert await broker_book.list_signals() == []
    assert await broker_book.get_cycle(signal.signal_uxid) == []
    assert not await broker_book.mark_delivery(delivery_id, status="sent")
    assert (await paper_book.get_delivery(delivery_id)).delivery_status == "prepared"
    assert len(await paper_book.pending_deliveries()) == 1


async def test_legacy_positions_are_quarantined_and_mismatched_json_fails_closed(
    monkeypatch, database
):
    select_scope(monkeypatch, execution_mode="live")
    repository = OpenPositionRepository(database)
    assert await repository.upsert(_held())
    with Session(database.engine) as transaction, transaction.begin():
        stored = transaction.scalar(select(OpenPositionRow))
        stored.namespace = "legacy"
    assert await repository.list_open() == []
    assert await repository.get("DELIVERY_PROBE", "XAUUSD") is None
    with Session(database.engine) as transaction, transaction.begin():
        stored = transaction.scalar(select(OpenPositionRow))
        stored.namespace = repository._namespace
        stored.state = {**stored.state, "state_namespace": "prod:shadow:tiingo"}
    with pytest.raises(ValueError, match="provenance"):
        await repository.get("DELIVERY_PROBE", "XAUUSD")


async def test_events_and_backtests_retain_their_execution_identity(monkeypatch, database):
    for execution_mode in ("shadow", "live"):
        select_scope(monkeypatch, execution_mode=execution_mode)
        repository = BacktestRepository(database)
        assert await repository.list_backtests() == []
        assert await repository.record_backtest(
            strategy="EXAMPLE",
            symbol="XAUUSD",
            timeframe="M15",
            period_start=None,
            period_end=None,
            params={},
            metrics={},
        )
        assert len(await repository.list_backtests(with_trades=True)) == 1
        await EventRepository(database).record_event(service="example", event="started")
    with Session(database.engine) as transaction:
        assert set(transaction.scalars(select(EngineEvent.namespace))) == {
            "prod:shadow:tiingo",
            "prod:live:tiingo",
        }
