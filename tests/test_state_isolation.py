"""Different execution books cannot share prices, controls or position recovery."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from test_broker_sink import FakeBus, _signal
from test_runner_catch_up import CandleStore, bar_at, build_runner
from test_runner_delivery import _enter, _held, _runner, _slot

from qte_ingestion.service import IngestionService
from qte_ingestion.settings import ingestion_settings
from qte_shared.bus.subjects import Subjects
from qte_shared.cache.redis_state import RedisState
from qte_shared.config import StateSettings, settings
from qte_shared.models import Tick, TickEvent
from qte_shared.state_scope import StateScope, stamp_market_data
from qte_strategy_engine.broker_sink import BrokerSink
from qte_strategy_engine.control import _set_shadow_mode


def select_scope(monkeypatch, environment="prod", execution_mode="shadow", provider="tiingo"):
    monkeypatch.setattr(settings, "env", environment)
    monkeypatch.setattr(settings.state_config, "execution_mode", execution_mode)
    monkeypatch.setattr(settings.market_data, "provider", provider)


@pytest.mark.parametrize(
    "environment,execution_mode,provider",
    [
        ("dev", "live", "tiingo"),
        ("prod", "dev", "tiingo"),
        ("prod", "shadow", "simulator"),
        ("prod", "live", "simulator"),
        ("prod", "live", "bad.provider"),
    ],
)
def test_invalid_state_identity_is_rejected(environment, execution_mode, provider):
    with pytest.raises(ValueError):
        StateScope(environment, execution_mode, provider)


def test_state_mode_defaults_to_paper_and_reads_its_explicit_variable(monkeypatch):
    monkeypatch.delenv("QTE_STATE__MODE")
    assert StateSettings().execution_mode == "shadow"
    monkeypatch.setenv("QTE_STATE__MODE", "live")
    assert StateSettings().execution_mode == "live"


async def test_shared_redis_keeps_every_book_and_provider_independent(monkeypatch):
    backend = FakeServer()
    stores = []
    subjects = []
    identities = [
        ("dev", "dev", "simulator"),
        ("dev", "dev", "tiingo"),
        ("prod", "shadow", "tiingo"),
        ("prod", "live", "tiingo"),
    ]
    try:
        for environment, execution_mode, provider in identities:
            select_scope(monkeypatch, environment, execution_mode, provider)
            storage = RedisState(prefix="shared")
            storage._client = FakeRedis(server=backend, decode_responses=True)
            stores.append(storage)
            subjects.append(Subjects(prefix="CUSTOM").engine_control())
            assert await storage.get_last_tick("XAUUSD") is None
            assert await storage.get_candles("XAUUSD", "M15") == []
            assert await storage.get_open_position("DELIVERY_PROBE", "XAUUSD") is None
            assert await storage.get_flag("shadow_mode") is None
            assert await storage.claim_runner("runner-owner")
            await storage.set_last_tick(Tick(symbol="XAUUSD", ts=datetime.now(UTC), last=2000))
            await storage.stage_closed_candle(bar_at(datetime(2026, 1, 1, tzinfo=UTC)))
            await storage.set_open_position(_held())
            await storage.set_flag("shadow_mode", False)
        assert len(set(subjects)) == len(identities)
        for storage in stores:
            assert (await storage.get_last_tick("XAUUSD")).origin == storage._scope.origin()
            assert len(await storage.pending_candles()) == 1
            assert (
                await storage.get_open_position("DELIVERY_PROBE", "XAUUSD")
            ).state_namespace == storage._scope.namespace
        # Changing global configuration never retargets an existing accessor.
        assert stores[0].key("tick", "XAUUSD") != stores[-1].key("tick", "XAUUSD")
    finally:
        for storage in stores:
            await storage.close()


async def test_redis_rejects_foreign_and_unattributed_prices_and_positions(monkeypatch):
    select_scope(monkeypatch, execution_mode="live")
    storage = RedisState()
    storage._client = FakeRedis(decode_responses=True)
    try:
        tick = Tick(symbol="XAUUSD", ts=datetime.now(UTC), last=2000)
        foreign = StateScope("dev", "dev", "simulator").origin()
        with pytest.raises(ValueError, match="another"):
            await storage.set_last_tick(stamp_market_data(tick, foreign))
        for origin in (None, foreign):
            await storage.client.set(
                storage.key("tick", "XAUUSD"),
                tick.model_copy(update={"origin": origin}).model_dump_json(),
            )
            assert await storage.get_last_tick("XAUUSD") is None
        await storage.set_last_tick(
            tick.model_copy(update={"ts": datetime.now(UTC) + timedelta(days=1)})
        )
        assert await storage.get_last_tick("XAUUSD") is None
        position = _held(state_namespace=foreign.namespace)
        with pytest.raises(ValueError, match="another"):
            await storage.set_open_position(position)
        await storage.client.hset(
            storage.key("cycle", "DELIVERY_PROBE"), "XAUUSD", position.model_dump_json()
        )
        with pytest.raises(ValueError, match="provenance"):
            await storage.get_open_position("DELIVERY_PROBE", "XAUUSD")
    finally:
        await storage.close()


@pytest.mark.parametrize("execution_mode", ["dev", "shadow"])
async def test_paper_book_cannot_be_turned_live(monkeypatch, execution_mode):
    select_scope(monkeypatch, "dev", execution_mode)
    transport = FakeBus()
    broker = BrokerSink(bus=transport, shadow_mode=False)
    broker.set_shadow_mode(False)
    assert (await broker.send(_signal())).status == "shadow"
    assert transport.published == []
    with pytest.raises(SystemExit, match="QTE_STATE__MODE=live"):
        await _set_shadow_mode(False)


async def test_pausing_live_keeps_the_broker_position_and_creates_no_paper_orders(monkeypatch):
    select_scope(monkeypatch, execution_mode="live")
    runner, strategy_slot = _runner(), _slot()
    await _enter(runner, strategy_slot)
    position = strategy_slot.factory.open_position("XAUUSD").model_copy(deep=True)
    runner.state.flags["shadow_mode"] = True
    await _enter(runner, strategy_slot)
    assert len(runner.signals.rows) == len(runner.sink.delivery_ids) == 1
    assert strategy_slot.factory.open_position("XAUUSD") == position
    broker = BrokerSink(bus=FakeBus(), shadow_mode=True)
    assert (await broker.send(_signal())).status == "failed"


async def test_runner_drops_unattributed_and_foreign_bus_prices(monkeypatch):
    select_scope(monkeypatch, "dev", "dev")
    runner, strategy_slot, strategy = build_runner(monkeypatch, CandleStore([]))
    ticks_seen = []
    strategy.on_tick = lambda quoted_price, context: ticks_seen.append(quoted_price)
    await runner.start()
    try:
        for origin in (None, StateScope("dev", "dev", "simulator").origin()):
            candle = bar_at(datetime.now(UTC)).model_copy(update={"origin": origin})
            await runner._feed_candle(strategy_slot, candle)
            strategy_slot.started = True
            tick = Tick(symbol="XAUUSD", ts=datetime.now(UTC), last=100, origin=origin)
            event = TickEvent(symbol="XAUUSD", tick=tick)
            await runner._on_tick_message(SimpleNamespace(data=event.model_dump_json().encode()))
        assert not strategy_slot.buffer
        assert strategy.decided_on == []
        assert ticks_seen == []
        trusted_tick = Tick(
            symbol="XAUUSD", ts=datetime.now(UTC), last=100, origin=settings.state_scope.origin()
        )
        trusted_event = TickEvent(symbol="XAUUSD", tick=trusted_tick)
        await runner._on_tick_message(
            SimpleNamespace(data=trusted_event.model_dump_json().encode())
        )
        assert ticks_seen == [100]
    finally:
        await runner.stop()


async def test_unattributed_and_foreign_history_cannot_warm_the_runner(monkeypatch):
    candle = bar_at(datetime(2026, 1, 1, tzinfo=UTC))
    history = [
        candle.model_copy(update={"origin": origin})
        for origin in (None, StateScope("dev", "dev", "simulator").origin())
    ]
    runner, strategy_slot, _ = build_runner(monkeypatch, CandleStore(history))
    await runner.start()
    try:
        assert not strategy_slot.buffer
        assert not strategy_slot.is_warm
    finally:
        await runner.stop()


async def test_ingestion_attributes_ticks_before_persistence_and_publication(monkeypatch):
    select_scope(monkeypatch)
    monkeypatch.setattr(ingestion_settings, "publish_ticks", True)
    service = object.__new__(IngestionService)
    service._scope = settings.state_scope
    service._origin = service._scope.origin()
    service._resamplers = {}
    service.state = SimpleNamespace(set_last_tick=AsyncMock())
    service.bus = SimpleNamespace(publish=AsyncMock())
    service.subjects = Subjects()
    tick = Tick(symbol="XAUUSD", ts=datetime.now(UTC), last=2000)
    await service._handle_tick_serialized(tick)
    assert tick.origin is None
    assert service.state.set_last_tick.call_args.args[0].origin == service._origin
    assert service.bus.publish.call_args.args[1]["tick"]["origin"] == service._origin.model_dump()
    with pytest.raises(ValueError, match="another"):
        await service._handle_tick_serialized(
            stamp_market_data(tick, StateScope("dev", "dev", "simulator").origin())
        )
    assert service.state.set_last_tick.call_count == service.bus.publish.call_count == 1
