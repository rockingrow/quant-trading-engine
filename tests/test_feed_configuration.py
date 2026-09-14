"""Environment overrides and tick requirements agree across feed consumers."""

import argparse

import pytest
from test_runner_catch_up import CandleStore, build_runner

from qte_backtest import __main__ as backtest_cli
from qte_ingestion import service
from qte_ingestion.settings import IngestionSettings
from qte_shared import config
from qte_shared.config import EngineSettings, MarketStreamSettings, settings
from qte_shared.market_data_plan import MarketDataPlan, SymbolFeed
from qte_strategy_engine.settings import runner_settings


@pytest.fixture
def market_plan(tmp_path, monkeypatch):
    plan_path = tmp_path / "provider.toml"
    plan_path.write_text(
        '[symbols.XAUUSD]\nmarket = "fx"\ntimeframes = ["M15"]\n'
        '[symbols.BTCUSD]\nmarket = "crypto"\ntimeframes = ["M5", "M15"]\n',
        encoding="utf-8",
    )
    resolved_plan = MarketDataPlan.load(plan_path)
    monkeypatch.setattr(config, "market_data_plan", lambda: resolved_plan)
    monkeypatch.setattr(service, "market_data_plan", lambda: resolved_plan)
    monkeypatch.setattr(backtest_cli, "market_data_plan", lambda: resolved_plan)
    monkeypatch.setattr(settings, "engine", EngineSettings())
    monkeypatch.setattr(settings, "market_stream", MarketStreamSettings())
    monkeypatch.setattr(service, "ingestion_settings", IngestionSettings())
    return resolved_plan


@pytest.mark.parametrize(
    "symbol_override,timeframe_override,expected",
    [
        (None, None, [("XAUUSD", ("M15",)), ("BTCUSD", ("M5", "M15"))]),
        ('["btcusd"]', None, [("BTCUSD", ("M5", "M15"))]),
        (None, '["5m"]', [("XAUUSD", ("M5",)), ("BTCUSD", ("M5",))]),
        ('["EURUSD"]', '["5m"]', [("EURUSD", ("M5",))]),
        ('["EURUSD"]', None, [("EURUSD", ("M15", "M5"))]),
        ("[]", None, []),
    ],
)
def test_environment_overrides_resolve_identically_for_ingestion_and_download(
    monkeypatch, market_plan, symbol_override, timeframe_override, expected
):
    if symbol_override is not None:
        monkeypatch.setenv("QTE_ENGINE__SYMBOLS", symbol_override)
    if timeframe_override is not None:
        monkeypatch.setenv("QTE_ENGINE__TIMEFRAMES", timeframe_override)
        monkeypatch.setenv("QTE_ENGINE__SIGNAL_TIMEFRAME", "M5")
    monkeypatch.setenv("QTE_INGESTION__MARKET_OVERRIDES", '{"EURUSD":"fx"}')
    monkeypatch.setattr(settings, "engine", EngineSettings())
    monkeypatch.setattr(settings, "market_stream", MarketStreamSettings())
    monkeypatch.setattr(service, "ingestion_settings", IngestionSettings())
    subscriptions = service.resolve_subscriptions()
    assert [
        (symbol_feed.symbol, symbol_feed.timeframes) for symbol_feed in subscriptions
    ] == expected
    targets = backtest_cli._download_targets(
        argparse.Namespace(symbol=None, timeframe=None, market=None)
    )
    assert targets == [
        (symbol_feed.symbol, timeframe, symbol_feed.market)
        for symbol_feed in subscriptions
        for timeframe in symbol_feed.timeframes
    ]


def test_an_unplanned_symbol_requires_an_explicit_market(monkeypatch, market_plan):
    monkeypatch.setenv("QTE_ENGINE__SYMBOLS", '["EURUSD"]')
    monkeypatch.setattr(settings, "engine", EngineSettings())
    with pytest.raises(ValueError, match="No market for"):
        service.resolve_subscriptions()


def test_planned_market_is_preserved_under_symbol_override(monkeypatch, market_plan):
    monkeypatch.setenv("QTE_ENGINE__SYMBOLS", '["BTCUSD"]')
    monkeypatch.setattr(settings, "engine", EngineSettings())
    monkeypatch.setattr(service.ingestion_settings, "market_overrides", {"BTCUSD": "fx"})
    assert service.resolve_subscriptions() == [
        SymbolFeed(symbol="BTCUSD", market="crypto", timeframes=("M5", "M15"))
    ]


def test_empty_plan_never_falls_back_to_default_symbols(monkeypatch, tmp_path):
    plan_path = tmp_path / "empty.toml"
    plan_path.write_text("[symbols.XAUUSD]\nenabled = false\n", encoding="utf-8")
    empty_plan = MarketDataPlan.load(plan_path)
    monkeypatch.setattr(config, "market_data_plan", lambda: empty_plan)
    monkeypatch.setattr(service, "market_data_plan", lambda: empty_plan)
    monkeypatch.setattr(backtest_cli, "market_data_plan", lambda: empty_plan)
    monkeypatch.setattr(settings, "engine", EngineSettings())
    assert settings.engine.symbols == settings.engine.timeframes == []
    assert service.resolve_subscriptions() == []
    with pytest.raises(ValueError, match="no subscriptions"):
        service.IngestionService()
    assert (
        backtest_cli._download_targets(argparse.Namespace(symbol=None, timeframe=None, market=None))
        == []
    )


def test_missing_plan_still_uses_explicit_fallback_markets(monkeypatch):
    monkeypatch.setattr(config, "market_data_plan", MarketDataPlan)
    engine_config = EngineSettings()
    assert engine_config.symbols == ["XAUUSD"]
    assert engine_config.resolve_subscriptions(MarketDataPlan(), {"XAUUSD": "fx"}) == [
        SymbolFeed(symbol="XAUUSD", market="fx", timeframes=("M15",))
    ]


@pytest.mark.parametrize("publication_enabled", [False, True])
@pytest.mark.parametrize("explicit_subscription", [False, True])
async def test_tick_requirement_is_checked_before_restoring_or_recovering_orders(
    monkeypatch, publication_enabled, explicit_subscription
):
    monkeypatch.setenv("QTE_INGESTION__PUBLISH_TICKS", str(publication_enabled).lower())
    monkeypatch.setattr(settings, "market_stream", MarketStreamSettings())
    assert IngestionSettings().publish_ticks is publication_enabled
    monkeypatch.setattr(runner_settings, "subscribe_ticks", explicit_subscription)
    runner, strategy_slot, strategy = build_runner(monkeypatch, CandleStore([]))
    if not explicit_subscription:
        monkeypatch.setattr(type(strategy), "on_tick", lambda self, price, context: None)
    restored = []

    async def restore_state():
        restored.append(True)

    monkeypatch.setattr(runner, "_restore_state", restore_state)
    if not publication_enabled:
        with pytest.raises(RuntimeError, match="QTE_INGESTION__PUBLISH_TICKS"):
            await runner.start()
        assert restored == []
        assert not runner._ownership_acquired
    else:
        await runner.start()
        try:
            assert restored == [True]
            assert runner.subjects.tick_wildcard() in runner.bus.handlers
            assert not strategy_slot.is_warm
        finally:
            await runner.stop()


async def test_candle_only_book_starts_without_tick_publication(monkeypatch):
    monkeypatch.setattr(settings, "market_stream", MarketStreamSettings(publish_ticks=False))
    monkeypatch.setattr(runner_settings, "subscribe_ticks", False)
    runner, _, _ = build_runner(monkeypatch, CandleStore([]))
    await runner.start()
    try:
        assert runner.subjects.tick_wildcard() not in runner.bus.handlers
    finally:
        await runner.stop()
