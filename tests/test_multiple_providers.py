"""Several market-data providers at once: ``QTE_MARKET_DATA__PROVIDER=mt5,binance``.

One ingestion process can take forex from one vendor and crypto from another.
What is pinned here is what keeps that safe: the list is one state identity,
each symbol belongs to exactly one provider's plan, each bar carries the origin
of the provider that produced it, and a provider that cannot serve fails the
start instead of leaving its symbols silently unfed.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest
from pydantic import ValidationError
from test_history_backfill import FakeSource, FakeState, bars, install_source

from qte_ingestion import service as ingestion_service
from qte_ingestion.backfill import HistoryBackfiller
from qte_ingestion.repair import RoutedBarRepairer
from qte_ingestion.service import IngestionService
from qte_shared import config
from qte_shared.config import EngineSettings, MarketDataSettings, settings
from qte_shared.interfaces import Capability, MarketDataProvider, ProviderError
from qte_shared.market_data_plan import MarketDataPlan, SymbolFeed
from qte_shared.models import Candle
from qte_shared.providers import (
    create_provider,
    get_provider_class,
    register_provider,
    unregister_provider,
)
from qte_shared.state_scope import StateScope, stamp_in_scope

MOMENT = datetime(2026, 9, 30, 8, 0, tzinfo=UTC)


def candle_for(symbol: str, origin=None) -> Candle:
    return Candle(
        origin=origin,
        symbol=symbol,
        timeframe="M15",
        open_time=MOMENT,
        open=1.0,
        high=1.0,
        low=1.0,
        close=1.0,
        is_closed=True,
    )


# ── The provider list ─────────────────────────────────────────────────────


def test_the_list_is_normalised_deduplicated_and_keyed_in_sorted_order():
    market_data = MarketDataSettings(provider=" MT5 , binance,mt5 ", config_file=None)
    assert market_data.providers == ("mt5", "binance")
    assert market_data.provider_key == "binance-mt5"
    assert MarketDataSettings(provider="binance,mt5", config_file=None).provider_key == (
        market_data.provider_key
    ), "the order written must not select another state"


def test_a_single_provider_keeps_its_own_key_and_plan():
    market_data = MarketDataSettings(provider="mt5", config_file=None)
    assert market_data.provider_key == "mt5"
    assert market_data.plan_files["mt5"].name == "mt5.toml"


def test_each_provider_reads_its_own_plan_file():
    market_data = MarketDataSettings(provider="mt5,binance", config_file=None)
    assert {name: path.name for name, path in market_data.plan_files.items()} == {
        "mt5": "mt5.toml",
        "binance": "binance.toml",
    }


@pytest.mark.parametrize("configured", ["", " , ", "mt5,bin-ance"])
def test_an_empty_list_or_a_dashed_name_is_refused(configured):
    with pytest.raises(ValueError):
        _ = MarketDataSettings(provider=configured, config_file=None).providers


def test_one_plan_file_cannot_serve_several_providers():
    with pytest.raises(ValidationError, match="CONFIG_FILE"):
        MarketDataSettings(provider="mt5,binance", config_file=Path("plan.toml"))


def test_the_state_identity_joins_the_providers(monkeypatch):
    monkeypatch.setattr(settings.market_data, "provider", "mt5,binance")
    assert settings.state_scope.namespace == "dev:dev:binance-mt5"
    assert settings.state_scope.providers == ("binance", "mt5")


def test_a_stale_compose_key_is_refused(monkeypatch):
    monkeypatch.setattr(settings.market_data, "provider", "mt5,binance")
    monkeypatch.setattr(settings.state_config, "provider_key", "mt5")
    with pytest.raises(ValueError, match="QTE_STATE__PROVIDER_KEY"):
        _ = settings.state_scope
    monkeypatch.setattr(settings.state_config, "provider_key", "binance-mt5")
    assert settings.state_scope.provider == "binance-mt5"


# ── Provenance inside a shared scope ──────────────────────────────────────


def test_a_shared_scope_accepts_each_listed_provider_and_no_other():
    scope = StateScope("prod", "shadow", "binance-mt5")
    assert scope.accepts(scope.origin(provider="mt5"))
    assert scope.accepts(scope.origin(provider="binance"))
    assert not scope.accepts(StateScope("prod", "shadow", "tiingo").origin())
    with pytest.raises(ValueError, match="not part of"):
        scope.origin(provider="tiingo")
    with pytest.raises(ValueError, match="name the producing one"):
        scope.origin()


def test_the_simulator_never_shares_a_scope():
    with pytest.raises(ValueError, match="simulator"):
        StateScope("dev", "dev", "mt5-simulator")


def test_redis_keeps_the_producing_provider_and_refuses_an_unattributed_record():
    scope = StateScope("prod", "shadow", "binance-mt5")
    stamped = stamp_in_scope(candle_for("BTCUSDT", scope.origin(provider="binance")), scope)
    assert stamped.origin.provider == "binance"
    with pytest.raises(ValueError, match="name the producing one"):
        stamp_in_scope(candle_for("BTCUSDT"), scope)
    with pytest.raises(ValueError, match="another"):
        stamp_in_scope(candle_for("XAUUSD", StateScope("prod", "shadow", "mt5").origin()), scope)


# ── Plans ─────────────────────────────────────────────────────────────────


def plan_of(*feeds: SymbolFeed, options=None) -> MarketDataPlan:
    return MarketDataPlan(
        feeds=feeds, options=options or {}, source=Path("p.toml"), sources=(Path("p.toml"),)
    )


def test_merged_plans_tag_every_symbol_with_its_provider():
    merged = MarketDataPlan.merge(
        {
            "mt5": plan_of(SymbolFeed("XAUUSD", "fx", ("M15",)), options={"stream": "X"}),
            "binance": plan_of(SymbolFeed("BTCUSDT", "crypto", ("M5",))),
        }
    )
    assert [(feed.symbol, feed.provider) for feed in merged.feeds] == [
        ("XAUUSD", "mt5"),
        ("BTCUSDT", "binance"),
    ]
    assert merged.options == {}, "each vendor reads its own [provider], never a merged one"


def test_a_single_plan_keeps_its_provider_table():
    merged = MarketDataPlan.merge({"mt5": plan_of(options={"stream": "X"})})
    assert merged.options == {"stream": "X"}


def test_a_symbol_planned_by_two_providers_is_refused():
    with pytest.raises(ValueError, match="one symbol must have one provider"):
        MarketDataPlan.merge(
            {
                "mt5": plan_of(SymbolFeed("BTCUSD", "crypto", ("M15",))),
                "binance": plan_of(SymbolFeed("BTCUSD", "crypto", ("M15",))),
            }
        )


def test_a_symbol_on_no_plan_needs_a_single_provider(monkeypatch):
    monkeypatch.setattr(settings.market_data, "provider", "mt5,binance")
    with pytest.raises(ValueError, match="on no provider's plan"):
        settings.engine.resolve_subscriptions(MarketDataPlan(), {"XAUUSD": "fx"})


# ── Registry ──────────────────────────────────────────────────────────────


def test_the_default_provider_is_ambiguous_with_several(monkeypatch):
    monkeypatch.setattr(settings.market_data, "provider", "mt5,binance")
    with pytest.raises(ProviderError, match="name the one to create"):
        create_provider()


def test_binance_is_a_registered_placeholder_that_refuses_to_start():
    assert get_provider_class("binance").markets == ("crypto",)
    with pytest.raises(ProviderError, match="not implemented"):
        create_provider("binance")


# ── Ingestion ─────────────────────────────────────────────────────────────


class BarVendor(MarketDataProvider):
    name = "alpha"
    capabilities = frozenset({Capability.LIVE_BARS})

    def __init__(self) -> None:
        self.asked: list[list[SymbolFeed]] = []

    def bar_feeds(self, subscriptions, on_bar):
        self.asked.append(list(subscriptions))
        return ["alpha-feed"]


class TickVendor(MarketDataProvider):
    name = "beta"
    capabilities = frozenset({Capability.LIVE})

    def __init__(self) -> None:
        self.asked: list[list[str]] = []

    def live_feeds(self, specs, on_tick):
        self.asked.append([spec.symbol for spec in specs])
        return ["beta-feed"]


@pytest.fixture
def two_vendors(monkeypatch):
    register_provider(BarVendor)
    register_provider(TickVendor)
    monkeypatch.setattr(settings.market_data, "provider", "beta,alpha")
    plan = MarketDataPlan.merge(
        {
            "alpha": plan_of(SymbolFeed("XAUUSD", "fx", ("M15",))),
            "beta": plan_of(SymbolFeed("BTCUSDT", "crypto", ("M15",))),
        }
    )
    monkeypatch.setattr(ingestion_service, "market_data_plan", lambda: plan)
    monkeypatch.setattr(config, "market_data_plan", lambda: plan)
    # A fresh block: an earlier test's patched symbols stay in model_fields_set
    # and would read as an explicit override of the plan.
    monkeypatch.setattr(settings, "engine", EngineSettings())
    try:
        yield
    finally:
        unregister_provider("alpha")
        unregister_provider("beta")


def test_each_provider_feeds_only_its_own_symbols(two_vendors):
    service = IngestionService()
    assert sorted(service.providers) == ["alpha", "beta"]

    opened = service._open_feeds()

    assert sorted(opened) == [("alpha", "alpha-feed"), ("beta", "beta-feed")]
    assert [feed.symbol for feed in service.providers["alpha"].asked[0]] == ["XAUUSD"]
    assert service.providers["beta"].asked == [["BTCUSDT"]]


def test_each_bar_carries_the_origin_of_the_provider_that_fed_it(two_vendors):
    service = IngestionService()
    assert service._origin_for("XAUUSD").provider == "alpha"
    assert service._origin_for("BTCUSDT").provider == "beta"
    assert {origin.namespace for origin in service._origins.values()} == {"dev:dev:alpha-beta"}
    with pytest.raises(ValueError, match="No configured provider feeds"):
        service._origin_for("EURUSD")


async def test_a_provider_that_starts_no_feed_fails_the_start(two_vendors, monkeypatch):
    service = IngestionService()
    service.providers["beta"].live_feeds = lambda specs, on_tick: []
    for resource in (service.bus, service.state):
        monkeypatch.setattr(resource, "connect", _no_op)
        monkeypatch.setattr(resource, "close", _no_op)
    monkeypatch.setattr(ingestion_service, "discard_foreign_candle_state", _guard_passes)
    monkeypatch.setattr(service, "_drain_candle_outbox", _no_op)
    monkeypatch.setattr(service, "_restore_open_candles", _no_op)
    monkeypatch.setattr(service, "_build_repairer", lambda: None)
    monkeypatch.setattr(ingestion_service, "HistoryBackfiller", _SilentBackfiller)

    class StartedFeed:
        def start(self):
            return object()

        async def stop(self):
            return None

    service.providers["alpha"].bar_feeds = lambda subscriptions, on_bar: [StartedFeed()]

    with pytest.raises(RuntimeError, match="'beta' started no feeds"):
        await service.start()


async def _no_op(*arguments, **keywords) -> None:
    return None


async def _guard_passes(*arguments, **keywords) -> bool:
    return False


class _SilentBackfiller:
    def __init__(self, *arguments, **keywords) -> None:
        pass

    async def run(self) -> None:
        return None


async def test_partial_bars_are_repaired_by_their_own_provider():
    class Recorder:
        def __init__(self, label: str) -> None:
            self.label = label
            self.repaired: list[str] = []

        async def repair(self, candle, *, close_is_current=True):
            self.repaired.append(candle.symbol)
            return candle

    alpha, beta = Recorder("alpha"), Recorder("beta")
    routed = RoutedBarRepairer({"XAUUSD": alpha, "BTCUSDT": beta})

    for symbol in ("XAUUSD", "BTCUSDT", "EURUSD"):
        await routed.repair(candle_for(symbol))

    assert alpha.repaired == ["XAUUSD"]
    assert beta.repaired == ["BTCUSDT"], "an unrouted symbol is published as built"


async def test_backfilled_history_is_stamped_with_its_provider(monkeypatch):
    monkeypatch.setattr(settings.market_data, "provider", "mt5,binance")
    source = FakeSource(bars(3, start=MOMENT - timedelta(hours=2)))
    install_source(monkeypatch, source)
    fake_state = FakeState()
    backfiller = HistoryBackfiller(
        fake_state,
        [SymbolFeed("BTCUSDT", "crypto", ("M15",), provider="binance")],
        cache=_NoCache(),
        utc_clock=lambda: MOMENT,
        provider_name="binance",
    )

    await backfiller.run()

    assert fake_state.written, "the fetched bars were written"
    assert {candle.origin.provider for candle in fake_state.written} == {"binance"}


def test_a_backfiller_must_be_told_its_provider_when_there_are_several(monkeypatch):
    monkeypatch.setattr(settings.market_data, "provider", "mt5,binance")
    with pytest.raises(ValueError, match="name the one whose history"):
        HistoryBackfiller(FakeState(), [])


class _NoCache:
    def load(self, symbol, timeframe, start=None, end=None):
        return pd.DataFrame()

    def store(self, frame, symbol, timeframe):
        return None
