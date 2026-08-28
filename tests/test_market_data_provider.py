"""The market-data seam: engines talk to interfaces, vendors stay replaceable.

The point of `qte_shared.interfaces` is that "which vendor" is a configuration
value rather than an import. These tests hold that line: a fake provider must
be able to serve both engines end to end, and nothing outside
`qte_shared/providers/` may name a vendor.
"""

from __future__ import annotations

import ast
from datetime import date

import pandas as pd
import pytest
from qte_shared.config import REPO_ROOT
from qte_shared.indicators import OHLCV_COLUMNS
from qte_shared.interfaces import (
    Capability,
    HistoryRequest,
    HistorySource,
    LiveFeed,
    MarketDataProvider,
    UnknownProvider,
    UnsupportedCapability,
    normalize_ohlcv,
)
from qte_shared.providers import (
    available_providers,
    create_provider,
    get_provider_class,
    register_provider,
    unregister_provider,
)
from qte_shared.providers.tiingo import TiingoProvider, TiingoSettings
from qte_shared.symbols import build_specs

ROWS = [
    {"date": "2024-01-01T00:00:00Z", "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5},
    {"date": "2024-01-01T00:15:00Z", "open": 1.5, "high": 2.5, "low": 1.0, "close": 2.0},
]


# ── A vendor nobody has heard of ──────────────────────────────────────────


class FakeHistory(HistorySource):
    def __init__(self) -> None:
        self.seen: list[HistoryRequest] = []

    async def fetch(self, request: HistoryRequest) -> pd.DataFrame:
        self.seen.append(request)
        return normalize_ohlcv(ROWS, request.timeframe)


class FakeFeed(LiveFeed):
    def __init__(self, symbols: tuple[str, ...]) -> None:
        self.name = "fake"
        self._symbols = symbols
        self.started = False
        self.stopped = False

    @property
    def symbols(self) -> tuple[str, ...]:
        return self._symbols

    def start(self):
        self.started = True
        return None

    async def stop(self) -> None:
        self.stopped = True


class FakeProvider(MarketDataProvider):
    name = "fake"
    capabilities = frozenset({Capability.HISTORY, Capability.LIVE})

    def ticker_for(self, spec):
        return f"X:{spec.symbol}"

    def history_source(self) -> HistorySource:
        return FakeHistory()

    def live_feeds(self, specs, on_tick):
        return [FakeFeed(tuple(spec.symbol for spec in specs))]


@pytest.fixture
def fake_provider():
    register_provider(FakeProvider)
    yield FakeProvider
    unregister_provider("fake")


# ── Registry ──────────────────────────────────────────────────────────────


def test_tiingo_is_registered_and_is_only_reached_by_name():
    assert "tiingo" in available_providers()
    assert get_provider_class("tiingo") is TiingoProvider


def test_a_third_party_provider_can_join_without_touching_qte(fake_provider):
    provider = create_provider("fake")
    assert isinstance(provider, FakeProvider)
    assert "fake" in available_providers()


def test_unregistering_restores_the_builtin():
    register_provider(FakeProvider, name="tiingo")
    assert get_provider_class("tiingo") is FakeProvider
    unregister_provider("tiingo")
    assert get_provider_class("tiingo") is TiingoProvider


def test_an_unknown_name_fails_with_the_list_of_what_exists():
    with pytest.raises(UnknownProvider, match="tiingo"):
        create_provider("bloomberg")


def test_the_configured_provider_is_the_default(monkeypatch, fake_provider):
    monkeypatch.setattr("qte_shared.config.settings.market_data.provider", "fake")
    assert isinstance(create_provider(), FakeProvider)


def test_a_missing_capability_fails_at_creation_not_mid_run():
    class HistoryOnly(MarketDataProvider):
        name = "history-only"
        capabilities = frozenset({Capability.HISTORY})

    register_provider(HistoryOnly)
    try:
        with pytest.raises(UnsupportedCapability, match="live"):
            create_provider("history-only", capability=Capability.LIVE)
        # ...and the base class refuses the call itself rather than failing later.
        with pytest.raises(UnsupportedCapability):
            HistoryOnly().live_feeds([], None)  # type: ignore[arg-type]
    finally:
        unregister_provider("history-only")


# ── The canonical frame every provider must return ────────────────────────


def test_normalize_ohlcv_produces_the_one_shape_qte_reads():
    frame = normalize_ohlcv(ROWS, "M15")
    assert list(frame.columns) == list(OHLCV_COLUMNS)
    assert frame.index.name == "open_time"
    assert str(frame.index.tz) == "UTC"
    assert frame.index.is_monotonic_increasing
    assert frame.attrs["timeframe"] == "M15"
    assert frame.attrs["timeframe_seconds"] == 900
    # FX carries no volume; the column exists anyway so strategies never branch.
    assert (frame["volume"] == 0.0).all()


def test_a_repeated_bar_is_collapsed_rather_than_double_counted():
    rows = [*ROWS, {**ROWS[0], "close": 9.9}]
    frame = normalize_ohlcv(rows, "M15")
    assert len(frame) == 2
    assert frame["close"].iloc[0] == 9.9, "the later row wins"


def test_no_rows_is_an_empty_frame_not_an_error():
    frame = normalize_ohlcv([], "M15")
    assert frame.empty
    assert list(frame.columns) == list(OHLCV_COLUMNS)


def test_rows_without_a_timestamp_column_are_refused():
    with pytest.raises(ValueError, match="timestamp"):
        normalize_ohlcv([{"open": 1.0}], "M15")


# ── Tiingo, now just one implementation of the contract ───────────────────


def test_tiingo_splits_the_symbols_across_its_two_sockets():
    provider = TiingoProvider(TiingoSettings(api_key="k"))
    feeds = provider.live_feeds(build_specs(["XAUUSD", "BTCUSDT"]), _noop)
    assert [feed.name for feed in feeds] == ["tiingo-fx", "tiingo-crypto"]
    assert feeds[0].symbols == ("XAUUSD",)
    assert feeds[1].symbols == ("BTCUSDT",)


def test_tiingo_makes_no_socket_for_a_market_with_no_symbols():
    provider = TiingoProvider(TiingoSettings(api_key="k"))
    feeds = provider.live_feeds(build_specs(["EURUSD"]), _noop)
    assert [feed.name for feed in feeds] == ["tiingo-fx"]


def test_tiingo_owns_the_ticker_spelling_so_symbolspec_does_not():
    from qte_shared.symbols import SymbolSpec

    spec = SymbolSpec(symbol="XAUUSD", market="fx")
    assert TiingoProvider(TiingoSettings()).ticker_for(spec) == "xauusd"
    assert not hasattr(spec, "tiingo_ticker"), "vendor spelling belongs to the provider"


def test_tiingo_history_needs_its_key_before_it_needs_the_network():
    from qte_shared.interfaces import ProviderNotConfigured

    with pytest.raises(ProviderNotConfigured, match="QTE_TIINGO__API_KEY"):
        TiingoProvider(TiingoSettings(api_key="")).history_source()


def test_tiingo_settings_keep_their_env_names():
    import os

    os.environ["QTE_TIINGO__API_KEY"] = "from-env"
    try:
        assert TiingoSettings().api_key == "from-env"
    finally:
        del os.environ["QTE_TIINGO__API_KEY"]


@pytest.mark.parametrize(
    ("market", "expected_path"),
    [("fx", "/tiingo/fx/xauusd/prices"), ("crypto", "/tiingo/crypto/prices")],
)
def test_tiingo_reaches_a_different_endpoint_per_market(market, expected_path):
    from qte_shared.providers.tiingo.rest import TiingoHistorySource

    source = TiingoHistorySource(TiingoSettings(api_key="k"))
    request = HistoryRequest(
        symbol="XAUUSD" if market == "fx" else "BTCUSDT",
        timeframe="M15",
        start=date(2024, 1, 1),
        end=date(2024, 1, 2),
        market=market,
    )
    normalized = request.normalized()
    url, params = source._endpoint(normalized, normalized.start, normalized.end)
    assert url.endswith(expected_path if market == "fx" else expected_path)
    assert params["resampleFreq"] == "15min"


# ── The seam holds ────────────────────────────────────────────────────────


async def test_the_downloader_writes_whatever_provider_it_is_given(tmp_path, fake_provider):
    """The backtest engine's download path must be vendor-blind."""
    from qte_backtest.downloader import DownloadRequest, HistoryDownloader

    downloader = HistoryDownloader(provider=FakeProvider(), parquet_dir=tmp_path)
    path = await downloader.download(DownloadRequest(symbol="xauusd", timeframe="M15"))

    assert path == tmp_path / "XAUUSD_M15.parquet"
    frame = pd.read_parquet(path)
    assert list(frame.columns) == list(OHLCV_COLUMNS)


def test_no_engine_outside_the_providers_package_imports_a_vendor():
    """A vendor name in engine *code* is the coupling this refactor removed.

    Docstrings may still say "Tiingo" — naming the example that motivated a
    design is not a dependency. An import, a class or an attribute is.
    """
    providers_dir = REPO_ROOT / "engines" / "shared" / "src" / "qte_shared" / "providers"
    offenders: list[str] = []
    for path in (REPO_ROOT / "engines").rglob("*.py"):
        if providers_dir in path.parents or "__pycache__" in path.parts:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            names: list[str] = []
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [node.module or "", *(alias.name for alias in node.names)]
            elif isinstance(node, ast.Name):
                names = [node.id]
            elif isinstance(node, ast.Attribute):
                names = [node.attr]
            if any("tiingo" in name.lower() for name in names):
                offenders.append(f"{path.relative_to(REPO_ROOT)}:{node.lineno}")
    assert not offenders, f"vendor reached outside qte_shared/providers: {offenders}"


def test_only_the_simulator_engine_knows_the_simulator_provider_exists():
    """The one provider an engine may name, and only the engine that serves it.

    `qte-simulator` is the other half of `qte_shared.providers.simulator` — it
    is the server that provider dials — so it imports the shared wire protocol
    on purpose. Every other engine must reach it the way it reaches any vendor:
    through `create_provider`, by a name in configuration.
    """
    allowed = REPO_ROOT / "engines" / "market_simulator"
    providers_dir = REPO_ROOT / "engines" / "shared" / "src" / "qte_shared" / "providers"
    offenders: list[str] = []
    for path in (REPO_ROOT / "engines").rglob("*.py"):
        if providers_dir in path.parents or allowed in path.parents:
            continue
        if "qte_simulator" in path.read_text(encoding="utf-8"):
            offenders.append(str(path.relative_to(REPO_ROOT)))
    assert not offenders, f"the simulator reached outside its own engine: {offenders}"


async def _noop(_tick) -> None:  # pragma: no cover - handler is never invoked here
    return None
