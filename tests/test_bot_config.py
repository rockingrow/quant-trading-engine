"""``/mapping`` and ``/provider``: the two configuration files, read back.

Both commands exist to answer "what is this book configured to do", and the
only way to get that wrong dangerously is to collapse two states that differ by
a deploy — a file that is absent against a file that says "nothing". The
mapping table's absence means every strategy keeps the symbols it declares on
itself; a table that maps no pairs means trade nothing. The same distinction
holds for a provider plan. These pin both, and that no credential can reach a
message.
"""

from __future__ import annotations

from pathlib import Path

from qte_bot import presenters
from qte_shared.market_data_plan import MarketDataPlan, SymbolFeed
from qte_shared.strategies.mapping import Pairing, SymbolMapping

NAMESPACE = "dev-shadow-mt5"


def _mapping() -> SymbolMapping:
    return SymbolMapping(
        pairings=(
            Pairing(symbol="XAUUSD", strategy="MT5_GOLD_M15_V2", params={"risk_percent": 0.5}),
            Pairing(symbol="EURUSD", strategy="MT5_FX_M15_V1", params={}),
        ),
        strategy_defaults={"MT5_GOLD_M15_V2": {"risk_percent": 1.0}},
        source=Path("config/strategies_mapping.toml"),
    )


# ── /mapping ──────────────────────────────────────────────────────────────


def test_an_absent_mapping_table_says_strategies_keep_their_own_symbols():
    [body] = presenters.strategy_mapping(SymbolMapping(), NAMESPACE)
    assert "No mapping table is deployed" in body
    assert "declares on itself" in body


def test_a_table_that_maps_nothing_is_not_the_same_as_no_table():
    """Trading nothing is a deliberate state and has to read as one."""
    [body] = presenters.strategy_mapping(
        SymbolMapping(source=Path("config/strategies_mapping.toml")), NAMESPACE
    )
    assert "maps no pairs" in body
    assert "No mapping table is deployed" not in body


def test_every_pair_is_listed_with_the_parameters_it_runs_with():
    [body] = presenters.strategy_mapping(_mapping(), NAMESPACE)
    assert "XAUUSD" in body and "MT5_GOLD_M15_V2" in body
    assert "EURUSD" in body and "MT5_FX_M15_V1" in body
    assert "risk_percent=0.5" in body
    assert "2 pair(s)" in body


def test_the_per_strategy_defaults_are_shown_as_defaults():
    [body] = presenters.strategy_mapping(_mapping(), NAMESPACE)
    assert "Per-strategy defaults" in body
    assert "risk_percent=1.0" in body


def test_a_long_listing_is_split_into_several_messages():
    pairings = tuple(
        Pairing(symbol=f"SYM{index:03d}", strategy="MT5_GOLD_M15_V2")
        for index in range(presenters.CONFIG_ROWS_PER_MESSAGE + 5)
    )
    messages = presenters.strategy_mapping(
        SymbolMapping(pairings=pairings, source=Path("config/strategies_mapping.toml")),
        NAMESPACE,
    )
    assert len(messages) > 1
    assert all(body.strip() for body in messages)
    # The count belongs to the last message, where the reader ends up.
    assert f"{len(pairings)} pair(s)" in messages[-1]


# ── /provider ─────────────────────────────────────────────────────────────


def _plan() -> MarketDataPlan:
    return MarketDataPlan(
        feeds=(
            SymbolFeed(symbol="XAUUSD", market="fx", timeframes=("M5", "M15")),
            SymbolFeed(symbol="BTCUSDT", market="crypto", timeframes=("M15",)),
        ),
        options={"backfill_history": True, "max_rows_per_request": 5000},
        source=Path("config/data_providers/mt5.toml"),
    )


def test_no_configured_provider_names_the_variable_that_configures_one():
    [body] = presenters.data_providers({}, NAMESPACE)
    assert "QTE_MARKET_DATA__PROVIDER" in body


def test_a_provider_with_no_plan_file_falls_back_to_the_environment():
    [body] = presenters.data_providers({"mt5": MarketDataPlan()}, NAMESPACE)
    assert "No plan file is deployed" in body
    assert "QTE_ENGINE__" in body


def test_a_plan_shows_each_symbol_its_market_and_its_timeframes():
    [body] = presenters.data_providers({"mt5": _plan()}, NAMESPACE)
    assert "XAUUSD" in body and "M5, M15" in body
    assert "BTCUSDT" in body
    assert "mt5.toml" in body


def test_the_vendors_own_knobs_are_shown_beside_its_symbols():
    [body] = presenters.data_providers({"mt5": _plan()}, NAMESPACE)
    assert "backfill_history" in body and "max_rows_per_request" in body


def test_an_enabled_plan_with_no_symbol_says_nothing_is_subscribed():
    [body] = presenters.data_providers(
        {"mt5": MarketDataPlan(source=Path("config/data_providers/mt5.toml"))}, NAMESPACE
    )
    assert "no symbol" in body


def test_each_provider_gets_its_own_message_so_its_table_travels_with_it():
    messages = presenters.data_providers({"mt5": _plan(), "tiingo": _plan()}, NAMESPACE)
    assert len(messages) == 2
    assert "mt5" in messages[0] and "tiingo" in messages[1]


def test_a_credential_in_the_plan_cannot_reach_a_message():
    """The parser drops credential-shaped keys; this is the second lock on it.

    A key is dropped as the file is read, so a plan object that reached a
    presenter should never carry one — but a message is the one place a leaked
    secret would be *readable*, so the presenter is asserted on too.
    """
    plan = MarketDataPlan.load(
        Path(__file__).resolve().parents[1] / "config" / "data_providers" / "mt5.example.toml"
    )
    [body] = presenters.data_providers({"mt5": plan}, NAMESPACE)
    for secret in ("api_key", "password", "_token", "secret"):
        assert secret not in body
