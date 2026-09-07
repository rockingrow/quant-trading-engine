"""The market-data plan: what the file says, and what reads it.

The plan replaced three environment variables — ``QTE_ENGINE__SYMBOLS``,
``QTE_ENGINE__TIMEFRAMES`` and ``QTE_INGESTION__MARKET_OVERRIDES`` — with one
file, because only a file can say *per symbol* which market it trades on and
which bars it is resampled to. (``QTE_ENGINE__SIGNAL_TIMEFRAME`` stayed in the
environment: one value for the whole engine, not a per-symbol one.) What is
asserted here is that per-symbol part, the precedence around it (env beats file
beats default), and the two refusals that stop a bad plan reaching a live feed:
a malformed file, and a credential written into a tracked file.
"""

from __future__ import annotations

import pytest

from qte_shared.market_data_plan import MarketDataPlan, SymbolFeed

EXAMPLE = "config/tiingo.example.toml"


def write_plan(tmp_path, body: str):
    path = tmp_path / "vendor.toml"
    path.write_text(body, encoding="utf-8")
    return path


# ── Reading a plan ────────────────────────────────────────────────────────


def test_each_symbol_carries_its_own_market_and_timeframes(tmp_path):
    plan = MarketDataPlan.load(
        write_plan(
            tmp_path,
            """
            [symbols.XAUUSD]
            market = "fx"

            [symbols.BTCUSDT]
            market = "crypto"
            timeframes = ["M1", "M15"]
            """,
        )
    )

    assert plan.symbols == ["XAUUSD", "BTCUSDT"]
    # XAUUSD names no timeframes and falls back to the module default;
    # BTCUSDT states its own.
    assert plan.timeframes_for("XAUUSD") == ["M15"]
    assert plan.timeframes_for("BTCUSDT") == ["M1", "M15"]
    # The union is what a caller with no symbol in hand gets.
    assert plan.timeframes == ["M15", "M1"]
    assert [spec.market for spec in plan.specs] == ["fx", "crypto"]


def test_a_stated_market_beats_the_guess(tmp_path):
    """BTCUSD is a crypto pair on an exchange and an FX CFD on a broker's book."""
    plan = MarketDataPlan.load(
        write_plan(tmp_path, '[symbols.BTCUSD]\nmarket = "fx"\n[symbols.ETHUSDT]\n')
    )
    assert [(feed.symbol, feed.market) for feed in plan.feeds] == [
        ("BTCUSD", "fx"),
        ("ETHUSDT", "crypto"),
    ]


def test_a_disabled_symbol_is_kept_in_the_file_and_out_of_the_feed(tmp_path):
    plan = MarketDataPlan.load(
        write_plan(tmp_path, "[symbols.XAUUSD]\n[symbols.EURUSD]\nenabled = false\n")
    )
    assert plan.symbols == ["XAUUSD"]


def test_no_file_is_not_an_error_but_is_distinguishable(tmp_path):
    """ "Nobody wrote a plan" and "the plan lists nothing" differ by a deploy."""
    missing = MarketDataPlan.load(tmp_path / "nothing.toml")
    assert not missing
    assert missing.symbols == []

    empty = MarketDataPlan.load(write_plan(tmp_path, "[symbols]\n"))
    assert empty
    assert empty.symbols == []


def test_timeframe_labels_are_normalised(tmp_path):
    body = '[symbols.XAUUSD]\ntimeframes = ["15m", "1h"]\n'
    plan = MarketDataPlan.load(write_plan(tmp_path, body))
    assert plan.timeframes_for("XAUUSD") == ["M15", "H1"]


# ── Refusals ──────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "body",
    [
        '[symbols.XAUUSD]\ntimeframes = "M15"\n',  # a string, not a list
        '[symbols.XAUUSD]\ntimeframes = ["M7"]\n',  # not a timeframe
        '[symbols.XAUUSD]\nmarket = "equities"\n',  # not a market QTE quotes
        "symbols = 1\n",  # not a table
    ],
)
def test_a_malformed_plan_is_refused_rather_than_half_read(tmp_path, body):
    """Subscribing to less than the file says must never pass silently."""
    with pytest.raises(ValueError):
        MarketDataPlan.load(write_plan(tmp_path, body))


def test_a_credential_in_the_plan_is_ignored(tmp_path, caplog):
    """The file is tracked in a public repository; the key lives in .env."""
    with caplog.at_level("WARNING"):
        plan = MarketDataPlan.load(
            write_plan(tmp_path, '[provider]\napi_key = "leaked"\nmax_rows_per_request = 10\n')
        )
    assert plan.options == {"max_rows_per_request": 10}
    assert "QTE_DATA_PROVIDER_API_KEY" in caplog.text


# ── The template that ships with the repository ───────────────────────────


def test_the_shipped_template_parses_and_documents_the_schema():
    """`make tiingo` copies this file; a template that cannot load is a trap."""
    plan = MarketDataPlan.load(EXAMPLE)
    assert plan.symbols
    assert plan.timeframes == ["M15"]
    assert plan.option("max_rows_per_request") == 5000
    assert plan.option("backfill_history") is True


def test_the_simulator_template_parses_and_pins_one_symbol():
    """`make simulator` copies this file; the guard now requires it like Tiingo's."""
    plan = MarketDataPlan.load("config/simulator.example.toml")
    assert plan.symbols == ["XAUUSD"]
    assert plan.timeframes == ["M15"]
    # The [provider] table is kept for shape but the simulator has no knobs.
    assert plan.options == {}


def test_a_retired_feed_key_is_warned_about_not_obeyed(tmp_path, caplog):
    """`[feed]` and a top-level `signal_timeframe` used to live in the plan."""
    with caplog.at_level("WARNING"):
        plan = MarketDataPlan.load(
            write_plan(
                tmp_path,
                '[feed]\ntimeframes = ["H1"]\nsignal_timeframe = "H1"\n[symbols.XAUUSD]\n',
            )
        )
    # The stray table is ignored: XAUUSD still falls back to the module default.
    assert plan.timeframes_for("XAUUSD") == ["M15"]
    assert "[feed]" in caplog.text
    assert "QTE_ENGINE__SIGNAL_TIMEFRAME" in caplog.text


# ── What ingestion does with it ───────────────────────────────────────────


def test_ingestion_subscribes_to_the_plan_when_there_is_one(monkeypatch, tmp_path):
    from qte_ingestion import service

    plan = MarketDataPlan.load(
        write_plan(tmp_path, '[symbols.EURUSD]\ntimeframes = ["M5"]\nmarket = "fx"\n')
    )
    monkeypatch.setattr(service, "market_data_plan", lambda: plan)
    assert service.resolve_subscriptions() == [
        SymbolFeed(symbol="EURUSD", market="fx", timeframes=("M5",))
    ]

    # And falls back to the environment when no plan is on disk, which is what
    # a one-off `QTE_ENGINE__SYMBOLS=... make backtest` override rides on.
    monkeypatch.setattr(service, "market_data_plan", MarketDataPlan)
    monkeypatch.setattr(service.settings.engine, "symbols", ["XAUUSD"])
    monkeypatch.setattr(service.settings.engine, "timeframes", ["M15"])
    assert service.resolve_subscriptions() == [
        SymbolFeed(symbol="XAUUSD", market="fx", timeframes=("M15",))
    ]
