"""The symbol → strategies table: what it pairs, and what it refuses.

The table is git-ignored, so nothing about it is reviewable in this repo's
history except these tests and `config/strategies_mapping.example.toml`. That makes the
parser's *rejections* the interesting half: a table that half-parses trades a
book nobody wrote.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
from qte_shared.routing import SymbolRouting

TABLE = """
    [symbols.XAUUSD]
    strategies = ["GOLD_M15", "GOLD_SCALP"]

    [symbols.XAUUSD.params.GOLD_M15]
    risk_percent = 1.0

    [symbols.BTCUSDT]
    strategies = ["GOLD_M15"]
"""


def write(tmp_path: Path, body: str, name: str = "strategies_mapping.toml") -> Path:
    path = tmp_path / name
    path.write_text(textwrap.dedent(body).lstrip(), encoding="utf-8")
    return path


@pytest.fixture
def routing(tmp_path: Path) -> SymbolRouting:
    return SymbolRouting.load(write(tmp_path, TABLE))


# ── Reading it ───────────────────────────────────────────────────────────


def test_a_symbol_can_run_several_strategies(routing):
    assert routing.strategies_for("XAUUSD") == ["GOLD_M15", "GOLD_SCALP"]


def test_a_strategy_can_run_on_several_symbols(routing):
    """The runner asks this way round: it loops over what the loader found."""
    assert routing.symbols_for("GOLD_M15") == ["XAUUSD", "BTCUSDT"]
    assert routing.symbols_for("GOLD_SCALP") == ["XAUUSD"]


def test_symbols_are_upper_cased_on_the_way_in(tmp_path):
    routing = SymbolRouting.load(write(tmp_path, '[symbols.xauusd]\nstrategies = ["A"]\n'))
    assert routing.symbols == ["XAUUSD"]
    assert routing.strategies_for("xauusd") == ["A"]


def test_params_are_per_pair_not_per_strategy(routing):
    """One strategy running tighter on gold than on bitcoin is the point."""
    assert routing.params_for("XAUUSD", "GOLD_M15") == {"risk_percent": 1.0}
    assert routing.params_for("BTCUSDT", "GOLD_M15") == {}


def test_params_for_a_pair_that_is_not_routed_is_empty(routing):
    assert routing.params_for("EURUSD", "GOLD_M15") == {}


def test_an_unknown_strategy_routes_to_nothing_rather_than_raising(routing):
    """The runner reports this itself; the parser is not where it is decided."""
    assert routing.symbols_for("NOT_DEPLOYED") == []


# ── Absence, and switching things off ────────────────────────────────────


def test_no_file_is_an_empty_table_not_an_error(tmp_path):
    """A fresh clone has none — the real one never reaches git.

    Empty is falsy, which is what the runner tests to decide whether to fall
    back to each strategy's own ``symbols`` attribute.
    """
    routing = SymbolRouting.load(tmp_path / "absent.toml")
    assert not routing
    assert routing.routes == () and routing.source is None


def test_a_disabled_symbol_keeps_its_configuration_but_trades_nothing(tmp_path):
    routing = SymbolRouting.load(
        write(
            tmp_path,
            """
            [symbols.EURUSD]
            enabled = false
            strategies = ["FX_M15"]
            """,
        )
    )
    assert routing.strategies_for("EURUSD") == []
    assert routing, "a table that routes nothing is still a table — trade nothing, not the fallback"


def test_defaults_apply_only_where_a_symbol_named_nothing(tmp_path):
    routing = SymbolRouting.load(
        write(
            tmp_path,
            """
            [defaults]
            strategies = ["HOUSE_EDGE"]

            [symbols.XAUUSD]
            strategies = ["GOLD_M15"]

            [symbols.BTCUSDT]
            """,
        )
    )
    assert routing.strategies_for("XAUUSD") == ["GOLD_M15"]
    assert routing.strategies_for("BTCUSDT") == ["HOUSE_EDGE"]


# ── Refusing a table that would trade the wrong book ─────────────────────


def test_the_same_pair_twice_is_refused(tmp_path):
    """Two slots for one pair run the same strategy against itself."""
    with pytest.raises(ValueError, match="twice"):
        SymbolRouting.load(write(tmp_path, '[symbols.XAUUSD]\nstrategies = ["A", "A"]\n'))


def test_a_bare_string_says_to_write_a_list(tmp_path):
    """``strategies = "GOLD_M15"`` would otherwise iterate as ten characters."""
    with pytest.raises(ValueError, match=r'strategies = \["GOLD_M15"\]'):
        SymbolRouting.load(write(tmp_path, '[symbols.XAUUSD]\nstrategies = "GOLD_M15"\n'))


def test_a_non_table_symbol_entry_is_refused(tmp_path):
    with pytest.raises(ValueError, match="must be a table"):
        SymbolRouting.load(write(tmp_path, "[symbols]\nXAUUSD = 1\n"))


def test_params_that_are_not_keyed_by_strategy_are_refused(tmp_path):
    with pytest.raises(ValueError, match="params"):
        SymbolRouting.load(
            write(
                tmp_path,
                """
                [symbols.XAUUSD]
                strategies = ["A"]
                params = 1
                """,
            )
        )


def test_malformed_toml_reaches_the_caller(tmp_path):
    """Silently treating it as "no file" would fall back to trading something."""
    import tomllib

    with pytest.raises(tomllib.TOMLDecodeError):
        SymbolRouting.load(write(tmp_path, "[symbols.XAUUSD\n"))


# ── The template is the schema ───────────────────────────────────────────


def test_the_committed_template_parses():
    """It is the only version of this file anyone can review, so it must be valid."""
    from qte_shared.config import REPO_ROOT

    routing = SymbolRouting.load(REPO_ROOT / "config" / "strategies_mapping.example.toml")

    assert routing, "the template should demonstrate at least one pairing"
    assert "XAUUSD" in routing.symbols
    assert "EURUSD" not in routing.symbols, "the template's disabled symbol stays disabled"


def test_the_real_table_is_not_committed():
    """The template is tracked; the book is not. See .gitignore."""
    from qte_shared.config import REPO_ROOT

    ignore = (REPO_ROOT / ".gitignore").read_text(encoding="utf-8")
    assert "/config/strategies_mapping.toml" in ignore
    assert "!/config/strategies_mapping.example.toml" in ignore
