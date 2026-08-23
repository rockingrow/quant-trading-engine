"""The plugin seam: whatever is dropped in the directory gets picked up."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
from qte_shared.plugin_loader import StrategyLoader, load_strategies

STRATEGY_SOURCE = textwrap.dedent(
    """
    from qte_shared.models import SignalAction
    from qte_shared.strategy_base import SignalIntent, StrategyBase


    class MyEdge(StrategyBase):
        name = "MY_EDGE"
        symbols = ("XAUUSD",)
        timeframe = "M15"
        warmup = 10

        def on_candle_closed(self, df, context):
            return None
    """
)


@pytest.fixture
def plugin_dir(tmp_path: Path) -> Path:
    (tmp_path / "my_edge.py").write_text(STRATEGY_SOURCE)
    return tmp_path


def test_a_strategy_file_is_discovered_and_instantiable(plugin_dir):
    found = StrategyLoader(plugin_dir).discover()
    assert [entry.name for entry in found] == ["MY_EDGE"]
    strategy = found[0].instantiate({"risk": 2})
    assert strategy.describe()["params"] == {"risk": 2}
    assert strategy.symbols == ("XAUUSD",)


def test_a_broken_file_is_skipped_not_fatal(plugin_dir):
    # One bad strategy must not stop the others from trading.
    (plugin_dir / "broken.py").write_text("this is not python(")
    found = StrategyLoader(plugin_dir).discover()
    assert [entry.name for entry in found] == ["MY_EDGE"]


def test_private_and_cache_files_are_ignored(plugin_dir):
    (plugin_dir / "_helpers.py").write_text(STRATEGY_SOURCE.replace("MyEdge", "Hidden"))
    assert [entry.name for entry in StrategyLoader(plugin_dir).discover()] == ["MY_EDGE"]


def test_the_abstract_base_is_never_registered_as_a_strategy(plugin_dir):
    (plugin_dir / "reexport.py").write_text("from qte_shared.strategy_base import StrategyBase\n")
    assert [entry.name for entry in StrategyLoader(plugin_dir).discover()] == ["MY_EDGE"]


def test_load_one_finds_by_strategy_name_or_class_name(plugin_dir):
    loader = StrategyLoader(plugin_dir)
    assert loader.load_one("MY_EDGE").name == "MY_EDGE"
    assert loader.load_one("MyEdge").name == "MY_EDGE"


def test_load_one_reports_what_is_available_when_it_misses(plugin_dir):
    with pytest.raises(LookupError, match="MY_EDGE"):
        StrategyLoader(plugin_dir).load_one("NOPE")


def test_the_allow_list_filters_discovery(plugin_dir):
    (plugin_dir / "other.py").write_text(
        STRATEGY_SOURCE.replace("MyEdge", "Other").replace("MY_EDGE", "OTHER")
    )
    assert len(load_strategies(plugin_dir)) == 2
    assert [entry.name for entry in load_strategies(plugin_dir, ["OTHER"])] == ["OTHER"]


def test_a_missing_directory_is_empty_not_an_error(tmp_path):
    assert StrategyLoader(tmp_path / "nope").discover() == []


def test_a_cloned_repos_furniture_is_not_imported(plugin_dir):
    """The directory is a whole repo, not a tidy folder of strategy files."""
    for noise in (".git", ".venv/lib/python3.11/site-packages/pandas", "tests", "docs"):
        directory = plugin_dir / noise
        directory.mkdir(parents=True)
        # Each would register a second strategy if it were scanned.
        (directory / "thing.py").write_text(
            STRATEGY_SOURCE.replace("MyEdge", "Noise").replace("MY_EDGE", "NOISE")
        )

    assert [entry.name for entry in StrategyLoader(plugin_dir).discover()] == ["MY_EDGE"]


def test_strategies_in_ordinary_subpackages_are_still_found(plugin_dir):
    # Excluding repo furniture must not exclude a real folder of strategies.
    nested = plugin_dir / "gold" / "m15"
    nested.mkdir(parents=True)
    (nested / "edge.py").write_text(
        STRATEGY_SOURCE.replace("MyEdge", "GoldEdge").replace("MY_EDGE", "GOLD_EDGE")
    )

    found = sorted(entry.name for entry in StrategyLoader(plugin_dir).discover())
    assert found == ["GOLD_EDGE", "MY_EDGE"]


def test_two_files_of_the_same_name_do_not_shadow_each_other(plugin_dir):
    # Both are `strategy.py`; the module name is namespaced by its subpath.
    for folder in ("alpha", "beta"):
        directory = plugin_dir / folder
        directory.mkdir()
        (directory / "strategy.py").write_text(
            STRATEGY_SOURCE.replace("MyEdge", folder.title()).replace("MY_EDGE", folder.upper())
        )

    found = sorted(entry.name for entry in StrategyLoader(plugin_dir).discover())
    assert found == ["ALPHA", "BETA", "MY_EDGE"]
