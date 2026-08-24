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


def test_a_dunder_directory_name_does_not_hide_its_own_contents(tmp_path):
    """The default directory is `__strategies__`, and `_`-prefixed names are skipped.

    Those two rules meet at the root: if the exclusion looked at the directory's
    own name rather than the paths beneath it, the engine would load nothing and
    say only "no strategies found".
    """
    root = tmp_path / "__strategies__"
    (root / "gold").mkdir(parents=True)
    (root / "gold" / "edge.py").write_text(STRATEGY_SOURCE)

    assert [entry.name for entry in StrategyLoader(root).discover()] == ["MY_EDGE"]


def test_underscore_files_inside_a_dunder_directory_are_still_skipped(tmp_path):
    root = tmp_path / "__strategies__"
    root.mkdir()
    (root / "edge.py").write_text(STRATEGY_SOURCE)
    (root / "_helpers.py").write_text(
        STRATEGY_SOURCE.replace("MyEdge", "Helper").replace("MY_EDGE", "HELPER")
    )

    assert [entry.name for entry in StrategyLoader(root).discover()] == ["MY_EDGE"]


def test_the_module_namespace_matches_the_directory_convention(tmp_path):
    # Modules are registered under a `__strategies__.` prefix so a plugin named
    # utils.py cannot shadow anything real in sys.modules.
    import sys

    root = tmp_path / "__strategies__"
    (root / "gold").mkdir(parents=True)
    (root / "gold" / "edge.py").write_text(STRATEGY_SOURCE)

    StrategyLoader(root).discover()
    assert "__strategies__.gold.edge" in sys.modules


# ── Manifests: a plugin repo that declares what it publishes ─────────────

#: A plugin repository that never imports qte_shared. It restates the contract
#: on its own side — its own base class, its own intent type, its own action
#: enum — which is exactly what a repo with its own lockfile and CI has to do.
STANDALONE_REPO = {
    "src/edges/contract.py": """
        from abc import ABC, abstractmethod
        from dataclasses import dataclass, field
        from enum import Enum


        class Action(str, Enum):
            LONG = "LONG"
            FLAT = "FLAT"


        @dataclass
        class Intent:
            action: Action
            symbol: str | None = None
            price: float | None = None
            quantity: float | None = None
            sl: float | None = None
            reason: str = ""
            indicators: dict = field(default_factory=dict)


        class Base(ABC):
            name = ""
            symbols = ()
            timeframe = "M15"
            warmup = 10
            max_history = None

            def __init__(self, params=None):
                self.params = dict(params or {})

            def on_start(self, context): pass
            def on_stop(self): pass

            @abstractmethod
            def on_candle_closed(self, df, context): ...

            def on_tick(self, price, context): return None
            def history_window(self): return self.max_history or 400
            def describe(self): return {"name": self.name, "params": self.params}
    """,
    "src/edges/gold.py": """
        from edges.contract import Action, Base, Intent


        class GoldEdge(Base):
            name = "GOLD_EDGE_V1"
            symbols = ("XAUUSD",)

            def on_candle_closed(self, df, context):
                return Intent(action=Action.LONG, price=2000.0, quantity=1.0, sl=1990.0)
    """,
    "strategies.py": """
        import sys
        from pathlib import Path

        sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

        from edges.gold import GoldEdge

        ALIASES = {"GOLD_EDGE_V1": GoldEdge}


        def load_all():
            return dict(ALIASES)
    """,
}


def write_repo(root: Path, files: dict[str, str]) -> Path:
    for relative, source in files.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(source).lstrip(), encoding="utf-8")
    return root


@pytest.fixture
def manifest_repo(tmp_path: Path) -> Path:
    """A cloned plugin repo one level below the strategies directory."""
    write_repo(tmp_path / "my-strategies", STANDALONE_REPO)
    return tmp_path


def test_a_repo_that_never_imports_the_engine_is_still_loaded(manifest_repo):
    """The whole point of the manifest: no qte_shared on the plugin's side.

    A strategy repository has its own lockfile, its own CI and its own release
    cycle. Requiring it to subclass our base would mean it could not run its
    own test suite without this repo checked out beside it.
    """
    found = StrategyLoader(manifest_repo).discover()

    assert [entry.name for entry in found] == ["GOLD_EDGE_V1"]
    assert found[0].source.name == "strategies.py"
    strategy = found[0].instantiate({"risk": 3})
    assert strategy.describe()["params"] == {"risk": 3}


def test_the_alias_comes_from_the_manifest_not_the_class(manifest_repo):
    """The repo decides what a strategy is published as; we do not guess it."""
    (manifest_repo / "my-strategies" / "strategies.py").write_text(
        (manifest_repo / "my-strategies" / "strategies.py")
        .read_text(encoding="utf-8")
        .replace('"GOLD_EDGE_V1": GoldEdge', '"RENAMED_ON_THE_WIRE": GoldEdge'),
        encoding="utf-8",
    )
    found = StrategyLoader(manifest_repo).discover()
    assert [entry.name for entry in found] == ["RENAMED_ON_THE_WIRE"]


def test_a_manifest_repo_is_not_also_scanned(manifest_repo):
    """Scanning it too would register every class twice and import files the
    manifest deliberately left out."""
    found = StrategyLoader(manifest_repo).discover()
    assert len(found) == 1


def test_a_manifest_at_the_root_of_the_directory_works_too(tmp_path):
    """`__strategies__/` may *be* the checkout rather than contain it."""
    write_repo(tmp_path, STANDALONE_REPO)
    assert [entry.name for entry in StrategyLoader(tmp_path).discover()] == ["GOLD_EDGE_V1"]


def test_loose_files_still_work_beside_a_manifest_repo(manifest_repo):
    """Copying one example file in has to keep working — no ceremony required."""
    (manifest_repo / "my_edge.py").write_text(STRATEGY_SOURCE)

    found = sorted(entry.name for entry in StrategyLoader(manifest_repo).discover())
    assert found == ["GOLD_EDGE_V1", "MY_EDGE"]


def test_a_manifest_without_the_hook_is_reported_and_skipped(manifest_repo, caplog):
    (manifest_repo / "my-strategies" / "strategies.py").write_text(
        "ALIASES = {}\n", encoding="utf-8"
    )
    with caplog.at_level("ERROR"):
        assert StrategyLoader(manifest_repo).discover() == []
    assert "load_all" in caplog.text


def test_a_manifest_that_raises_does_not_take_the_runner_down(manifest_repo, caplog):
    (manifest_repo / "my-strategies" / "strategies.py").write_text(
        "def load_all():\n    raise RuntimeError('boom')\n", encoding="utf-8"
    )
    (manifest_repo / "my_edge.py").write_text(STRATEGY_SOURCE)

    with caplog.at_level("ERROR"):
        found = StrategyLoader(manifest_repo).discover()

    assert [entry.name for entry in found] == ["MY_EDGE"], "the other strategy still loads"
    assert "boom" in caplog.text


def test_a_manifest_publishing_a_non_strategy_is_refused(manifest_repo, caplog):
    (manifest_repo / "my-strategies" / "strategies.py").write_text(
        "def load_all():\n    return {'NOT_A_STRATEGY': dict}\n", encoding="utf-8"
    )
    with caplog.at_level("ERROR"):
        assert StrategyLoader(manifest_repo).discover() == []
    assert "NOT_A_STRATEGY" in caplog.text


def test_a_manifest_repos_own_base_class_is_not_registered(manifest_repo):
    """It declares on_candle_closed abstract exactly as ours does, so it never
    looks concrete — and it is not in the manifest anyway."""
    found = StrategyLoader(manifest_repo).discover()
    assert [entry.cls.__name__ for entry in found] == ["GoldEdge"]


def test_the_manifest_may_be_called_manifest_py(manifest_repo):
    """``strategies.py`` and ``manifest.py`` are the same doorway.

    The file answers to two readings — what it contains, and what it is — and a
    plugin repo that picked the other name should not silently fall through to
    the directory scan, which would bypass its alias table and import every
    module in the tree.
    """
    repo = manifest_repo / "my-strategies"
    (repo / "strategies.py").rename(repo / "manifest.py")

    found = StrategyLoader(manifest_repo).discover()

    assert [entry.name for entry in found] == ["GOLD_EDGE_V1"]
    assert found[0].source.name == "manifest.py"


def test_a_repo_declaring_two_manifests_is_refused(manifest_repo):
    """Which alias table is deployed must not depend on the lookup order."""
    repo = manifest_repo / "my-strategies"
    (repo / "manifest.py").write_text(
        (repo / "strategies.py").read_text(encoding="utf-8"), encoding="utf-8"
    )

    with pytest.raises(RuntimeError, match="more than one strategy manifest"):
        StrategyLoader(manifest_repo).discover()
