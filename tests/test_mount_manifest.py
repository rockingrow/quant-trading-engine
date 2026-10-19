"""The mount record: what `make strategy-mount` writes and who reads it back.

The loader's side of this — which strategies actually get imported — lives in
``test_plugin_loader.py``. Here it is the file itself: its shape, what the two
consumers ask it, and the CLI that writes it.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from qte_shared.strategies.mount_manifest import (
    MOUNT_MANIFEST_FILENAME,
    MountManifest,
)
from qte_strategy_audit.mount import main, record_repository

REPO_FILES = {
    "src/edges/contract.py": """
        from abc import ABC, abstractmethod
        from dataclasses import dataclass
        from enum import Enum


        class Action(str, Enum):
            LONG = "long"
            SHORT = "short"
            FLAT = "flat"


        @dataclass
        class Intent:
            action: Action
            price: float
            quantity: float
            sl: float | None = None


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

            # The five the audit insists on. Names fixed by the broker
            # contract, which is why they are shorter than anything else here.
            def long(self, *args, **kwargs): return None
            def short(self, *args, **kwargs): return None
            def tp1(self, *args, **kwargs): return None
            def tp2(self, *args, **kwargs): return None
            def sl(self, *args, **kwargs): return None
    """,
    "src/edges/gold.py": """
        from edges.contract import Action, Base, Intent


        class GoldEdge(Base):
            name = "GOLD_EDGE_V1"
            symbols = ("XAUUSD",)

            def on_candle_closed(self, df, context):
                return Intent(action=Action.LONG, price=2000.0, quantity=1.0, sl=1990.0)


        class SilverEdge(Base):
            name = "SILVER_EDGE_V1"
            symbols = ("XAGUSD",)

            def on_candle_closed(self, df, context):
                return Intent(action=Action.LONG, price=25.0, quantity=1.0, sl=24.0)
    """,
    "strategies.py": """
        import sys
        from pathlib import Path

        sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

        from edges.gold import GoldEdge, SilverEdge


        def load_all():
            return {"GOLD_EDGE_V1": GoldEdge, "SILVER_EDGE_V1": SilverEdge}
    """,
}


@pytest.fixture
def mounted(tmp_path: Path) -> Path:
    """A strategies directory holding one repo that publishes two strategies."""
    for relative, source in REPO_FILES.items():
        path = tmp_path / "my-strategies" / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(source).lstrip(), encoding="utf-8")
    return tmp_path


# ── The file ─────────────────────────────────────────────────────────────


def test_a_missing_manifest_reads_as_an_empty_one(tmp_path):
    """The mount step has simply not run yet — that is not a failure."""
    assert MountManifest.load(tmp_path).repositories == {}


def test_it_round_trips_through_the_file(tmp_path):
    written = MountManifest(
        repositories={"my-strategies": {"GOLD_EDGE_V1": True, "SILVER_EDGE_V1": False}}
    )
    written.write(tmp_path)

    assert MountManifest.load(tmp_path).repositories == written.repositories


def test_rendering_is_stable_so_a_diff_only_shows_a_moved_verdict(tmp_path):
    scrambled = MountManifest(
        repositories={
            "zeta-strategies": {"ZED_V1": True},
            "alpha-strategies": {"BETA_V1": True, "ALPHA_V1": False},
        }
    )
    ordered = MountManifest(
        repositories={
            "alpha-strategies": {"ALPHA_V1": False, "BETA_V1": True},
            "zeta-strategies": {"ZED_V1": True},
        }
    )

    assert scrambled.render() == ordered.render()


def test_a_repo_publishing_nothing_keeps_an_empty_table(tmp_path):
    """ "Mounted and publishes nothing" is not the same claim as "never mounted"."""
    MountManifest(repositories={"my-strategies": {}}).write(tmp_path)

    reloaded = MountManifest.load(tmp_path)

    assert reloaded.repositories == {"my-strategies": {}}
    assert reloaded.disabled_repositories == frozenset()


def test_the_flat_shape_this_format_replaced_is_refused(tmp_path):
    """`<repo> = true` never said which of the repo's strategies passed."""
    (tmp_path / MOUNT_MANIFEST_FILENAME).write_text(
        "[strategies]\nmy-strategies = true\n", encoding="utf-8"
    )

    with pytest.raises(ValueError, match="re-run `make strategy-mount`"):
        MountManifest.load(tmp_path)


def test_a_name_toml_cannot_leave_bare_is_quoted(tmp_path):
    MountManifest(repositories={"my strategies": {"GOLD.EDGE V1": True}}).write(tmp_path)

    assert MountManifest.load(tmp_path).verdicts == {"GOLD.EDGE V1": True}


# ── What the two consumers ask it ────────────────────────────────────────


def test_disabled_strategies_are_the_failing_ones_across_every_repo():
    manifest = MountManifest(
        repositories={
            "one-repo": {"ALPHA_V1": True, "BETA_V1": False},
            "two-repo": {"GAMMA_V1": False},
        }
    )

    assert manifest.disabled_strategies == frozenset({"BETA_V1", "GAMMA_V1"})


def test_a_repo_is_disabled_outright_only_when_nothing_in_it_passed():
    manifest = MountManifest(
        repositories={
            "half-dead": {"ALPHA_V1": True, "BETA_V1": False},
            "all-dead": {"GAMMA_V1": False},
            "publishes-nothing": {},
        }
    )

    assert manifest.disabled_repositories == frozenset({"all-dead"})


def test_only_repos_with_something_left_to_run_are_frozen():
    """`make strategy-requirements` reads this to decide whose deps ship."""
    manifest = MountManifest(
        repositories={
            "half-dead": {"ALPHA_V1": True, "BETA_V1": False},
            "all-dead": {"GAMMA_V1": False},
            "publishes-nothing": {},
        }
    )

    assert manifest.passing_repositories == ("half-dead",)


# ── Recording, which is what `make strategy-mount` calls ─────────────────


def test_recording_writes_one_verdict_per_published_strategy(mounted):
    record_repository(mounted, "my-strategies")

    assert MountManifest.load(mounted).repositories == {
        "my-strategies": {"GOLD_EDGE_V1": True, "SILVER_EDGE_V1": True}
    }


def test_recording_one_repo_leaves_the_others_alone(mounted):
    """`make strategy-mount STRATEGY=<name>` must not discard its neighbours."""
    MountManifest(repositories={"other-strategies": {"OTHER_V1": True}}).write(mounted)

    record_repository(mounted, "my-strategies")

    assert MountManifest.load(mounted).repositories == {
        "other-strategies": {"OTHER_V1": True},
        "my-strategies": {"GOLD_EDGE_V1": True, "SILVER_EDGE_V1": True},
    }


def test_recording_replaces_a_repos_table_rather_than_merging_it(mounted):
    """A renamed strategy must not keep the pass its old name was given."""
    MountManifest(
        repositories={"my-strategies": {"RENAMED_AWAY_V1": True, "GOLD_EDGE_V1": False}}
    ).write(mounted)

    record_repository(mounted, "my-strategies")

    assert MountManifest.load(mounted).repositories == {
        "my-strategies": {"GOLD_EDGE_V1": True, "SILVER_EDGE_V1": True}
    }


def test_recording_marks_a_strategy_the_engine_cannot_drive(mounted):
    """The verdict is the audit's, per strategy, which is the point of the file.

    The manifest here is self-contained rather than importing from ``src/``:
    every repo in this file mounts a package called ``edges``, and the first one
    imported would answer for all of them.
    """
    (mounted / "my-strategies" / "strategies.py").write_text(
        textwrap.dedent(
            """
            class GoodEdge:
                name = "GOLD_EDGE_V1"
                symbols = ("XAUUSD",)
                timeframe = "M15"
                warmup = 10

                def __init__(self, params=None):
                    self.params = dict(params or {})

                def on_start(self, context): pass
                def on_stop(self): pass
                def on_candle_closed(self, df, context): return None
                def on_tick(self, price, context): return None
                def history_window(self): return 400
                def describe(self): return {"name": self.name, "params": self.params}
                def long(self, *args, **kwargs): return None
                def short(self, *args, **kwargs): return None
                def tp1(self, *args, **kwargs): return None
                def tp2(self, *args, **kwargs): return None
                def sl(self, *args, **kwargs): return None


            class BrokenEdge:
                name = "BROKEN_V1"


            def load_all():
                return {"GOLD_EDGE_V1": GoodEdge, "BROKEN_V1": BrokenEdge}
            """
        ).lstrip(),
        encoding="utf-8",
    )

    record_repository(mounted, "my-strategies")

    assert MountManifest.load(mounted).repositories == {
        "my-strategies": {"GOLD_EDGE_V1": True, "BROKEN_V1": False}
    }


def test_recording_a_repo_that_is_not_mounted_says_so(mounted):
    with pytest.raises(FileNotFoundError, match="nope"):
        record_repository(mounted, "nope")


def test_recording_starts_over_rather_than_choking_on_a_stale_file(mounted, caplog):
    """A generated file must never be able to block its own regeneration."""
    (mounted / MOUNT_MANIFEST_FILENAME).write_text(
        "[strategies]\nmy-strategies = true\n", encoding="utf-8"
    )

    with caplog.at_level("WARNING"):
        record_repository(mounted, "my-strategies")

    assert MountManifest.load(mounted).repositories == {
        "my-strategies": {"GOLD_EDGE_V1": True, "SILVER_EDGE_V1": True}
    }
    assert "Starting the mount manifest over" in caplog.text


# ── The CLI the Makefile drives ──────────────────────────────────────────


def test_passing_prints_one_repo_per_line_for_the_deploy_step(mounted, capsys):
    MountManifest(
        repositories={
            "my-strategies": {"GOLD_EDGE_V1": True},
            "all-dead": {"GAMMA_V1": False},
        }
    ).write(mounted)

    assert main(["--dir", str(mounted), "--passing"]) == 0
    assert capsys.readouterr().out.split() == ["my-strategies"]


def test_the_deploy_step_stops_on_a_manifest_it_cannot_parse(mounted, capsys):
    """Freezing half a book because a file lost a bracket is the worse failure."""
    (mounted / MOUNT_MANIFEST_FILENAME).write_text("[strategies", encoding="utf-8")

    assert main(["--dir", str(mounted), "--passing"]) == 1
    assert MOUNT_MANIFEST_FILENAME in capsys.readouterr().err


def test_ensure_leaves_a_manifest_behind_for_a_directory_with_no_repos(tmp_path, capsys):
    """`make up` refuses to start without the file, even when it lists nothing."""
    assert main(["--dir", str(tmp_path), "--ensure"]) == 0

    capsys.readouterr()
    assert MountManifest.path_in(tmp_path).is_file()
    assert MountManifest.load(tmp_path).repositories == {}


def test_ensure_repairs_a_manifest_in_the_shape_this_format_replaced(tmp_path):
    (tmp_path / MOUNT_MANIFEST_FILENAME).write_text(
        "[strategies]\nmy-strategies = true\n", encoding="utf-8"
    )

    assert main(["--dir", str(tmp_path), "--ensure"]) == 0
    assert MountManifest.load(tmp_path).repositories == {}


def test_show_names_every_strategy_and_its_verdict(mounted, capsys):
    MountManifest(
        repositories={"my-strategies": {"GOLD_EDGE_V1": True, "SILVER_EDGE_V1": False}}
    ).write(mounted)

    assert main(["--dir", str(mounted), "--show"]) == 0

    printed = capsys.readouterr().out
    assert "GOLD_EDGE_V1" in printed
    assert "SILVER_EDGE_V1" in printed
    assert "1 passing audit" in printed
