"""Executable spec for ``/strategies``: the mount, its stamps and the boxes.

Pinned here: which folders of ``__strategies__/`` count as strategy
repositories (the two templates never do), what is read from each one's
``BUNDLE_VERSION.json`` and what happens when it is missing or broken, and the
shape of the message — one ``<pre>`` box per repository, escaped, and split
before it can pass the Bot API's size limit.

The scan reads a directory and the presenters are pure functions of what it
returned, so none of this needs Telegram, Redis or a runner.
"""

from __future__ import annotations

import json
from pathlib import Path

from qte_bot import presenters
from qte_bot.commands import COMMANDS, HELP
from qte_bot.mounts import (
    BUNDLE_VERSION_FILENAME,
    MountedRepository,
    read_bundle_version,
    scan_mounted_repositories,
)
from qte_shared.config import REPO_ROOT
from qte_shared.strategies.plugin_loader import SKIPPED_REPOSITORIES

STAMP = {
    "branch": "dev",
    "built_at": "2026-10-20T01:29:14Z",
    "commit": "1185c39",
    "content": "e9d4e243285de83e",
    "dirty": True,
    "mode": "minified",
    "tree": "e5e209e41d1180cc98e1e5ce203b875651b7352b",
    "version": "20261020T012914Z-1185c39-e5e209e4-dirty",
}

NAMESPACE = "dev.shadow.mt5"


def make_repository(root: Path, name: str, *, stamp: object = None, manifest: bool = True) -> Path:
    folder = root / name
    folder.mkdir(parents=True)
    if manifest:
        (folder / "manifest.py").write_text("def load_all():\n    return {}\n", encoding="utf-8")
    if stamp is not None:
        (folder / BUNDLE_VERSION_FILENAME).write_text(json.dumps(stamp), encoding="utf-8")
    return folder


def write_record(root: Path, text: str) -> None:
    (root / "strategies.toml").write_text(text, encoding="utf-8")


# ── The scan ──────────────────────────────────────────────────────────────


def test_every_mounted_repository_is_listed_by_name(tmp_path):
    make_repository(tmp_path, "zeta-strategies")
    make_repository(tmp_path, "alpha-strategies")
    names = [repository.name for repository in scan_mounted_repositories(tmp_path)]
    assert names == ["alpha-strategies", "zeta-strategies"]


def test_the_two_template_folders_are_never_listed(tmp_path):
    for name in sorted(SKIPPED_REPOSITORIES):
        make_repository(tmp_path, name, stamp=STAMP)
    make_repository(tmp_path, "real-strategies")
    write_record(tmp_path, "[strategies._boilerplate]\nQTE_BOILERPLATE_M15 = true\n")
    assert SKIPPED_REPOSITORIES == {"_boilerplate", "qte-signal-simulator"}
    assert [repository.name for repository in scan_mounted_repositories(tmp_path)] == [
        "real-strategies"
    ]


def test_folders_that_are_not_strategy_repositories_are_ignored(tmp_path):
    make_repository(tmp_path, "real-strategies")
    make_repository(tmp_path, ".hidden", manifest=True)
    make_repository(tmp_path, "__pycache__", manifest=True)
    make_repository(tmp_path, "no-manifest-here", manifest=False)
    (tmp_path / "loose_strategy.py").write_text("x = 1\n", encoding="utf-8")
    assert [repository.name for repository in scan_mounted_repositories(tmp_path)] == [
        "real-strategies"
    ]


def test_a_missing_directory_lists_nothing(tmp_path):
    assert scan_mounted_repositories(tmp_path / "absent") == []


def test_strategies_come_from_the_mount_record_with_their_audit_verdict(tmp_path):
    make_repository(tmp_path, "real-strategies")
    write_record(
        tmp_path,
        "[strategies.real-strategies]\nMT5_GOLD_M15_V2 = true\nMT5_OIL_M15_V1 = false\n",
    )
    [repository] = scan_mounted_repositories(tmp_path)
    assert repository.recorded
    assert repository.strategies == {"MT5_GOLD_M15_V2": True, "MT5_OIL_M15_V1": False}


def test_a_repository_the_record_never_saw_is_still_listed_and_says_so(tmp_path):
    make_repository(tmp_path, "fresh-clone")
    write_record(tmp_path, "[strategies.other]\nSOME_ALIAS = true\n")
    [repository] = scan_mounted_repositories(tmp_path)
    assert not repository.recorded
    assert repository.strategies == {}


def test_a_repository_the_record_lists_without_a_manifest_is_still_listed(tmp_path):
    make_repository(tmp_path, "recorded-only", manifest=False)
    write_record(tmp_path, "[strategies.recorded-only]\n")
    [repository] = scan_mounted_repositories(tmp_path)
    assert repository.recorded and repository.strategies == {}


# ── The stamp ─────────────────────────────────────────────────────────────


def test_the_stamp_is_read_from_the_root_of_the_repository(tmp_path):
    folder = make_repository(tmp_path, "real-strategies", stamp=STAMP)
    assert read_bundle_version(folder) == (STAMP, None)
    [repository] = scan_mounted_repositories(tmp_path)
    assert repository.bundle == STAMP and repository.bundle_problem is None


def test_a_repository_without_a_stamp_is_listed_with_nothing_to_show(tmp_path):
    folder = make_repository(tmp_path, "real-strategies")
    assert read_bundle_version(folder) == (None, None)


def test_a_broken_stamp_is_reported_not_raised(tmp_path):
    folder = make_repository(tmp_path, "real-strategies")
    (folder / BUNDLE_VERSION_FILENAME).write_text('{"version": ', encoding="utf-8")
    stamp, problem = read_bundle_version(folder)
    assert stamp is None and problem


def test_a_stamp_that_is_not_an_object_is_refused(tmp_path):
    folder = make_repository(tmp_path, "real-strategies", stamp=["not", "an", "object"])
    stamp, problem = read_bundle_version(folder)
    assert stamp is None and "object" in problem


def test_a_stamp_saved_with_a_byte_order_mark_still_reads(tmp_path):
    folder = make_repository(tmp_path, "real-strategies")
    (folder / BUNDLE_VERSION_FILENAME).write_text(json.dumps(STAMP), encoding="utf-8-sig")
    assert read_bundle_version(folder) == (STAMP, None)


def test_the_committed_templates_carry_the_example_stamp():
    for name in sorted(SKIPPED_REPOSITORIES):
        stamp, problem = read_bundle_version(REPO_ROOT / "__strategies__" / name)
        assert problem is None, name
        assert stamp is not None and set(stamp) == set(STAMP), name


# ── The boxes ─────────────────────────────────────────────────────────────


def mounted(name: str = "real-strategies", **overrides) -> MountedRepository:
    fields = {
        "name": name,
        "strategies": {"MT5_GOLD_M15_V2": True, "MT5_OIL_M15_V1": True},
        "recorded": True,
        "bundle": STAMP,
    }
    return MountedRepository(**{**fields, **overrides})


def test_a_box_shows_the_build_and_what_the_repository_publishes():
    [body] = presenters.strategy_bundles([mounted()], NAMESPACE)
    assert "[📦 real-strategies]" in body
    assert "20261020T012914Z-1185c39-e5e209e4-dirty" in body
    assert presenters.moment(STAMP["built_at"], full=True) in body
    assert "Branch: dev | Commit: 1185c39" in body
    assert "Tree: e5e209e4 | Content: e9d4e243285de83e" in body
    assert "Mode: minified | Dirty: ⚠️ yes" in body
    assert "Strategies (2):" in body
    assert "MT5_GOLD_M15_V2" in body and "MT5_OIL_M15_V1" in body
    assert NAMESPACE in body


def test_each_repository_is_its_own_box():
    [body] = presenters.strategy_bundles([mounted("one-repo"), mounted("two-repo")], NAMESPACE)
    assert body.count("<pre>") == body.count("</pre>") == 2
    assert body.index("[📦 one-repo]") < body.index("</pre>") < body.index("[📦 two-repo]")


def test_a_repository_without_a_stamp_leaves_the_version_blank_and_still_lists_strategies():
    [body] = presenters.strategy_bundles([mounted(bundle=None)], NAMESPACE)
    assert "Version: — (no BUNDLE_VERSION.json)" in body
    assert "Built:" not in body
    assert "MT5_GOLD_M15_V2" in body


def test_a_broken_stamp_is_flagged_in_its_box():
    broken = mounted(bundle=None, bundle_problem="Expecting value: line 1 column 1 (char 0)")
    [body] = presenters.strategy_bundles([broken], NAMESPACE)
    assert "⚠️ BUNDLE_VERSION.json unreadable" in body
    assert "MT5_GOLD_M15_V2" in body


def test_a_clean_build_is_not_marked_dirty():
    [body] = presenters.strategy_bundles([mounted(bundle={**STAMP, "dirty": False})], NAMESPACE)
    assert "Dirty: no" in body


def test_a_stamp_missing_some_keys_blanks_only_those():
    [body] = presenters.strategy_bundles(
        [mounted(bundle={"version": "20261020T012914Z-1185c39"})], NAMESPACE
    )
    assert "Version: 20261020T012914Z-1185c39" in body
    assert "Branch: — | Commit: —" in body
    assert "Tree: — | Content: —" in body
    assert "Dirty: —" in body


def test_a_strategy_that_failed_its_audit_is_marked():
    failing = mounted(strategies={"GOOD_ONE_M15": True, "BAD_ONE_M15": False})
    [body] = presenters.strategy_bundles([failing], NAMESPACE)
    assert "🟢 GOOD_ONE_M15" in body
    assert "🔴 BAD_ONE_M15 (failed audit)" in body


def test_a_repository_the_record_never_saw_points_at_the_mount_step():
    [body] = presenters.strategy_bundles([mounted(strategies={}, recorded=False)], NAMESPACE)
    assert "not in strategies.toml" in body and "make strategy-mount" in body


def test_a_repository_that_publishes_nothing_says_so():
    [body] = presenters.strategy_bundles([mounted(strategies={})], NAMESPACE)
    assert "Strategies: none published" in body


def test_values_from_disk_cannot_inject_markup():
    hostile = mounted(
        name="evil<b>repo",
        strategies={"A&B<i>": True},
        bundle={**STAMP, "branch": "<script>", "version": "v&1"},
    )
    [body] = presenters.strategy_bundles([hostile], NAMESPACE)
    assert "evil&lt;b&gt;repo" in body
    assert "A&amp;B&lt;i&gt;" in body
    assert "&lt;script&gt;" in body and "v&amp;1" in body
    assert "<script>" not in body and "<i>" not in body and "<b>repo" not in body


def test_a_very_long_alias_table_is_counted_not_listed_in_full():
    many = {f"STRATEGY_{index:03d}": True for index in range(presenters.MAX_STRATEGIES_PER_BOX + 7)}
    [body] = presenters.strategy_bundles([mounted(strategies=many)], NAMESPACE)
    assert "… 7 more" in body
    assert f"STRATEGY_{presenters.MAX_STRATEGIES_PER_BOX + 6:03d}" not in body


def test_a_large_mount_is_split_into_messages_that_each_fit_and_balance():
    repositories = [mounted(f"repository-{index:02d}") for index in range(30)]
    messages = presenters.strategy_bundles(repositories, NAMESPACE)
    assert len(messages) > 1
    for body in messages:
        assert len(body) < 4096
        assert body.count("<pre>") == body.count("</pre>")
    # Every repository appears exactly once, in order, across the messages.
    joined = "\n".join(messages)
    positions = [joined.index(f"[📦 repository-{index:02d}]") for index in range(30)]
    assert positions == sorted(positions)
    assert all(joined.count(f"[📦 repository-{index:02d}]") == 1 for index in range(30))
    # The title opens the first message only; the footer closes the last only.
    assert NAMESPACE in messages[0] and NAMESPACE not in messages[1]
    assert "read from disk" in messages[-1] and "read from disk" not in messages[0]


def test_an_empty_mount_says_how_to_fill_it():
    [body] = presenters.strategy_bundles([], NAMESPACE)
    assert "No strategy repository is mounted" in body
    assert "make strategy-mount" in body


def test_the_footer_counts_repositories_in_the_singular():
    [body] = presenters.strategy_bundles([mounted()], NAMESPACE)
    assert "1 repository ·" in body


# ── The menu ──────────────────────────────────────────────────────────────


def test_the_command_is_in_the_menu_and_in_the_help():
    assert "strategies" in {command.command for command in COMMANDS}
    assert "/strategies" in HELP
