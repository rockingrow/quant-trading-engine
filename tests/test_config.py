"""The repo root drives .env, __strategies__/ and data/ — it must not drift.

A hardcoded `parents[N]` does not fail when a package moves; it resolves one
level off and the engine quietly looks for everything in the wrong place. This
file exists so that failure is loud instead.
"""

from __future__ import annotations

from pathlib import Path

from qte_shared.config import REPO_ROOT, _find_repo_root, settings


def test_the_repo_root_is_the_directory_holding_the_workspace_manifest():
    manifest = REPO_ROOT / "pyproject.toml"
    assert manifest.is_file()
    assert "[tool.uv.workspace]" in manifest.read_text(encoding="utf-8")


def test_the_root_is_identified_not_counted(tmp_path, monkeypatch):
    # Same package, moved one level deeper: the answer must not change shape.
    nested = tmp_path / "engines" / "shared" / "qte_shared"
    nested.mkdir(parents=True)
    (tmp_path / "pyproject.toml").write_text(
        '[project]\nname = "x"\n\n[tool.uv.workspace]\nmembers = ["engines/*"]\n'
    )
    monkeypatch.setattr("qte_shared.config.__file__", str(nested / "config.py"), raising=False)
    assert _find_repo_root() == tmp_path.resolve()


def test_a_pyproject_without_the_workspace_table_is_not_the_root(tmp_path, monkeypatch):
    # Every member has a pyproject.toml; only the root declares the workspace.
    member = tmp_path / "engines" / "shared"
    package = member / "qte_shared"
    package.mkdir(parents=True)
    (member / "pyproject.toml").write_text('[project]\nname = "qte-shared"\n')
    (tmp_path / "pyproject.toml").write_text(
        '[project]\nname = "x"\n\n[tool.uv.workspace]\nmembers = ["engines/*"]\n'
    )
    monkeypatch.setattr("qte_shared.config.__file__", str(package / "config.py"), raising=False)
    assert _find_repo_root() == tmp_path.resolve()


def test_the_defaults_hang_off_the_root_rather_than_the_working_directory():
    assert settings.engine.strategies_dir == REPO_ROOT / "__strategies__"
    assert settings.engine.parquet_dir == REPO_ROOT / "data" / "parquet"
    assert settings.engine.reports_dir == REPO_ROOT / "data" / "reports"


def test_the_shared_package_really_lives_under_engines():
    assert (REPO_ROOT / "engines" / "shared" / "qte_shared" / "config.py").is_file()
    assert not (REPO_ROOT / "shared").exists()


def test_every_workspace_member_is_an_engine():
    members = sorted(path.name for path in (REPO_ROOT / "engines").iterdir() if path.is_dir())
    assert members == ["backtest-engine", "data-ingestion", "shared", "strategy-engine"]
    for name in members:
        assert (REPO_ROOT / "engines" / name / "pyproject.toml").is_file()


def test_no_stray_path_assumptions_survive_outside_the_root():
    # REPO_ROOT must be an ancestor of the package, never a sibling or below it.
    package = Path(__file__).resolve().parents[1] / "engines" / "shared"
    assert package.is_relative_to(REPO_ROOT)
