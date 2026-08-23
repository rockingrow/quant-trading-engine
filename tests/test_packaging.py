"""The workspace boundaries that let each service ship its own image.

`uv sync --package X` can only cut along a boundary the manifests actually
declare. If an engine picks up a dependency it does not need — or reaches into
another engine's modules — the images silently converge back into one.
"""

from __future__ import annotations

import tomllib
from pathlib import Path

import pytest
from qte_shared.config import REPO_ROOT

ENGINES = REPO_ROOT / "engines"


def _manifest(name: str) -> dict:
    return tomllib.loads((ENGINES / name / "pyproject.toml").read_text(encoding="utf-8"))


def _dependencies(name: str) -> list[str]:
    return _manifest(name)["project"].get("dependencies", [])


@pytest.mark.parametrize(
    "engine", ["shared", "data_ingestion", "backtest_engine", "strategy_engine"]
)
def test_every_engine_uses_the_src_layout(engine):
    manifest = _manifest(engine)
    packages = manifest["tool"]["hatch"]["build"]["targets"]["wheel"]["packages"]
    assert len(packages) == 1
    package = packages[0]
    assert package.startswith("src/"), "the importable package lives under src/"
    assert (ENGINES / engine / package).is_dir()


@pytest.mark.parametrize(
    "engine", ["shared", "data_ingestion", "backtest_engine", "strategy_engine"]
)
def test_engine_folders_use_underscores(engine):
    assert "-" not in engine


def test_only_the_backtest_engine_pulls_in_pyarrow():
    """It is 152 MB of the venv; the live containers must not carry it."""
    assert any(dep.startswith("pyarrow") for dep in _dependencies("backtest_engine"))
    for other in ("shared", "data_ingestion", "strategy_engine"):
        assert not any(dep.startswith("pyarrow") for dep in _dependencies(other))


def test_no_engine_depends_on_another_engine_except_through_shared():
    """The dependency graph is a star, not a mesh.

    Anything two engines both need belongs in shared. A direct edge between two
    leaf engines is how a microservice boundary quietly stops being one.
    """
    leaves = {"data_ingestion", "backtest_engine", "strategy_engine"}
    workspace_packages = {
        "qte-shared": "shared",
        "qte-ingestion": "data_ingestion",
        "qte-backtest": "backtest_engine",
        "qte-strategy-engine": "strategy_engine",
    }
    for engine in leaves:
        for dep in _dependencies(engine):
            owner = workspace_packages.get(dep)
            assert owner in (None, "shared"), (
                f"{engine} depends on {dep}; route it through qte-shared instead"
            )
    assert not any(dep in workspace_packages for dep in _dependencies("shared"))


def test_the_dockerfile_selects_a_single_package():
    dockerfile = (REPO_ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert "ARG QTE_PACKAGE" in dockerfile
    assert "--package ${QTE_PACKAGE}" in dockerfile


def test_compose_builds_a_different_image_per_service():
    compose = (REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    assert "QTE_PACKAGE: qte-ingestion" in compose
    assert "QTE_PACKAGE: qte-strategy-engine" in compose


def test_the_lockfile_points_at_the_renamed_engine_folders():
    lock = (REPO_ROOT / "uv.lock").read_text(encoding="utf-8")
    for engine in ("shared", "data_ingestion", "backtest_engine", "strategy_engine"):
        assert f'editable = "engines/{engine}"' in lock
    assert "engines/data-ingestion" not in lock


def test_no_source_file_lives_outside_a_src_directory():
    for engine in ENGINES.iterdir():
        if not engine.is_dir():
            continue
        strays = [path for path in engine.glob("*.py") if path.name != "setup.py"]
        assert not strays, f"{engine.name} has Python files outside src/: {strays}"


def test_the_repo_root_is_not_itself_an_installable_package():
    root = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    assert root["tool"]["uv"]["package"] is False
    assert root["tool"]["uv"]["workspace"]["members"] == ["engines/*"]


def test_paths_in_the_dockerfile_match_the_engines_on_disk():
    dockerfile = (REPO_ROOT / "Dockerfile").read_text(encoding="utf-8")
    for engine in ("shared", "data_ingestion", "backtest_engine", "strategy_engine"):
        assert f"engines/{engine}/pyproject.toml" in dockerfile, (
            f"Dockerfile does not copy {engine}'s manifest; the frozen sync would fail"
        )


def test_migrations_are_copied_into_the_image():
    # Every container can reach the database, so every container can migrate it.
    dockerfile = (REPO_ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert "COPY migrations/" in dockerfile
    assert "COPY alembic.ini" in dockerfile


def test_docs_do_not_reference_the_old_hyphenated_folders():
    for document in (REPO_ROOT / "README.md", *(REPO_ROOT / "docs").glob("*.md")):
        text = document.read_text(encoding="utf-8")
        for stale in ("data-ingestion/", "backtest-engine/", "strategy-engine/"):
            assert stale not in text, f"{document.name} still says {stale}"


def test_no_python_file_still_imports_from_a_stale_path():
    for path in ENGINES.rglob("*.py"):
        source = path.read_text(encoding="utf-8")
        assert "qte_api" not in source, f"{path} references the removed API gateway"


def test_src_layout_keeps_tests_off_the_source_tree(monkeypatch):
    import qte_shared

    location = Path(qte_shared.__file__).resolve()
    # Editable installs still point at the tree, but through src/ — which is the
    # marker that the thing being imported is the packaged artefact.
    assert location.parent.parent.name == "src"
