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

#: Every workspace member, so a new engine is covered by these checks the
#: moment it exists rather than whenever someone remembers to list it.
ALL_ENGINES = (
    "shared",
    "data_ingestion",
    "backtest_engine",
    "strategy_engine",
    "strategy_audit",
    "market_simulator",
)

#: The leaves. `shared` is the hub every one of them depends on.
LEAF_ENGINES = tuple(name for name in ALL_ENGINES if name != "shared")

#: The one edge between two leaves, and the reason it is not a hole in the star.
#:
#: The runner audits its own book before it trades — QTE_RUNNER__AUDIT_ON_START,
#: see `qte_strategy_engine.preflight` — so the runner image carries the auditor.
#: The rule exists so an image does not pick up weight it has no use for, and
#: qte-strategy-audit brings nothing but qte-shared; the test below keeps it that
#: way, which is what makes this exception cost nothing. Any other pair of leaves
#: still has to meet in shared.
ALLOWED_LEAF_EDGES = {("strategy_engine", "strategy_audit")}


def _manifest(name: str) -> dict:
    return tomllib.loads((ENGINES / name / "pyproject.toml").read_text(encoding="utf-8"))


def _dependencies(name: str) -> list[str]:
    return _manifest(name)["project"].get("dependencies", [])


def _distribution(requirement: str) -> str:
    """The distribution a requirement names, with any extras/specifier stripped.

    `qte-shared[tiingo]` is still an edge to `qte-shared`; without this the
    extras syntax would slip past the star-graph check below unnoticed.
    """
    for separator in ("[", ">", "<", "=", "!", "~", ";", " "):
        requirement = requirement.split(separator)[0]
    return requirement.strip()


@pytest.mark.parametrize("engine", ALL_ENGINES)
def test_every_engine_uses_the_src_layout(engine):
    manifest = _manifest(engine)
    packages = manifest["tool"]["hatch"]["build"]["targets"]["wheel"]["packages"]
    assert len(packages) == 1
    package = packages[0]
    assert package.startswith("src/"), "the importable package lives under src/"
    assert (ENGINES / engine / package).is_dir()


@pytest.mark.parametrize("engine", ALL_ENGINES)
def test_engine_folders_use_underscores(engine):
    assert "-" not in engine


def test_only_the_backtest_engine_pulls_in_pyarrow():
    """It is 152 MB of the venv; the live containers must not carry it."""
    assert any(dep.startswith("pyarrow") for dep in _dependencies("backtest_engine"))
    for other in (name for name in ALL_ENGINES if name != "backtest_engine"):
        assert not any(dep.startswith("pyarrow") for dep in _dependencies(other))


def test_no_engine_depends_on_another_engine_except_through_shared():
    """The dependency graph is a star, not a mesh.

    Anything two engines both need belongs in shared. A direct edge between two
    leaf engines is how a microservice boundary quietly stops being one.
    """
    workspace_packages = {
        "qte-shared": "shared",
        "qte-ingestion": "data_ingestion",
        "qte-backtest": "backtest_engine",
        "qte-strategy-engine": "strategy_engine",
        "qte-strategy-audit": "strategy_audit",
        "qte-simulator": "market_simulator",
    }
    for engine in LEAF_ENGINES:
        for dep in _dependencies(engine):
            owner = workspace_packages.get(_distribution(dep))
            if (engine, owner) in ALLOWED_LEAF_EDGES:
                continue
            assert owner in (None, "shared"), (
                f"{engine} depends on {dep}; route it through qte-shared instead"
            )
    assert not any(_distribution(dep) in workspace_packages for dep in _dependencies("shared"))


def test_the_audited_leaf_edge_carries_no_third_party_weight():
    """What makes ALLOWED_LEAF_EDGES safe, asserted rather than assumed.

    The runner is allowed to depend on the auditor because depending on it
    installs four modules and nothing else. The day qte-strategy-audit grows a
    dependency of its own, that stops being true and this fails — which is the
    moment to decide whether the runner should still carry it.
    """
    assert [_distribution(dep) for dep in _dependencies("strategy_audit")] == ["qte-shared"]


def test_the_dockerfile_selects_a_single_package():
    dockerfile = (REPO_ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert "ARG QTE_PACKAGE" in dockerfile
    assert "--package ${QTE_PACKAGE}" in dockerfile


def test_compose_builds_a_different_image_per_service():
    compose = (REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    assert "QTE_PACKAGE: qte-ingestion" in compose
    assert "QTE_PACKAGE: qte-strategy-engine" in compose
    assert "QTE_PACKAGE: qte-simulator" in compose


def test_the_simulator_still_refuses_outside_dev():
    """`docker compose up` starts the simulator, so the in-process guard is
    what stands between an invented feed and a non-dev environment. Both
    server and provider call ``require_dev_env()``; if either loses that call
    a compose ``up`` would happily fabricate prices in staging or prod."""
    server = (
        REPO_ROOT / "engines/market_simulator/src/qte_simulator/server.py"
    ).read_text(encoding="utf-8")
    provider = (
        REPO_ROOT / "engines/shared/src/qte_shared/providers/simulator/provider.py"
    ).read_text(encoding="utf-8")
    assert "require_dev_env" in server
    assert "require_dev_env" in provider


def test_the_lockfile_points_at_the_renamed_engine_folders():
    lock = (REPO_ROOT / "uv.lock").read_text(encoding="utf-8")
    for engine in ALL_ENGINES:
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
    for engine in ALL_ENGINES:
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


def test_every_manifest_pins_the_same_python() -> None:
    """One process runs the engine *and* the plugins, so one interpreter does.

    `pandas-ta` — which the mounted strategy repo builds its indicators on —
    requires >=3.12 and hard-pins a numba with no 3.14 wheel. A workspace member
    left on a wider range would resolve differently from the rest and only fail
    on whichever machine picked the other version.
    """
    root = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    expected = root["project"]["requires-python"]

    assert expected == ">=3.13,<3.14"
    for engine in ALL_ENGINES:
        assert _manifest(engine)["project"]["requires-python"] == expected, engine


def test_the_numpy_ceiling_the_plugins_need_is_declared() -> None:
    """numba refuses NumPy above 2.2, and it is in the runner's process.

    Expressed as a uv constraint rather than an upper bound on qte-shared's own
    numpy dependency, because nothing in this repo actually needs the ceiling —
    see `[tool.uv]` in the root manifest.
    """
    root = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    assert "numpy<2.3" in root["tool"]["uv"]["constraint-dependencies"]


def test_a_market_data_vendors_client_libraries_are_an_extra_not_a_core_dependency():
    """The runner opens no socket to a data vendor; it must not install one.

    Vendor clients hang off `qte-shared` as extras, so an image pulls in only
    the provider it is configured to use — see `qte_shared.providers`.
    """
    shared = _manifest("shared")["project"]
    extras = shared.get("optional-dependencies", {})
    assert "tiingo" in extras
    vendor_clients = ("httpx", "websockets")
    for dep in shared["dependencies"]:
        assert not _distribution(dep).startswith(vendor_clients), (
            f"{dep} belongs in a provider extra, not in qte-shared's core dependencies"
        )
    for engine in ("strategy_engine", "strategy_audit"):
        for dep in _dependencies(engine):
            assert "[" not in dep or not dep.startswith("qte-shared["), (
                f"{engine} pulls a market data vendor it does not use: {dep}"
            )
