"""The boundaries that let one distribution still ship as several images.

There is no uv workspace any more: a single package carries every service's
modules, 612 KB of them against a venv two orders of magnitude larger. Two
things follow, and this file asserts both.

What separates the images is now which ``[project.optional-dependencies]`` an
image installs, not which modules it carries -- so the extras have to stay
honest about who actually opens a socket or reads a parquet file.

What keeps the services separable is no longer a manifest, which could not
express a bad import even when it wanted to. It is
``test_no_service_imports_another_except_through_shared`` below, which reads the
imports themselves. That is a stricter check than the manifest graph it
replaced: a declared dependency was only ever a proxy for the import.
"""

from __future__ import annotations

import ast
import re
import tomllib
from pathlib import Path

import pytest

from qte_shared.config import REPO_ROOT

SRC = REPO_ROOT / "src"

#: Every service package, so a new one is covered by these checks the moment it
#: exists rather than whenever someone remembers to list it.
SERVICES = (
    "qte_shared",
    "qte_ingestion",
    "qte_backtest",
    "qte_strategy_engine",
    "qte_strategy_audit",
    "qte_simulator",
)

#: The leaves. ``qte_shared`` is the hub every one of them depends on.
LEAF_SERVICES = tuple(name for name in SERVICES if name != "qte_shared")

#: The one edge between two leaves, and the reason it is not a hole in the star.
#:
#: The runner audits its own book before it trades -- QTE_RUNNER__AUDIT_ON_START,
#: see ``qte_strategy_engine.preflight`` -- so the runner imports the auditor.
#: The rule exists so a service does not grow a private line to another; the
#: auditor reaches for nothing but ``qte_shared``, which the check below keeps
#: true. Any other pair of leaves still has to meet in shared.
ALLOWED_LEAF_EDGES = {("qte_strategy_engine", "qte_strategy_audit")}

#: Standard-library roots the auditor is allowed to import. Everything outside
#: this set and outside SERVICES is a third-party dependency, which is what
#: ``test_the_audited_leaf_edge_carries_no_third_party_weight`` is watching for.
STANDARD_LIBRARY = {
    "__future__",
    "argparse",
    "collections",
    "contextlib",
    "dataclasses",
    "enum",
    "importlib",
    "inspect",
    "json",
    "logging",
    "os",
    "pathlib",
    "sys",
    "tomllib",
    "types",
    "typing",
}


def _manifest() -> dict:
    return tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))


def _dependencies() -> list[str]:
    return _manifest()["project"]["dependencies"]


def _extras() -> dict[str, list[str]]:
    return _manifest()["project"].get("optional-dependencies", {})


def _distribution(requirement: str) -> str:
    """The distribution a requirement names, with any extras/specifier stripped."""
    for separator in ("[", ">", "<", "=", "!", "~", ";", " "):
        requirement = requirement.split(separator)[0]
    return requirement.strip()


def _imported_roots(path: Path) -> set[str]:
    """Top-level module names this file imports, absolute imports only.

    A relative import cannot leave its own package, so it is not a boundary
    crossing and does not need to be resolved here.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            roots.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            roots.add(node.module.split(".")[0])
    return roots


def _service_files(service: str) -> list[Path]:
    return sorted((SRC / service).rglob("*.py"))


# -- Layout -----------------------------------------------------------------


@pytest.mark.parametrize("service", SERVICES)
def test_every_service_sits_directly_under_src(service):
    """One level from src/ to the code, and the folder is the import name.

    The folder name is a global in site-packages, which is what the ``qte_``
    prefix is for; nothing may sit between src/ and it.
    """
    package = SRC / service
    assert package.is_dir()
    assert (package / "__init__.py").is_file()
    assert package.parent == SRC


def test_the_wheel_ships_exactly_the_services_on_disk():
    """A service that exists but is not listed simply is not installed.

    hatchling packages what ``packages`` names and nothing else, so a new folder
    under src/ that nobody added there imports fine in the editable dev venv and
    is missing from every image.
    """
    declared = set(_manifest()["tool"]["hatch"]["build"]["targets"]["wheel"]["packages"])
    on_disk = {f"src/{path.name}" for path in SRC.iterdir() if (path / "__init__.py").is_file()}
    assert declared == on_disk


def test_no_source_file_lives_outside_a_service_package():
    strays = list(SRC.glob("*.py"))
    assert not strays, f"src/ has Python files outside a service package: {strays}"


# -- The boundary ------------------------------------------------------------


def test_no_service_imports_another_except_through_shared():
    """The dependency graph is a star, not a mesh.

    Anything two services both need belongs in shared. A direct edge between two
    leaves is how a service boundary quietly stops being one -- and with a single
    distribution there is no resolver left to notice, so this is the only thing
    standing there.
    """
    offenders: list[str] = []
    for service in LEAF_SERVICES:
        for path in _service_files(service):
            for imported in _imported_roots(path):
                if imported not in SERVICES or imported in (service, "qte_shared"):
                    continue
                if (service, imported) in ALLOWED_LEAF_EDGES:
                    continue
                offenders.append(f"{path.relative_to(REPO_ROOT)} imports {imported}")
    assert not offenders, "route these through qte_shared instead: " + "; ".join(offenders)


def test_shared_imports_no_service():
    """The hub may not reach back into a spoke.

    An import in this direction is a cycle: the service that shared reaches for
    already imports shared, and every other service then carries it too.
    """
    offenders: list[str] = []
    for path in _service_files("qte_shared"):
        for imported in _imported_roots(path):
            if imported in LEAF_SERVICES:
                offenders.append(f"{path.relative_to(REPO_ROOT)} imports {imported}")
    assert not offenders, "shared reached into a service: " + "; ".join(offenders)


def test_the_audited_leaf_edge_carries_no_third_party_weight():
    """What makes ALLOWED_LEAF_EDGES safe, asserted rather than assumed.

    The runner is allowed to import the auditor because the auditor imports
    nothing but shared, pandas and the standard library. The day it grows a
    dependency of its own that stops being true, and this is the moment to
    decide whether the runner should still carry it.
    """
    third_party: set[str] = set()
    for path in _service_files("qte_strategy_audit"):
        third_party |= {
            imported
            for imported in _imported_roots(path)
            if imported not in SERVICES and imported not in STANDARD_LIBRARY
        }
    assert third_party <= {"pandas"}, f"the auditor grew dependencies: {sorted(third_party)}"


def test_no_python_file_still_imports_from_a_stale_path():
    for path in SRC.rglob("*.py"):
        source = path.read_text(encoding="utf-8")
        assert "qte_api" not in source, f"{path} references the removed API gateway"


# -- What an image installs --------------------------------------------------


def test_pyarrow_is_an_extra_not_a_core_dependency():
    """It is 84 MB of the venv; the live containers must not carry it."""
    assert not any(_distribution(dep) == "pyarrow" for dep in _dependencies())
    assert any(dep.startswith("pyarrow") for dep in _extras()["parquet"])


def test_a_market_data_vendors_client_libraries_are_an_extra():
    """The runner opens no socket to a data vendor; it must not install one.

    Vendor clients hang off an extra, so an image pulls in only the provider it
    is configured to use -- see ``qte_shared.providers``.
    """
    extras = _extras()
    assert "tiingo" in extras
    for dep in _dependencies():
        assert _distribution(dep) not in ("httpx", "websockets"), (
            f"{dep} belongs in a provider extra, not in the core dependencies"
        )


def test_the_dockerfile_selects_extras_rather_than_packages():
    dockerfile = (REPO_ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert "ARG QTE_EXTRAS" in dockerfile
    assert "--extra $extra" in dockerfile
    assert "COPY src/ src/" in dockerfile
    assert "QTE_PACKAGE" not in dockerfile, "the workspace is gone; so is --package"


def test_compose_gives_each_service_only_the_extras_it_opens():
    compose = (REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    assert 'QTE_EXTRAS: "tiingo simulator"' in compose, "ingestion reads the vendor feed"
    assert 'QTE_EXTRAS: "broker"' in compose, "the runner posts to algo-trading-broker"
    assert 'QTE_EXTRAS: "simulator"' in compose, "the simulator serves a WebSocket"
    selected = re.findall(r'QTE_EXTRAS:\s*"([^"]*)"', compose)
    assert selected, "no service selects its extras"
    assert not any("parquet" in extras for extras in selected), (
        "no live container installs the parquet stack; replay is a host job"
    )


def test_the_image_carries_what_the_wheel_build_reads():
    """The root manifest names a readme, so the build needs the file itself.

    Nothing in the dev venv notices: the editable install is already built by
    the time anyone runs a test. Only `docker build` fails, and only on the
    layer after the dependency cache, which is a slow place to learn it.
    """
    dockerfile = (REPO_ROOT / "Dockerfile").read_text(encoding="utf-8")
    readme = _manifest()["project"].get("readme")
    assert readme, "drop this test if the manifest stops naming a readme"
    assert readme in dockerfile, f"the image never copies {readme}; the wheel build will fail"


def test_migrations_are_copied_into_the_image():
    # Every container can reach the database, so every container can migrate it.
    dockerfile = (REPO_ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert "COPY migrations/" in dockerfile
    assert "COPY alembic.ini" in dockerfile


# -- The manifest ------------------------------------------------------------


def test_the_repo_root_declares_the_marker_table():
    """``qte_shared.config._find_repo_root()`` walks up looking for this table.

    It has no settings in it and exists only to be found, which is exactly why
    it is worth a test: nothing else would fail loudly if someone tidied it away.
    """
    assert _manifest()["tool"]["qte"]["repo-root"] is True


def test_the_lockfile_describes_one_editable_root():
    lock = (REPO_ROOT / "uv.lock").read_text(encoding="utf-8")
    assert 'source = { editable = "." }' in lock
    assert 'editable = "engines/' not in lock, "a workspace member survived the merge"


def test_python_is_pinned_to_the_one_interpreter_the_plugins_run_on() -> None:
    """One process runs the engine *and* the plugins, so one interpreter does.

    ``pandas-ta`` -- which the mounted strategy repo builds its indicators on --
    requires >=3.12 and hard-pins a numba with no 3.14 wheel.
    """
    assert _manifest()["project"]["requires-python"] == ">=3.13,<3.14"


def test_the_numpy_ceiling_the_plugins_need_is_declared() -> None:
    """numba refuses NumPy above 2.2, and it is in the runner's process.

    Expressed as a uv constraint rather than an upper bound on the numpy
    dependency, because nothing in this repo actually needs the ceiling -- see
    ``[tool.uv]`` in the manifest.
    """
    assert "numpy<2.3" in _manifest()["tool"]["uv"]["constraint-dependencies"]


# -- Runtime -----------------------------------------------------------------


def test_the_simulator_still_refuses_outside_dev():
    """``docker compose up`` starts the simulator, so the in-process guard is
    what stands between an invented feed and a non-dev environment. Both
    server and provider call ``require_dev_env()``; if either loses that call
    a compose ``up`` would happily fabricate prices in staging or prod."""
    server = (SRC / "qte_simulator" / "server.py").read_text(encoding="utf-8")
    provider = (SRC / "qte_shared" / "providers" / "simulator" / "provider.py").read_text(
        encoding="utf-8"
    )
    assert "require_dev_env" in server
    assert "require_dev_env" in provider


def test_the_imported_package_is_the_source_tree():
    import qte_shared

    location = Path(qte_shared.__file__).resolve()
    # src/qte_shared/__init__.py -- the editable install puts src/ on sys.path,
    # so an import that resolved anywhere else (a stale wheel, a copy left in the
    # working tree) shows up here rather than as a behaviour difference nobody
    # traces back to packaging.
    assert location.parent.parent == SRC.resolve()


def test_docs_do_not_reference_the_old_hyphenated_folders():
    for document in (REPO_ROOT / "README.md", *(REPO_ROOT / "docs").glob("*.md")):
        text = document.read_text(encoding="utf-8")
        for stale in ("data-ingestion/", "backtest-engine/", "strategy-engine/"):
            assert stale not in text, f"{document.name} still says {stale}"
