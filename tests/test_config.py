"""The repo root drives .env, __strategies__/ and data/ — it must not drift.

A hardcoded `parents[N]` does not fail when a package moves; it resolves one
level off and the engine quietly looks for everything in the wrong place. This
file exists so that failure is loud instead.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy.engine import make_url

from qte_shared.config import REPO_ROOT, PostgresSettings, _find_repo_root, settings

MARKER = '[project]\nname = "x"\n\n[tool.qte]\nrepo-root = true\n'


def test_the_repo_root_is_the_directory_holding_the_marked_manifest():
    manifest = REPO_ROOT / "pyproject.toml"
    assert manifest.is_file()
    assert "[tool.qte]" in manifest.read_text(encoding="utf-8")


def test_the_root_is_identified_not_counted(tmp_path, monkeypatch):
    # Same package, moved one level deeper: the answer must not change shape.
    nested = tmp_path / "engines" / "shared" / "qte_shared"
    nested.mkdir(parents=True)
    (tmp_path / "pyproject.toml").write_text(MARKER)
    monkeypatch.setattr("qte_shared.config.__file__", str(nested / "config.py"), raising=False)
    assert _find_repo_root() == tmp_path.resolve()


def test_a_pyproject_without_the_marker_table_is_not_the_root(tmp_path, monkeypatch):
    # A mounted strategy repo brings its own pyproject.toml and sits under
    # __strategies__/, i.e. *below* the root. Only the marker separates them.
    checkout = tmp_path / "__strategies__" / "my-strategies"
    package = checkout / "src" / "edges"
    package.mkdir(parents=True)
    (checkout / "pyproject.toml").write_text('[project]\nname = "my-strategies"\n')
    (tmp_path / "pyproject.toml").write_text(MARKER)
    monkeypatch.setattr("qte_shared.config.__file__", str(package / "config.py"), raising=False)
    assert _find_repo_root() == tmp_path.resolve()


def test_the_defaults_hang_off_the_root_rather_than_the_working_directory():
    assert settings.engine.strategies_dir == REPO_ROOT / "__strategies__"
    assert settings.engine.parquet_dir == REPO_ROOT / "data" / "parquet"
    assert settings.engine.reports_dir == REPO_ROOT / "data" / "reports"


def test_the_shared_package_really_lives_under_src():
    assert (REPO_ROOT / "src" / "qte_shared" / "config.py").is_file()
    assert not (REPO_ROOT / "engines").exists()
    assert not (REPO_ROOT / "qte_shared").exists()


def test_src_holds_the_services_and_nothing_else():
    members = sorted(path.name for path in (REPO_ROOT / "src").iterdir() if path.is_dir())
    assert members == [
        "qte_backtest",
        "qte_ingestion",
        "qte_shared",
        "qte_simulator",
        "qte_strategy_audit",
        "qte_strategy_engine",
    ]
    for name in members:
        # One level from src/ to the code: the folder *is* the import name, so
        # there is nothing between them to get out of step.
        assert (REPO_ROOT / "src" / name / "__init__.py").is_file()


def test_no_stray_path_assumptions_survive_outside_the_root():
    # REPO_ROOT must be an ancestor of the package, never a sibling or below it.
    package = Path(__file__).resolve().parents[1] / "src" / "qte_shared"
    assert package.is_relative_to(REPO_ROOT)


@pytest.mark.parametrize("credential", ['literal@:/?#%$&" value', "", "ordinary"])
def test_postgres_components_round_trip_reserved_password_characters(credential):
    database_config = PostgresSettings(
        dsn="",
        username="reader@desk",
        password=credential,
        hostname="postgres-audit",
        database="custom_audit",
        port_number=6432,
    )
    database_url = make_url(database_config.dsn)
    assert database_url.username == "reader@desk"
    assert database_url.password == credential
    assert database_url.host == "postgres-audit"
    assert database_url.port == 6432
    assert database_url.database == "custom_audit"


def test_explicit_postgres_url_wins_over_component_defaults():
    explicit_dsn = "postgresql+asyncpg://reader:synthetic%40value@database.example/audit"
    database_config = PostgresSettings(dsn=explicit_dsn, hostname="ignored")
    assert database_config.dsn == explicit_dsn


def test_test_process_does_not_inherit_operator_credentials_or_deployment_mode():
    assert settings.env == "dev"
    assert settings.broker.token == settings.broker.nats_token == settings.nats.token == ""
    assert settings.broker.shadow_mode is True
    assert settings.account.capital == 1000
    assert not settings.market_data.plan_file.exists()
