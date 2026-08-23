"""Ownership rules for the database layer, and the Alembic wiring that depends on them.

Two failures this file exists to prevent, both silent:

* a model registered on a *different* declarative base — invisible to
  autogenerate, so its table is never created and a later migration proposes
  dropping it;
* an engine's models not imported by ``migrations/env.py`` — same outcome, for
  the same reason.
"""

from __future__ import annotations

import ast

import pytest
from qte_backtest.db import BacktestRepository, BacktestRun, BacktestTrade
from qte_shared.config import REPO_ROOT
from qte_shared.db import Base, EngineEvent, EventRepository
from qte_strategy_engine.db import SignalAudit, SignalRepository

MIGRATIONS = REPO_ROOT / "migrations"


def test_every_engine_registers_its_tables_on_the_one_shared_base():
    for model in (EngineEvent, SignalAudit, BacktestRun, BacktestTrade):
        assert issubclass(model, Base), f"{model.__name__} is on a different base"


def test_the_metadata_holds_exactly_the_tables_we_expect():
    assert set(Base.metadata.tables) == {
        "engine_events",
        "signals",
        "backtest_runs",
        "backtest_trades",
    }


def test_tables_are_owned_by_the_engine_that_writes_them():
    # signals is written by the runner, the backtest tables by the replay, and
    # engine_events by all three — which is why only that one lives in shared.
    assert SignalAudit.__module__.startswith("qte_strategy_engine.")
    assert BacktestRun.__module__.startswith("qte_backtest.")
    assert BacktestTrade.__module__.startswith("qte_backtest.")
    assert EngineEvent.__module__.startswith("qte_shared.")


def test_each_owner_ships_a_repository_beside_its_models():
    assert SignalRepository.__module__.startswith("qte_strategy_engine.db")
    assert BacktestRepository.__module__.startswith("qte_backtest.db")
    assert EventRepository.__module__.startswith("qte_shared.db")


def test_ingestion_owns_no_tables_of_its_own():
    """It writes only engine_events, so it has no db package — by design.

    Inventing a table to justify a folder would be the wrong way round.
    """
    assert not (REPO_ROOT / "engines" / "data-ingestion" / "qte_ingestion" / "db").exists()


# ── Alembic wiring ────────────────────────────────────────────────────


def _env_imports() -> set[str]:
    tree = ast.parse((MIGRATIONS / "env.py").read_text(encoding="utf-8"))
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    return names


def test_env_imports_the_models_of_every_engine_that_owns_tables():
    imports = _env_imports()
    for module in (
        "qte_shared.db.models",
        "qte_strategy_engine.db.models",
        "qte_backtest.db.models",
    ):
        assert module in imports, (
            f"migrations/env.py does not import {module}; autogenerate would not see "
            "its tables and a later revision would propose dropping them"
        )


def test_the_migration_chain_is_linear_and_rooted():
    revisions = {}
    for path in (MIGRATIONS / "versions").glob("*.py"):
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        found = {}
        for node in tree.body:
            if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                if node.target.id in ("revision", "down_revision"):
                    found[node.target.id] = ast.literal_eval(node.value)
        revisions[found["revision"]] = found["down_revision"]

    assert revisions, "no migrations found"
    roots = [rev for rev, parent in revisions.items() if parent is None]
    assert len(roots) == 1, f"expected one root revision, found {roots}"

    heads = set(revisions) - {parent for parent in revisions.values() if parent}
    assert len(heads) == 1, f"expected one head, found {heads} — the chain has branched"


def test_the_init_sql_hook_is_gone():
    # It only ever ran on an empty volume; Alembic replaced it.
    assert not (REPO_ROOT / "deploy" / "postgres").exists()


def test_alembic_reads_the_dsn_from_settings_not_from_the_ini():
    ini = (REPO_ROOT / "alembic.ini").read_text(encoding="utf-8")
    # The file *mentions* sqlalchemy.url in a comment explaining its absence;
    # what must not exist is a line that actually sets it.
    active = [
        line for line in ini.splitlines() if line.strip() and not line.lstrip().startswith("#")
    ]
    assert not any(line.startswith("sqlalchemy.url") for line in active), (
        "a URL in alembic.ini is a second place for the DSN to drift from the engines'"
    )
    assert "settings.postgres.dsn" in (MIGRATIONS / "env.py").read_text(encoding="utf-8")


@pytest.mark.parametrize("model", [SignalAudit, BacktestRun, EngineEvent])
def test_every_top_level_table_records_when_its_row_was_written(model):
    assert "created_at" in model.__table__.columns


def test_a_backtest_trade_is_timestamped_by_the_market_not_the_clock():
    # Child rows of a run: what matters is when the trade opened and closed,
    # and the run's created_at already records when the replay was written.
    columns = BacktestTrade.__table__.columns
    assert "opened_at" in columns and "closed_at" in columns
    assert "created_at" not in columns


def test_backtest_trades_are_never_lazy_loaded_by_accident():
    """A run leaves its repository detached, so a lazy `.trades` is always a bug.

    The listing returns runs after the session has closed. Left on the default
    strategy, `run.trades` would try to emit SQL — outside a session it fails
    confusingly, inside one it fires a silent SELECT per run.
    """
    assert BacktestRun.trades.property.lazy == "raise_on_sql"


def test_the_listing_can_be_asked_for_trades_explicitly():
    import inspect

    signature = inspect.signature(BacktestRepository.list_backtests)
    parameter = signature.parameters["with_trades"]
    assert parameter.default is False, "loading every trade must be opt-in"
    assert parameter.kind is inspect.Parameter.KEYWORD_ONLY


def test_the_schema_needs_no_postgres_extensions():
    """Stock postgres is enough — nothing in the migrations installs an extension."""
    versions = sorted((MIGRATIONS / "versions").glob("*.py"))
    assert versions, "no migrations found"
    for path in versions:
        source = path.read_text(encoding="utf-8").lower()
        assert "create extension" not in source, (
            f"{path.name} installs an extension; the stack runs on postgres:16-alpine"
        )


def test_compose_pins_a_stock_postgres_image():
    compose = (REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    assert "pgvector/pgvector" not in compose
    assert "image: postgres:" in compose
