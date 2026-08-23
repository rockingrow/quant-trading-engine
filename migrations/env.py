"""Alembic environment.

Two things here are load-bearing and easy to get wrong:

**Every engine's models must be imported.** Alembic autogenerate diffs the
database against ``Base.metadata``, and a model class that was never imported is
not on that metadata. The failure is not a crash — it is a migration that
silently omits the table, and a later autogenerate that sees the table in the
database, cannot find it in the metadata, and proposes ``DROP TABLE``. So the
imports below are not incidental; adding an engine that owns tables means adding
it here.

**The URL comes from settings, not from alembic.ini.** Migrations must run
against exactly the database the engines use. Two places to configure it is one
place for it to drift.
"""

from __future__ import annotations

import asyncio
from logging.config import fileConfig

import qte_backtest.db.models  # noqa: F401,E402  (backtest_runs, backtest_trades)

# ── Model registration ────────────────────────────────────────────────
# Imported for the side effect of registering tables on Base.metadata.
import qte_shared.db.models  # noqa: F401,E402  (shared: engine_events)
import qte_strategy_engine.db.models  # noqa: F401,E402  (signals)
from alembic import context
from qte_shared.config import settings
from qte_shared.db.base import Base
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def _database_url() -> str:
    """The engines' DSN, with the async driver kept — we use an async engine."""
    return settings.postgres.dsn


def _include_object(obj, name, type_, reflected, compare_to) -> bool:
    """Keep autogenerate from proposing changes to things we do not manage.

    ``signals.embedding`` is a pgvector column created by migration and never
    mapped on the ORM side, because nothing in QTE writes it — it exists so an
    agent can embed a signal's context later. Unmapped means autogenerate sees
    it as an extra column and offers to drop it, which would silently discard
    embeddings on the next migration.
    """
    if type_ == "column" and reflected and name == "embedding":
        return False
    return True


def run_migrations_offline() -> None:
    """Emit SQL to stdout without connecting — for review or a DBA handoff."""
    context.configure(
        url=_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_object=_include_object,
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def _run(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        include_object=_include_object,
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


async def _run_async() -> None:
    section = config.get_section(config.config_ini_section, {})
    section["sqlalchemy.url"] = _database_url()
    engine = async_engine_from_config(section, prefix="sqlalchemy.", poolclass=pool.NullPool)
    async with engine.connect() as connection:
        await connection.run_sync(_run)
    await engine.dispose()


def run_migrations_online() -> None:
    asyncio.run(_run_async())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
