"""Async SQLAlchemy engine/session management."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from functools import lru_cache

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from qte_shared.config import settings
from qte_shared.db.models import Base
from qte_shared.logging_setup import get_logger

log = get_logger(__name__)


class Database:
    """Owns the engine and hands out sessions.

    Nothing here is on the trading hot path: a signal is published to the
    broker first and audited second, so a slow or unreachable Postgres delays a
    log line, never a trade.
    """

    def __init__(self, dsn: str | None = None) -> None:
        self._dsn = dsn or settings.postgres.dsn
        self._engine: AsyncEngine | None = None
        self._sessionmaker: async_sessionmaker[AsyncSession] | None = None

    @property
    def engine(self) -> AsyncEngine:
        if self._engine is None:
            self._engine = create_async_engine(
                self._dsn,
                echo=settings.postgres.echo,
                pool_size=settings.postgres.pool_size,
                max_overflow=settings.postgres.max_overflow,
                pool_pre_ping=True,
            )
            self._sessionmaker = async_sessionmaker(
                self._engine, expire_on_commit=False, class_=AsyncSession
            )
        return self._engine

    @property
    def sessionmaker(self) -> async_sessionmaker[AsyncSession]:
        self.engine  # noqa: B018 - force lazy construction
        assert self._sessionmaker is not None
        return self._sessionmaker

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        async with self.sessionmaker() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    async def create_all(self) -> None:
        """Create tables if missing.

        Convenience for tests and a bare local run. In Docker the schema comes
        from ``deploy/postgres/init/001_schema.sql``, which also installs
        pgvector and the ``embedding`` column this metadata does not describe.
        """
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        log.info("Audit schema ensured")

    async def ping(self) -> bool:
        try:
            async with self.engine.connect() as connection:
                await connection.exec_driver_sql("SELECT 1")
            return True
        except Exception as exc:
            log.warning("Postgres ping failed: %s", exc)
            return False

    async def close(self) -> None:
        if self._engine is not None:
            await self._engine.dispose()
            self._engine = None
            self._sessionmaker = None


@lru_cache(maxsize=1)
def get_database() -> Database:
    return Database()
