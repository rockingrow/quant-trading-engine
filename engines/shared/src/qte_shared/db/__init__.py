"""Shared database plumbing: the declarative base, the engine, and lifecycle events.

Each engine owns its own tables under its own ``db`` package
(``qte_strategy_engine.db``, ``qte_backtest.db``); what lives here is what they
all need — the connection, the session factory, the one base their models share,
and the ``engine_events`` table nobody owns alone.
"""

from qte_shared.db.base import Base, new_uuid
from qte_shared.db.models import EngineEvent
from qte_shared.db.repository import EventRepository
from qte_shared.db.session import Database, get_database

__all__ = [
    "Base",
    "Database",
    "EngineEvent",
    "EventRepository",
    "get_database",
    "new_uuid",
]
