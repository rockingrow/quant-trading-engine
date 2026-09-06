"""The single declarative base every engine's models hang off.

It lives here, alone, for one reason: Alembic autogenerate compares the database
against ``Base.metadata``, and that comparison is only correct if every table in
the system is registered on the *same* metadata object. An engine that declared
its own base would be invisible to autogenerate — its tables would never be
created, and worse, a migration generated later would see them as unknown and
propose dropping them.

So each engine owns its models but borrows this base.
"""

from __future__ import annotations

import uuid

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Declarative base shared by qte_shared, qte_strategy_engine and qte_backtest."""


def new_uuid() -> uuid.UUID:
    """Default for UUID primary keys, applied client-side.

    Generating the id in Python rather than leaning on a server default means a
    row's id is known before the INSERT returns, which is what lets the audit
    write log it without a round trip.
    """
    return uuid.uuid4()
