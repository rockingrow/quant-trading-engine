"""Add pgvector and the signals.embedding column.

Split from the core schema on purpose. Nothing in QTE writes ``embedding`` — it
exists so an AI agent can embed a signal's context later and ask "which past
trades looked like this one". Keeping it in its own revision means:

* the engine runs on a stock ``postgres`` image if you do not want vector
  search — apply revision one and stop;
* the requirement on ``pgvector`` is visible as a discrete step rather than
  buried in the table that holds the audit trail.

The column stays unmapped on the ORM side (see ``migrations/env.py``, which
tells autogenerate not to propose dropping it).

Revision ID: 4a1c9e77b3d2
Revises: 7d2d0fa9cd86
Created: 2026-08-23
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "4a1c9e77b3d2"
down_revision: str | None = "7d2d0fa9cd86"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

#: Matches the common text-embedding width. Change it before storing anything,
#: not after — pgvector fixes the dimension at column definition.
EMBEDDING_DIMENSIONS = 1536


def upgrade() -> None:
    connection = op.get_bind()
    available = connection.execute(
        sa.text("SELECT 1 FROM pg_available_extensions WHERE name = 'vector'")
    ).scalar()
    if not available:
        # Postgres would otherwise fail with "could not open extension control
        # file", which says nothing about what to install or which image to use.
        raise RuntimeError(
            "pgvector is not available on this server. Use the pgvector/pgvector "
            "image (docker-compose.yml already does), or install the extension. "
            "To run without vector search, stay on revision 7d2d0fa9cd86."
        )

    op.execute("CREATE EXTENSION IF NOT EXISTS vector")
    op.execute(f"ALTER TABLE signals ADD COLUMN embedding vector({EMBEDDING_DIMENSIONS})")


def downgrade() -> None:
    op.execute("ALTER TABLE signals DROP COLUMN IF EXISTS embedding")
    # The extension is left installed: other schemas in the same database may
    # be using it, and dropping it would take their columns with it.
