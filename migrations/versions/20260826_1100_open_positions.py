"""``open_positions``: the trade cycle each (strategy, symbol) pair is holding.

Redis already carried the cycle id, and a flushed cache made the runner forget
it — at which point the next entry mints a new cycle and the broker is left
holding a position nobody will ever close. This is the durable copy, and the
unique constraint is what makes "one live cycle per pair" a fact about the
database rather than a rule the runner has to keep remembering.

Revision ID: 4a91c6e3b70f
Revises: 7d2d0fa9cd86
Created: 2026-08-26
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "4a91c6e3b70f"
down_revision: str | None = "7d2d0fa9cd86"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "open_positions",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("strategy", sa.String(length=128), nullable=False),
        sa.Column("symbol", sa.String(length=64), nullable=False),
        sa.Column("signal_uxid", sa.String(length=32), nullable=False),
        sa.Column("action", sa.String(length=16), nullable=False),
        sa.Column("opened_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("price", sa.Float(), nullable=True),
        sa.Column("quantity", sa.Float(), nullable=True),
        sa.Column("remaining", sa.Float(), nullable=True),
        sa.Column("sl", sa.Float(), nullable=True),
        sa.Column("tp1", sa.Float(), nullable=True),
        sa.Column("tp2", sa.Float(), nullable=True),
        sa.Column("state", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("strategy", "symbol", name="uq_open_positions_pair"),
    )
    op.create_index("ix_open_positions_uxid", "open_positions", ["signal_uxid"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_open_positions_uxid", table_name="open_positions")
    op.drop_table("open_positions")
