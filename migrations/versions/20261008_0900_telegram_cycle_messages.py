"""Hold the Telegram message that represents each trade cycle.

One message per chat is edited in place as a position progresses, so the chat
needs the ``message_id`` to rewrite and the events to re-render the body from.
Both have to outlive a restart: a runner that forgot them would post a second
message for a cycle already on screen and lose its entry block.

Keyed by (namespace, strategy, symbol, ``signal_uxid``) — the triple the broker
groups a broadcast by, plus the state namespace so a paper book and a live one
never edit each other's messages.
"""

from collections.abc import Sequence

import sqlalchemy as sqlalchemy
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "e6b3d9a1c247"
down_revision: str | None = "d5a2c8e7f913"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "telegram_cycle_messages",
        sqlalchemy.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sqlalchemy.Column(
            "namespace",
            sqlalchemy.String(160),
            nullable=False,
            server_default="legacy",
        ),
        sqlalchemy.Column(
            "created_at",
            sqlalchemy.DateTime(timezone=True),
            server_default=sqlalchemy.func.now(),
            nullable=False,
        ),
        sqlalchemy.Column(
            "updated_at",
            sqlalchemy.DateTime(timezone=True),
            server_default=sqlalchemy.func.now(),
            nullable=False,
        ),
        sqlalchemy.Column("strategy", sqlalchemy.String(128), nullable=False),
        sqlalchemy.Column("symbol", sqlalchemy.String(64), nullable=False),
        sqlalchemy.Column("signal_uxid", sqlalchemy.String(32), nullable=False),
        sqlalchemy.Column("timeframe", sqlalchemy.String(16), nullable=False, server_default=""),
        sqlalchemy.Column(
            "status", sqlalchemy.String(16), nullable=False, server_default="RUNNING"
        ),
        sqlalchemy.Column(
            "events",
            postgresql.JSONB,
            nullable=False,
            server_default=sqlalchemy.text("'[]'::jsonb"),
        ),
        sqlalchemy.Column(
            "messages",
            postgresql.JSONB,
            nullable=False,
            server_default=sqlalchemy.text("'{}'::jsonb"),
        ),
        sqlalchemy.UniqueConstraint(
            "namespace",
            "strategy",
            "symbol",
            "signal_uxid",
            name="uq_telegram_cycle_messages_cycle",
        ),
    )
    op.create_index(
        "ix_telegram_cycle_messages_namespace", "telegram_cycle_messages", ["namespace"]
    )


def downgrade() -> None:
    # Nothing but notification bookkeeping lives here: dropping it loses the
    # ability to edit messages already in a chat, no trade state.
    op.drop_index("ix_telegram_cycle_messages_namespace", table_name="telegram_cycle_messages")
    op.drop_table("telegram_cycle_messages")
