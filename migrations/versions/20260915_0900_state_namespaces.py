"""Isolate state by environment, execution mode and market-data provider.

Existing rows remain in the legacy namespace for explicit reconciliation.
They cannot become broker positions merely by selecting live mode.
"""

from collections.abc import Sequence

import sqlalchemy as sqlalchemy
from alembic import op

revision: str = "b86f190e2c34"
down_revision: str | None = "4a91c6e3b70f"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

STATE_TABLES = ("signals", "open_positions", "engine_events", "backtest_runs")


def upgrade() -> None:
    for table_name in STATE_TABLES:
        op.add_column(
            table_name,
            sqlalchemy.Column(
                "namespace", sqlalchemy.String(160), nullable=False, server_default="legacy"
            ),
        )
        op.create_index(f"ix_{table_name}_namespace", table_name, ["namespace"])
    op.drop_constraint("uq_open_positions_pair", "open_positions", type_="unique")
    op.create_unique_constraint(
        "uq_open_positions_pair", "open_positions", ["namespace", "strategy", "symbol"]
    )


def downgrade() -> None:
    # Multiple scoped books can hold the same pair. Refuse an ambiguous merge.
    connection = op.get_bind()
    duplicates = connection.execute(
        sqlalchemy.text(
            "SELECT 1 FROM open_positions GROUP BY strategy, symbol HAVING count(*) > 1 LIMIT 1"
        )
    ).first()
    if duplicates:
        raise RuntimeError("Reconcile duplicate position pairs before removing namespaces")
    op.drop_constraint("uq_open_positions_pair", "open_positions", type_="unique")
    op.create_unique_constraint("uq_open_positions_pair", "open_positions", ["strategy", "symbol"])
    for table_name in reversed(STATE_TABLES):
        op.drop_index(f"ix_{table_name}_namespace", table_name=table_name)
        op.drop_column(table_name, "namespace")
