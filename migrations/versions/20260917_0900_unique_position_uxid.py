"""One pair per open cycle id, enforced by the database.

``signal_uxid`` carried a plain index, which enforced nothing. The broker groups
a whole trade by that id, so two pairs holding the same one would let a close on
either of them close the other's position — and nothing in the audit trail would
explain the loss.

Refuses to run while duplicates exist rather than failing halfway through
creating the constraint: the rows have to be reconciled by hand, and which of
the two positions is the real one is not a question a migration can answer.
"""

from collections.abc import Sequence

import sqlalchemy as sqlalchemy
from alembic import op

revision: str = "c3517af4d81b"
down_revision: str | None = "b86f190e2c34"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

DUPLICATE_QUERY = """
SELECT namespace, signal_uxid, count(*) AS holders
FROM open_positions
GROUP BY namespace, signal_uxid
HAVING count(*) > 1
"""


def upgrade() -> None:
    connection = op.get_bind()
    duplicates = connection.execute(sqlalchemy.text(DUPLICATE_QUERY)).fetchall()
    if duplicates:
        listed = ", ".join(
            f"{row.signal_uxid} in {row.namespace} ({row.holders} pairs)" for row in duplicates
        )
        raise RuntimeError(
            "Reconcile duplicate open-position cycle ids before making them unique: "
            f"{listed}. Decide which pair really holds each cycle, close or delete the "
            "other row, then run this migration again."
        )
    op.drop_index("ix_open_positions_uxid", table_name="open_positions")
    op.create_unique_constraint(
        "uq_open_positions_uxid", "open_positions", ["namespace", "signal_uxid"]
    )


def downgrade() -> None:
    op.drop_constraint("uq_open_positions_uxid", "open_positions", type_="unique")
    op.create_index("ix_open_positions_uxid", "open_positions", ["signal_uxid"], unique=False)
