"""Let one (strategy, symbol) pair hold several open cycles.

``uq_open_positions_pair`` allowed one row per pair, so a pair mapped with
``allow_multiple_cycles`` could not persist its second position. It becomes a
plain index. How many cycles a pair may hold is configuration the database
cannot see, so the signal factory enforces that limit; the table keeps the rule
that holds for every pair, one row per cycle id (``uq_open_positions_uxid``),
which is also the conflict target of the repository's upsert from now on.

Downgrading refuses to run while any pair holds more than one row: which of the
positions is kept is a question for an operator and the broker, not a migration.
"""

from collections.abc import Sequence

import sqlalchemy as sqlalchemy
from alembic import context, op

revision: str = "d5a2c8e7f913"
down_revision: str | None = "c3517af4d81b"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SHARED_PAIRS_QUERY = """
SELECT namespace, strategy, symbol, count(*) AS cycles
FROM open_positions
GROUP BY namespace, strategy, symbol
HAVING count(*) > 1
"""


def upgrade() -> None:
    op.drop_constraint("uq_open_positions_pair", "open_positions", type_="unique")
    op.create_index("ix_open_positions_pair", "open_positions", ["namespace", "strategy", "symbol"])


def downgrade() -> None:
    # Offline (`--sql`) there are no rows to inspect; the generated script is
    # read by whoever runs it, against a book they have reconciled.
    shared = (
        []
        if context.is_offline_mode()
        else op.get_bind().execute(sqlalchemy.text(SHARED_PAIRS_QUERY)).fetchall()
    )
    if shared:
        listed = ", ".join(
            f"{row.strategy}/{row.symbol} in {row.namespace} ({row.cycles} cycles)"
            for row in shared
        )
        raise RuntimeError(
            "Close the extra cycles before restoring one open position per pair: "
            f"{listed}. Reconcile with the broker, then run this downgrade again."
        )
    op.drop_index("ix_open_positions_pair", table_name="open_positions")
    op.create_unique_constraint(
        "uq_open_positions_pair", "open_positions", ["namespace", "strategy", "symbol"]
    )
