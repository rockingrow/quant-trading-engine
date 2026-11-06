"""The states a signal's outbox row moves through, and the moves allowed.

Every live signal is staged as a Postgres row before the broker is called, and
that row's ``delivery_status`` is what a restart reads to decide whether to
resend, reconcile or leave a signal alone. Those statuses used to be bare
strings spread over the runner and the repository, with the legal moves between
them implied by a run of ``if status in {...}`` checks. They are named here once,
as a ``StrEnum`` — equal to the strings already stored, so no migration — and the
moves are a table :meth:`SignalRepository.mark_delivery` enforces.

The table fails closed. A move it does not list updates no row, the repository
reports the write as failed, and the runner keeps the pair blocked for
reconciliation — a stuck pair is recoverable, a signal recorded in the wrong
state is a trade decided on a lie.

    prepared ──► unknown ──► sent_pending ──► sent        (live)
        │           └──────► failed
        └──────► shadow_pending ──► shadow                (paper)

Self-moves are allowed so an interrupted checkpoint can be repeated, and so is
``sent → sent_pending`` (``shadow → shadow_pending``): a final mark that
committed but reported failure leaves the result in memory, and the recovery
path writes the checkpoint again before finishing. ``pending`` is the legacy
spelling of ``unknown`` that earlier runners wrote; it is read, never written.
"""

from __future__ import annotations

from enum import StrEnum


class DeliveryStatus(StrEnum):
    PREPARED = "prepared"
    PENDING = "pending"
    UNKNOWN = "unknown"
    SENT_PENDING = "sent_pending"
    SHADOW_PENDING = "shadow_pending"
    SENT = "sent"
    SHADOW = "shadow"
    FAILED = "failed"

    @property
    def checkpoint(self) -> DeliveryStatus:
        """The ``*_pending`` checkpoint written before a delivered outcome is final."""
        if self is DeliveryStatus.SENT:
            return DeliveryStatus.SENT_PENDING
        if self is DeliveryStatus.SHADOW:
            return DeliveryStatus.SHADOW_PENDING
        raise ValueError(f"{self.value!r} has no checkpoint; only sent and shadow do")

    @property
    def settled(self) -> DeliveryStatus:
        """The outcome a ``*_pending`` checkpoint stands for."""
        if self is DeliveryStatus.SENT_PENDING:
            return DeliveryStatus.SENT
        if self is DeliveryStatus.SHADOW_PENDING:
            return DeliveryStatus.SHADOW
        raise ValueError(f"{self.value!r} is not a checkpoint")


#: Rows whose final outcome is not known yet: what the outbox scan reads.
UNFINISHED = frozenset(
    {
        DeliveryStatus.PREPARED,
        DeliveryStatus.PENDING,
        DeliveryStatus.UNKNOWN,
        DeliveryStatus.SENT_PENDING,
        DeliveryStatus.SHADOW_PENDING,
    }
)
#: Outcomes the broker (or the paper book) took: these commit the position.
DELIVERED = frozenset({DeliveryStatus.SENT, DeliveryStatus.SHADOW})
#: Final outcomes; a row in one is done unless its result is still in memory.
SETTLED = frozenset({DeliveryStatus.SENT, DeliveryStatus.SHADOW, DeliveryStatus.FAILED})
#: A send whose result is not known: may or may not have reached the broker.
AMBIGUOUS = frozenset({DeliveryStatus.PENDING, DeliveryStatus.UNKNOWN})
#: Statuses only a paper book writes, and only a live book writes.
PAPER = frozenset({DeliveryStatus.SHADOW, DeliveryStatus.SHADOW_PENDING})
LIVE = frozenset({DeliveryStatus.SENT, DeliveryStatus.SENT_PENDING})

#: For each status, the statuses a row may hold immediately before it.
PREDECESSORS: dict[DeliveryStatus, frozenset[DeliveryStatus]] = {
    DeliveryStatus.PREPARED: frozenset(),
    DeliveryStatus.PENDING: frozenset(),
    DeliveryStatus.UNKNOWN: frozenset(
        {DeliveryStatus.PREPARED, DeliveryStatus.PENDING, DeliveryStatus.UNKNOWN}
    ),
    DeliveryStatus.SENT_PENDING: frozenset(
        {
            DeliveryStatus.UNKNOWN,
            DeliveryStatus.PENDING,
            DeliveryStatus.SENT_PENDING,
            DeliveryStatus.SENT,
        }
    ),
    DeliveryStatus.SHADOW_PENDING: frozenset(
        {
            DeliveryStatus.PREPARED,
            DeliveryStatus.PENDING,
            DeliveryStatus.SHADOW_PENDING,
            DeliveryStatus.SHADOW,
        }
    ),
    DeliveryStatus.SENT: frozenset({DeliveryStatus.SENT_PENDING, DeliveryStatus.SENT}),
    DeliveryStatus.SHADOW: frozenset({DeliveryStatus.SHADOW_PENDING, DeliveryStatus.SHADOW}),
    DeliveryStatus.FAILED: frozenset(
        {
            DeliveryStatus.PREPARED,
            DeliveryStatus.PENDING,
            DeliveryStatus.UNKNOWN,
            DeliveryStatus.FAILED,
        }
    ),
}


def may_follow(previous: str, status: str) -> bool:
    """Whether a row holding *previous* may be marked *status*."""
    return DeliveryStatus(previous) in PREDECESSORS[DeliveryStatus(status)]


__all__ = [
    "AMBIGUOUS",
    "DELIVERED",
    "LIVE",
    "PAPER",
    "PREDECESSORS",
    "SETTLED",
    "UNFINISHED",
    "DeliveryStatus",
    "may_follow",
]
