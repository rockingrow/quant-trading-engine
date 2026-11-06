"""The outbox transition table admits every move the runner makes, and no other."""

from __future__ import annotations

import pytest

from qte_strategy_engine.delivery_status import PREDECESSORS, UNFINISHED, DeliveryStatus


def may_follow(previous: str, status: str) -> bool:
    """Read the table the way ``mark_delivery``'s conditional update does."""
    return DeliveryStatus(previous) in PREDECESSORS[DeliveryStatus(status)]


#: Every (from, to) the runner, its recovery path and the orphan FLAT write.
RUNNER_MOVES = [
    ("prepared", "unknown"),  # live: marked before crossing the broker boundary
    ("unknown", "sent_pending"),
    ("sent_pending", "sent"),
    ("unknown", "failed"),
    ("unknown", "unknown"),  # an ambiguous send finishing ambiguous
    ("prepared", "shadow_pending"),  # paper: never marked unknown
    ("shadow_pending", "shadow"),
    ("pending", "unknown"),  # a legacy row resent inside the retry horizon
    ("sent", "sent_pending"),  # re-checkpoint after a lost final acknowledgement
    ("shadow", "shadow_pending"),
    ("failed", "failed"),
]


@pytest.mark.parametrize(("previous", "status"), RUNNER_MOVES)
def test_every_move_the_runner_makes_is_allowed(previous, status):
    assert may_follow(previous, status)


@pytest.mark.parametrize(
    ("previous", "status"),
    [
        ("prepared", "sent"),  # skipped the pre-send marker
        ("sent", "failed"),
        ("shadow", "sent"),  # paper can never become live
        ("sent", "shadow"),
        ("failed", "sent"),
        ("unknown", "prepared"),
    ],
)
def test_moves_that_would_misstate_a_signal_are_refused(previous, status):
    assert not may_follow(previous, status)


def test_every_status_has_a_row_in_the_table():
    assert set(PREDECESSORS) == set(DeliveryStatus)


def test_the_statuses_equal_what_is_stored():
    assert DeliveryStatus.SENT_PENDING == "sent_pending"
    assert DeliveryStatus("shadow").checkpoint is DeliveryStatus.SHADOW_PENDING
    assert DeliveryStatus.SENT_PENDING.settled is DeliveryStatus.SENT
    assert DeliveryStatus.SENT not in UNFINISHED
