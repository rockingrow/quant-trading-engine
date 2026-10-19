"""Outbox acknowledgements must reflect row updates and successful commits."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from qte_strategy_engine.db.repository import SignalRepository


class TransactionDatabase:
    def __init__(self, affected_rows, *, fail_commit=False):
        self.transaction = SimpleNamespace(
            execute=AsyncMock(return_value=SimpleNamespace(rowcount=affected_rows))
        )
        self.fail_commit = fail_commit

    @asynccontextmanager
    async def session(self):
        yield self.transaction
        if self.fail_commit:
            raise ConnectionError("Commit acknowledgement lost")


@pytest.mark.parametrize("affected_rows,expected", [(0, False), (1, True)])
async def test_mark_delivery_requires_an_existing_row(affected_rows, expected):
    repository = SignalRepository(TransactionDatabase(affected_rows))
    assert await repository.mark_delivery(str(uuid4()), status="sent") is expected


async def test_mark_delivery_does_not_acknowledge_a_failed_commit():
    repository = SignalRepository(TransactionDatabase(1, fail_commit=True))
    assert not await repository.mark_delivery(str(uuid4()), status="sent_pending")
