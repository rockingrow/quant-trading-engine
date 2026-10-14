"""Closing a NATS connection must not look like an outage.

``NatsBus.close`` drains rather than closes hard, and nats-py answers a drain
that ran out of time by calling the error callback and closing anyway. That
path used to be logged at ERROR, which the Telegram forwarder turned into an
operator notification every time a subscription — a JetStream pull consumer
with a fetch in flight, typically — was slow to let go, while the feed had
already reconnected and was delivering bars. These tests pin the bounded
timeout and the quieter log, because both are one line away from regressing.
"""

from __future__ import annotations

import logging

from nats import errors as nats_errors

from qte_shared.bus import NatsBus
from qte_shared.bus import nats_bus as nats_module
from qte_shared.config import NatsSettings


class RecordingClient:
    """Stands in for a connected ``nats.aio.client.Client``."""

    is_connected = True


async def test_the_drain_timeout_reaches_the_client_options(monkeypatch):
    captured: dict[str, object] = {}

    async def capture(**keywords):
        captured.update(keywords)
        return RecordingClient()

    monkeypatch.setattr(nats_module.nats, "connect", capture)
    monkeypatch.setattr(nats_module.settings.nats, "drain_timeout", 5.0)
    bus = NatsBus(url="nats://engine-node:4222", token="tailnet-secret")

    await bus.connect()

    assert captured["drain_timeout"] == 5.0


def test_the_default_drain_timeout_is_shorter_than_the_nats_py_default():
    # nats-py defaults to 30s, long enough to stall every reconnect.
    assert NatsSettings(_env_file=None).drain_timeout == 5.0


async def test_a_drain_timeout_while_closing_is_only_a_warning(caplog):
    bus = NatsBus(url="nats://engine-node:4222", token="tailnet-secret", name="qte-feed")
    bus._closing = True

    with caplog.at_level(logging.WARNING, logger="qte_shared.bus.nats_bus"):
        await bus._on_error(nats_errors.DrainTimeoutError())

    assert [record.levelno for record in caplog.records] == [logging.WARNING]
    assert "qte-feed" in caplog.records[0].getMessage()


async def test_any_other_error_is_still_reported_at_error_level(caplog):
    bus = NatsBus(url="nats://engine-node:4222", token="tailnet-secret", name="qte-feed")
    bus._closing = True

    with caplog.at_level(logging.WARNING, logger="qte_shared.bus.nats_bus"):
        await bus._on_error(nats_errors.NoServersError())

    assert [record.levelno for record in caplog.records] == [logging.ERROR]


async def test_a_drain_timeout_outside_a_deliberate_close_is_still_an_error(caplog):
    bus = NatsBus(url="nats://engine-node:4222", token="tailnet-secret", name="qte-feed")

    with caplog.at_level(logging.WARNING, logger="qte_shared.bus.nats_bus"):
        await bus._on_error(nats_errors.DrainTimeoutError())

    assert [record.levelno for record in caplog.records] == [logging.ERROR]
