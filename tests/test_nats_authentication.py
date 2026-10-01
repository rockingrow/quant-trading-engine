"""NATS is on the tailnet, so every connection has to carry a token.

The bus used to treat the token as optional: a blank one simply produced an
anonymous connection, which was harmless while the port only existed inside the
compose network. Over Tailscale every device can dial it, so an empty token is
a misconfiguration to refuse rather than a default to accept. These tests pin
the refusal and the pass-through, because both are one `if` away from silently
regressing.
"""

from __future__ import annotations

import pytest

from qte_shared.bus import NatsBus
from qte_shared.bus import nats_bus as nats_module
from qte_shared.config import NatsSettings


class RecordingClient:
    """Stands in for a connected ``nats.aio.client.Client``."""

    is_connected = True


async def test_connecting_without_a_token_is_refused_before_dialling(monkeypatch):
    async def refuse_to_be_called(**keywords):  # pragma: no cover - must not run
        raise AssertionError("connect() dialled NATS without a token")

    monkeypatch.setattr(nats_module.nats, "connect", refuse_to_be_called)
    bus = NatsBus(url="nats://quanghuynhpc:4222", token="")

    with pytest.raises(RuntimeError, match="QTE_NATS__TOKEN"):
        await bus.connect()


async def test_the_token_reaches_the_client_options(monkeypatch):
    captured: dict[str, object] = {}

    async def capture(**keywords):
        captured.update(keywords)
        return RecordingClient()

    monkeypatch.setattr(nats_module.nats, "connect", capture)
    bus = NatsBus(url="nats://quanghuynhpc:4222", token="tailnet-secret")

    await bus.connect()

    assert captured["token"] == "tailnet-secret"
    assert captured["servers"] == ["nats://quanghuynhpc:4222"]


def test_the_default_url_is_the_tailnet_host_not_localhost():
    assert NatsSettings(_env_file=None).url == "nats://quanghuynhpc:4222"
