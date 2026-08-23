"""Delivery behaviour — including the shadow-mode safety catch."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from qte_shared.models import BrokerSignal, PositionBlock, SignalAction
from qte_strategy_engine.broker_sink import BrokerSink


class FakeAck:
    seq = 42
    duplicate = False


class FakeBus:
    """Records what would have gone to JetStream."""

    def __init__(self, fail: bool = False) -> None:
        self.published: list[tuple[str, dict, str | None]] = []
        self.fail = fail
        self.connected = False

    async def connect(self) -> None:
        self.connected = True

    async def close(self) -> None:
        self.connected = False

    async def publish_jetstream(self, subject, payload, *, msg_id=None, timeout=None):
        if self.fail:
            raise ConnectionError("NATS is not connected")
        self.published.append((subject, payload, msg_id))
        return FakeAck()


def _signal(action: SignalAction = SignalAction.LONG) -> BrokerSignal:
    return BrokerSignal(
        strategy="MT5_GOLD_M5_V1",
        symbol="XAUUSD",
        timeframe="15",
        timestamp=datetime(2026, 5, 1, tzinfo=UTC),
        position=PositionBlock(action=action, price=2000.0, quantity=1.0, sl=1990.0),
        token="tok",
    )


async def test_shadow_mode_builds_and_logs_but_never_sends():
    bus = FakeBus()
    sink = BrokerSink(transport="nats", bus=bus, shadow_mode=True)
    await sink.start()

    result = await sink.send(_signal())

    assert result.status == "shadow"
    assert result.delivered is False
    assert bus.published == []


async def test_live_send_goes_to_the_strategys_own_subject():
    bus = FakeBus()
    sink = BrokerSink(transport="nats", bus=bus, shadow_mode=False)
    await sink.start()

    result = await sink.send(_signal())

    assert result.status == "sent"
    subject, payload, msg_id = bus.published[0]
    # Workers subscribe per strategy; the subject is the strategy name.
    assert subject == "SIGNALS.MT5_GOLD_M5_V1"
    assert list(payload) == ["payload"]
    assert payload["payload"]["symbol"] == "XAUUSD"
    assert msg_id  # Nats-Msg-Id, so a retried publish de-duplicates


async def test_each_signal_carries_its_own_dedup_id():
    bus = FakeBus()
    sink = BrokerSink(transport="nats", bus=bus, shadow_mode=False)
    await sink.start()

    await sink.send(_signal())
    await sink.send(_signal(SignalAction.TP1))

    assert bus.published[0][2] != bus.published[1][2]


async def test_a_delivery_failure_is_reported_not_raised():
    # Raising here would take the runner down and stop every other strategy.
    sink = BrokerSink(transport="nats", bus=FakeBus(fail=True), shadow_mode=False)
    await sink.start()

    result = await sink.send(_signal())

    assert result.status == "failed"
    assert "not connected" in result.detail


async def test_an_invalid_signal_is_rejected_before_any_transport_runs():
    bus = FakeBus()
    sink = BrokerSink(transport="nats", bus=bus, shadow_mode=False)
    await sink.start()

    naked = BrokerSignal(
        strategy="s",
        symbol="XAUUSD",
        timeframe="15",
        position=PositionBlock(action=SignalAction.LONG, price=2000.0),
        token="t",
    )
    with pytest.raises(ValueError):
        await sink.send(naked)
    assert bus.published == []


async def test_the_shadow_switch_flips_at_runtime():
    bus = FakeBus()
    sink = BrokerSink(transport="nats", bus=bus, shadow_mode=True)
    await sink.start()

    await sink.send(_signal())
    sink.set_shadow_mode(False)
    await sink.send(_signal())

    assert len(bus.published) == 1
