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

    def __init__(self, failure: Exception | None = None) -> None:
        self.published: list[tuple[str, dict, str | None]] = []
        self.failure = failure
        self.connected = False

    async def connect(self) -> None:
        self.connected = True

    async def close(self) -> None:
        self.connected = False

    async def publish_jetstream(self, subject, payload, *, msg_id=None, timeout=None):
        self.published.append((subject, payload, msg_id))
        if self.failure:
            raise self.failure
        return FakeAck()


def _signal(action: SignalAction = SignalAction.LONG) -> BrokerSignal:
    return BrokerSignal(
        strategy="MT5_GOLD_SCALP",
        symbol="XAUUSD",
        timeframe="15",
        timestamp=datetime(2026, 5, 1, tzinfo=UTC),
        position=PositionBlock(action=action, price=2000.0, quantity=1.0, sl=1990.0),
        token="tok",
    )


async def test_shadow_mode_builds_and_logs_but_never_sends(monkeypatch):
    from qte_shared.config import settings

    monkeypatch.setattr(settings.state_config, "execution_mode", "shadow")
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
    assert subject == "SIGNALS.MT5_GOLD_SCALP"
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


async def test_retrying_the_same_signal_reuses_the_same_dedup_id():
    bus = FakeBus()
    sink = BrokerSink(transport="nats", bus=bus, shadow_mode=False)
    await sink.start()
    signal = _signal()

    await sink.send(signal)
    await sink.send(signal)

    assert bus.published[0][2] == bus.published[1][2]


async def test_an_outbox_delivery_id_is_forwarded_to_jetstream():
    bus = FakeBus()
    sink = BrokerSink(transport="nats", bus=bus, shadow_mode=False)
    await sink.start()

    await sink.send(_signal(), delivery_id="stable-row-id")

    assert bus.published[0][2] == "stable-row-id"


async def test_a_delivery_failure_is_reported_not_raised():
    # Raising here would take the runner down and stop every other strategy.
    sink = BrokerSink(
        transport="nats",
        bus=FakeBus(failure=ConnectionError("NATS is not connected")),
        shadow_mode=False,
    )
    await sink.start()

    result = await sink.send(_signal())

    assert result.status == "failed"
    assert "not connected" in result.detail


async def test_a_timeout_is_ambiguous_instead_of_a_definite_failure():
    sink = BrokerSink(
        transport="nats",
        bus=FakeBus(failure=TimeoutError("ack timed out")),
        shadow_mode=False,
    )
    await sink.start()

    result = await sink.send(_signal(), delivery_id="stable-row-id")

    assert result.status == "unknown"
    assert "timed out" in result.detail


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
    # Refused, not raised: nothing was sent, so the outcome is a definite
    # `failed` rather than an exception the runner would record as `unknown`.
    result = await sink.send(naked)
    assert result.status == "failed"
    assert "price and quantity" in result.detail
    assert bus.published == []


async def test_the_shadow_switch_flips_at_runtime():
    bus = FakeBus()
    sink = BrokerSink(transport="nats", bus=bus, shadow_mode=True)
    await sink.start()

    await sink.send(_signal())
    sink.set_shadow_mode(False)
    await sink.send(_signal())

    assert len(bus.published) == 1


@pytest.fixture(autouse=True)
def live_state_scope(monkeypatch):
    """Exercise broker paths with an explicitly selected live book and fake transports."""
    from qte_shared.config import settings

    monkeypatch.setattr(settings, "env", "prod")
    monkeypatch.setattr(settings.state_config, "execution_mode", "live")


# ── HTTP: what counts as "nothing reached the broker" ────────────────


def _http_sink(handler) -> BrokerSink:
    import httpx

    sink = BrokerSink(transport="http", shadow_mode=False)
    sink._http = httpx.AsyncClient(
        base_url="http://broker.test", transport=httpx.MockTransport(handler)
    )
    return sink


def _raising(error_type):
    import httpx

    def handler(request: httpx.Request) -> httpx.Response:
        raise error_type("boom", request=request)

    return handler


def _answering(status_code: int):
    import httpx

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code, request=request)

    return handler


@pytest.mark.parametrize(
    ("handler_name", "expected"),
    [
        ("ConnectError", "failed"),
        ("ConnectTimeout", "failed"),
        ("ReadTimeout", "unknown"),
        ("ReadError", "unknown"),
        ("RemoteProtocolError", "unknown"),
        ("WriteError", "unknown"),
    ],
)
async def test_an_http_transport_error_is_unknown_unless_nothing_left(
    monkeypatch, handler_name, expected
):
    import httpx

    from qte_shared.config import settings

    monkeypatch.setattr(settings.state_config, "execution_mode", "live")
    sink = _http_sink(_raising(getattr(httpx, handler_name)))
    result = await sink.send(_signal())
    assert result.status == expected


@pytest.mark.parametrize(
    ("status_code", "expected"),
    [(200, "sent"), (400, "failed"), (422, "failed"), (502, "unknown"), (504, "unknown")],
)
async def test_an_http_answer_is_classified_by_who_could_have_acted(
    monkeypatch, status_code, expected
):
    from qte_shared.config import settings

    monkeypatch.setattr(settings.state_config, "execution_mode", "live")
    sink = _http_sink(_answering(status_code))
    result = await sink.send(_signal())
    assert result.status == expected
