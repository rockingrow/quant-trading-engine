"""Pins the payload shape against ``rockingrow/algo-trading-broker``.

The examples asserted here are copied from that repo's ``examples/nats/`` and
``examples/webhook/`` fixtures. If one of these tests fails, QTE and the broker
have drifted and signals will be rejected at ingress — fix the model, do not
relax the test.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
from qte_shared.models import BrokerSignal, PositionBlock, SignalAction, is_valid_uxid, new_uxid


def _entry() -> BrokerSignal:
    return BrokerSignal(
        strategy="MT5_GOLD_M5_V1",
        symbol="XAUUSD",
        timeframe="15",
        timestamp=datetime(2026, 4, 10, 22, 55, tzinfo=UTC),
        signal_uxid="9F2C4B7E18A3D605",
        position=PositionBlock(
            action=SignalAction.LONG,
            price=2334.5,
            quantity=6.0,
            sl=2329.5,
            tp1=2340.0,
            tp2=2345.0,
            risk_percent=3.0,
            tp1_percent=50.0,
            move_sl_to_be=False,
        ),
        token="secret",
    )


def test_payload_carries_every_field_the_broker_validates():
    payload = _entry().model_dump(mode="json")
    assert set(payload) == {
        "strategy",
        "symbol",
        "timeframe",
        "timestamp",
        "signal_uxid",
        "position",
        "indicators",
        "inputs",
        "token",
    }
    assert set(payload["position"]) == {
        "action",
        "price",
        "quantity",
        "sl",
        "tp1",
        "tp2",
        "risk_percent",
        "tp1_percent",
        "move_sl_to_be",
        "is_running",
        "is_scale_position",
        "scale_strategy",
        "scaling",
    }


def test_action_is_serialised_as_the_broker_enum_string():
    payload = _entry().model_dump(mode="json")
    assert payload["position"]["action"] == "LONG"


def test_envelope_matches_the_jetstream_webhook_wrapper():
    # The broker's SignalWorker reads envelope["payload"], nothing else.
    envelope = _entry().to_envelope()
    assert list(envelope) == ["payload"]
    assert envelope["payload"]["strategy"] == "MT5_GOLD_M5_V1"
    json.dumps(envelope)  # must be plainly serialisable — no datetimes left


def test_uxid_shape_matches_the_brokers_validator():
    uxid = new_uxid()
    assert len(uxid) == 16
    assert uxid.isupper()
    assert is_valid_uxid(uxid)


def test_lowercase_uxid_is_normalised_not_rejected():
    signal = _entry().model_copy(update={})
    normalised = BrokerSignal(**{**signal.model_dump(), "signal_uxid": "9f2c4b7e18a3d605"})
    assert normalised.signal_uxid == "9F2C4B7E18A3D605"


@pytest.mark.parametrize("bad", ["short", "9f2c-4b7e-18a3-d605", "9F2C4B7E18A3D6055"])
def test_malformed_uxid_fails_here_rather_than_as_a_422(bad):
    with pytest.raises(ValueError):
        BrokerSignal(**{**_entry().model_dump(), "signal_uxid": bad})


def test_blank_uxid_is_treated_as_absent():
    signal = BrokerSignal(**{**_entry().model_dump(), "signal_uxid": "  "})
    assert is_valid_uxid(signal.signal_uxid)


def test_flat_needs_neither_price_nor_quantity():
    flat = BrokerSignal(
        strategy="MT5_GOLD_M5_V1",
        symbol="XAUUSD",
        timeframe="15",
        position=PositionBlock(action=SignalAction.FLAT),
        token="secret",
    )
    flat.validate_shape()  # must not raise — FLAT is a bare close-all directive


def test_entry_without_size_is_caught_before_it_reaches_the_wire():
    entry = BrokerSignal(
        strategy="s",
        symbol="XAUUSD",
        timeframe="15",
        position=PositionBlock(action=SignalAction.LONG, price=2000.0),
        token="t",
    )
    with pytest.raises(ValueError, match="price and quantity"):
        entry.validate_shape()


def test_entry_with_zero_quantity_is_rejected():
    entry = BrokerSignal(
        strategy="s",
        symbol="XAUUSD",
        timeframe="15",
        position=PositionBlock(action=SignalAction.LONG, price=2000.0, quantity=0.0),
        token="t",
    )
    with pytest.raises(ValueError, match="positive"):
        entry.validate_shape()
