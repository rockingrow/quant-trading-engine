"""QTE's payloads against the request bodies in ``examples/algo-trading-broker/``.

Those files are what the broker's webhook actually receives, so they are the
closest thing to a specification this repo holds. ``test_broker_contract.py``
pins the *model*; this pins the model against those documents — every key they
carry is a key we know, and a whole trade cycle driven through the factory
reproduces their numbers.

One inconsistency in the fixtures is worth knowing about before reading the
figures below: ``entry.long.json`` and ``close.tp1.json`` both say
``tp1_percent: 50.0``, while their quantities (6.0 entry, 1.8 banked, 4.2 left)
are a 30% partial — which is what ``close.tp2.json`` then reports. The
quantities are self-consistent and the percentage field is not, so the
quantities are what is pinned here.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
from qte_shared.config import REPO_ROOT
from qte_shared.models import BrokerSignal, PositionBlock, SignalAction
from qte_shared.signal_factory import SignalFactory
from qte_shared.sizing import PositionSizer
from qte_shared.strategy_base import SignalIntent

EXAMPLES = REPO_ROOT / "examples" / "algo-trading-broker"
NOW = datetime(2026, 4, 10, 22, 55, tzinfo=UTC)

#: The account entry.long.json was sized on: $1,000 risking 3% over a $5 stop.
CAPITAL = 1000.0
RISK_PERCENT = 3.0


def _documents() -> list[tuple[str, dict]]:
    return [
        (path.stem, json.loads(path.read_text(encoding="utf-8")))
        for path in sorted(EXAMPLES.glob("*.json"))
    ]


#: Collected once so the parametrised cases and their ids cannot disagree.
DOCUMENTS = _documents()
IDS = [name for name, _ in DOCUMENTS]


def _factory() -> SignalFactory:
    return SignalFactory(
        "MT5_GOLD_M5_V1",
        timeframe="M5",
        token="secret_token_tu_tradingview",
        sizer=PositionSizer(capital=CAPITAL, risk_percent=RISK_PERCENT),
        inputs={"use_equity_sizing": False},
    )


def _entry(factory: SignalFactory) -> BrokerSignal:
    return factory.build(
        SignalIntent(
            action=SignalAction.LONG,
            price=2334.50,
            sl=2329.50,
            tp1=2340.0,
            tp2=2345.0,
            tp1_percent=30.0,
            move_sl_to_be=False,
        ),
        symbol="XAUUSD",
        moment=NOW,
    )


# ── Shape ────────────────────────────────────────────────────────────────


def test_there_are_examples_to_check_against():
    # A silently empty glob would make every test below vacuously pass.
    assert set(IDS) == {
        "entry.long",
        "entry.short",
        "close.tp1",
        "close.tp2",
        "close.sl",
        "close.r_sl",
        "close.flat",
    }


@pytest.mark.parametrize(("name", "document"), DOCUMENTS, ids=IDS)
def test_every_field_the_broker_is_sent_is_one_we_model(name, document):
    assert set(document) <= set(BrokerSignal.model_fields), name
    assert set(document["position"]) <= set(PositionBlock.model_fields), name


@pytest.mark.parametrize(("name", "document"), DOCUMENTS, ids=IDS)
def test_every_example_body_validates_as_our_own_model(name, document):
    """If one of these raises, the broker is being sent something we cannot
    even parse back — the drift is already live."""
    signal = BrokerSignal(**document)
    assert signal.position.action.value == document["position"]["action"]


def test_a_lowercase_cycle_id_is_normalised_rather_than_rejected():
    # Every example uses lowercase hex; the broker's validator wants uppercase.
    document = dict(json.loads((EXAMPLES / "entry.long.json").read_text(encoding="utf-8")))
    assert document["signal_uxid"].islower()
    assert BrokerSignal(**document).signal_uxid == document["signal_uxid"].upper()


def test_a_flat_carrying_nothing_but_its_action_is_a_valid_close():
    document = json.loads((EXAMPLES / "close.flat.json").read_text(encoding="utf-8"))
    assert sorted(document["position"]) == ["action"]
    BrokerSignal(**document).validate_shape()


# ── The numbers ──────────────────────────────────────────────────────────


def test_the_entry_reproduces_the_documented_payload():
    payload = _entry(_factory()).model_dump(mode="json")
    document = json.loads((EXAMPLES / "entry.long.json").read_text(encoding="utf-8"))

    assert payload["timeframe"] == document["timeframe"] == "5"
    for field in ("price", "quantity", "sl", "tp1", "tp2", "risk_percent", "move_sl_to_be"):
        assert payload["position"][field] == document["position"][field], field
    assert payload["position"]["use_equity_sizing"] is False


def test_the_cycle_reproduces_the_documented_partial_and_the_close():
    factory = _factory()
    entry = _entry(factory)
    tp1 = factory.build(
        SignalIntent(action=SignalAction.TP1, price=2345.0), symbol="XAUUSD", moment=NOW
    )
    tp2 = factory.build(
        SignalIntent(action=SignalAction.TP2, price=2340.0), symbol="XAUUSD", moment=NOW
    )

    documented = {
        name: json.loads((EXAMPLES / f"{name}.json").read_text(encoding="utf-8"))
        for name in ("close.tp1", "close.tp2")
    }
    assert tp1.position.quantity == documented["close.tp1"]["position"]["quantity"] == 1.8
    assert tp2.position.quantity == documented["close.tp2"]["position"]["quantity"] == 4.2

    # One id for the whole trade — it is how the broker groups a cycle into a
    # single broadcast, and the examples share one across all six files.
    assert tp1.signal_uxid == tp2.signal_uxid == entry.signal_uxid
    assert factory.open_cycle("XAUUSD") is None


def test_is_running_says_whether_a_runner_is_left_behind():
    factory = _factory()
    _entry(factory)
    tp1 = factory.build(
        SignalIntent(action=SignalAction.TP1, price=2345.0), symbol="XAUUSD", moment=NOW
    )
    tp2 = factory.build(
        SignalIntent(action=SignalAction.TP2, price=2340.0), symbol="XAUUSD", moment=NOW
    )

    assert tp1.position.is_running is True
    assert tp2.position.is_running is False
    assert json.loads((EXAMPLES / "close.tp1.json").read_text(encoding="utf-8"))["position"][
        "is_running"
    ]
    assert not json.loads((EXAMPLES / "close.tp2.json").read_text(encoding="utf-8"))["position"][
        "is_running"
    ]


def test_a_stop_closes_the_whole_position_the_way_the_example_does():
    factory = _factory()
    _entry(factory)
    stop = factory.build(
        SignalIntent(action=SignalAction.SL, price=2330.0), symbol="XAUUSD", moment=NOW
    )
    document = json.loads((EXAMPLES / "close.sl.json").read_text(encoding="utf-8"))

    assert stop.position.quantity == document["position"]["quantity"] == 6.0
    assert stop.position.is_running is document["position"]["is_running"] is False
    assert factory.open_cycle("XAUUSD") is None


def test_the_scaling_block_stated_at_entry_is_restated_on_the_close():
    # close.tp1.json carries is_scale_position/scale_strategy/scaling even
    # though only the entry decided them: a worker reading one message must be
    # able to re-sync the trade from it.
    factory = _factory()
    factory.build(
        SignalIntent(
            action=SignalAction.LONG,
            price=2334.50,
            sl=2329.50,
            tp1_percent=30.0,
            is_scale_position=True,
            scale_strategy="LOW_RR_TIER",
        ),
        symbol="XAUUSD",
        moment=NOW,
    )
    tp1 = factory.build(
        SignalIntent(action=SignalAction.TP1, price=2345.0), symbol="XAUUSD", moment=NOW
    )
    assert tp1.position.is_scale_position is True
    assert tp1.position.scale_strategy == "LOW_RR_TIER"
