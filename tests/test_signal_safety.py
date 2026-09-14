"""Risk rejection, slot routing and credential boundaries shared by both drivers."""

import json
from datetime import UTC, datetime

import httpx
import pytest
from test_broker_sink import FakeBus, _signal
from test_replay import BuyOnceStrategy
from test_runner_delivery import _enter, _runner, _slot

from qte_backtest.replay import BacktestEngine
from qte_backtest.report import build_report
from qte_shared.config import settings
from qte_shared.models import SignalAction
from qte_shared.strategies.signal_factory import SignalFactory
from qte_shared.strategies.signal_serialization import signal_record
from qte_shared.strategies.sizing import PositionSizer
from qte_shared.strategies.strategy_base import SignalIntent
from qte_strategy_engine.broker_sink import BrokerSink, signal_delivery_id
from qte_strategy_engine.db.repository import _signal_row

TEST_MOMENT = datetime(2026, 9, 14, tzinfo=UTC)
DUMMY_CREDENTIAL = "AUDIT_SYNTHETIC_CREDENTIAL"


@pytest.mark.parametrize("proposed", [None, 0.01, 10.0])
def test_a_substep_cap_rejects_entry_without_fallback(proposed):
    factory = SignalFactory(
        "RISK_PROBE",
        sizer=PositionSizer(capital=1000, risk_percent=1, max_quantity=0.00001),
        default_quantity=0.01,
    )
    with pytest.raises(ValueError, match="quantity step"):
        factory.build(
            SignalIntent(action=SignalAction.LONG, price=2000, sl=1990, quantity=proposed),
            symbol="XAUUSD",
            moment=TEST_MOMENT,
        )
    assert factory.open_positions() == {}


@pytest.mark.parametrize("risk_percent", [0, -1, float("nan"), float("inf")])
def test_invalid_budgets_cannot_fall_back_to_a_strategy_quantity(risk_percent):
    with pytest.raises(ValueError):
        factory = SignalFactory(
            "RISK_PROBE", sizer=PositionSizer(capital=1000, risk_percent=risk_percent)
        )
        factory.build(
            SignalIntent(action=SignalAction.LONG, price=2000, sl=1990, quantity=10),
            symbol="XAUUSD",
            moment=TEST_MOMENT,
        )


@pytest.mark.parametrize(
    "configuration",
    [{"capital": -1}, {"contract_size": 0}, {"max_quantity": -1}, {"precision": -1}],
)
def test_invalid_account_configuration_is_rejected(configuration):
    with pytest.raises(ValueError):
        PositionSizer(**{"capital": 1000, "risk_percent": 1, **configuration})


def test_sizing_rounds_down_at_both_risk_and_quantity_ceilings():
    sizing = PositionSizer(capital=1000, risk_percent=1, precision=2)
    assert sizing.size(100, 94) == 1.66
    assert sizing.replace(max_quantity=1.669).size(100, 94) == 1.66
    assert sizing.limit_quantity(1.669) == 1.66
    assert sizing.replace(max_quantity=0.125).limit_quantity(10) == 0.12


@pytest.mark.parametrize("action", [SignalAction.LONG, SignalAction.SL, SignalAction.FLAT])
def test_cross_symbol_intents_are_rejected_before_any_cycle_changes(action):
    factory = SignalFactory("SYMBOL_PROBE")
    with pytest.raises(ValueError, match="differs from slot"):
        factory.build(
            SignalIntent(action=action, symbol="EURUSD", price=2000, sl=1990, quantity=1),
            symbol="XAUUSD",
            moment=TEST_MOMENT,
        )
    assert factory.open_positions() == {}
    assert factory._pending_scale == {}


async def test_runner_rejects_cross_symbol_before_staging_or_sending():
    runner, strategy_slot = _runner(), _slot()
    await _enter(runner, strategy_slot, symbol="EURUSD")
    assert runner.signals.rows == []
    assert runner.sink.delivery_ids == []
    assert runner.state.held == runner.positions.held == {}


def test_replay_rejects_cross_symbol_intents(trending_frame):
    class CrossSymbolStrategy(BuyOnceStrategy):
        def on_candle_closed(self, candles_frame, context):
            intent = super().on_candle_closed(candles_frame, context)
            if intent is not None:
                intent.symbol = "EURUSD"
            return intent

    result = BacktestEngine(CrossSymbolStrategy(), symbol="XAUUSD").run(trending_frame)
    assert result.signals == result.positions == []


def test_default_factory_never_reads_the_configured_broker_credential(monkeypatch):
    monkeypatch.setattr(settings.broker, "token", DUMMY_CREDENTIAL)
    factory = SignalFactory("TOKEN_PROBE")
    signal = factory.build(
        SignalIntent(action=SignalAction.LONG, price=2000, sl=1990),
        symbol="XAUUSD",
        moment=TEST_MOMENT,
    )
    assert signal.token == ""
    assert DUMMY_CREDENTIAL not in signal.model_dump_json()


def test_legacy_credential_is_removed_from_report_and_audit(trending_frame):
    report = build_report(BacktestEngine(BuyOnceStrategy(), symbol="XAUUSD").run(trending_frame))
    for signal in report.result.signals:
        signal.token = DUMMY_CREDENTIAL
    assert report.result.signals
    assert DUMMY_CREDENTIAL not in json.dumps(report.to_dict())
    audit_row = _signal_row(
        report.result.signals[0], transport="nats", delivery_status="pending", shadow=True
    )
    assert "token" not in audit_row.payload["payload"]
    assert DUMMY_CREDENTIAL not in json.dumps(audit_row.payload)


async def test_internal_signal_mirror_never_contains_factory_credentials():
    runner, strategy_slot = _runner(), _slot()
    strategy_slot.factory.token = DUMMY_CREDENTIAL
    await _enter(runner, strategy_slot)
    assert "token" not in runner.bus.messages[0][1]["signal"]
    assert DUMMY_CREDENTIAL not in json.dumps(runner.bus.messages)


@pytest.mark.parametrize("transport", ["nats", "http"])
async def test_delivery_injects_current_credentials_without_mutating_stored_signals(
    monkeypatch, transport
):
    monkeypatch.setattr(settings.broker, "token", DUMMY_CREDENTIAL)
    broker_bus = FakeBus()
    broker_sink = BrokerSink(transport=transport, bus=broker_bus, shadow_mode=False)
    captured = []

    def capture_request(request):
        captured.append(json.loads(request.content))
        return httpx.Response(200, json={"accepted": True})

    await broker_sink.start()
    if transport == "http":
        await broker_sink._http.aclose()
        broker_sink._http = httpx.AsyncClient(
            base_url="https://broker.invalid", transport=httpx.MockTransport(capture_request)
        )
    signal = _signal()
    original_record = signal_record(signal)
    original_identifier = signal_delivery_id(signal)
    try:
        await broker_sink.send(signal)
        monkeypatch.setattr(settings.broker, "token", "ROTATED_SYNTHETIC_CREDENTIAL")
        await broker_sink.send(signal)
    finally:
        await broker_sink.stop()
    bodies = (
        captured
        if transport == "http"
        else [message[1]["payload"] for message in broker_bus.published]
    )
    assert [payload["token"] for payload in bodies] == [
        DUMMY_CREDENTIAL,
        "ROTATED_SYNTHETIC_CREDENTIAL",
    ]
    assert signal.token == "tok"
    assert signal_record(signal) == original_record
    signal.token = "DIFFERENT_STORED_CREDENTIAL"
    assert signal_delivery_id(signal) == original_identifier
