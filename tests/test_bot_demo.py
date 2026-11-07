"""``/demo``: the drill, run from the chat.

The drill itself is pinned by ``tests/test_demo_signal.py``; what is worth
pinning here is the part the bot adds. Three things: the strategy names offered
come from the mount rather than from the runner, because the drill loads the
strategies off disk itself; two drills cannot run at once in one bot process,
since both would drive the same strategies and double every card in the
channel; and the report tells apart an envelope that did not leave, a strategy
that *raised*, and a strategy that merely declined every regime — three
outcomes an operator acts on differently.
"""

from __future__ import annotations

import asyncio

from qte_bot import presenters
from qte_bot.engine_client import EngineClient
from qte_bot.handlers import _matching_strategy
from qte_shared.models import BrokerSignal, PositionBlock, SignalAction
from qte_strategy_engine.demo_signal import SYNTHETIC, DemoDelivery, DemoSignal

NAMESPACE = "dev-shadow-mt5"
TARGET = "every mounted strategy"


def _report(*arguments) -> str:
    """The whole report, however many messages it is sent as."""
    return "\n".join(presenters.demo_result(*arguments))


def _signal(strategy: str = "MT5_GOLD_M15_V2", symbol: str = "XAUUSD") -> BrokerSignal:
    return BrokerSignal(
        strategy=strategy,
        symbol=symbol,
        timeframe="M15",
        position=PositionBlock(action=SignalAction.LONG, price=2400.0, quantity=0.41, sl=2388.0),
    )


def _delivery(
    *,
    strategy: str = "MT5_GOLD_M15_V2",
    source: str = "uptrend",
    error: str = "",
    strategy_error: str = "",
) -> DemoDelivery:
    demo = DemoSignal(
        pair=None,  # type: ignore[arg-type]  # the presenter reads the payload only
        signal=_signal(strategy),
        source=source,
        strategy_error=strategy_error,
    )
    return DemoDelivery(demo=demo, subject=f"SIGNALS_DEMO.{strategy}", error=error)


# ── Naming what to fire ───────────────────────────────────────────────────

MOUNTED = ["MT5_GOLD_M15_V2", "MT5_FX_M15_V1"]


def test_a_mounted_strategy_is_matched_whatever_case_it_is_typed_in():
    assert _matching_strategy("mt5_gold_m15_v2", MOUNTED) == "MT5_GOLD_M15_V2"
    assert _matching_strategy("  MT5_FX_M15_V1  ", MOUNTED) == "MT5_FX_M15_V1"


def test_a_symbol_is_not_a_demo_target():
    """A demo fires per strategy; a symbol two strategies share would fire two."""
    assert _matching_strategy("XAUUSD", MOUNTED) is None


def test_a_name_that_is_not_in_the_mount_is_refused():
    assert _matching_strategy("MT5_GOLD_M15_V3", MOUNTED) is None


def test_the_usage_line_offers_the_mounted_names_and_all():
    body = presenters.usage("demo", MOUNTED)
    assert "/demo all" in body
    assert "/demo MT5_GOLD_M15_V2" in body


# ── The report ────────────────────────────────────────────────────────────


def test_a_fired_drill_lists_each_strategy_and_that_it_left():
    body = _report([_delivery()], [], NAMESPACE, TARGET)
    assert "MT5_GOLD_M15_V2" in body and "XAUUSD" in body
    assert "LONG" in body and "uptrend" in body
    assert "1 of 1 fired" in body
    # The claim the whole drill rests on, in the message that reports it.
    assert "forwards nothing to a worker" in body


def test_two_strategies_that_differ_by_a_suffix_are_both_named_in_full():
    """Why the report is not a table: a truncated column would merge these two."""
    body = _report(
        [
            _delivery(strategy="MT5_SILVER_STOCH_RSI_M15_V1"),
            _delivery(strategy="MT5_SILVER_STOCH_RSI_SHORT_M15_V1"),
        ],
        [],
        NAMESPACE,
        TARGET,
    )
    assert "MT5_SILVER_STOCH_RSI_M15_V1" in body
    assert "MT5_SILVER_STOCH_RSI_SHORT_M15_V1" in body
    assert "…" not in body


def test_an_envelope_that_did_not_leave_is_named_with_its_reason():
    body = _report([_delivery(error="TimeoutError: nats")], [], NAMESPACE, TARGET)
    assert "not delivered" in body and "TimeoutError" in body
    assert "0 of 1 fired" in body


def test_a_strategy_that_raised_is_reported_as_the_drills_own_finding():
    body = _report([_delivery(strategy_error="KeyError: 'atr_period'")], [], NAMESPACE, TARGET)
    assert "raised while being driven" in body
    assert "live bar too" in body


def test_a_strategy_that_only_declined_is_not_reported_as_a_failure():
    body = _report([_delivery(source=SYNTHETIC)], [], NAMESPACE, TARGET)
    assert "synthetic fallback" in body
    assert "raised" not in body
    assert "1 of 1 fired" in body


def test_a_pair_whose_payload_could_not_be_built_is_counted_and_named():
    body = _report([_delivery()], ["MT5_FX_M15_V1 on EURUSD: no sizing"], NAMESPACE, TARGET)
    assert "No payload could be built" in body and "MT5_FX_M15_V1" in body
    assert "1 of 2 fired" in body


def test_nothing_mapped_points_at_the_two_commands_that_explain_why():
    body = _report([], [], NAMESPACE, TARGET)
    assert "/mapping" in body and "/strategies" in body


def test_a_strategy_name_cannot_inject_markup_into_the_report():
    body = _report([], ["<b>bold</b> on EURUSD: broken"], NAMESPACE, TARGET)
    assert "&lt;b&gt;bold&lt;/b&gt;" in body
    assert "<b>bold</b>" not in body


# ── One drill at a time ───────────────────────────────────────────────────


async def test_a_second_drill_waits_for_the_first_rather_than_overlapping():
    """Two concurrent runs would drive the same strategies and double the cards."""
    client = EngineClient.__new__(EngineClient)
    client._demo_lock = asyncio.Lock()
    assert client.demo_running is False
    async with client._demo_lock:
        assert client.demo_running is True
    assert client.demo_running is False


def test_the_busy_message_names_the_book_it_is_drilling():
    assert NAMESPACE in presenters.DEMO_RUNNING.format(namespace=NAMESPACE)
