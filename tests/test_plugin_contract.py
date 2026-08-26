"""Accepting a strategy that was written against a copy of our contract.

``__strategies__/`` is a separate repository. It restates ``StrategyBase``,
``SignalIntent`` and ``SignalAction`` on its own side so it can build, lint and
test without this repo checked out beside it — which means the engine cannot
recognise a strategy with ``issubclass`` or a signal with ``isinstance``. It
recognises them by shape instead, and converts at the boundary.

The classes below are a plugin's side of that agreement, written the way a real
one is: no import of ``qte_shared`` anywhere in this file's fixtures.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field, fields
from datetime import UTC, datetime
from enum import Enum

import pytest
from qte_shared.models import Scaling, SignalAction
from qte_shared.signal_factory import SignalFactory
from qte_shared.strategy_base import (
    INTENT_FIELDS,
    SignalIntent,
    StrategyBase,
    StrategyContext,
    as_intents,
    coerce_intent,
    implements_strategy_contract,
    overrides_on_tick,
)

# ── A plugin's own copy of the contract ──────────────────────────────────


class PluginAction(str, Enum):
    LONG = "LONG"
    TP1 = "TP1"
    FLAT = "FLAT"


@dataclass
class PluginScaling:
    tp: float | None = None
    sl: float | None = None
    quantity: float | None = None


@dataclass
class PluginIntent:
    action: PluginAction
    symbol: str | None = None
    price: float | None = None
    quantity: float | None = None
    sl: float | None = None
    tp1: float | None = None
    tp2: float | None = None
    risk_percent: float | None = None
    tp1_percent: float | None = None
    move_sl_to_be: bool | None = None
    is_scale_position: bool | None = None
    scale_strategy: str | None = None
    scaling: PluginScaling | None = None
    signal_uxid: str | None = None
    indicators: dict = field(default_factory=dict)
    inputs: dict = field(default_factory=dict)
    reason: str = ""


class PluginBase(ABC):
    name = ""
    symbols = ()
    timeframe = "M15"
    warmup = 10
    max_history = None

    def __init__(self, params=None):
        self.params = dict(params or {})

    def on_start(self, context):
        pass

    def on_stop(self):
        pass

    @abstractmethod
    def on_candle_closed(self, df, context): ...

    def on_tick(self, price, context):
        return None

    def history_window(self):
        return self.max_history or 400

    def describe(self):
        return {"name": self.name, "params": self.params}


class PluginStrategy(PluginBase):
    name = "PLUGIN_EDGE_V1"
    symbols = ("XAUUSD",)

    def on_candle_closed(self, df, context):
        return PluginIntent(action=PluginAction.LONG, price=2000.0, quantity=1.0, sl=1990.0)


class PluginTickStrategy(PluginStrategy):
    def on_tick(self, price, context):
        return PluginIntent(action=PluginAction.FLAT, price=price)


# ── The field list is the contract ───────────────────────────────────────


def test_intent_fields_track_the_dataclass() -> None:
    """``coerce_intent`` copies these by name. A field added to
    :class:`SignalIntent` and not to a plugin's intent is fine — it takes the
    default — but the tuple itself must never fall behind the dataclass, or the
    engine would stop copying a field it declares."""
    assert INTENT_FIELDS == tuple(field.name for field in fields(SignalIntent))
    assert "action" in INTENT_FIELDS and "scaling" in INTENT_FIELDS


# ── Recognising a strategy class ─────────────────────────────────────────


class OwnStrategy(StrategyBase):
    name = "OWN"

    def on_candle_closed(self, df, context):
        return None


class OwnTickStrategy(OwnStrategy):
    def on_tick(self, price, context):
        return None


@pytest.mark.parametrize(
    ("candidate", "expected", "why"),
    [
        (PluginStrategy, True, "a plugin's concrete strategy"),
        (OwnStrategy, True, "our own concrete strategy"),
        (PluginBase, False, "a plugin's abstract base declares on_candle_closed abstract"),
        (StrategyBase, False, "our abstract base"),
        (PluginIntent, False, "a dataclass with none of the hooks"),
        (dict, False, "an ordinary builtin"),
        ("PLUGIN_EDGE_V1", False, "not a class at all"),
    ],
)
def test_strategies_are_recognised_by_shape(candidate, expected: bool, why: str) -> None:
    assert implements_strategy_contract(candidate) is expected, why


def test_a_class_missing_a_declared_attribute_is_not_a_strategy() -> None:
    """The runner reads ``name``/``timeframe``/``warmup`` before the first bar."""

    class Nameless:
        timeframe = "M15"
        warmup = 10

        def on_candle_closed(self, df, context):
            return None

        def on_start(self, context):
            pass

        def on_stop(self):
            pass

        def history_window(self):
            return 400

    assert not implements_strategy_contract(Nameless)


# ── Recognising a strategy that wants ticks ──────────────────────────────


@pytest.mark.parametrize(
    ("strategy", "expected"),
    [
        (OwnStrategy(), False),
        (OwnTickStrategy(), True),
        (PluginStrategy(), False),
        (PluginTickStrategy(), True),
    ],
)
def test_only_a_real_override_puts_ticks_on_the_wire(strategy, expected: bool) -> None:
    """The runner subscribes to the whole tick feed when some strategy wants it.

    Comparing against ``StrategyBase.on_tick`` cannot answer this for a plugin
    — its inherited default is *its* base's method, not ours.
    """
    assert overrides_on_tick(strategy) is expected


# ── Converting an intent ─────────────────────────────────────────────────


def test_our_own_intent_passes_through_untouched() -> None:
    intent = SignalIntent(action=SignalAction.LONG, price=1.0)
    assert coerce_intent(intent) is intent


def test_a_plugin_intent_becomes_ours_field_for_field() -> None:
    plugin = PluginIntent(
        action=PluginAction.LONG,
        symbol="XAUUSD",
        price=2000.5,
        quantity=1.5,
        sl=1990.0,
        tp1=2010.0,
        tp2=2020.0,
        risk_percent=3.0,
        tp1_percent=30.0,
        move_sl_to_be=True,
        is_scale_position=True,
        scale_strategy="LOW_RR_TIER",
        scaling=PluginScaling(tp=0.5, sl=0.5, quantity=0.8),
        indicators={"wt1": 61.2},
        inputs={"bb_len": 20},
        reason="DIRECT_BUY|2000.5",
    )

    intent = coerce_intent(plugin)

    assert isinstance(intent, SignalIntent)
    assert intent.action is SignalAction.LONG
    assert (intent.symbol, intent.price, intent.quantity) == ("XAUUSD", 2000.5, 1.5)
    assert (intent.sl, intent.tp1, intent.tp2) == (1990.0, 2010.0, 2020.0)
    assert (intent.risk_percent, intent.tp1_percent) == (3.0, 30.0)
    assert intent.move_sl_to_be is True and intent.is_scale_position is True
    assert intent.scale_strategy == "LOW_RR_TIER"
    assert intent.indicators == {"wt1": 61.2} and intent.inputs == {"bb_len": 20}
    assert intent.reason == "DIRECT_BUY|2000.5"


def test_a_plugin_scaling_block_becomes_our_model() -> None:
    intent = coerce_intent(
        PluginIntent(action=PluginAction.LONG, scaling=PluginScaling(tp=0.5, sl=0.4, quantity=0.8))
    )
    assert isinstance(intent.scaling, Scaling)
    assert (intent.scaling.tp, intent.scaling.sl, intent.scaling.quantity) == (0.5, 0.4, 0.8)


def test_fields_a_plugin_does_not_carry_take_our_defaults() -> None:
    """A plugin may legitimately declare fewer fields than we do."""

    @dataclass
    class Minimal:
        action: str = "FLAT"

    intent = coerce_intent(Minimal())

    assert intent.action is SignalAction.FLAT
    assert intent.indicators == {} and intent.inputs == {}
    assert intent.reason == "" and intent.signal_uxid is None
    assert intent.is_running is None, "a field the plugin never heard of stays unset"


def test_a_bare_string_action_is_accepted() -> None:
    """A plugin is entitled to use plain strings rather than an enum."""

    @dataclass
    class Stringly:
        action: str = "R_SL"
        price: float = 1995.0

    assert coerce_intent(Stringly()).action is SignalAction.R_SL


def test_an_action_the_broker_does_not_know_is_refused() -> None:
    """Better here, with a stack trace, than as a 422 after the trade decision."""

    @dataclass
    class Wrong:
        action: str = "BUY"

    with pytest.raises(ValueError, match="LONG"):
        coerce_intent(Wrong())


def test_something_that_is_not_an_intent_says_so() -> None:
    with pytest.raises(TypeError, match="action"):
        coerce_intent(object())


# ── Normalising what a hook returned ─────────────────────────────────────


def test_nothing_returned_is_no_intents() -> None:
    assert as_intents(None) == []


def test_one_intent_or_many_or_a_generator() -> None:
    one = PluginIntent(action=PluginAction.LONG, price=1.0)
    two = PluginIntent(action=PluginAction.TP1, price=2.0)

    assert [intent.price for intent in as_intents(one)] == [1.0]
    assert [intent.price for intent in as_intents([one, two])] == [1.0, 2.0]
    assert [intent.price for intent in as_intents((one, two))] == [1.0, 2.0]
    assert [intent.price for intent in as_intents(intent for intent in (one, two))] == [1.0, 2.0]
    assert all(isinstance(intent, SignalIntent) for intent in as_intents([one, two]))


# ── End to end: a plugin's decision reaches the broker payload ───────────


def test_a_plugin_intent_survives_all_the_way_to_a_broker_signal() -> None:
    """The seam only counts if what comes out the far end is a valid payload."""
    strategy = PluginStrategy()
    context = StrategyContext("XAUUSD", "M15", datetime(2024, 3, 5, 12, tzinfo=UTC))
    factory = SignalFactory(strategy.name, timeframe=strategy.timeframe)

    intents = as_intents(strategy.on_candle_closed(None, context))
    signal = factory.build(intents[0], symbol="XAUUSD", moment=context.now)

    assert signal.strategy == "PLUGIN_EDGE_V1"
    assert signal.timeframe == "15", "QTE says M15; the broker contract says 15"
    assert signal.position.action is SignalAction.LONG
    assert (signal.position.price, signal.position.quantity) == (2000.0, 1.0)
    assert signal.position.sl == 1990.0
    # The bracket policy filled the targets the plugin left off.
    assert signal.position.tp1 == pytest.approx(2010.0)
    assert signal.position.tp2 == pytest.approx(2020.0)
    signal.validate_shape()


def test_a_plugin_exit_closes_the_cycle_its_entry_opened() -> None:
    """A close carries no signal_uxid of its own; the factory reuses the entry's."""
    factory = SignalFactory("PLUGIN_EDGE_V1", timeframe="M15")
    moment = datetime(2024, 3, 5, 12, tzinfo=UTC)

    opened = PluginIntent(action=PluginAction.LONG, price=2000.0, quantity=1.0, sl=1990.0)
    entry = factory.build(coerce_intent(opened), symbol="XAUUSD", moment=moment)
    exit_signal = factory.build(
        coerce_intent(PluginIntent(action=PluginAction.FLAT, price=2005.0)),
        symbol="XAUUSD",
        moment=moment,
    )

    assert exit_signal.signal_uxid == entry.signal_uxid
