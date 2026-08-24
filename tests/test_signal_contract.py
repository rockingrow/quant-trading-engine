"""The seven-method interface every strategy in ``__strategies__/`` presents.

Two things are under test here and they are separable. One is the *dispatch*:
given a strategy that implements ``long``/``short``/``tp1``/``tp2``/``sl``, does
``on_candle_closed`` ask them in the documented order and refuse a method that
emits somebody else's action. The other is the *recognition*: can the engine
tell an implemented method from an inherited placeholder — which is the whole
basis of the audit, and which cannot use ``issubclass`` because a plugin repo
restates the interface rather than importing it.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import UTC, datetime

import pytest
from qte_shared.models import SignalAction
from qte_shared.strategy_base import (
    ENTRY_SIGNAL_ORDER,
    EXIT_SIGNAL_ORDER,
    OPTIONAL_SIGNAL_METHODS,
    REQUIRED_SIGNAL_METHODS,
    SIGNAL_METHOD_ACTIONS,
    SIGNAL_METHODS,
    SignalIntent,
    SignalStrategy,
    StrategyContext,
    as_intents,
    defines_signal_method,
    implemented_signal_methods,
    implements_signal_contract,
    implements_strategy_contract,
    missing_signal_methods,
)


def context(*, open_uxid: str | None = None) -> StrategyContext:
    return StrategyContext(
        "XAUUSD", "M15", datetime(2026, 3, 5, 12, tzinfo=UTC), open_uxid=open_uxid
    )


class Recorder(SignalStrategy):
    """Answers every hook, and remembers which ones were asked, in order."""

    name = "RECORDER"
    warmup = 10

    #: Methods that should return an intent rather than None, per instance.
    fires: tuple[str, ...] = ()

    def __init__(self, params=None, fires: tuple[str, ...] = ()):
        super().__init__(params)
        self.asked: list[str] = []
        self.fires = fires

    def _answer(self, name: str) -> SignalIntent | None:
        self.asked.append(name)
        if name not in self.fires:
            return None
        return SignalIntent(action=SIGNAL_METHOD_ACTIONS[name], price=2000.0, reason=name)

    def long(self, df, context):
        return self._answer("long")

    def short(self, df, context):
        return self._answer("short")

    def tp1(self, df, context):
        return self._answer("tp1")

    def tp2(self, df, context):
        return self._answer("tp2")

    def sl(self, df, context):
        return self._answer("sl")

    def r_sl(self, df, context):
        return self._answer("r_sl")

    def flat(self, df, context):
        return self._answer("flat")


# ── The contract's own shape ─────────────────────────────────────────────


def test_the_required_and_optional_split_is_what_the_docs_promise():
    assert REQUIRED_SIGNAL_METHODS == ("long", "short", "tp1", "tp2", "sl")
    assert OPTIONAL_SIGNAL_METHODS == ("r_sl", "flat")
    assert SIGNAL_METHODS == REQUIRED_SIGNAL_METHODS + OPTIONAL_SIGNAL_METHODS


def test_every_signal_method_maps_to_a_broker_action():
    """The names are not decoration — each one *is* a ``SignalAction``.

    If the broker ever grew an eighth action, a method for it would have to
    appear here rather than being smuggled through an existing hook.
    """
    assert set(SIGNAL_METHOD_ACTIONS) == set(SIGNAL_METHODS)
    assert {action.value for action in SIGNAL_METHOD_ACTIONS.values()} == {
        action.value for action in SignalAction
    }


def test_the_dispatch_orders_cover_the_whole_surface_exactly_once():
    assert sorted(EXIT_SIGNAL_ORDER + ENTRY_SIGNAL_ORDER) == sorted(SIGNAL_METHODS)


def test_the_stop_is_asked_before_any_target():
    """One bar can both stop out and reach a target; the stop is what happened."""
    assert EXIT_SIGNAL_ORDER.index("sl") < EXIT_SIGNAL_ORDER.index("tp1")
    assert EXIT_SIGNAL_ORDER.index("r_sl") < EXIT_SIGNAL_ORDER.index("tp1")


# ── Dispatch ─────────────────────────────────────────────────────────────


def test_flat_asks_only_the_entries(trending_frame):
    strategy = Recorder()
    strategy.on_candle_closed(trending_frame, context())
    assert strategy.asked == list(ENTRY_SIGNAL_ORDER)


def test_holding_a_position_asks_only_the_exits(trending_frame):
    strategy = Recorder()
    strategy.on_candle_closed(trending_frame, context(open_uxid="CYCLE-1"))
    assert strategy.asked == list(EXIT_SIGNAL_ORDER)


def test_the_first_entry_that_answers_wins(trending_frame):
    """A bar cannot be both a long and a short entry, so ``short`` is never asked."""
    strategy = Recorder(fires=("long",))
    intents = as_intents(strategy.on_candle_closed(trending_frame, context()))

    assert strategy.asked == ["long"]
    assert [intent.action for intent in intents] == [SignalAction.LONG]


def test_every_exit_is_asked_even_after_one_answers(trending_frame):
    """Unlike entries: taking tp1 and moving the stop on the same bar is normal."""
    strategy = Recorder(fires=("r_sl", "tp1"))
    intents = as_intents(strategy.on_candle_closed(trending_frame, context(open_uxid="C1")))

    assert strategy.asked == list(EXIT_SIGNAL_ORDER)
    assert [intent.action for intent in intents] == [SignalAction.R_SL, SignalAction.TP1]


def test_a_method_emitting_someone_elses_action_is_refused(trending_frame):
    """Otherwise it reaches the broker as a perfectly valid-looking payload."""

    class Confused(Recorder):
        def tp1(self, df, context):
            return SignalIntent(action=SignalAction.SHORT, price=2000.0)

    with pytest.raises(ValueError, match=r"tp1\(\) returned a SHORT"):
        Confused().on_candle_closed(trending_frame, context(open_uxid="C1"))


def test_the_error_names_the_method_the_decision_belongs_in(trending_frame):
    class Confused(Recorder):
        def long(self, df, context):
            return SignalIntent(action=SignalAction.FLAT, price=2000.0)

    with pytest.raises(ValueError, match="belongs in flat"):
        Confused().on_candle_closed(trending_frame, context())


def test_a_signal_strategy_is_drivable_without_writing_on_candle_closed():
    """The point of the base: implement the seven, get the driver hook free."""
    assert implements_strategy_contract(Recorder)
    assert implements_signal_contract(Recorder)


def test_describe_reports_which_actions_the_class_can_emit():
    assert Recorder().describe()["signals"] == list(SIGNAL_METHODS)


# ── Recognition: implemented, or inherited placeholder? ──────────────────


class Partial(SignalStrategy):
    """Half-migrated: entries done, exits not."""

    name = "PARTIAL"

    def long(self, df, context):
        return None

    def short(self, df, context):
        return None


def test_an_unimplemented_required_method_is_not_credited():
    assert missing_signal_methods(Partial) == ("tp1", "tp2", "sl")
    assert not implements_signal_contract(Partial)


def test_an_inherited_optional_no_op_is_not_an_implementation():
    """``r_sl`` and ``flat`` have concrete defaults on the base.

    Finding a callable therefore proves nothing about the strategy — only the
    class it is *defined on* does, which is why the audit can report "this one
    manages its stop mid-trade" and be right.
    """
    assert callable(Recorder.flat) and callable(Partial.flat)
    assert defines_signal_method(Recorder, "flat")
    assert not defines_signal_method(Partial, "flat")
    assert implemented_signal_methods(Partial) == ("long", "short")


def test_a_plugins_own_restated_interface_is_recognised_the_same_way():
    """No ``qte_shared`` import on the plugin's side — the audit still reads it.

    A strategy repository has its own lockfile and its own CI, so it restates
    the interface rather than importing ours. Recognition is structural for
    exactly this case; ``issubclass`` would call every such repo unaudited.
    """

    class PluginBase(ABC):
        name = ""
        timeframe = "M15"
        warmup = 10
        max_history = None

        def __init__(self, params=None):
            self.params = dict(params or {})

        def on_start(self, context): ...
        def on_stop(self): ...
        def on_candle_closed(self, df, context): ...
        def history_window(self):
            return 400

        @abstractmethod
        def long(self, df, context): ...
        @abstractmethod
        def short(self, df, context): ...
        @abstractmethod
        def tp1(self, df, context): ...
        @abstractmethod
        def tp2(self, df, context): ...
        @abstractmethod
        def sl(self, df, context): ...

        def r_sl(self, df, context):
            return None

        def flat(self, df, context):
            return None

    class PluginEdge(PluginBase):
        name = "PLUGIN_EDGE"

        def long(self, df, context): ...
        def short(self, df, context): ...
        def tp1(self, df, context): ...
        def tp2(self, df, context): ...
        def sl(self, df, context): ...

    assert implements_signal_contract(PluginEdge)
    assert implemented_signal_methods(PluginEdge) == REQUIRED_SIGNAL_METHODS
    assert not implements_signal_contract(PluginBase)


def test_something_that_is_not_a_class_is_simply_not_a_strategy():
    assert not implements_signal_contract("long")
    assert not defines_signal_method(object(), "long")
