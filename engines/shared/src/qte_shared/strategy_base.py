"""The contract every plugin in ``__strategies__/`` implements.

Write-once, run-anywhere lives here: the backtest replay and the live runner
both drive a strategy through *this* interface and nothing else, so the same
file that produced a backtest curve is the file that trades.

A strategy never publishes, never touches NATS, never sizes against a live
account. It looks at data and returns :class:`SignalIntent` objects; turning an
intent into a broker-shaped payload (bracket levels, correlation ids, transport)
is the runner's job. That split is what lets the backtest fill an intent against
a simulator and the runner hand the identical intent to the broker.

**The contract is structural, not nominal.** ``__strategies__/`` is a private
repository cloned in, with its own lockfile and its own test suite; making it
``import qte_shared`` would mean it could not be built, linted or released
without this repo checked out beside it, and its test run would depend on
whichever revision of the engine happened to be on disk. So a plugin is free to
restate the classes below on its own side, and the engine accepts anything that
*behaves* like a strategy — see :func:`implements_strategy_contract` — and
converts what it returns with :func:`coerce_intent`. Subclassing
:class:`StrategyBase` still works and is the simplest thing for a strategy that
lives in the same tree as the engine; ``examples/__strategies__/`` does exactly
that.

**Two contracts live here, and they are not the same one.**
:class:`StrategyBase` is what the engine *drives* — one hook, one frame,
intents out — and it is small on purpose, because a plugin repo has to be able
to restate it. :class:`SignalStrategy` is the interface a strategy in
``__strategies__/`` is *required to present*: ``long``, ``short``, ``tp1``,
``tp2`` and ``sl``, plus an optional ``r_sl`` and ``flat``. It implements
``on_candle_closed`` by asking those methods in order, so a strategy written
against it is drivable without further ceremony. ``qte-strategy-audit`` is what
enforces it across the mounted repos.
"""

from __future__ import annotations

import inspect
from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass, field, fields
from datetime import datetime
from typing import Any

import pandas as pd

from qte_shared.models import Candle, PositionBlock, Scaling, SignalAction, Tick


@dataclass(slots=True)
class SignalIntent:
    """What a strategy decided, before it becomes a broker payload.

    ``signal_uxid`` ties a close back to the entry that opened it, under the
    same name the Pine strategies and the broker payload use. Leave it ``None``
    on an entry — the runner mints the cycle id and remembers it — and set it
    on a follow-up when the strategy is managing an exit it opened itself.

    ``quantity`` is a *proposal*. The runner risk-sizes every entry against the
    configured account and rescales the strategy's closes by the same factor,
    so a strategy that sizes off its own notional capital still trades the book
    the operator configured. Leave it ``None`` and the sizing is entirely the
    runner's; set it and the proportions you asked for are preserved.
    """

    action: SignalAction
    symbol: str | None = None
    price: float | None = None
    quantity: float | None = None
    sl: float | None = None
    tp1: float | None = None
    tp2: float | None = None
    risk_percent: float | None = None
    tp1_percent: float | None = None
    move_sl_to_be: bool | None = None
    #: Leave it ``None`` and the runner fills it from the pair's
    #: ``use_equity_sizing`` param. It reaches the broker either way and it
    #: never changes the size QTE sends — see :mod:`qte_shared.sizing`.
    use_equity_sizing: bool | None = None
    is_running: bool | None = None
    is_scale_position: bool | None = None
    scale_strategy: str | None = None
    scaling: Scaling | None = None
    signal_uxid: str | None = None
    indicators: dict[str, Any] = field(default_factory=dict)
    inputs: dict[str, Any] = field(default_factory=dict)
    reason: str = ""

    def to_position_block(self) -> PositionBlock:
        return PositionBlock(
            action=self.action,
            price=self.price,
            quantity=self.quantity,
            sl=self.sl,
            tp1=self.tp1,
            tp2=self.tp2,
            risk_percent=self.risk_percent,
            tp1_percent=self.tp1_percent,
            move_sl_to_be=self.move_sl_to_be,
            use_equity_sizing=self.use_equity_sizing,
            is_running=self.is_running,
            is_scale_position=self.is_scale_position,
            scale_strategy=self.scale_strategy,
            scaling=self.scaling,
        )


@dataclass(slots=True)
class StrategyContext:
    """Everything a strategy is allowed to know about the world around it.

    Deliberately small. A strategy that needs an account balance or an open-
    position list is reaching past the boundary that keeps backtest and live
    identical — put that state in the runner instead.
    """

    symbol: str
    timeframe: str
    now: datetime
    mode: str = "backtest"
    params: dict[str, Any] = field(default_factory=dict)
    #: Cycle id of the position this strategy currently believes it holds, as
    #: tracked by whoever is driving it. ``None`` means flat.
    open_uxid: str | None = None

    @property
    def is_live(self) -> bool:
        return self.mode == "live"


IntentResult = SignalIntent | Sequence[SignalIntent] | None

#: Floor for the default history window. A strategy with a short warm-up still
#: wants enough bars for a chart-length view of what it is trading.
MIN_HISTORY_WINDOW = 400


class StrategyBase(ABC):
    """Base class every plugin in ``__strategies__/`` must subclass.

    Subclasses are discovered by :mod:`qte_strategy_engine.loader`, which
    instantiates them with ``params`` from configuration and calls the hooks
    below. Only :meth:`on_candle_closed` is required.
    """

    #: Strategy name as the broker knows it. It is the NATS subject workers
    #: subscribe to, so it must match the worker's configured strategy exactly.
    name: str = ""
    #: Symbols this strategy wants; empty means "whatever the engine feeds me".
    symbols: Sequence[str] = ()
    #: Timeframe whose closes drive :meth:`on_candle_closed`.
    timeframe: str = "M15"
    #: Minimum closed candles before the first call — the runner buffers until
    #: it has this many, so indicators never see a half-warm window.
    warmup: int = 200
    #: How many closed candles ``on_candle_closed`` receives, in **both** the
    #: backtest and the live runner.
    #:
    #: ``None`` (the default) means ``max(warmup * 2, MIN_HISTORY_WINDOW)``: an
    #: indicator needing N bars needs N *valid* bars, and its own warm-up eats
    #: the oldest few. Set an integer to widen it. Set ``0`` for every bar
    #: available — but read :meth:`history_window` first, because "available"
    #: does not mean the same thing in a backtest as it does live.
    max_history: int | None = None

    def __init__(self, params: dict[str, Any] | None = None) -> None:
        self.params: dict[str, Any] = dict(params or {})
        if not self.name:
            self.name = self.__class__.__name__

    # ── Lifecycle ─────────────────────────────────────────────────────

    def on_start(self, context: StrategyContext) -> None:
        """Called once before the first candle. Override to warm up state."""

    def on_stop(self) -> None:
        """Called on shutdown, and after the last bar of a backtest."""

    # ── Hooks ─────────────────────────────────────────────────────────

    @abstractmethod
    def on_candle_closed(self, df: pd.DataFrame, context: StrategyContext) -> IntentResult:
        """Decide on a completed bar.

        *df* is an OHLCV frame indexed by candle **open time** in UTC, oldest
        first, with the just-closed candle as its last row — so ``df.iloc[-1]``
        is always a finished bar and never repaints.

        Return ``None`` to do nothing, one :class:`SignalIntent`, or several.
        """

    def on_tick(self, price: float, context: StrategyContext) -> IntentResult:
        """Optional per-tick risk management between bar closes.

        Default is to do nothing: most strategies hand their stop and targets
        to the broker as a bracket at entry and let the worker manage them.
        Override when an exit has to react faster than the bar close.
        """
        return None

    # ── Convenience for subclasses ────────────────────────────────────

    def history_window(self) -> int | None:
        """Bars handed to :meth:`on_candle_closed`; ``None`` means unbounded.

        Both drivers call this, which is the point. Before it existed the live
        runner kept a bounded deque while the backtest passed the entire file,
        so a strategy using a running sum or a session VWAP computed one thing
        on the chart and another in production — the exact divergence
        write-once-run-anywhere is supposed to rule out.

        A word on ``max_history = 0``: it gives every bar the driver has, and a
        backtest has the whole parquet file while a restarted runner has only
        what Redis retained. Unbounded therefore means "the same code sees
        different amounts of history in the two places", which is the thing
        this method exists to prevent. The runner warns when a strategy asks
        for more than Redis keeps.
        """
        if self.max_history == 0:
            return None
        return self.max_history or max(self.warmup * 2, MIN_HISTORY_WINDOW)

    def param(self, key: str, default: Any = None) -> Any:
        return self.params.get(key, default)

    def describe(self) -> dict[str, Any]:
        """Metadata the runner logs and the control-plane API exposes."""
        return {
            "name": self.name,
            "class": self.__class__.__name__,
            "module": self.__class__.__module__,
            "symbols": list(self.symbols),
            "timeframe": self.timeframe,
            "warmup": self.warmup,
            "history_window": self.history_window(),
            "params": self.params,
        }


def candles_to_frame(candles: Sequence[Candle]) -> pd.DataFrame:
    """Build the OHLCV frame strategies receive, from QTE candle objects."""
    if not candles:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
    frame = pd.DataFrame(
        [
            {
                "open_time": candle.open_time,
                "open": candle.open,
                "high": candle.high,
                "low": candle.low,
                "close": candle.close,
                "volume": candle.volume,
            }
            for candle in candles
        ]
    )
    return frame.set_index("open_time").sort_index()


def tick_price(tick: Tick) -> float:
    """Price a strategy sees on :meth:`StrategyBase.on_tick`."""
    return tick.price


# ─────────────────────────────────────────────────────────────────────────
# Accepting a plugin that implements this contract independently
#
# Everything below is the adapter layer between "a class in a repository we
# do not control" and "the pydantic models this engine puts on the wire". It
# runs once per discovered class and once per emitted intent, and it is the
# only place that knows a plugin's objects might not be ours.
# ─────────────────────────────────────────────────────────────────────────

#: What the engine drives. Not necessarily a :class:`StrategyBase` subclass —
#: see the module docstring — so this is a documented alias rather than a
#: promise a type checker can keep.
StrategyLike = Any

#: Methods a class must have to be driven as a strategy. ``on_candle_closed``
#: is the decision; the other three are called by both drivers on every run.
REQUIRED_HOOKS = ("on_candle_closed", "on_start", "on_stop", "history_window")

#: Attributes both drivers read off a strategy before the first bar.
REQUIRED_ATTRIBUTES = ("name", "timeframe", "warmup")

#: Read off a plugin's intent, by name, to build ours. This tuple *is* the
#: published contract — ``tests/test_plugin_contract.py`` pins it against
#: :class:`SignalIntent` so the two cannot fall out of step.
INTENT_FIELDS = tuple(field.name for field in fields(SignalIntent))


def implements_strategy_contract(candidate: Any) -> bool:
    """Whether *candidate* is a class this engine can drive as a strategy.

    Structural on purpose. A plugin repository restates :class:`StrategyBase`
    on its own side so it can build and test without the engine, which means
    ``issubclass`` would reject the very strategies we are here to load.

    Abstract classes are excluded, which is what keeps a plugin's *own* base
    class out of the results: it declares ``on_candle_closed`` abstract exactly
    as this one does, so it never looks concrete.
    """
    if not inspect.isclass(candidate) or inspect.isabstract(candidate):
        return False
    if candidate is StrategyBase:
        return False
    return all(callable(getattr(candidate, hook, None)) for hook in REQUIRED_HOOKS) and all(
        hasattr(candidate, attribute) for attribute in REQUIRED_ATTRIBUTES
    )


#: How many members of the two contracts a class in a *scanned* file must
#: carry before it is treated as an attempted strategy. A manifest needs no
#: such guess — it says what it publishes — but a directory scan has to tell a
#: half-written strategy (worth reporting) from a helper class (not). Two is
#: the smallest number no ordinary class reaches by accident.
STRATEGY_CANDIDATE_THRESHOLD = 2


def looks_like_a_strategy(candidate: Any) -> bool:
    """Whether *candidate* is a class that was *trying* to be a strategy.

    Weaker than :func:`implements_strategy_contract` on purpose. The loader
    wants to know what it can drive; the auditor wants to know what someone
    meant to write, so that a class missing ``tp2`` is reported rather than
    silently passed over as though it were a utility.

    Abstract classes are excluded here too, which is what keeps a plugin repo's
    own restated base out of both answers.
    """
    if not inspect.isclass(candidate) or inspect.isabstract(candidate):
        return False
    if candidate is StrategyBase or candidate is SignalStrategy:
        return False
    members = set(REQUIRED_HOOKS) | set(SIGNAL_METHODS)
    present = sum(1 for member in members if callable(getattr(candidate, member, None)))
    return present >= STRATEGY_CANDIDATE_THRESHOLD


def overrides_on_tick(strategy: StrategyLike) -> bool:
    """Whether *strategy* actually manages risk between bar closes.

    The runner subscribes to the whole tick feed when some strategy wants it,
    so answering "yes" for a plugin that only inherited the no-op default would
    put every tick on the wire for nothing.

    Comparing against ``StrategyBase.on_tick`` cannot work here — a plugin's
    inherited default is *its* base's method, not ours. What both bases have in
    common is being abstract, so the question becomes: is the class that
    defines ``on_tick`` a base class, or the strategy itself?
    """
    for klass in type(strategy).__mro__:
        if "on_tick" in klass.__dict__:
            return not getattr(klass, "__abstractmethods__", frozenset())
    return False


def coerce_intent(intent: Any) -> SignalIntent:
    """Return *intent* as one of ours, converting a plugin's object if needed.

    Reads the fields by name rather than calling a method on the object, so a
    plugin owes us data and not an implementation. Anything missing takes this
    dataclass's own default, which means a plugin may legitimately carry fewer
    fields than we do — but a plugin that renames one will lose it silently,
    and that is what ``INTENT_FIELDS`` and its test exist to prevent.
    """
    if isinstance(intent, SignalIntent):
        return intent

    action = getattr(intent, "action", None)
    if action is None:
        raise TypeError(
            f"{type(intent).__name__} is not a signal intent — it has no 'action'. A strategy "
            "must return intent objects, one or a sequence of them, or None."
        )

    values: dict[str, Any] = {"action": _coerce_action(action)}
    for name in INTENT_FIELDS:
        if name == "action":
            continue
        value = getattr(intent, name, None)
        if value is not None:
            values[name] = _coerce_scaling(value) if name == "scaling" else value
    return SignalIntent(**values)


def as_intents(result: Any) -> list[SignalIntent]:
    """Normalise whatever a hook returned into a list of our intents.

    Both drivers funnel through this, which is the point: the backtest and the
    live runner must agree bar for bar on what a strategy said, including when
    a plugin returns its own intent type.
    """
    if result is None:
        return []
    # An intent carries an action; anything else that arrives here is the
    # sequence (or generator) of them a strategy is also allowed to return.
    if isinstance(result, SignalIntent) or hasattr(result, "action"):
        return [coerce_intent(result)]
    return [coerce_intent(intent) for intent in result]


def _coerce_action(action: Any) -> SignalAction:
    """A plugin's own ``(str, Enum)`` action, or a bare string, becomes ours."""
    if isinstance(action, SignalAction):
        return action
    raw = getattr(action, "value", action)
    try:
        return SignalAction(str(raw))
    except ValueError:
        raise ValueError(
            f"{raw!r} is not a broker action. Expected one of "
            f"{', '.join(member.value for member in SignalAction)}."
        ) from None


def _coerce_scaling(scaling: Any) -> Scaling:
    """A plugin's scaling block — any object with the three fields — becomes ours."""
    if isinstance(scaling, Scaling):
        return scaling
    return Scaling(
        tp=getattr(scaling, "tp", None),
        sl=getattr(scaling, "sl", None),
        quantity=getattr(scaling, "quantity", None),
    )


# ─────────────────────────────────────────────────────────────────────────
# The QTE signal interface
#
# StrategyBase above says how the engine *drives* a strategy: one hook, one
# frame, intents out. It is deliberately minimal because a plugin repository
# restates it on its own side.
#
# What follows is the interface every strategy in ``__strategies__/`` is
# required to present: one method per broker action, so that reading a class
# tells you what it can emit without reading the body of a 300-line
# ``on_candle_closed``. ``qte-strategy-audit`` enforces it across the mounted
# repos; the loader does not, so a half-migrated repo still backtests.
# ─────────────────────────────────────────────────────────────────────────

#: Every strategy must answer for these five. They are the two actions that
#: open a position and the three that close one — a strategy with no stated
#: stop is not a strategy.
REQUIRED_SIGNAL_METHODS = ("long", "short", "tp1", "tp2", "sl")

#: These two may be left off. ``r_sl`` (the stop moved to break-even and
#: beyond) is a refinement of ``sl``; ``flat`` is a discretionary close that
#: many strategies delegate entirely to the broker-side bracket.
OPTIONAL_SIGNAL_METHODS = ("r_sl", "flat")

#: The whole surface, in declaration order.
SIGNAL_METHODS = REQUIRED_SIGNAL_METHODS + OPTIONAL_SIGNAL_METHODS

#: Which broker action each method is allowed to emit. The dispatcher checks
#: this: a ``tp1()`` that returns a ``SHORT`` is a bug that would otherwise
#: reach the broker as a valid-looking payload.
SIGNAL_METHOD_ACTIONS: dict[str, SignalAction] = {
    "long": SignalAction.LONG,
    "short": SignalAction.SHORT,
    "tp1": SignalAction.TP1,
    "tp2": SignalAction.TP2,
    "sl": SignalAction.SL,
    "r_sl": SignalAction.R_SL,
    "flat": SignalAction.FLAT,
}

#: Asked in this order while a position is open. Protective first: if one bar
#: both stopped out and reached a target, the stop is what happened.
EXIT_SIGNAL_ORDER = ("sl", "r_sl", "tp1", "tp2", "flat")

#: Asked in this order while flat, and the first one that answers wins — a bar
#: cannot be both a long and a short entry.
ENTRY_SIGNAL_ORDER = ("long", "short")

#: Positional parameters a signal method takes after ``self``. Parameter names
#: are the author's business; the count is not, because the dispatcher calls
#: them positionally.
SIGNAL_METHOD_ARITY = 2


class SignalStrategy(StrategyBase):
    """The standard interface for a strategy in ``__strategies__/``.

    One method per broker action instead of one method with seven branches.
    Each is handed the same frame and context :meth:`on_candle_closed` gets,
    and returns an intent, several, or ``None`` for "not this bar".

    :meth:`on_candle_closed` is implemented here and sequences them:

    * holding a position → :data:`EXIT_SIGNAL_ORDER`, every hook asked;
    * flat → :data:`ENTRY_SIGNAL_ORDER`, first answer wins.

    "Holding" is ``context.open_uxid``, which the runner and the backtest both
    maintain, so the sequencing is identical in the two drivers. Override
    :meth:`on_candle_closed` if a strategy needs a different order — the seven
    methods remain the published surface either way, and that surface is what
    the audit reads.

    Subclassing this is the simplest route for a strategy living beside the
    engine. A separate plugin repository restates it instead; the audit
    recognises the shape rather than the ancestry, exactly as the loader does.
    """

    # ── Entries ───────────────────────────────────────────────────────

    @abstractmethod
    def long(self, df: pd.DataFrame, context: StrategyContext) -> IntentResult:
        """Open long, or ``None``. Carry the ``sl`` on the intent — see :meth:`sl`."""

    @abstractmethod
    def short(self, df: pd.DataFrame, context: StrategyContext) -> IntentResult:
        """Open short, or ``None``."""

    # ── Exits ─────────────────────────────────────────────────────────

    @abstractmethod
    def tp1(self, df: pd.DataFrame, context: StrategyContext) -> IntentResult:
        """Take the first target — usually partial; see ``tp1_percent``."""

    @abstractmethod
    def tp2(self, df: pd.DataFrame, context: StrategyContext) -> IntentResult:
        """Take the runner off. Closes whatever :meth:`tp1` left behind."""

    @abstractmethod
    def sl(self, df: pd.DataFrame, context: StrategyContext) -> IntentResult:
        """Stop out.

        Required even when the broker-side bracket does the stopping, because
        "the bracket handles it" is worth one explicit line rather than leaving
        a reader to infer it from an absence.
        """

    def r_sl(self, df: pd.DataFrame, context: StrategyContext) -> IntentResult:
        """Optional: re-stop — break-even, or a trailed level. Default: never."""
        return None

    def flat(self, df: pd.DataFrame, context: StrategyContext) -> IntentResult:
        """Optional: close for a reason that is neither a target nor a stop."""
        return None

    # ── The dispatcher ────────────────────────────────────────────────

    def on_candle_closed(self, df: pd.DataFrame, context: StrategyContext) -> IntentResult:
        """Ask the signal methods in order. See the class docstring."""
        if context.open_uxid is not None:
            collected: list[SignalIntent] = []
            for name in EXIT_SIGNAL_ORDER:
                collected.extend(self._ask(name, df, context))
            return collected

        for name in ENTRY_SIGNAL_ORDER:
            produced = self._ask(name, df, context)
            if produced:
                return produced
        return []

    def _ask(self, name: str, df: pd.DataFrame, context: StrategyContext) -> list[SignalIntent]:
        """Call one signal method and check it emitted the action it is named for."""
        intents = as_intents(getattr(self, name)(df, context))
        expected = SIGNAL_METHOD_ACTIONS[name]
        for intent in intents:
            if intent.action is not expected:
                raise ValueError(
                    f"{type(self).__name__}.{name}() returned a {intent.action.value} intent. "
                    f"A signal method emits only {expected.value}; that decision belongs in "
                    f"{_method_for_action(intent.action)}()."
                )
        return intents

    # ── Metadata ──────────────────────────────────────────────────────

    def describe(self) -> dict[str, Any]:
        """As :meth:`StrategyBase.describe`, plus which actions this class emits."""
        described = super().describe()
        described["signals"] = list(implemented_signal_methods(type(self)))
        return described


def _method_for_action(action: SignalAction) -> str:
    for name, candidate in SIGNAL_METHOD_ACTIONS.items():
        if candidate is action:
            return name
    return action.value.lower()


def defines_signal_method(candidate: Any, name: str) -> bool:
    """Whether *candidate* implements ``name`` rather than inheriting a placeholder.

    The distinction is the whole check. Both bases in play — ours, and the one
    a plugin repo restates — declare all seven methods: the five required ones
    abstract, the two optional ones as no-ops returning ``None``. So finding
    the attribute proves nothing, and for ``r_sl``/``flat`` finding a *callable*
    proves nothing either. What answers it is the class the method is defined
    on: if that class is the one *declaring the interface* — the marker being
    that it leaves at least one required method abstract in its own body — then
    what it hands down is a placeholder. Anywhere else, it is an
    implementation. Same reasoning as :func:`overrides_on_tick`.

    "Declares the interface" rather than the simpler "is abstract" because a
    half-migrated strategy is abstract too, and its finished ``long`` is a real
    implementation the audit should credit while still reporting the three
    methods it has not written yet.

    A plugin whose base restates the interface without ``abstractmethod`` — a
    base of plain no-ops — defeats this and is credited with the defaults it
    hands down. That is the cost of a structural check over a repository we do
    not control, and it errs toward accepting a working strategy rather than
    rejecting one.
    """
    if not inspect.isclass(candidate):
        return False
    for klass in candidate.__mro__:
        member = klass.__dict__.get(name)
        if member is None:
            continue
        if getattr(member, "__isabstractmethod__", False):
            return False
        return callable(member) and not _declares_the_signal_interface(klass)
    return False


def _declares_the_signal_interface(klass: type) -> bool:
    """Whether *klass* is a base publishing the interface rather than using it.

    True when it leaves a required signal method abstract in its own body,
    which both :class:`SignalStrategy` and a plugin repo's restatement of it
    do, and no actual strategy does.
    """
    return any(
        getattr(klass.__dict__.get(required), "__isabstractmethod__", False)
        for required in REQUIRED_SIGNAL_METHODS
    )


def implemented_signal_methods(candidate: Any) -> tuple[str, ...]:
    """Which of :data:`SIGNAL_METHODS` *candidate* actually implements."""
    return tuple(name for name in SIGNAL_METHODS if defines_signal_method(candidate, name))


def missing_signal_methods(candidate: Any) -> tuple[str, ...]:
    """Which of :data:`REQUIRED_SIGNAL_METHODS` *candidate* still owes."""
    return tuple(
        name for name in REQUIRED_SIGNAL_METHODS if not defines_signal_method(candidate, name)
    )


def implements_signal_contract(candidate: Any) -> bool:
    """Whether *candidate* presents the full required signal surface.

    Structural, like :func:`implements_strategy_contract`, and for the same
    reason: a plugin repository restates the interface rather than importing
    it. This is what ``qte-strategy-audit`` enforces over ``__strategies__/``.
    """
    return inspect.isclass(candidate) and not missing_signal_methods(candidate)
