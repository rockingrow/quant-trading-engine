"""The contract a QTE strategy implements, restated on this side of the seam.

The engine drives every strategy through three things and nothing else: it
hands over an OHLCV frame and a :class:`StrategyContext`, and it takes back
:class:`SignalIntent` objects. That is the whole interface, which is why it can
be restated here in stdlib and pandas instead of imported from ``qte_shared``.

**Structural, not nominal.** The engine does not check ``issubclass`` against
its own base — it checks that a class *behaves* like a strategy (a concrete
``on_candle_closed``, a ``name``, a ``history_window()``) and adapts the
intents it returns into its own pydantic models at the boundary. So this base
class and the engine's are two independent implementations of one agreement,
and neither imports the other.

**What a strategy is not allowed to know.** No account balance, no open
position list, no broker. Sizing against live equity, minting correlation ids
and placing the bracket are the runner's job; a strategy that reached for them
would compute one thing in a backtest and another in production, which is the
exact divergence the seam exists to rule out.
"""

from __future__ import annotations

import secrets
from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any

import pandas as pd

from qss.core.models import Scaling, SignalAction


@dataclass(slots=True)
class SignalIntent:
    """What a strategy decided, before it becomes a broker payload.

    Field names *are* the contract: the engine reads them off this object by
    name to build its own payload, so renaming one here breaks delivery
    silently rather than loudly. ``tests/test_contract.py`` pins the set.

    ``signal_uxid`` ties a close back to the entry that opened it, under the
    same name the Pine strategies and the broker payload use. Leave it ``None``
    on an entry — the runner mints the cycle id and remembers it — and set it
    on a follow-up when the strategy is managing an exit it opened itself. A
    pair allowed several cycles (``context.max_open_cycles > 1``) needs it on
    both: mint it on the entry with :func:`new_uxid`, keep it with the trade,
    and set it on every close, since an unnamed close is refused while more
    than one cycle is open.
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
    is_running: bool | None = None
    is_scale_position: bool | None = None
    scale_strategy: str | None = None
    scaling: Scaling | None = None
    signal_uxid: str | None = None
    indicators: dict[str, Any] = field(default_factory=dict)
    inputs: dict[str, Any] = field(default_factory=dict)
    reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Plain-data view, for logging and for tests that assert on shape."""
        data = asdict(self)
        data["action"] = self.action.value
        return data


@dataclass(slots=True)
class StrategyContext:
    """Everything a strategy is allowed to know about the world around it.

    Deliberately small — see the module docstring. ``mode`` is the only hint
    that live and backtest differ, and it is there for logging, not for
    branching the decision.
    """

    symbol: str
    timeframe: str
    now: datetime
    mode: str = "backtest"
    params: dict[str, Any] = field(default_factory=dict)
    #: Cycle id of the position this strategy currently believes it holds, as
    #: tracked by whoever is driving it. ``None`` means flat. With several
    #: cycles open it is the most recent; :attr:`open_uxids` lists them all.
    open_uxid: str | None = None
    #: Every cycle open on this pair, oldest first. Empty when flat.
    open_uxids: tuple[str, ...] = ()
    #: How many cycles the pair may hold at once — 1 unless the engine's mapping
    #: table sets ``allow_multiple_cycles`` and ``max_open_cycles`` for it.
    max_open_cycles: int = 1

    @property
    def is_live(self) -> bool:
        return self.mode == "live"

    @property
    def can_open(self) -> bool:
        """Whether a new entry would be accepted — fewer cycles open than allowed."""
        held = len(self.open_uxids) or (1 if self.open_uxid is not None else 0)
        return held < self.max_open_cycles


def new_uxid() -> str:
    """A fresh trade-cycle id in the broker's shape: 16 uppercase hex characters."""
    return secrets.token_hex(8).upper()


def open_cycles(context: Any) -> tuple[str, ...]:
    """The cycles *context* reports, tolerating a driver that predates ``open_uxids``."""
    reported = tuple(getattr(context, "open_uxids", ()) or ())
    if reported:
        return reported
    single = getattr(context, "open_uxid", None)
    return (single,) if single is not None else ()


def cycle_limit(context: Any) -> int:
    """How many cycles *context* allows, 1 for a driver that predates the setting."""
    return max(1, int(getattr(context, "max_open_cycles", 1) or 1))


IntentResult = SignalIntent | Sequence[SignalIntent] | None

#: Floor for the default history window. A strategy with a short warm-up still
#: wants enough bars for a chart-length view of what it is trading.
MIN_HISTORY_WINDOW = 400


class StrategyBase(ABC):
    """Base class the strategies in this repo subclass.

    The engine instantiates a subclass with ``params`` from configuration and
    calls the hooks below. Only :meth:`on_candle_closed` is required.
    """

    #: Strategy name as the broker knows it. It is the NATS subject workers
    #: subscribe to, so it must match the worker's configured strategy exactly
    #: — and it is the alias this repo publishes it under. :mod:`qss.registry`
    #: refuses to load a class whose ``name`` disagrees with its alias.
    name: str = ""
    #: Symbols this strategy wants; empty means "whatever the engine feeds me".
    symbols: Sequence[str] = ()
    #: Timeframe whose closes drive :meth:`on_candle_closed`.
    timeframe: str = "M15"
    #: Minimum closed candles before the first call — the driver buffers until
    #: it has this many, so indicators never see a half-warm window.
    warmup: int = 200
    #: How many closed candles ``on_candle_closed`` receives, in **both** the
    #: backtest and the live runner. ``None`` means
    #: ``max(warmup * 2, MIN_HISTORY_WINDOW)``; ``0`` means every bar available.
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
        Override when an exit has to react faster than the bar close — the
        runner subscribes to the tick feed only when some strategy does.
        """
        return None

    # ── Convenience for subclasses ────────────────────────────────────

    def history_window(self) -> int | None:
        """Bars handed to :meth:`on_candle_closed`; ``None`` means unbounded.

        Both drivers call this, which is the point. A live runner keeps a
        bounded buffer while a backtest holds the whole file, so without one
        agreed bound a strategy using a running sum or a session VWAP would
        compute one thing on the chart and another in production.

        ``max_history = 0`` opts out and is nearly always a mistake: a backtest
        then sees the whole parquet file while a restarted runner sees only
        what Redis retained. The runner warns when a strategy asks for it.
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


# ─────────────────────────────────────────────────────────────────────────
# The QTE signal interface
#
# StrategyBase above says how the engine *drives* a strategy: one hook, one
# frame, intents out. What follows is the interface every strategy in a
# mounted plugin repository is required to *present* — one method per broker
# action, so reading a class tells you what it can emit without reading the
# body of a 900-line ``on_candle_closed``. ``qte-strategy-audit`` enforces it;
# the engine's loader does not, so a half-migrated repo still backtests.
#
# Restated here for the same reason the rest of this module is: the audit
# recognises the shape, never the ancestry, so this file and the engine's
# ``qte_shared.strategy_base`` stay two implementations of one agreement.
# ─────────────────────────────────────────────────────────────────────────

#: Every strategy must answer for these five: the two actions that open a
#: position and the three that close one. A strategy with no stated stop is
#: not a strategy — "the broker bracket does it" is an answer, an absence is
#: not.
REQUIRED_SIGNAL_METHODS = ("long", "short", "tp1", "tp2", "sl")

#: These two may be left off. ``r_sl`` (the stop moved to break-even and
#: beyond) is a refinement of ``sl``; ``flat`` is a discretionary close many
#: strategies delegate entirely to the broker-side bracket.
OPTIONAL_SIGNAL_METHODS = ("r_sl", "flat")

#: The whole surface, in declaration order.
SIGNAL_METHODS = REQUIRED_SIGNAL_METHODS + OPTIONAL_SIGNAL_METHODS

#: Which broker action each method is allowed to emit. :meth:`SignalStrategy._ask`
#: checks it: a ``tp1()`` that returns a ``SHORT`` is a bug that would otherwise
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


def as_intents(result: IntentResult) -> list[SignalIntent]:
    """``None``, one intent or several, flattened to a list."""
    if result is None:
        return []
    if isinstance(result, SignalIntent):
        return [result]
    return list(result)


class SignalStrategy(StrategyBase):
    """The interface a strategy in this repo presents to the engine.

    One method per broker action instead of one method with seven branches.
    Each is handed the same frame and context :meth:`on_candle_closed` gets,
    and returns an intent, several, or ``None`` for "not this bar".

    :meth:`on_candle_closed` is implemented here and sequences them:

    * holding a position → :data:`EXIT_SIGNAL_ORDER`, every hook asked;
    * flat → :data:`ENTRY_SIGNAL_ORDER`, first answer wins.

    "Holding" is ``context.open_uxid``, which the live runner and the backtest
    both maintain, so the sequencing is identical in the two drivers. A pair
    allowed several cycles is asked for its exits and then, while it has room,
    for one more entry.

    A strategy whose per-bar bookkeeping has to run whether or not it is
    holding — folding higher timeframes, advancing a mirrored position —
    overrides :meth:`on_candle_closed` and calls the seven methods itself. The
    surface is what the audit reads, not the dispatcher.
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
            for method_name in EXIT_SIGNAL_ORDER:
                collected.extend(self._ask(method_name, df, context))
            if len(open_cycles(context)) < cycle_limit(context):
                for method_name in ENTRY_SIGNAL_ORDER:
                    produced = self._ask(method_name, df, context)
                    if produced:
                        return collected + produced
            return collected or None

        for method_name in ENTRY_SIGNAL_ORDER:
            produced = self._ask(method_name, df, context)
            if produced:
                return produced
        return None

    def _ask(
        self, method_name: str, df: pd.DataFrame, context: StrategyContext
    ) -> list[SignalIntent]:
        """Call one signal method and check it emitted the action it is named for."""
        intents = as_intents(getattr(self, method_name)(df, context))
        expected = SIGNAL_METHOD_ACTIONS[method_name]
        for intent in intents:
            if intent.action is not expected:
                raise ValueError(
                    f"{self.name}.{method_name}() returned a {intent.action.value} intent; "
                    f"that method may only emit {expected.value}"
                )
        return intents
