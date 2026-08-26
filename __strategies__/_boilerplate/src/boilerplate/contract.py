"""The engine's contract, restated here — this repository imports nothing from it.

That is the point of the plugin seam, and it is worth being precise about why
a copy is better than an import:

* **the strategy is the core; the engine is a delivery layer.** A rule about
  price should not depend on the process that happens to run it. Depending
  upward means the edge cannot be reasoned about, tested or moved without the
  machinery underneath it coming along;
* **this repo builds, lints and tests with the engine nowhere in sight.** Its
  own lockfile, its own Python, its own release cycle. `pytest` here needs
  pandas and numpy, and nothing else;
* **the engine accepts it anyway.** It recognises a strategy *structurally* —
  a concrete ``on_candle_closed``, a ``name``, a ``history_window()`` — and
  converts the intents it gets back at the boundary. Ancestry is never checked,
  by the loader or by ``qte-strategy-audit``.

The cost is the one every duplicated contract has: a field renamed on this side
is a field silently lost at the boundary, because the conversion reads by name.
The engine pins this list in its own test suite (``INTENT_FIELDS`` in
``tests/test_plugin_contract.py``), so what you must not do is rename; adding
your own extra fields is free — the engine ignores what it does not know.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

import pandas as pd

# ── What the broker accepts ───────────────────────────────────────────


class SignalAction(str, Enum):
    """The seven actions a signal may carry.

    ``(str, Enum)`` rather than ``StrEnum`` on purpose: it mirrors how the
    broker declares the same enum, and ``StrEnum`` would change what ``str()``
    returns and drift this repo's wire format from theirs.
    """

    LONG = "LONG"
    SHORT = "SHORT"
    TP1 = "TP1"
    TP2 = "TP2"
    R_SL = "R_SL"
    SL = "SL"
    FLAT = "FLAT"


@dataclass(slots=True)
class Scaling:
    """Levels and size used when adding to an existing position."""

    tp: float | None = None
    sl: float | None = None
    quantity: float | None = None


@dataclass(slots=True)
class SignalIntent:
    """What a strategy decided, before anyone turns it into a broker payload.

    Every field name here is read by the engine when it converts this object
    into its own — so the names are the contract, and the set below is the
    whole of it. A strategy may carry fewer than these; it must not rename one.

    ``signal_uxid`` ties a close back to the entry that opened it, under the same
    name the Pine strategies and the broker payload use. Leave it ``None`` on
    an entry — the runner mints the trade-cycle id and remembers it — and set it
    to ``context.open_uxid`` on a follow-up that manages an exit.

    ``quantity`` is a proposal, not the size that trades. The runner risk-sizes
    every entry against the account it was configured with and rescales your
    closes by the same factor, so the *proportions* you ask for survive and the
    absolute size stays the operator's decision.
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
    use_equity_sizing: bool | None = None
    is_running: bool | None = None
    is_scale_position: bool | None = None
    scale_strategy: str | None = None
    scaling: Scaling | None = None
    signal_uxid: str | None = None
    indicators: dict[str, Any] = field(default_factory=dict)
    inputs: dict[str, Any] = field(default_factory=dict)
    reason: str = ""


@dataclass(slots=True)
class StrategyContext:
    """Everything a strategy is allowed to know about the world around it.

    Deliberately small: a strategy that needs an account balance or an open-
    position list is reaching past the boundary that keeps backtest and live
    identical.

    The engine passes *its* context object, not this one. Only the attribute
    names matter, which is why this class exists here at all — for type hints
    and for the tests in this repo, which have no engine to borrow one from.
    """

    symbol: str
    timeframe: str
    now: datetime
    mode: str = "backtest"
    params: dict[str, Any] = field(default_factory=dict)
    #: Cycle id of the position the driver believes this strategy holds.
    #: ``None`` means flat, and that is what selects entries over exits below.
    open_uxid: str | None = None

    @property
    def is_live(self) -> bool:
        return self.mode == "live"


#: Return type of every signal method: nothing, one intent, or several.
IntentResult = SignalIntent | Sequence[SignalIntent] | None

#: Floor for the default history window. A strategy with a short warm-up still
#: wants enough bars for a chart-length view of what it is trading.
MIN_HISTORY_WINDOW = 400


# ── What the engine drives ────────────────────────────────────────────


class StrategyBase(ABC):
    """The four members both drivers call, and the attributes they read first.

    ``on_candle_closed``, ``on_start``, ``on_stop`` and ``history_window`` are
    what "the engine can drive this" means; ``name``, ``timeframe`` and
    ``warmup`` are read before the first bar. Keep this class small — every
    line added here is a line a plugin repository has to restate.
    """

    #: Strategy name as the broker knows it: the NATS subject its workers
    #: subscribe to, so it must match what they are configured for exactly.
    name: str = ""
    #: Symbols this strategy wants; empty means "whatever the engine feeds me".
    symbols: Sequence[str] = ()
    #: Timeframe whose closes drive :meth:`on_candle_closed`.
    timeframe: str = "M15"
    #: Closed candles required before the first call, so indicators never see a
    #: half-warm window.
    warmup: int = 200
    #: How many closed candles the hook receives, in **both** drivers. ``None``
    #: means ``max(warmup * 2, MIN_HISTORY_WINDOW)``.
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
        first, whose last row is the just-closed candle — so ``df.iloc[-1]`` is
        always finished and never repaints.
        """

    def on_tick(self, price: float, context: StrategyContext) -> IntentResult:
        """Optional risk management between bar closes. Default: do nothing.

        The runner puts the whole tick feed on the wire only when some strategy
        overrides this, so inheriting the default costs nothing.
        """
        return None

    # ── Convenience ───────────────────────────────────────────────────

    def history_window(self) -> int | None:
        """Bars handed to :meth:`on_candle_closed`; ``None`` means unbounded.

        Both drivers call this, which is the point: the live runner and the
        backtest must hand the same number of bars to the same code, or a
        strategy with a running sum computes one thing on the chart and another
        in production.
        """
        if self.max_history == 0:
            return None
        return self.max_history or max(self.warmup * 2, MIN_HISTORY_WINDOW)

    def param(self, key: str, default: Any = None) -> Any:
        """One tunable, from the params the driver was configured with."""
        return self.params.get(key, default)

    def describe(self) -> dict[str, Any]:
        """Metadata the runner logs and the control plane exposes."""
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


# ── What a strategy presents ──────────────────────────────────────────

#: Asked in this order while holding, and every one is asked: taking tp1 and
#: trailing the stop on the same bar is normal. The stop is asked first because
#: if one bar both stopped out and reached a target, the stop is what happened.
EXIT_SIGNAL_ORDER = ("sl", "r_sl", "tp1", "tp2", "flat")

#: Asked in this order while flat, first answer wins — a bar cannot be both.
ENTRY_SIGNAL_ORDER = ("long", "short")

#: Which action each method is allowed to emit. A ``tp1()`` returning a
#: ``SHORT`` is a bug that would otherwise reach the broker looking valid.
SIGNAL_METHOD_ACTIONS: dict[str, SignalAction] = {
    "long": SignalAction.LONG,
    "short": SignalAction.SHORT,
    "tp1": SignalAction.TP1,
    "tp2": SignalAction.TP2,
    "sl": SignalAction.SL,
    "r_sl": SignalAction.R_SL,
    "flat": SignalAction.FLAT,
}

#: The five that must be written, and the two that may be left to the defaults.
REQUIRED_SIGNAL_METHODS = ("long", "short", "tp1", "tp2", "sl")
OPTIONAL_SIGNAL_METHODS = ("r_sl", "flat")
SIGNAL_METHODS = REQUIRED_SIGNAL_METHODS + OPTIONAL_SIGNAL_METHODS


class SignalStrategy(StrategyBase):
    """One method per broker action, instead of one method with seven branches.

    Each is handed the same frame and context ``on_candle_closed`` gets, and
    returns an intent, several, or ``None`` for "not this bar". A reader can
    then tell what a strategy is able to emit without opening a single body,
    and ``qte-strategy-audit`` reads exactly this surface.

    ``on_candle_closed`` is implemented here and sequences them. Override it if
    a strategy needs a different order; the seven methods stay the published
    surface either way.
    """

    # ── Entries ───────────────────────────────────────────────────────

    @abstractmethod
    def long(self, df: pd.DataFrame, context: StrategyContext) -> IntentResult:
        """Open long, or ``None``. Carry the stop on the intent — see :meth:`sl`."""

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

        Required even when a broker-side bracket does the stopping, because
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
        """Ask the signal methods in order. "Holding" is ``context.open_uxid``,
        which both drivers maintain, so live and backtest sequence identically.
        """
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
                    f"A signal method emits only {expected.value}."
                )
        return intents

    # ── Metadata ──────────────────────────────────────────────────────

    def describe(self) -> dict[str, Any]:
        """As :meth:`StrategyBase.describe`, plus which actions this class emits."""
        described = super().describe()
        described["signals"] = [
            name for name in SIGNAL_METHODS if getattr(type(self), name, None) is not None
        ]
        return described


def as_intents(result: IntentResult) -> list[SignalIntent]:
    """Normalise whatever a signal method returned into a list."""
    if result is None:
        return []
    if isinstance(result, SignalIntent):
        return [result]
    return list(result)
