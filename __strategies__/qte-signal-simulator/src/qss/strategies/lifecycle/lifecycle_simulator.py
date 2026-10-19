"""A strategy with no edge: it drives full trade cycles to test algo-trading-broker.

Every closed bar opens a position, and every position then walks one scripted
path, one step per bar, to one of the three ways a real cycle ends:

======================  ==================  ==================
outcome                 bar after entry     the bar after that
======================  ==================  ==================
``tp1_tp2``             ``TP1``             ``TP2``
``sl``                  ``SL``              —
``tp1_rsl``             ``TP1``             ``R_SL``
======================  ==================  ==================

Each cycle keeps one ``signal_uxid`` from its entry to its last exit, exactly as
a real strategy's does, so what the broker sees is a real cycle.

**Nothing about the market is written here.** ``symbols`` is empty and the
timeframe comes from the ``timeframe`` param, so which symbol and which bar it
runs on is decided in the engine's ``config/strategies_mapping.toml``::

    [symbols.XAUUSD]
    strategies = ["SIGNAL_SIMULATOR_LIFECYCLE_V1"]

    [symbols.XAUUSD.params.SIGNAL_SIMULATOR_LIFECYCLE_V1]
    timeframe = "M1"

The class attribute inherited from :class:`~qss.core.StrategyBase` is only the
contract's fallback for a caller that passes no params — the engine's audit
does, to check the class instantiates. The runner and the backtest both build
the instance from the mapping first, then read the timeframe off it. The bar
must be one ingestion produces for that symbol, or no candle ever arrives.

**The randomness is reproducible.** The direction and the outcome are drawn
from a generator seeded by the cycle's ``signal_uxid``, and that id is derived
from the strategy, the symbol, the entry bar and the ``seed`` param. The same
bars therefore give the same cycles in a backtest, in the runner, and in the
runner again after a restart replays a bar. ``seed`` changes the sequence,
``outcome`` forces one path, ``weights`` skews the draw.

**Nothing caps the open cycles**, so it enters on every bar it is asked on and
stacks cycles while earlier ones are still being walked.

``tp1_qty_pc`` must stay below 100: a ``TP1`` that closes everything ends the
cycle, and the ``TP2``/``R_SL`` after it would name one that no longer exists.
The mapping's ``tp1_qty_percent`` is the engine's sizing of the same close.

Scripts live in memory. A cycle restored after a restart that this process never
opened has no script, so it is closed once with ``FLAT`` instead of being left
open with nothing deciding on it.
"""

from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass
from typing import Any

import pandas as pd

from qss.core import (
    IntentResult,
    SignalAction,
    SignalIntent,
    SignalStrategy,
    StrategyContext,
    get_logger,
    normalize_timeframe,
    open_cycles,
)

log = get_logger(__name__)

#: The three ways a scripted cycle ends. The order is the order of ``weights``.
OUTCOMES = ("tp1_tp2", "sl", "tp1_rsl")

#: Which exits each outcome emits, one per bar after the entry.
EXIT_PATHS: dict[str, tuple[SignalAction, ...]] = {
    "tp1_tp2": (SignalAction.TP1, SignalAction.TP2),
    "sl": (SignalAction.SL,),
    "tp1_rsl": (SignalAction.TP1, SignalAction.R_SL),
}


@dataclass(slots=True)
class CyclePlan:
    """What one cycle was scripted to do, and how far it has got."""

    outcome: str
    direction: int
    entry_price: float
    stop: float
    take_profit_1: float
    take_profit_2: float
    #: Exits emitted so far.
    steps_taken: int = 0
    #: The bar the last intent was produced on — one intent per bar per cycle.
    last_bar: pd.Timestamp | None = None


class LifecycleSimulator(SignalStrategy):
    """Opens on every bar, then TP1 → TP2, SL, or TP1 → R_SL at random."""

    name = "SIGNAL_SIMULATOR_LIFECYCLE_V1"
    #: Deliberately empty: the mapping table decides where this runs.
    symbols = ()
    #: Only the ATR that sizes the bracket needs warming.
    warmup = 30

    def __init__(self, params: dict[str, Any] | None = None) -> None:
        super().__init__(params)
        configured = self.param("timeframe")
        if configured is not None:
            # An instance attribute, read by the runner and the backtest after
            # they build the instance with the mapping's params.
            self.timeframe = normalize_timeframe(str(configured))
        self._plans: dict[str, CyclePlan] = {}
        #: Cycles closed with FLAT because no script existed for them.
        self._orphans_closed: set[str] = set()
        #: The exits decided for the current bar, keyed by action.
        self._pending: dict[SignalAction, list[SignalIntent]] = {}

    def on_start(self, context: StrategyContext) -> None:
        if self.param("timeframe") is None:
            log.warning(
                "%s has no `timeframe` param for %s — running on the fallback %s. "
                "Set it in config/strategies_mapping.toml.",
                self.name,
                context.symbol,
                self.timeframe,
            )

    # -- Dispatch --------------------------------------------------------

    def on_candle_closed(self, df: pd.DataFrame, context: StrategyContext) -> IntentResult:
        """Decide every open cycle's next exit once, then let the base class ask.

        The base class asks ``sl`` → ``r_sl`` → ``tp1`` → ``tp2`` → ``flat``;
        each method only hands back what was decided here for its own action.
        """
        self._pending = self._decide_exits(df, context)
        try:
            return super().on_candle_closed(df, context)
        finally:
            self._pending = {}

    def _decide_exits(
        self, df: pd.DataFrame, context: StrategyContext
    ) -> dict[SignalAction, list[SignalIntent]]:
        bar_time = df.index[-1]
        close = float(df["close"].iloc[-1])
        held = open_cycles(context)
        # Forget cycles the driver no longer holds, so memory follows the book.
        for uxid in set(self._plans) - set(held):
            del self._plans[uxid]

        pending: dict[SignalAction, list[SignalIntent]] = {}
        for uxid in held:
            intent = self._next_exit(uxid, bar_time, close, context)
            if intent is not None:
                pending.setdefault(intent.action, []).append(intent)
        return pending

    def _next_exit(
        self, uxid: str, bar_time: pd.Timestamp, close: float, context: StrategyContext
    ) -> SignalIntent | None:
        plan = self._plans.get(uxid)
        if plan is None:
            if uxid in self._orphans_closed:
                return None
            self._orphans_closed.add(uxid)
            return self._exit_intent(SignalAction.FLAT, uxid, close, context, reason="no script")
        if plan.last_bar is not None and bar_time <= plan.last_bar:
            return None  # Already acted on this bar — the entry bar included.
        path = EXIT_PATHS[plan.outcome]
        if plan.steps_taken >= len(path):
            return None
        action = path[plan.steps_taken]
        plan.steps_taken += 1
        plan.last_bar = bar_time

        fields: dict[str, Any] = {}
        if action is SignalAction.TP1:
            fields["tp1_percent"] = self._tp1_percent()
        elif action is SignalAction.SL:
            fields["sl"] = plan.stop
        elif action is SignalAction.R_SL:
            # Break-even: TP1 moved the stop to the entry, and that is what hit.
            fields["sl"] = plan.entry_price
        return self._exit_intent(action, uxid, close, context, plan=plan, **fields)

    def _exit_intent(
        self,
        action: SignalAction,
        uxid: str,
        close: float,
        context: StrategyContext,
        *,
        plan: CyclePlan | None = None,
        reason: str = "",
        **fields: Any,
    ) -> SignalIntent:
        step = f"{plan.outcome} step {plan.steps_taken}" if plan else reason
        return SignalIntent(
            action=action,
            symbol=context.symbol,
            price=close,
            signal_uxid=uxid,
            indicators={"close": close},
            inputs={"outcome": plan.outcome} if plan else {},
            reason=f"lifecycle simulator: {step}",
            **fields,
        )

    # -- Entries ---------------------------------------------------------

    def long(self, df: pd.DataFrame, context: StrategyContext) -> IntentResult:
        return self._entry(df, context, direction=1)

    def short(self, df: pd.DataFrame, context: StrategyContext) -> IntentResult:
        return self._entry(df, context, direction=-1)

    def _entry(self, df: pd.DataFrame, context: StrategyContext, *, direction: int) -> IntentResult:
        atr_value = _average_true_range(df, int(self.param("atr_len", 14)))
        if atr_value is None or atr_value <= 0:
            return None
        bar_time = df.index[-1]
        uxid = self._uxid_for(context.symbol, bar_time)
        if uxid in self._plans:
            return None  # This bar already opened its cycle.

        generator = random.Random(uxid)
        if generator.choice((1, -1)) != direction:
            return None  # `long` and `short` are both asked; only one answers.
        outcome = self._draw_outcome(generator)

        close = float(df["close"].iloc[-1])
        risk = atr_value * float(self.param("atr_sl_mult", 1.5))
        reward_ratio = float(self.param("min_rr_ratio", 1.5))
        stop = round(close - direction * risk, 5)
        take_profit_1 = round(close + direction * risk * reward_ratio, 5)
        take_profit_2 = round(close + direction * risk * reward_ratio * 2, 5)
        self._plans[uxid] = CyclePlan(
            outcome=outcome,
            direction=direction,
            entry_price=close,
            stop=stop,
            take_profit_1=take_profit_1,
            take_profit_2=take_profit_2,
            last_bar=bar_time,
        )
        return SignalIntent(
            action=SignalAction.LONG if direction > 0 else SignalAction.SHORT,
            symbol=context.symbol,
            price=close,
            quantity=self.param("quantity", 0.01),
            sl=stop,
            tp1=take_profit_1,
            tp2=take_profit_2,
            risk_percent=self.param("risk_percent", 1.0),
            tp1_percent=self._tp1_percent(),
            move_sl_to_be=True,
            signal_uxid=uxid,
            indicators={"close": close, "atr_val": round(atr_value, 5)},
            inputs={"outcome": outcome, "seed": self.param("seed", 0)},
            reason=f"lifecycle simulator: open, scripted to end {outcome}",
        )

    # -- Exits: each hands back only what `_decide_exits` chose for it ----

    def sl(self, df: pd.DataFrame, context: StrategyContext) -> IntentResult:
        return self._pending.get(SignalAction.SL)

    def r_sl(self, df: pd.DataFrame, context: StrategyContext) -> IntentResult:
        return self._pending.get(SignalAction.R_SL)

    def tp1(self, df: pd.DataFrame, context: StrategyContext) -> IntentResult:
        return self._pending.get(SignalAction.TP1)

    def tp2(self, df: pd.DataFrame, context: StrategyContext) -> IntentResult:
        return self._pending.get(SignalAction.TP2)

    def flat(self, df: pd.DataFrame, context: StrategyContext) -> IntentResult:
        return self._pending.get(SignalAction.FLAT)

    # -- Randomness, reproducibly ----------------------------------------

    def _uxid_for(self, symbol: str, bar_time: pd.Timestamp) -> str:
        """16 uppercase hex — the broker's shape — derived rather than random."""
        seed = self.param("seed", 0)
        material = f"{self.name}:{symbol}:{self.timeframe}:{bar_time.isoformat()}:{seed}"
        return hashlib.sha256(material.encode()).hexdigest()[:16].upper()

    def _draw_outcome(self, generator: random.Random) -> str:
        forced = self.param("outcome")
        if forced is not None:
            if forced not in OUTCOMES:
                raise ValueError(f"outcome must be one of {OUTCOMES}, got {forced!r}")
            return forced
        weights = self.param("weights", [1, 1, 1])
        return generator.choices(OUTCOMES, weights=weights)[0]

    def _tp1_percent(self) -> float:
        percent = float(self.param("tp1_qty_pc", 50.0))
        if not 0 < percent < 100:
            raise ValueError("tp1_qty_pc must be between 0 and 100, exclusive")
        return percent


def _average_true_range(df: pd.DataFrame, length: int) -> float | None:
    """Wilder's ATR on the last closed bar, or ``None`` while it is still warming."""
    previous_close = df["close"].shift(1)
    true_range = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - previous_close).abs(),
            (df["low"] - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    if len(true_range) <= length:
        return None
    value = true_range.ewm(alpha=1 / length, adjust=False, min_periods=length).mean().iloc[-1]
    return None if pd.isna(value) else float(value)
