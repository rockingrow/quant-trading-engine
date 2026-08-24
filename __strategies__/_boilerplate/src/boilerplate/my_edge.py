"""A strategy with every part of the contract filled in except the edge.

Copy this module, rename the class, put your rule in :meth:`MyEdge._rule` and
publish it from ``manifest.py``. Everything else here is the shape the engine
drives, and the comments say why each piece is where it is.

What the engine requires, and what this file shows:

* **one method per broker action** -- ``long``, ``short``, ``tp1``, ``tp2`` and
  ``sl`` are required; ``r_sl`` (re-stop: break-even or trailed) and ``flat``
  (a close that is neither a target nor a stop) are optional and shown anyway,
  because a reader should be able to tell what a strategy can emit without
  opening a single body;
* **class attributes** -- ``name`` is the NATS subject the broker's workers
  subscribe to, ``symbols`` is only a default that
  ``config/strategies_mapping.toml`` overrides, ``timeframe`` and ``warmup``
  size the frame the engine hands you;
* **decisions read only ``df``** -- an OHLCV frame indexed by candle open time
  in UTC, oldest first, whose last row is always a *closed* bar. It never
  contains a future one, which is what makes the backtest honest;
* **nothing is published here** -- return ``SignalIntent`` objects and the
  runner attaches the bracket, mints or reuses the trade-cycle id, delivers to
  the broker and writes the audit row.

Every import here is local to this repository -- the contract in
:mod:`boilerplate.contract`, the indicators in :mod:`boilerplate.indicators`,
pandas from the lockfile. Nothing from the engine, on purpose: a rule about
price should not depend on the process that happens to deliver its output. The
engine recognises this class structurally and converts what it returns at the
boundary.

As shipped this strategy is inert: :meth:`_rule` answers ``False``, so it can be
loaded, audited and backtested end to end without ever emitting a signal.
"""

from __future__ import annotations

import pandas as pd

from ._helpers import bracket, is_warm
from .contract import (
    IntentResult,
    SignalAction,
    SignalIntent,
    SignalStrategy,
    StrategyContext,
)
from .indicators import atr, ema


class MyEdge(SignalStrategy):
    """One instrument, one timeframe, one idea. Rename me."""

    #: The broker subject. Workers subscribe to ``SIGNALS.<name>``, so this has
    #: to match what they are configured for, and no two strategies mounted in
    #: ``__strategies__/`` may share it.
    name = "QTE_BOILERPLATE_M15"

    #: A default, not a routing table. ``config/strategies_mapping.toml`` pairs
    #: symbols with strategies at deploy time and beats whatever is written
    #: here; this is what runs when that table has nothing to say.
    symbols = ("XAUUSD",)

    #: The bar size this strategy thinks in. The runner subscribes to
    #: ``QTE.candle.closed.<symbol>.<timeframe>`` on its behalf.
    timeframe = "M15"

    #: Bars the slowest indicator needs before its values mean anything. The
    #: frame you are handed holds ``max(warmup * 2, 400)`` bars by default --
    #: identical in backtest and live, so a strategy that reads the whole frame
    #: computes the same number in both.
    warmup = 220

    # -- Lifecycle (optional) ------------------------------------------

    def on_start(self, context: StrategyContext) -> None:
        """Called once before the first bar. The default is a no-op; override
        it to prepare state that does not depend on price."""

    def on_stop(self) -> None:
        """Called once on shutdown. Release anything ``on_start`` acquired."""

    # -- Entries -------------------------------------------------------
    #
    # Asked only while flat, in the order long -> short, and the first that
    # answers wins: a bar cannot be both.

    def long(self, df: pd.DataFrame, context: StrategyContext) -> IntentResult:
        return self._entry(df, context, direction=1)

    def short(self, df: pd.DataFrame, context: StrategyContext) -> IntentResult:
        return self._entry(df, context, direction=-1)

    # -- Exits ---------------------------------------------------------
    #
    # Asked only while holding, in the order sl -> r_sl -> tp1 -> tp2 -> flat,
    # and every one is asked: taking tp1 and trailing the stop on the same bar
    # is normal. The stop comes first because if one bar both stopped out and
    # reached a target, the stop is what happened.

    def sl(self, df: pd.DataFrame, context: StrategyContext) -> IntentResult:
        """The bracket travelled with the entry and the broker's worker holds
        the stop, so there is nothing to decide per bar. Returning ``None`` in
        three lines is the point of the interface -- the alternative is a
        reader inferring it from an absence and never being sure they inferred
        right.
        """
        return None

    def r_sl(self, df: pd.DataFrame, context: StrategyContext) -> IntentResult:
        """Optional: move the stop mid-trade (break-even, or a trail).

        Return a ``SignalIntent(action=SignalAction.R_SL, sl=...)`` carrying
        ``uxid=context.open_uxid``, so the broker knows which open cycle it
        re-stops.
        """
        return None

    def tp1(self, df: pd.DataFrame, context: StrategyContext) -> IntentResult:
        """The first target travels with the entry; the worker manages it."""
        return None

    def tp2(self, df: pd.DataFrame, context: StrategyContext) -> IntentResult:
        """Likewise for the runner portion."""
        return None

    def flat(self, df: pd.DataFrame, context: StrategyContext) -> IntentResult:
        """Optional: close for a reason that is neither a target nor a stop --
        a session ending, a regime filter flipping, news."""
        return None

    # -- The edge ------------------------------------------------------

    def _entry(self, df: pd.DataFrame, context: StrategyContext, *, direction: int) -> IntentResult:
        """One body for both directions -- write the rules symmetrically.

        Splitting long and short into two bodies means two places to change
        when the filter changes, and a long/short asymmetry nobody meant to
        introduce.
        """
        # Every tunable reads through `param`, so config/strategies_mapping.toml
        # (and QTE_RUNNER__STRATEGY_PARAMS) can retune this strategy per pair
        # without editing the file that produced the backtest.
        fast_length = self.param("fast", 21)
        slow_length = self.param("slow", 55)
        atr_length = self.param("atr_len", 14)
        atr_multiple = self.param("atr_sl_mult", 1.5)
        min_rr = self.param("min_rr_ratio", 1.5)
        risk_percent = self.param("risk_percent", 1.0)

        fast = ema(df["close"], fast_length)
        slow = ema(df["close"], slow_length)
        atr_series = atr(df, atr_length)
        if not is_warm(fast, slow, atr_series):
            return None

        close = float(df["close"].iloc[-1])
        atr_value = float(atr_series.iloc[-1])
        if atr_value <= 0:
            return None

        if not self._rule(df, direction=direction):
            return None

        stop, take_profit_1, take_profit_2 = bracket(
            close, atr_value * atr_multiple, direction, rr=min_rr
        )
        return SignalIntent(
            action=SignalAction.LONG if direction > 0 else SignalAction.SHORT,
            symbol=context.symbol,
            price=close,
            quantity=self.param("quantity", 0.01),
            sl=stop,
            tp1=take_profit_1,
            tp2=take_profit_2,
            risk_percent=risk_percent,
            tp1_percent=self.param("tp1_qty_pc", 50.0),
            move_sl_to_be=True,
            # What you want to read back in the audit trail, and what the
            # broker broadcasts, when this trade is questioned months later.
            indicators={
                "close": close,
                "ema_fast": round(float(fast.iloc[-1]), 5),
                "ema_slow": round(float(slow.iloc[-1]), 5),
                "atr_val": round(atr_value, 5),
            },
            inputs={
                "fast": fast_length,
                "slow": slow_length,
                "atr_len": atr_length,
                "atr_sl_mult": atr_multiple,
                "min_rr_ratio": min_rr,
                "risk_percent": risk_percent,
            },
            reason="boilerplate: no rule implemented",
        )

    def _rule(self, df: pd.DataFrame, *, direction: int) -> bool:
        """**Your edge goes here.** ``True`` opens in *direction* (1 long, -1 short).

        It deliberately answers ``False``: a template that traded the moment it
        was mounted would be a template nobody could safely leave in place.
        Replace the body -- ``crossover(fast, slow)`` for direction 1 and
        ``crossunder`` for -1, both from :mod:`boilerplate.indicators`, is the
        smallest real example -- and the entry above becomes a live signal with
        no other change.
        """
        return False
