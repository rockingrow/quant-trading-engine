"""A worked example of the plugin contract — not a strategy to trade.

Copy this file into ``__strategies__/`` to see the whole pipeline move, then
replace it with your own. The edge here is deliberately naive; what is worth
copying is the *shape*:

* it subclasses :class:`~qte_shared.strategy_base.SignalStrategy`, so it has one
  method per broker action — ``long``, ``short``, ``tp1``, ``tp2``, ``sl`` and
  the optional ``r_sl`` / ``flat`` — and a reader can tell what it can emit
  without reading a single body;
* indicators come from ``qte_shared.indicators``, so the backtest and the live
  runner compute identical values;
* the decision reads only ``df``, which never contains a future bar;
* the stop is derived from ATR and the targets from the resulting risk, so the
  bracket is real rather than a round number;
* it returns :class:`SignalIntent` objects and publishes nothing itself.

Note what ``tp1``, ``tp2`` and ``sl`` do here: nothing, on purpose. The bracket
travels with the entry and the broker's worker manages the exits, so this
strategy has no per-bar opinion about them. Saying that in three lines is the
point of the interface — the alternative is a reader inferring it from an
absence and never being sure they inferred right.
"""

from __future__ import annotations

import pandas as pd

from qte_shared.indicators import atr, crossover, crossunder, ema
from qte_shared.models import SignalAction
from qte_shared.strategy_base import IntentResult, SignalIntent, SignalStrategy, StrategyContext


class EmaAtrBreakout(SignalStrategy):
    """Trades an EMA cross, stopped at a multiple of ATR."""

    name = "QTE_EXAMPLE_EMA_ATR"
    symbols = ("XAUUSD",)
    timeframe = "M15"
    warmup = 220

    # ── Entries ───────────────────────────────────────────────────────

    def long(self, df: pd.DataFrame, context: StrategyContext) -> IntentResult:
        return self._entry(df, context, direction=1)

    def short(self, df: pd.DataFrame, context: StrategyContext) -> IntentResult:
        return self._entry(df, context, direction=-1)

    # ── Exits ─────────────────────────────────────────────────────────

    def tp1(self, df: pd.DataFrame, context: StrategyContext) -> IntentResult:
        """Handled by the bracket the entry carried; nothing to decide per bar."""
        return None

    def tp2(self, df: pd.DataFrame, context: StrategyContext) -> IntentResult:
        """Likewise — ``tp2`` travels with the entry."""
        return None

    def sl(self, df: pd.DataFrame, context: StrategyContext) -> IntentResult:
        """The worker holds the stop. This strategy never moves it mid-trade."""
        return None

    # ── The edge ──────────────────────────────────────────────────────

    def _entry(
        self, df: pd.DataFrame, context: StrategyContext, *, direction: int
    ) -> IntentResult:
        """One body for both directions — the rules are symmetrical.

        Splitting it into two would mean two places to change when the filter
        changes, and a long/short asymmetry nobody meant to introduce.
        """
        fast_length = self.param("fast", 21)
        slow_length = self.param("slow", 55)
        trend_length = self.param("trend", 200)
        atr_length = self.param("atr_len", 14)
        atr_multiple = self.param("atr_sl_mult", 1.5)
        min_rr = self.param("min_rr_ratio", 1.5)
        risk_percent = self.param("risk_percent", 1.0)

        fast = ema(df["close"], fast_length)
        slow = ema(df["close"], slow_length)
        trend = ema(df["close"], trend_length)
        atr_series = atr(df, atr_length)

        # A NaN in the last row means an indicator has not finished warming up.
        # Acting on it would be trading on a value that does not exist yet.
        latest = {"fast": fast, "slow": slow, "trend": trend, "atr": atr_series}
        if any(pd.isna(series.iloc[-1]) for series in latest.values()):
            return None

        close = float(df["close"].iloc[-1])
        atr_value = float(atr_series.iloc[-1])
        if atr_value <= 0:
            return None

        if direction > 0:
            crossed = bool(crossover(fast, slow).iloc[-1]) and close > float(trend.iloc[-1])
        else:
            crossed = bool(crossunder(fast, slow).iloc[-1]) and close < float(trend.iloc[-1])
        if not crossed:
            return None

        risk = atr_value * atr_multiple
        stop = close - direction * risk
        take_profit_1 = close + direction * risk * min_rr
        take_profit_2 = close + direction * risk * min_rr * 2

        return SignalIntent(
            action=SignalAction.LONG if direction > 0 else SignalAction.SHORT,
            symbol=context.symbol,
            price=close,
            quantity=self.param("quantity", 0.01),
            sl=round(stop, 5),
            tp1=round(take_profit_1, 5),
            tp2=round(take_profit_2, 5),
            risk_percent=risk_percent,
            tp1_percent=self.param("tp1_qty_pc", 50.0),
            move_sl_to_be=True,
            indicators={
                "close": close,
                "ema_fast": round(float(fast.iloc[-1]), 5),
                "ema_slow": round(float(slow.iloc[-1]), 5),
                "ema200_filter": round(float(trend.iloc[-1]), 5),
                "atr_val": round(atr_value, 5),
            },
            inputs={
                "fast": fast_length,
                "slow": slow_length,
                "trend": trend_length,
                "atr_len": atr_length,
                "atr_sl_mult": atr_multiple,
                "min_rr_ratio": min_rr,
                "risk_percent": risk_percent,
            },
            reason=f"EMA{fast_length}/{slow_length} cross with EMA{trend_length} trend filter",
        )
