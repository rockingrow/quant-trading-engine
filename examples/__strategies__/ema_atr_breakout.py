"""A worked example of the plugin contract — not a strategy to trade.

Copy this file into ``__strategies__/`` to see the whole pipeline move, then
replace it with your own. The edge here is deliberately naive; what is worth
copying is the *shape*:

* indicators come from ``qte_shared.indicators``, so the backtest and the live
  runner compute identical values;
* the decision reads only ``df``, which never contains a future bar;
* the stop is derived from ATR and the targets from the resulting risk, so the
  bracket is real rather than a round number;
* it returns :class:`SignalIntent` objects and publishes nothing itself.
"""

from __future__ import annotations

import pandas as pd

from qte_shared.indicators import atr, crossover, crossunder, ema
from qte_shared.strategy_base import IntentResult, SignalIntent, StrategyBase, StrategyContext
from qte_shared.models import SignalAction


class EmaAtrBreakout(StrategyBase):
    """Trades an EMA cross, stopped at a multiple of ATR."""

    name = "QTE_EXAMPLE_EMA_ATR"
    symbols = ("XAUUSD",)
    timeframe = "M15"
    warmup = 220

    def on_candle_closed(self, df: pd.DataFrame, context: StrategyContext) -> IntentResult:
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

        # Already in a trade: the bracket on the worker manages the exit, so
        # there is nothing to decide until it closes.
        if context.open_uxid is not None:
            return None

        long_signal = bool(crossover(fast, slow).iloc[-1]) and close > float(trend.iloc[-1])
        short_signal = bool(crossunder(fast, slow).iloc[-1]) and close < float(trend.iloc[-1])
        if not (long_signal or short_signal):
            return None

        direction = 1 if long_signal else -1
        risk = atr_value * atr_multiple
        stop = close - direction * risk
        take_profit_1 = close + direction * risk * min_rr
        take_profit_2 = close + direction * risk * min_rr * 2

        return SignalIntent(
            action=SignalAction.LONG if long_signal else SignalAction.SHORT,
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
