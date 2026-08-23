"""The contract every plugin in ``user_strategies/`` implements.

Write-once, run-anywhere lives here: the backtest replay and the live runner
both drive a strategy through *this* interface and nothing else, so the same
file that produced a backtest curve is the file that trades.

A strategy never publishes, never touches NATS, never sizes against a live
account. It looks at data and returns :class:`SignalIntent` objects; turning an
intent into a broker-shaped payload (bracket levels, correlation ids, transport)
is the runner's job. That split is what lets the backtest fill an intent against
a simulator and the runner hand the identical intent to the broker.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import pandas as pd

from qte_shared.models import Candle, PositionBlock, Scaling, SignalAction, Tick


@dataclass(slots=True)
class SignalIntent:
    """What a strategy decided, before it becomes a broker payload.

    ``uxid`` ties a close back to the entry that opened it. Leave it ``None``
    on an entry — the runner mints the cycle id and remembers it — and set it
    on a follow-up when the strategy is managing an exit it opened itself.
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
    uxid: str | None = None
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


class StrategyBase(ABC):
    """Base class every plugin in ``user_strategies/`` must subclass.

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
