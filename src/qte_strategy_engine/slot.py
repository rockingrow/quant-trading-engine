"""One strategy bound to one symbol: its window, its book and its lock.

The unit everything in the runner is keyed on. It lives in its own module so the
runner's collaborators (:mod:`qte_strategy_engine.delivery`,
:mod:`~qte_strategy_engine.recovery`, :mod:`~qte_strategy_engine.history_sync`,
:mod:`~qte_strategy_engine.control_plane`) can name it without importing the
runner they serve.
"""

from __future__ import annotations

import asyncio
from collections import deque

from qte_shared.models import Candle
from qte_shared.strategies.signal_factory import SignalFactory
from qte_shared.strategies.strategy_base import StrategyLike
from qte_shared.strategies.strategy_settings import NO_WEEKEND_FLAT, WeekendFlatPolicy
from qte_shared.timeframes import normalize_timeframe


class StrategySlot:
    """One strategy bound to one symbol, with its own candle buffer and cycle."""

    def __init__(
        self,
        strategy: StrategyLike,
        symbol: str,
        factory: SignalFactory,
        weekend_flat: WeekendFlatPolicy = NO_WEEKEND_FLAT,
        warmup: int | None = None,
    ) -> None:
        self.strategy = strategy
        self.symbol = symbol
        self.factory = factory
        # What this pair's mapping entry asked for, if it asked for anything.
        # Kept beside the strategy's own count rather than replacing it, so
        # `warmup` below still reads the declaration when nothing overrode it
        # and one strategy can run at two counts on two symbols.
        self._warmup_override = warmup
        # The market calendar this pair trades on, as its repository declared
        # it. It sits on the slot rather than on the strategy because going
        # flat before the market shuts is the engine's decision — a strategy
        # that could read this is a strategy that could ignore it.
        self.weekend_flat = weekend_flat
        self.timeframe = normalize_timeframe(strategy.timeframe)
        # Exactly the window the backtest hands the same strategy — the bound
        # lives on the strategy contract so the two drivers cannot drift apart.
        self.buffer: deque[Candle] = deque(maxlen=strategy.history_window())
        self.started = False
        # Candle and tick subscriptions are independent NATS callbacks. Keep a
        # strategy instance and its position cycle a single-writer aggregate so
        # both callbacks cannot decide they are flat and publish two entries.
        self.lock = asyncio.Lock()

    @property
    def key(self) -> tuple[str, str]:
        return (self.strategy.name, self.symbol)

    @property
    def warmup(self) -> int:
        """Bars this pair collects before it may decide — the mapping's, or the strategy's."""
        if self._warmup_override is not None:
            return self._warmup_override
        return self.strategy.warmup

    @property
    def is_warm(self) -> bool:
        return len(self.buffer) >= self.warmup


__all__ = ["StrategySlot"]
