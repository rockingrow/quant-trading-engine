"""The engine contract, owned by this repo.

Strategies import from here rather than from the submodules, so a later
reshuffle of the files behind it is not forty edits across the strategies::

    from qss.core import SignalAction, SignalIntent, StrategyBase

See :mod:`qss.core.strategy` for why this is a restatement of the
engine's contract rather than an import of it.
"""

from qss.core.logging import PACKAGE_LOGGER, get_logger
from qss.core.models import Scaling, SignalAction
from qss.core.strategy import (
    ENTRY_SIGNAL_ORDER,
    EXIT_SIGNAL_ORDER,
    MIN_HISTORY_WINDOW,
    OPTIONAL_SIGNAL_METHODS,
    REQUIRED_SIGNAL_METHODS,
    SIGNAL_METHOD_ACTIONS,
    SIGNAL_METHODS,
    IntentResult,
    SignalIntent,
    SignalStrategy,
    StrategyBase,
    StrategyContext,
    as_intents,
    new_uxid,
    open_cycles,
)
from qss.core.timeframes import (
    TIMEFRAME_SECONDS,
    as_utc,
    floor_to_bucket,
    next_bucket,
    normalize_timeframe,
    timeframe_seconds,
)

__all__ = [
    "ENTRY_SIGNAL_ORDER",
    "EXIT_SIGNAL_ORDER",
    "MIN_HISTORY_WINDOW",
    "OPTIONAL_SIGNAL_METHODS",
    "PACKAGE_LOGGER",
    "REQUIRED_SIGNAL_METHODS",
    "SIGNAL_METHODS",
    "SIGNAL_METHOD_ACTIONS",
    "TIMEFRAME_SECONDS",
    "IntentResult",
    "Scaling",
    "SignalAction",
    "SignalIntent",
    "SignalStrategy",
    "StrategyBase",
    "StrategyContext",
    "as_intents",
    "as_utc",
    "new_uxid",
    "open_cycles",
    "floor_to_bucket",
    "get_logger",
    "next_bucket",
    "normalize_timeframe",
    "timeframe_seconds",
]
