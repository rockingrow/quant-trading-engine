"""The plugin seam: everything between a strategy file and a broker payload.

One request travels through this package in a straight line. The loader finds
the classes in ``__strategies__/``; the mapping table says which symbols each of
them trades; the strategy itself returns :class:`SignalIntent` objects and
nothing else; the factory sizes an intent against the account and turns it into
the payload a worker executes.

* :mod:`~qte_shared.strategies.strategy_base` — the contract a plugin implements.
* :mod:`~qte_shared.strategies.plugin_loader` — discovery, manifests, aliases.
* :mod:`~qte_shared.strategies.mapping` — which strategies trade which symbols.
* :mod:`~qte_shared.strategies.sizing` — how big an entry is.
* :mod:`~qte_shared.strategies.signal_factory` — intent to broker payload.

The backtest replay and the live runner both drive a strategy through this
package and nothing else, which is what keeps a backtest predictive of live
behaviour. Nothing here knows about NATS, Redis or Postgres.
"""

from __future__ import annotations

from qte_shared.strategies.mapping import DEFAULTS_TABLE, SYMBOLS_TABLE, Pairing, SymbolMapping
from qte_shared.strategies.plugin_loader import (
    EXCLUDED_DIRECTORIES,
    MANIFEST_DEPTH,
    MANIFEST_FILENAMES,
    MANIFEST_HOOK,
    Candidate,
    LoadedStrategy,
    LoadFailure,
    StrategyLoader,
    load_strategies,
)
from qte_shared.strategies.signal_factory import BracketPolicy, SignalFactory
from qte_shared.strategies.sizing import (
    EQUITY_SIZING_KEY,
    RISK_PERCENT_KEY,
    PositionSizer,
    resolve_use_equity_sizing,
)
from qte_shared.strategies.strategy_base import (
    ENTRY_SIGNAL_ORDER,
    EXIT_SIGNAL_ORDER,
    INTENT_FIELDS,
    MIN_HISTORY_WINDOW,
    OPTIONAL_SIGNAL_METHODS,
    REQUIRED_ATTRIBUTES,
    REQUIRED_HOOKS,
    REQUIRED_SIGNAL_METHODS,
    SIGNAL_METHOD_ACTIONS,
    SIGNAL_METHOD_ARITY,
    SIGNAL_METHODS,
    STRATEGY_CANDIDATE_THRESHOLD,
    IntentResult,
    SignalIntent,
    SignalStrategy,
    StrategyBase,
    StrategyContext,
    StrategyLike,
    as_intents,
    candles_to_frame,
    coerce_intent,
    defines_signal_method,
    implemented_signal_methods,
    implements_signal_contract,
    implements_strategy_contract,
    looks_like_a_strategy,
    missing_signal_methods,
    overrides_on_tick,
    tick_price,
)

__all__ = [
    "DEFAULTS_TABLE",
    "ENTRY_SIGNAL_ORDER",
    "EQUITY_SIZING_KEY",
    "EXCLUDED_DIRECTORIES",
    "EXIT_SIGNAL_ORDER",
    "INTENT_FIELDS",
    "MANIFEST_DEPTH",
    "MANIFEST_FILENAMES",
    "MANIFEST_HOOK",
    "MIN_HISTORY_WINDOW",
    "OPTIONAL_SIGNAL_METHODS",
    "REQUIRED_ATTRIBUTES",
    "REQUIRED_HOOKS",
    "REQUIRED_SIGNAL_METHODS",
    "RISK_PERCENT_KEY",
    "SIGNAL_METHODS",
    "SIGNAL_METHOD_ACTIONS",
    "SIGNAL_METHOD_ARITY",
    "STRATEGY_CANDIDATE_THRESHOLD",
    "SYMBOLS_TABLE",
    "BracketPolicy",
    "Candidate",
    "IntentResult",
    "LoadFailure",
    "LoadedStrategy",
    "Pairing",
    "PositionSizer",
    "SignalFactory",
    "SignalIntent",
    "SignalStrategy",
    "StrategyBase",
    "StrategyContext",
    "StrategyLike",
    "StrategyLoader",
    "SymbolMapping",
    "as_intents",
    "candles_to_frame",
    "coerce_intent",
    "defines_signal_method",
    "implemented_signal_methods",
    "implements_signal_contract",
    "implements_strategy_contract",
    "load_strategies",
    "looks_like_a_strategy",
    "missing_signal_methods",
    "overrides_on_tick",
    "resolve_use_equity_sizing",
    "tick_price",
]
