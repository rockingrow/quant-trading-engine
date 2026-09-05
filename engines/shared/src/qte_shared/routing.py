"""Which strategies trade which symbols, read from a TOML file.

Without this table the runner falls back to what a strategy declares about
itself — ``symbols = ("XAUUSD",)``, or ``QTE_ENGINE__SYMBOLS`` when it declares
nothing. That is fine for one strategy and wrong for a book: the answer to
"what is trading gold right now" then lives scattered across a private repo's
class attributes, and changing it means editing and redeploying that repo.

So the pairing moves out of the code and into a file the operator owns:

.. code-block:: toml

    [symbols.XAUUSD]
    strategies = ["MT5_GOLD_M5_SCALP"]

    [symbols.XAUUSD.params.MT5_GOLD_M5_SCALP]
    risk_percent = 1.0

**The real file is git-ignored; the template beside it is not.** What pairs
with what — and at what risk — is position information, and this repo is
public. ``config/strategies_mapping.example.toml`` carries the schema and
dummy values so the shape stays reviewable in history;
``config/strategies_mapping.toml`` carries the book. Point
``QTE_ENGINE__ROUTING_FILE`` somewhere else to mount it as a secret in
production.

TOML rather than environment variables because this is a matrix — symbol ×
strategy × parameters — and flattening a matrix into ``QTE_ROUTING__XAUUSD_0``
is how it stops being reviewable. TOML rather than YAML because ``tomllib`` is
in the standard library and this file is read inside the trading process.
"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from qte_shared.logging_setup import get_logger

log = get_logger(__name__)

#: Top-level table holding the per-symbol entries.
SYMBOLS_TABLE = "symbols"

#: Table applied to a symbol that has no entry of its own.
DEFAULTS_TABLE = "defaults"


@dataclass(frozen=True, slots=True)
class Route:
    """One (symbol, strategy) pair the engine is to run, and its overrides."""

    symbol: str
    strategy: str
    params: dict[str, Any] = field(default_factory=dict)

    @property
    def key(self) -> tuple[str, str]:
        return (self.symbol, self.strategy)


@dataclass(slots=True)
class SymbolRouting:
    """The parsed routing table. Falsy means "no file — use the fallback"."""

    routes: tuple[Route, ...] = ()
    source: Path | None = None

    def __bool__(self) -> bool:
        """Whether a table was *read*, not whether it routed anything.

        The distinction decides what the runner does. A table listing no pairs
        — every symbol disabled for the weekend, say — means trade nothing, and
        must not be mistaken for the absent file that means "fall back to each
        strategy's own symbols". Those two states differ by a deploy.
        """
        return self.source is not None

    # ── Queries ───────────────────────────────────────────────────────

    def symbols_for(self, strategy: str) -> list[str]:
        """Every symbol *strategy* is routed to, in file order."""
        return [route.symbol for route in self.routes if route.strategy == strategy]

    def strategies_for(self, symbol: str) -> list[str]:
        """Every strategy routed to *symbol*, in file order."""
        upper = symbol.upper()
        return [route.strategy for route in self.routes if route.symbol == upper]

    def params_for(self, symbol: str, strategy: str) -> dict[str, Any]:
        """Parameter overrides for one pair; empty when the pair is not routed."""
        upper = symbol.upper()
        for route in self.routes:
            if route.symbol == upper and route.strategy == strategy:
                return dict(route.params)
        return {}

    @property
    def symbols(self) -> list[str]:
        """Every symbol mentioned, deduplicated, in file order."""
        return list(dict.fromkeys(route.symbol for route in self.routes))

    @property
    def strategies(self) -> list[str]:
        """Every strategy mentioned, deduplicated, in file order."""
        return list(dict.fromkeys(route.strategy for route in self.routes))

    # ── Loading ───────────────────────────────────────────────────────

    @classmethod
    def load(cls, path: Path | str) -> SymbolRouting:
        """Parse *path*, or return an empty table when it does not exist.

        A missing file is not an error: the fallback — a strategy's own
        ``symbols`` attribute — is the behaviour that existed before this file
        did, and a fresh clone has no routing table because the real one never
        reaches git. A *malformed* file is an error, and loudly, because
        "trades nothing" and "trades everything it used to" look identical in
        a log until the P&L arrives.
        """
        path = Path(path)
        if not path.is_file():
            log.info("No routing table at %s — strategies keep their own symbols", path)
            return cls()

        with path.open("rb") as handle:
            document = tomllib.load(handle)
        return cls(routes=tuple(_parse(document, path)), source=path)


def _parse(document: dict[str, Any], path: Path) -> list[Route]:
    """Turn the parsed TOML into a flat list of pairs, validating as it goes."""
    defaults = _strategy_names(document.get(DEFAULTS_TABLE, {}), path, DEFAULTS_TABLE)
    symbols = document.get(SYMBOLS_TABLE, {})
    if not isinstance(symbols, dict):
        raise ValueError(f"{path}: [{SYMBOLS_TABLE}] must be a table of symbols")

    routes: list[Route] = []
    seen: set[tuple[str, str]] = set()
    for raw_symbol, entry in symbols.items():
        symbol = str(raw_symbol).upper()
        where = f"{SYMBOLS_TABLE}.{raw_symbol}"
        if not isinstance(entry, dict):
            raise ValueError(f"{path}: [{where}] must be a table")
        # A symbol switched off keeps its configuration in the file rather than
        # being commented out, so turning it back on is a one-word edit and the
        # diff says what happened.
        if entry.get("enabled", True) is False:
            log.info("Routing: %s is disabled in %s", symbol, path)
            continue

        names = _strategy_names(entry, path, where) or defaults
        overrides = entry.get("params", {})
        if not isinstance(overrides, dict):
            raise ValueError(f"{path}: [{where}.params] must be a table keyed by strategy")

        for name in names:
            if (symbol, name) in seen:
                raise ValueError(
                    f"{path}: {symbol} lists {name!r} twice. Two slots for one pair would "
                    "run the same strategy against the same symbol in parallel."
                )
            seen.add((symbol, name))
            params = overrides.get(name, {})
            if not isinstance(params, dict):
                raise ValueError(f"{path}: [{where}.params.{name}] must be a table")
            routes.append(Route(symbol=symbol, strategy=name, params=dict(params)))
    return routes


def _strategy_names(entry: dict[str, Any], path: Path, where: str) -> list[str]:
    names = entry.get("strategies", [])
    if isinstance(names, str):
        raise ValueError(
            f"{path}: [{where}].strategies must be a list, not a string — write "
            f'strategies = ["{names}"]'
        )
    if not isinstance(names, list) or not all(isinstance(name, str) for name in names):
        raise ValueError(f"{path}: [{where}].strategies must be a list of strategy names")
    return names
