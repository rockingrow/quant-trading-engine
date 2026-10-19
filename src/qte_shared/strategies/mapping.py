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

    # Per-strategy defaults, applied wherever this strategy runs.
    [strategies.MT5_GOLD_M5_SCALP]
    risk_percent = 1.0

    # Per-pair overrides win over the per-strategy defaults above.
    [symbols.XAUUSD.params.MT5_GOLD_M5_SCALP]
    risk_percent = 0.5

**The real file is git-ignored; the template beside it is not.** What pairs
with what — and at what risk — is position information, and this repo is
public. ``config/strategies_mapping.example.toml`` carries the schema and
dummy values so the shape stays reviewable in history;
``config/strategies_mapping.toml`` carries the book. Point
``QTE_ENGINE__MAPPING_FILE`` somewhere else to mount it as a secret in
production.

TOML rather than environment variables because this is a matrix — symbol ×
strategy × parameters — and flattening a matrix into ``QTE_MAPPING__XAUUSD_0``
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

#: Top-level table holding one sub-table of default parameters per strategy.
STRATEGIES_TABLE = "strategies"


@dataclass(frozen=True, slots=True)
class Pairing:
    """One (symbol, strategy) pair the engine is to run, and its overrides."""

    symbol: str
    strategy: str
    params: dict[str, Any] = field(default_factory=dict)

    @property
    def key(self) -> tuple[str, str]:
        return (self.symbol, self.strategy)


@dataclass(slots=True)
class SymbolMapping:
    """The parsed mapping table. Falsy means "no file — use the fallback"."""

    pairings: tuple[Pairing, ...] = ()
    #: Default parameters per strategy, from the ``[strategies.<name>]`` tables.
    #: Applied to every pair running that strategy; a pair's own
    #: ``[symbols.<symbol>.params.<name>]`` overrides win over these.
    strategy_defaults: dict[str, dict[str, Any]] = field(default_factory=dict)
    source: Path | None = None

    def __bool__(self) -> bool:
        """Whether a table was *read*, not whether it mapped anything.

        The distinction decides what the runner does. A table listing no pairs
        — every symbol disabled for the weekend, say — means trade nothing, and
        must not be mistaken for the absent file that means "fall back to each
        strategy's own symbols". Those two states differ by a deploy.
        """
        return self.source is not None

    # ── Queries ───────────────────────────────────────────────────────

    def symbols_for(self, strategy: str) -> list[str]:
        """Every symbol *strategy* is mapped to, in file order."""
        return [pairing.symbol for pairing in self.pairings if pairing.strategy == strategy]

    def strategies_for(self, symbol: str) -> list[str]:
        """Every strategy mapped to *symbol*, in file order."""
        upper = symbol.upper()
        return [pairing.strategy for pairing in self.pairings if pairing.symbol == upper]

    def params_for(self, symbol: str, strategy: str) -> dict[str, Any]:
        """Parameter overrides for one pair; empty when the pair is not mapped."""
        upper = symbol.upper()
        for pairing in self.pairings:
            if pairing.symbol == upper and pairing.strategy == strategy:
                return dict(pairing.params)
        return {}

    def defaults_for(self, strategy: str) -> dict[str, Any]:
        """Per-strategy default parameters, applied to every pair it runs on.

        These sit under :meth:`params_for`: a value stated in
        ``[strategies.<strategy>]`` applies everywhere the strategy runs, and a
        pair that needs a different number restates just that key. Empty when
        the table has no entry for *strategy*, and empty on a mapping that was
        never read.
        """
        return dict(self.strategy_defaults.get(strategy, {}))

    @property
    def symbols(self) -> list[str]:
        """Every symbol mentioned, deduplicated, in file order."""
        return list(dict.fromkeys(pairing.symbol for pairing in self.pairings))

    @property
    def strategies(self) -> list[str]:
        """Every strategy mentioned, deduplicated, in file order."""
        return list(dict.fromkeys(pairing.strategy for pairing in self.pairings))

    # ── Loading ───────────────────────────────────────────────────────

    @classmethod
    def load(cls, path: Path | str) -> SymbolMapping:
        """Parse *path*, or return an empty table when it does not exist.

        A missing file is not an error: the fallback — a strategy's own
        ``symbols`` attribute — is the behaviour that existed before this file
        did, and a fresh clone has no mapping table because the real one never
        reaches git. A *malformed* file is an error, and loudly, because
        "trades nothing" and "trades everything it used to" look identical in
        a log until the P&L arrives.
        """
        path = Path(path)
        if not path.is_file():
            log.info("No mapping table at %s — strategies keep their own symbols", path)
            return cls()

        with path.open("rb") as handle:
            document = tomllib.load(handle)
        return cls(
            pairings=tuple(_parse(document, path)),
            strategy_defaults=_parse_strategy_defaults(document, path),
            source=path,
        )


def _parse(document: dict[str, Any], path: Path) -> list[Pairing]:
    """Turn the parsed TOML into a flat list of pairs, validating as it goes."""
    defaults = _strategy_names(document.get(DEFAULTS_TABLE, {}), path, DEFAULTS_TABLE)
    symbols = document.get(SYMBOLS_TABLE, {})
    if not isinstance(symbols, dict):
        raise ValueError(f"{path}: [{SYMBOLS_TABLE}] must be a table of symbols")

    pairings: list[Pairing] = []
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
            log.info("Mapping: %s is disabled in %s", symbol, path)
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
            pairings.append(Pairing(symbol=symbol, strategy=name, params=dict(params)))
    return pairings


def _parse_strategy_defaults(document: dict[str, Any], path: Path) -> dict[str, dict[str, Any]]:
    """Read ``[strategies.<name>]`` — one table of default parameters per strategy.

    These are the values a strategy runs at wherever it is mapped; the per-pair
    ``[symbols.<symbol>.params.<name>]`` overrides are merged on top. An absent
    table means no defaults, which is not an error — a strategy keeps whatever
    its own code sets.
    """
    table = document.get(STRATEGIES_TABLE, {})
    if not isinstance(table, dict):
        raise ValueError(f"{path}: [{STRATEGIES_TABLE}] must be a table keyed by strategy name")

    defaults: dict[str, dict[str, Any]] = {}
    for raw_name, params in table.items():
        if not isinstance(params, dict):
            raise ValueError(
                f"{path}: [{STRATEGIES_TABLE}.{raw_name}] must be a table of parameters"
            )
        defaults[str(raw_name)] = dict(params)
    return defaults


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
