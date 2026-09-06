"""What the engine trades and how the vendor is asked for it, read from TOML.

One provider, one file: ``config/<provider>.toml``, named after
``QTE_MARKET_DATA__PROVIDER``. It answers three questions that used to be four
environment variables and a JSON blob:

.. code-block:: toml

    [feed]
    timeframes = ["M15"]
    signal_timeframe = "M15"

    [provider]
    backfill_history = true
    max_rows_per_request = 5000

    [symbols.XAUUSD]
    market = "fx"
    timeframes = ["M15"]

TOML rather than environment variables because this is a *per-symbol* matrix,
and the flat form could not express it: ``QTE_ENGINE__SYMBOLS`` said which
symbols, ``QTE_ENGINE__TIMEFRAMES`` said which bars — for all of them at once —
and ``QTE_INGESTION__MARKET_OVERRIDES={"BTCUSD":"fx"}`` bolted the third column
back on as JSON inside a shell variable. Written as tables, the symbol and its
settings sit together and a reviewer can see what will actually be subscribed.

**The real file is git-ignored; the template beside it is not**, for the same
reason as :mod:`qte_shared.strategies.mapping`: what a desk trades is position
information and this repository is public. ``make tiingo`` writes
``config/tiingo.toml`` from ``config/tiingo.example.toml``.

**Credentials never appear here.** The vendor key is a secret and stays in
``.env`` as ``QTE_DATA_PROVIDER_API_KEY`` — one name whatever the vendor, which
is what lets ``[provider]`` be the same table for the next one.

A missing file is not an error. The plan is then empty and every reader falls
back to what it did before the file existed — the ``QTE_ENGINE__*`` defaults —
which is what keeps the simulator dev stack working with no plan at all. A
*malformed* file is an error, loudly: "subscribed to nothing" and "subscribed
to what it used to be" look identical in a log until the P&L arrives.
"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from qte_shared.logging_setup import get_logger
from qte_shared.symbols import Market, SymbolSpec, infer_market
from qte_shared.timeframes import normalize_timeframe

log = get_logger(__name__)

#: Table holding the per-symbol entries.
SYMBOLS_TABLE = "symbols"

#: Table of settings shared by every symbol that names none of its own.
FEED_TABLE = "feed"

#: Table of the vendor's own knobs, read by that provider's settings block.
PROVIDER_TABLE = "provider"

#: Never taken from the file, however it got written there — a key in a config
#: file is a key in a diff. It is read from ``QTE_DATA_PROVIDER_API_KEY``.
_SECRET_OPTIONS = ("api_key", "token", "password", "secret")


@dataclass(frozen=True, slots=True)
class SymbolFeed:
    """One symbol the engine subscribes to, and how it is resampled."""

    symbol: str
    market: Market
    timeframes: tuple[str, ...]

    @property
    def spec(self) -> SymbolSpec:
        """The symbol as the provider layer wants it — name plus market."""
        return SymbolSpec(symbol=self.symbol, market=self.market)


@dataclass(frozen=True, slots=True)
class MarketDataPlan:
    """The parsed plan. Falsy means "no file — use the environment defaults"."""

    feeds: tuple[SymbolFeed, ...] = ()
    signal_timeframe: str = ""
    options: dict[str, Any] = field(default_factory=dict)
    source: Path | None = None

    def __bool__(self) -> bool:
        """Whether a file was *read*, not whether it planned anything.

        A plan with every symbol disabled means "subscribe to nothing" and is a
        deliberate state; no file at all means "nobody has written a plan yet".
        The two differ by a deploy, so they must not collapse into one answer.
        """
        return self.source is not None

    # ── Queries ───────────────────────────────────────────────────────

    @property
    def symbols(self) -> list[str]:
        """Every enabled symbol, in file order."""
        return [feed.symbol for feed in self.feeds]

    @property
    def timeframes(self) -> list[str]:
        """Every timeframe any symbol asks for, deduplicated, in file order.

        The union, not a per-symbol answer: it is what a caller with no symbol
        in hand — ``qte-backtest download``, the CLI defaults — has to work
        with. Ingestion resamples :attr:`SymbolFeed.timeframes` instead.
        """
        return list(dict.fromkeys(tf for feed in self.feeds for tf in feed.timeframes))

    @property
    def specs(self) -> list[SymbolSpec]:
        """The plan as the provider layer reads it."""
        return [feed.spec for feed in self.feeds]

    def feed_for(self, symbol: str) -> SymbolFeed | None:
        upper = symbol.upper()
        return next((feed for feed in self.feeds if feed.symbol == upper), None)

    def timeframes_for(self, symbol: str) -> list[str]:
        """What *symbol* is resampled to; empty when it is not in the plan."""
        feed = self.feed_for(symbol)
        return list(feed.timeframes) if feed else []

    def option(self, name: str, default: Any = None) -> Any:
        """One value from ``[provider]``, or *default* when the file says none."""
        return self.options.get(name, default)

    # ── Loading ───────────────────────────────────────────────────────

    @classmethod
    def load(cls, path: Path | str) -> MarketDataPlan:
        """Parse *path*, or return an empty plan when it does not exist."""
        path = Path(path)
        if not path.is_file():
            log.info("No market-data plan at %s; falling back to the QTE_ENGINE__* defaults", path)
            return cls()

        with path.open("rb") as handle:
            document = tomllib.load(handle)
        return _parse(document, path)


def _parse(document: dict[str, Any], path: Path) -> MarketDataPlan:
    """Turn the parsed TOML into a plan, validating as it goes."""
    feed_table = _table(document, FEED_TABLE, path)
    default_timeframes = _timeframes(feed_table, path, FEED_TABLE) or ["M15"]
    signal_timeframe = feed_table.get("signal_timeframe", default_timeframes[0])
    if not isinstance(signal_timeframe, str):
        raise ValueError(f"{path}: [{FEED_TABLE}].signal_timeframe must be a timeframe label")
    signal_timeframe = normalize_timeframe(signal_timeframe)

    options = _table(document, PROVIDER_TABLE, path)
    for name in _SECRET_OPTIONS:
        if name in options:
            log.warning(
                "Ignoring [%s].%s in %s — credentials belong in .env "
                "(QTE_DATA_PROVIDER_API_KEY), not in a file that gets committed",
                PROVIDER_TABLE,
                name,
                path,
            )
    options = {key: value for key, value in options.items() if key not in _SECRET_OPTIONS}

    symbols = _table(document, SYMBOLS_TABLE, path)
    feeds: list[SymbolFeed] = []
    for raw_symbol, entry in symbols.items():
        symbol = str(raw_symbol).upper()
        where = f"{SYMBOLS_TABLE}.{raw_symbol}"
        if not isinstance(entry, dict):
            raise ValueError(f"{path}: [{where}] must be a table")
        # A symbol switched off keeps its settings in the file rather than
        # being commented out, so turning it back on is a one-word edit and the
        # diff says what happened.
        if entry.get("enabled", True) is False:
            log.info("Market-data plan: %s is disabled in %s", symbol, path)
            continue
        if any(feed.symbol == symbol for feed in feeds):
            raise ValueError(f"{path}: {symbol} has two entries; one symbol is one subscription")

        timeframes = _timeframes(entry, path, where) or default_timeframes
        feeds.append(
            SymbolFeed(
                symbol=symbol,
                market=_market(entry, symbol, path, where),
                timeframes=tuple(timeframes),
            )
        )

    if not feeds:
        log.warning(
            "Market-data plan %s lists no enabled symbol — ingestion will subscribe to none", path
        )
    return MarketDataPlan(
        feeds=tuple(feeds),
        signal_timeframe=signal_timeframe,
        options=options,
        source=path,
    )


def _table(document: dict[str, Any], name: str, path: Path) -> dict[str, Any]:
    value = document.get(name, {})
    if not isinstance(value, dict):
        raise ValueError(f"{path}: [{name}] must be a table")
    return value


def _timeframes(entry: dict[str, Any], path: Path, where: str) -> list[str]:
    """Normalised timeframe labels, so ``15m`` and ``M15`` name one bucket."""
    raw = entry.get("timeframes", [])
    if isinstance(raw, str):
        raise ValueError(
            f'{path}: [{where}].timeframes must be a list, not a string — write ["{raw}"]'
        )
    if not isinstance(raw, list) or not all(isinstance(label, str) for label in raw):
        raise ValueError(f"{path}: [{where}].timeframes must be a list of timeframe labels")
    try:
        return [normalize_timeframe(label) for label in raw]
    except ValueError as exc:
        raise ValueError(f"{path}: [{where}].timeframes — {exc}") from exc


def _market(entry: dict[str, Any], symbol: str, path: Path, where: str) -> Market:
    """The symbol's market, stated or inferred.

    Stating it is the point of writing the symbol down: ``BTCUSD`` is a crypto
    pair on an exchange and an FX CFD on a broker's book, and the guess decides
    which socket the provider opens.
    """
    market = entry.get("market")
    if market is None:
        return infer_market(symbol)
    if market not in ("fx", "crypto"):
        raise ValueError(f'{path}: [{where}].market must be "fx" or "crypto", got {market!r}')
    return market
