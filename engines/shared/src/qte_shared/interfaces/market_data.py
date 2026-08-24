"""The market-data seam: what QTE needs from a data vendor, and nothing more.

QTE asks a vendor for exactly two things:

* **history** -- completed OHLCV bars for a symbol/timeframe/date range, used
  by the backtest engine (:class:`HistorySource`);
* **live** -- a stream of :class:`~qte_shared.models.Tick` objects, used by
  ingestion (:class:`LiveFeed`).

A vendor is represented by one :class:`MarketDataProvider` -- a *factory* that
knows its own credentials, endpoints and ticker spelling, and hands back those
two objects on request. That is the whole contract. Nothing above this line
knows a URL, an auth header or a wire format, which is what makes a second
vendor an added file rather than an edit spread across three engines.

Providers are constructed through :mod:`qte_shared.providers`, never imported
by name from an engine.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import Any, ClassVar

import pandas as pd
from pydantic_settings import BaseSettings, SettingsConfigDict

from qte_shared.config import REPO_ROOT
from qte_shared.indicators import OHLCV_COLUMNS
from qte_shared.models import Tick
from qte_shared.symbols import Market, SymbolSpec
from qte_shared.timeframes import normalize_timeframe, timeframe_seconds

#: Called once per parsed tick. Async because every sink downstream is.
TickHandler = Callable[[Tick], Awaitable[None]]


class Capability(str, Enum):
    """What a provider can actually serve.

    Declared rather than discovered: a history-only vendor should fail when
    ingestion is pointed at it, at startup, instead of connecting to nothing.
    """

    HISTORY = "history"
    LIVE = "live"


# -- Errors ----------------------------------------------------------------


class ProviderError(RuntimeError):
    """Base class for every market-data provider failure."""


class UnknownProvider(ProviderError):
    """Raised when configuration names a provider nobody registered."""


class ProviderNotConfigured(ProviderError):
    """Raised when a provider exists but is missing credentials/settings."""


class UnsupportedCapability(ProviderError):
    """Raised when a provider is asked for something it does not serve."""


# -- Configuration ---------------------------------------------------------


class ProviderSettings(BaseSettings):
    """Base for a provider's own configuration block.

    Each provider owns its settings -- root :class:`~qte_shared.config.Settings`
    deliberately carries no vendor block, so adding a vendor never edits the
    core config. Subclasses set only ``env_prefix``; the ``.env`` wiring is
    inherited so an operator fills one file regardless of which vendor is on.
    """

    model_config = SettingsConfigDict(
        env_file=(REPO_ROOT / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )


# -- Values on the wire ----------------------------------------------------


@dataclass(frozen=True, slots=True)
class HistoryRequest:
    """One "give me these bars" question, in QTE's vocabulary.

    ``symbol`` is the broker spelling (``XAUUSD``); translating it into the
    vendor's ticker is the provider's job, not the caller's. ``start``/``end``
    are inclusive dates.
    """

    symbol: str
    timeframe: str
    start: date
    end: date
    market: Market = "fx"

    def normalized(self) -> HistoryRequest:
        """Same request with a canonical timeframe label and upper-cased symbol."""
        return HistoryRequest(
            symbol=self.symbol.upper(),
            timeframe=normalize_timeframe(self.timeframe),
            start=self.start,
            end=self.end,
            market=self.market,
        )


# -- The two things a vendor serves ----------------------------------------


class HistorySource(ABC):
    """Completed bars, already shaped the way the rest of QTE reads them."""

    @abstractmethod
    async def fetch(self, request: HistoryRequest) -> pd.DataFrame:
        """Return the canonical OHLCV frame for *request*.

        Indexed by bar **open time** in UTC, ascending, columns exactly
        :data:`~qte_shared.indicators.OHLCV_COLUMNS`. Build it with
        :func:`normalize_ohlcv` rather than by hand, and return an empty frame
        -- not ``None`` -- when the vendor has nothing for that range.
        """

    def supports_timeframe(self, timeframe: str) -> bool:
        """Whether the vendor can serve this timeframe. Override when it cannot."""
        try:
            normalize_timeframe(timeframe)
        except ValueError:
            return False
        return True


class LiveFeed(ABC):
    """A running connection that pushes ticks into a handler until stopped.

    One feed does not have to mean every symbol: a provider returns as many
    feeds as its transport needs -- Tiingo, for one, splits FX and crypto onto
    separate sockets -- and the caller simply starts all of them.
    """

    #: Identifies the feed in logs, e.g. ``"tiingo-fx"``.
    name: str = "market-data"

    @abstractmethod
    def start(self) -> asyncio.Task[None] | None:
        """Begin streaming. Returns the task, or ``None`` when there is nothing to do."""

    @abstractmethod
    async def stop(self) -> None:
        """Stop streaming and release the connection. Must be idempotent."""

    @property
    def symbols(self) -> tuple[str, ...]:
        """QTE symbols this feed carries. Empty means :meth:`start` is a no-op."""
        return ()


# -- The vendor itself -----------------------------------------------------


# Deliberately abstract-with-no-abstract-methods (B024): both capabilities are
# optional — a history-only vendor is a legitimate provider — so the base
# declares defaults that refuse politely rather than methods every subclass is
# forced to stub out.
class MarketDataProvider(ABC):  # noqa: B024
    """A market-data vendor, as a factory for the objects QTE consumes.

    Subclasses declare :attr:`name`, :attr:`capabilities` and :attr:`markets`,
    then implement whichever of :meth:`history_source` / :meth:`live_feeds`
    they claim. The base refuses the rest with a clear error instead of an
    ``AttributeError`` three layers down.
    """

    #: Registry key, matched against ``QTE_MARKET_DATA__PROVIDER``.
    name: ClassVar[str] = ""
    capabilities: ClassVar[frozenset[Capability]] = frozenset()
    #: Markets the vendor quotes; symbols on any other market are dropped.
    markets: ClassVar[tuple[Market, ...]] = ("fx", "crypto")

    def supports(self, capability: Capability) -> bool:
        return capability in self.capabilities

    def specs_for_markets(self, specs: list[SymbolSpec]) -> list[SymbolSpec]:
        """Keep only the symbols this vendor quotes."""
        return [spec for spec in specs if spec.market in self.markets]

    def ticker_for(self, spec: SymbolSpec) -> str:
        """How this vendor spells a QTE symbol. Lowercase is the common case."""
        return spec.symbol.lower()

    def history_source(self) -> HistorySource:
        """The vendor's history client."""
        raise UnsupportedCapability(f"{self.name!r} does not serve historical bars")

    def live_feeds(self, specs: list[SymbolSpec], on_tick: TickHandler) -> list[LiveFeed]:
        """One or more feeds covering *specs*; empty when none of them apply."""
        raise UnsupportedCapability(f"{self.name!r} does not serve a live feed")

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        served = ",".join(sorted(capability.value for capability in self.capabilities))
        return f"<{type(self).__name__} name={self.name!r} capabilities={served}>"


# -- Canonical frame -------------------------------------------------------


def empty_ohlcv_frame() -> pd.DataFrame:
    """The canonical frame with no rows -- what "no data" looks like."""
    return pd.DataFrame(
        columns=list(OHLCV_COLUMNS),
        index=pd.DatetimeIndex([], tz="UTC", name="open_time"),
    )


def normalize_ohlcv(rows: list[dict[str, Any]], timeframe: str) -> pd.DataFrame:
    """Coerce raw vendor rows into the one OHLCV shape QTE reads.

    Bars are keyed by **open** time in UTC and sorted ascending, with duplicate
    timestamps collapsed -- vendors occasionally repeat a bar at a page
    boundary, and a duplicated bar silently double-counts in any cumulative
    metric. The timeframe is stamped into ``frame.attrs`` so the frame stays
    self-describing once it leaves the provider.
    """
    if not rows:
        return empty_ohlcv_frame()

    frame = pd.DataFrame(rows)
    timestamp_column = next((c for c in ("date", "timestamp", "datetime") if c in frame), None)
    if timestamp_column is None:
        raise ValueError(f"Market data rows carry no recognisable timestamp column: {list(frame)}")

    frame["open_time"] = pd.to_datetime(frame[timestamp_column], utc=True)
    if "volume" not in frame:
        # FX bars have no volume at all; a zero column keeps the schema uniform
        # so a strategy can read df["volume"] without branching per asset class.
        frame["volume"] = 0.0

    frame = frame.set_index("open_time")[list(OHLCV_COLUMNS)].astype(float).sort_index()
    frame = frame[~frame.index.duplicated(keep="last")]
    frame.attrs["timeframe"] = normalize_timeframe(timeframe)
    frame.attrs["timeframe_seconds"] = timeframe_seconds(timeframe)
    return frame
