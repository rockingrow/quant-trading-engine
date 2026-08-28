"""Vendor history, kept on disk so a rehearsal does not spend a real request.

    data/parquet/tiingo/XAUUSD_M15.parquet

One file per provider, symbol and timeframe, written every time a fetch
succeeds and read back before the next one is attempted. A free Tiingo plan is
rate limited and depth limited, and a dev loop that restarts ingestion twenty
times an afternoon will exhaust it long before it exhausts the developer; the
same sixty days of M15 fetched once is enough for all twenty.

**The cache never stands in for the market in production.** ``QTE_ENV=prod``
fetches from the vendor unconditionally, because a stale file that warms an
indicator window with last week's bars is worse than a slow start -- the
strategy would trade live prices against history that never reached it. Dev and
staging read the cache first and fall back to the network when it does not
cover the range.

This lives in ``qte_shared`` rather than beside the backtest downloader because
both the downloader and ingestion's warm-up write to it, and an engine may not
import another engine. ``pyarrow`` is *not* a shared dependency -- the strategy
runner has no use for it -- so every entry point here degrades to "no cache"
when it is missing rather than failing the caller.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from qte_shared.config import settings
from qte_shared.interfaces.market_data import (
    HistoryRequest,
    HistorySource,
    empty_ohlcv_frame,
)
from qte_shared.logging_setup import get_logger
from qte_shared.timeframes import normalize_timeframe

log = get_logger(__name__)

#: Days of slack allowed at the end of a cached range before it counts as not
#: covering the request. A Friday-close request answered on a Sunday is whole;
#: so is one that ends on a public holiday.
COVERAGE_TOLERANCE_DAYS = 4


class HistoryCache:
    """Parquet history for one provider, merged rather than overwritten."""

    def __init__(self, provider_name: str, directory: Path | None = None) -> None:
        self.provider_name = provider_name
        base = directory or settings.engine.parquet_dir
        self.directory = Path(base) / provider_name

    @property
    def available(self) -> bool:
        """Whether this process can read and write parquet at all."""
        return _pyarrow_present()

    def path_for(self, symbol: str, timeframe: str) -> Path:
        return self.directory / f"{symbol.upper()}_{normalize_timeframe(timeframe)}.parquet"

    def load(
        self,
        symbol: str,
        timeframe: str,
        start: date | None = None,
        end: date | None = None,
    ) -> pd.DataFrame:
        """Cached bars for the range, or an empty frame when there are none."""
        path = self.path_for(symbol, timeframe)
        if not self.available or not path.is_file():
            return empty_ohlcv_frame()
        try:
            frame = pd.read_parquet(path, engine="pyarrow")
        except Exception:
            # A half-written file from a killed process must not take the
            # caller down: the vendor is still there to ask.
            log.exception("Could not read history cache %s — falling back to the vendor", path)
            return empty_ohlcv_frame()
        if start is not None:
            frame = frame[frame.index >= pd.Timestamp(start, tz="UTC")]
        if end is not None:
            frame = frame[frame.index <= pd.Timestamp(end, tz="UTC") + pd.Timedelta(days=1)]
        return frame

    def replace(self, frame: pd.DataFrame, symbol: str, timeframe: str) -> Path | None:
        """Overwrite the cached file, discarding whatever it held.

        The deliberate counterpart to :meth:`store`, for when the bars on disk
        are wrong rather than merely incomplete.
        """
        return self._write(frame, symbol, timeframe, merge=False)

    def store(self, frame: pd.DataFrame, symbol: str, timeframe: str) -> Path | None:
        """Merge *frame* into the cached file. Returns the path, or ``None``.

        Merging, not replacing: two fetches of adjacent ranges should leave one
        file spanning both, and a short answer must never shorten what is
        already on disk.
        """
        return self._write(frame, symbol, timeframe, merge=True)

    def _write(
        self, frame: pd.DataFrame, symbol: str, timeframe: str, *, merge: bool
    ) -> Path | None:
        if frame.empty or not self.available:
            return None
        path = self.path_for(symbol, timeframe)
        merged = merge_frames(self.load(symbol, timeframe), frame) if merge else frame
        try:
            self.directory.mkdir(parents=True, exist_ok=True)
            merged.to_parquet(path, engine="pyarrow", compression="snappy")
        except Exception:
            # The cache is an optimisation. Losing it costs a request, not the run.
            log.exception("Could not write history cache %s", path)
            return None
        log.debug("History cache %s now holds %d bars", path.name, len(merged))
        return path


def merge_frames(existing: pd.DataFrame, incoming: pd.DataFrame) -> pd.DataFrame:
    """Union of two OHLCV frames, newest wins on a shared timestamp."""
    if existing.empty:
        return incoming.sort_index()
    if incoming.empty:
        return existing.sort_index()
    merged = pd.concat([existing, incoming])
    merged = merged[~merged.index.duplicated(keep="last")].sort_index()
    merged.attrs.update(incoming.attrs)
    return merged


def covers(frame: pd.DataFrame, request: HistoryRequest) -> bool:
    """Whether *frame* spans the requested range closely enough to serve it."""
    if frame.empty:
        return False
    first = frame.index[0].date()
    last = frame.index[-1].date()
    tolerance = timedelta(days=COVERAGE_TOLERANCE_DAYS)
    # The tolerance applies at *both* ends. A range whose start lands on a
    # Saturday has its first bar on the Sunday open, and demanding an exact
    # match there would declare such a window uncovered every time -- roughly
    # two days in seven, on the very path the cache exists to keep off the wire.
    return first <= request.start + tolerance and last >= request.end - tolerance


def use_cache_by_default() -> bool:
    """Dev and staging may read the cache; production always asks the vendor."""
    return settings.env != "prod"


async def fetch_history(
    source: HistorySource,
    request: HistoryRequest,
    *,
    cache: HistoryCache | None = None,
    use_cache: bool | None = None,
) -> pd.DataFrame:
    """Bars for *request*, from the cache when allowed, from the vendor otherwise.

    Every successful vendor fetch is written back, so the caller that paid for
    a request is also the one that saves the next caller from paying for it.
    """
    request = request.normalized()
    allowed = use_cache_by_default() if use_cache is None else use_cache

    if allowed and cache is not None:
        cached = cache.load(request.symbol, request.timeframe, request.start, request.end)
        if covers(cached, request):
            log.info(
                "History cache hit %s %s %s..%s (%d bars) — no vendor request made",
                request.symbol,
                request.timeframe,
                request.start,
                request.end,
                len(cached),
            )
            return cached

    frame = await source.fetch(request)
    if cache is not None and not frame.empty:
        cache.store(frame, request.symbol, request.timeframe)
    return frame


def _pyarrow_present() -> bool:
    try:
        import pyarrow  # noqa: F401
    except ImportError:
        log.debug("pyarrow is not installed; the history cache is disabled in this process")
        return False
    return True


__all__ = [
    "COVERAGE_TOLERANCE_DAYS",
    "HistoryCache",
    "covers",
    "fetch_history",
    "merge_frames",
    "use_cache_by_default",
]
