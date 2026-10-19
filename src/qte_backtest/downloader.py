"""Provider history -> parquet.

Parquet, not CSV: three years of XAUUSD M1 is ~1.5M rows, and columnar storage
with dictionary compression turns a 90MB CSV into a few MB that loads in well
under a second. A replay that reloads the file on every run cares about that.

Nothing here knows a vendor. It asks the configured
:class:`~qte_shared.interfaces.market_data.MarketDataProvider` for a history
source, receives the canonical OHLCV frame, and writes it down; the endpoint
shapes and per-market quirks live behind that seam, in the provider.

Two things a vendor will not tell you are handled here. A response capped
mid-range comes back ``200 OK`` and simply stops early, so every write is
checked against the range that was asked for and the shortfall logged. And the
file is merged rather than overwritten, because the alternative is that one
short answer quietly replaces a good history under the same name.

There is exactly one file per symbol and timeframe, and it lives under the
provider's own directory -- ``data/parquet/tiingo/XAUUSD_M15.parquet``. Writing
a second, vendor-neutral copy alongside it used to make ingestion's warm-up and
a backtest read different files that only looked like the same history; now the
path names its source, and a replay is told which one to read.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd

from qte_shared.history_cache import (
    COVERAGE_TOLERANCE_DAYS,
    HistoryCache,
    fetch_history,
)
from qte_shared.interfaces.market_data import (
    Capability,
    HistoryRequest,
    MarketDataProvider,
)
from qte_shared.logging_setup import get_logger
from qte_shared.providers import create_provider
from qte_shared.symbols import Market

log = get_logger(__name__)

#: Default span when a request names no start date.
DEFAULT_HISTORY_DAYS = 365 * 3


@dataclass(slots=True)
class DownloadRequest:
    symbol: str
    market: Market
    timeframe: str = "M15"
    start: date | None = None
    end: date | None = None

    def to_history_request(self) -> HistoryRequest:
        """Fill in the open ends and hand the provider a fully specified window."""
        end = self.end or datetime.now(UTC).date()
        start = self.start or (end - timedelta(days=DEFAULT_HISTORY_DAYS))
        return HistoryRequest(
            symbol=self.symbol,
            timeframe=self.timeframe,
            start=start,
            end=end,
            market=self.market,
        ).normalized()


class HistoryDownloader:
    """Pulls history into ``<parquet_dir>/<provider>/<SYMBOL>_<TF>.parquet``."""

    def __init__(
        self,
        provider: MarketDataProvider | None = None,
        parquet_dir: Path | None = None,
    ) -> None:
        self.provider = provider or create_provider(capability=Capability.HISTORY)
        self._source = self.provider.history_source()
        #: The one file this command writes, and the same one ingestion's
        #: warm-up reads on a dev stack -- there is no second copy to drift.
        self._cache = HistoryCache(self.provider.name, parquet_dir)

    @property
    def directory(self) -> Path:
        return self._cache.directory

    def path_for(self, symbol: str, timeframe: str) -> Path:
        return self._cache.path_for(symbol, timeframe)

    async def download(self, request: DownloadRequest, *, replace: bool = False) -> Path:
        """Fetch one symbol/timeframe and write it to parquet. Returns the path.

        The file is *merged* by default. Re-downloading a narrower range than
        the one already on disk would otherwise throw the rest away, and the
        vendor truncating an answer would do it silently -- both leave a file
        whose name still claims to be the whole history. Pass *replace* to
        overwrite deliberately, which is the way to discard bars that are wrong
        rather than merely absent.
        """
        if not self._cache.available:
            raise RuntimeError(
                "Writing parquet history needs pyarrow, which ships in the optional "
                "`history-cache` extra. Install it with `uv sync --extra history-cache`."
            )
        history = request.to_history_request()
        # Never read the cache here. This command exists to fetch, and a cache
        # hit would make `make download` a no-op that quietly returns bars up
        # to COVERAGE_TOLERANCE_DAYS old. The write below is the only one, so
        # the merge-or-replace decision is made in a single place.
        frame = await fetch_history(self._source, history, cache=None, use_cache=False)
        if frame.empty:
            raise RuntimeError(
                f"{self.provider.name} returned no rows for {history.symbol} "
                f"{history.timeframe} {history.start}..{history.end} — check the ticker "
                "spelling and your plan's history depth"
            )
        _warn_if_short(frame, history, self.provider.name)

        write = self._cache.replace if replace else self._cache.store
        path = write(frame, history.symbol, history.timeframe)
        if path is None:
            raise RuntimeError(
                f"Could not write {self.path_for(history.symbol, history.timeframe)} — "
                "see the logged error above"
            )
        written = self._cache.load(history.symbol, history.timeframe)
        log.info(
            "Saved %s rows=%d span=%s..%s source=%s mode=%s",
            path,
            len(written),
            written.index[0].isoformat(),
            written.index[-1].isoformat(),
            self.provider.name,
            "replace" if replace else "merge",
        )
        return path

    async def download_many(
        self, requests: list[DownloadRequest], *, replace: bool = False
    ) -> list[Path]:
        return [await self.download(request, replace=replace) for request in requests]


def _warn_if_short(frame: pd.DataFrame, history: HistoryRequest, provider_name: str) -> None:
    """Say so when the answer does not reach the end of the range asked for.

    Tiingo signals its row cap by returning ``200 OK`` and stopping early, so
    "fewer bars than expected" is the *only* evidence a caller ever gets. The
    provider pages to stay under that cap; this is the check that the paging
    actually worked, and it belongs on the path that writes the file rather
    than inside the vendor module that has an interest in believing itself.
    """
    last = frame.index[-1].date()
    shortfall = (history.end - last).days
    if shortfall > COVERAGE_TOLERANCE_DAYS:
        log.warning(
            "%s returned history ending %s, %d days short of the requested %s for %s %s. "
            "The vendor caps a response and does not report it; lower "
            "QTE_TIINGO__MAX_ROWS_PER_REQUEST or check your plan's depth.",
            provider_name,
            last,
            shortfall,
            history.end,
            history.symbol,
            history.timeframe,
        )
