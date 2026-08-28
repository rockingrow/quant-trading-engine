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
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd
from qte_shared.config import settings
from qte_shared.history_cache import (
    COVERAGE_TOLERANCE_DAYS,
    HistoryCache,
    fetch_history,
    merge_frames,
)
from qte_shared.interfaces.market_data import (
    Capability,
    HistoryRequest,
    MarketDataProvider,
    empty_ohlcv_frame,
)
from qte_shared.logging_setup import get_logger
from qte_shared.providers import create_provider
from qte_shared.symbols import Market, infer_market
from qte_shared.timeframes import normalize_timeframe

log = get_logger(__name__)

#: Default span when a request names no start date.
DEFAULT_HISTORY_DAYS = 365 * 3


@dataclass(slots=True)
class DownloadRequest:
    symbol: str
    timeframe: str = "M15"
    start: date | None = None
    end: date | None = None
    market: str | None = None

    def resolved_market(self) -> Market:
        return self.market or infer_market(self.symbol)  # type: ignore[return-value]

    def to_history_request(self) -> HistoryRequest:
        """Fill in the open ends and hand the provider a fully specified window."""
        end = self.end or datetime.now(UTC).date()
        start = self.start or (end - timedelta(days=DEFAULT_HISTORY_DAYS))
        return HistoryRequest(
            symbol=self.symbol,
            timeframe=self.timeframe,
            start=start,
            end=end,
            market=self.resolved_market(),
        ).normalized()


class HistoryDownloader:
    """Pulls history and writes ``<parquet_dir>/<SYMBOL>_<TF>.parquet``."""

    def __init__(
        self,
        provider: MarketDataProvider | None = None,
        parquet_dir: Path | None = None,
    ) -> None:
        self.provider = provider or create_provider(capability=Capability.HISTORY)
        self._source = self.provider.history_source()
        self._dir = Path(parquet_dir or settings.engine.parquet_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        #: Vendor-side copy, shared with ingestion's warm-up. Writing it here is
        #: what lets a dev stack replay real bars without spending a request.
        self._cache = HistoryCache(self.provider.name, self._dir)

    def path_for(self, symbol: str, timeframe: str) -> Path:
        return self._dir / f"{symbol.upper()}_{normalize_timeframe(timeframe)}.parquet"

    async def download(self, request: DownloadRequest, *, replace: bool = False) -> Path:
        """Fetch one symbol/timeframe and write it to parquet. Returns the path.

        The file is *merged* by default. Re-downloading a narrower range than
        the one already on disk would otherwise throw the rest away, and the
        vendor truncating an answer would do it silently -- both leave a file
        whose name still claims to be the whole history. Pass *replace* to
        overwrite deliberately, which is the way to discard bars that are wrong
        rather than merely absent.
        """
        history = request.to_history_request()
        # Never read the cache here. This command exists to fetch, and a cache
        # hit would make `make download` a no-op that quietly returns bars up
        # to COVERAGE_TOLERANCE_DAYS old. It still *writes* the cache, which
        # is what ingestion's warm-up reads.
        # On a replace the cache is written once, below, with the frame alone;
        # letting fetch_history merge into it first would read and rewrite the
        # whole file only to have it immediately overwritten.
        frame = await fetch_history(
            self._source,
            history,
            cache=None if replace else self._cache,
            use_cache=False,
        )
        if frame.empty:
            raise RuntimeError(
                f"{self.provider.name} returned no rows for {history.symbol} "
                f"{history.timeframe} {history.start}..{history.end} — check the ticker "
                "spelling and your plan's history depth"
            )
        _warn_if_short(frame, history, self.provider.name)

        path = self.path_for(history.symbol, history.timeframe)
        if replace:
            # The vendor copy is merged on write, so replacing only the canonical
            # file would leave the two disagreeing about the same symbol.
            self._cache.replace(frame, history.symbol, history.timeframe)
        written = frame if replace else merge_frames(self._read_existing(path), frame)
        written.to_parquet(path, engine="pyarrow", compression="snappy")
        log.info(
            "Saved %s rows=%d span=%s..%s source=%s mode=%s",
            path.name,
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

    @staticmethod
    def _read_existing(path: Path) -> pd.DataFrame:
        if not path.is_file():
            return empty_ohlcv_frame()
        try:
            return pd.read_parquet(path, engine="pyarrow")
        except Exception:
            log.exception("Could not read %s to merge into — writing the new frame alone", path)
            return empty_ohlcv_frame()


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
