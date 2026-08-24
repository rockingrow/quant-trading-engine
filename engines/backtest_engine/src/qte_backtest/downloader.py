"""Provider history -> parquet.

Parquet, not CSV: three years of XAUUSD M1 is ~1.5M rows, and columnar storage
with dictionary compression turns a 90MB CSV into a few MB that loads in well
under a second. A replay that reloads the file on every run cares about that.

Nothing here knows a vendor. It asks the configured
:class:`~qte_shared.interfaces.market_data.MarketDataProvider` for a history
source, receives the canonical OHLCV frame, and writes it down; the endpoint
shapes and per-market quirks live behind that seam, in the provider.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from qte_shared.config import settings
from qte_shared.interfaces.market_data import (
    Capability,
    HistoryRequest,
    MarketDataProvider,
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

    def path_for(self, symbol: str, timeframe: str) -> Path:
        return self._dir / f"{symbol.upper()}_{normalize_timeframe(timeframe)}.parquet"

    async def download(self, request: DownloadRequest) -> Path:
        """Fetch one symbol/timeframe and write it to parquet. Returns the path."""
        history = request.to_history_request()
        frame = await self._source.fetch(history)
        if frame.empty:
            raise RuntimeError(
                f"{self.provider.name} returned no rows for {history.symbol} "
                f"{history.timeframe} {history.start}..{history.end} — check the ticker "
                "spelling and your plan's history depth"
            )
        path = self.path_for(history.symbol, history.timeframe)
        frame.to_parquet(path, engine="pyarrow", compression="snappy")
        log.info(
            "Saved %s rows=%d span=%s..%s source=%s",
            path.name,
            len(frame),
            frame.index[0].isoformat(),
            frame.index[-1].isoformat(),
            self.provider.name,
        )
        return path

    async def download_many(self, requests: list[DownloadRequest]) -> list[Path]:
        return [await self.download(request) for request in requests]
