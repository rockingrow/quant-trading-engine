"""Tiingo REST history → parquet.

Parquet, not CSV: three years of XAUUSD M1 is ~1.5M rows, and columnar storage
with dictionary compression turns a 90MB CSV into a few MB that loads in well
under a second. A replay that reloads the file on every run cares about that.

Tiingo splits history across two APIs with different shapes — ``/tiingo/fx`` and
``/tiingo/crypto`` — so this module normalises both into one OHLCV frame with a
UTC ``open_time`` index, which is the only shape the rest of QTE knows.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
from qte_shared.config import settings
from qte_shared.logging_setup import get_logger
from qte_shared.symbols import infer_market
from qte_shared.timeframes import normalize_timeframe, timeframe_seconds

log = get_logger(__name__)

OHLCV = ["open", "high", "low", "close", "volume"]

#: Tiingo's own resample-frequency spelling, per QTE timeframe.
_FREQUENCY = {
    "M1": "1min",
    "M5": "5min",
    "M15": "15min",
    "M30": "30min",
    "H1": "1hour",
    "H4": "4hour",
    "D1": "1day",
}


@dataclass(slots=True)
class DownloadRequest:
    symbol: str
    timeframe: str = "M15"
    start: date | None = None
    end: date | None = None
    market: str | None = None

    def resolved_market(self) -> str:
        return self.market or infer_market(self.symbol)


class TiingoDownloader:
    """Pulls history and writes ``<parquet_dir>/<SYMBOL>_<TF>.parquet``."""

    def __init__(self, api_key: str | None = None, parquet_dir: Path | None = None) -> None:
        self._api_key = api_key if api_key is not None else settings.tiingo.api_key
        self._dir = Path(parquet_dir or settings.engine.parquet_dir)
        self._dir.mkdir(parents=True, exist_ok=True)

    def path_for(self, symbol: str, timeframe: str) -> Path:
        return self._dir / f"{symbol.upper()}_{normalize_timeframe(timeframe)}.parquet"

    async def download(self, request: DownloadRequest) -> Path:
        """Fetch one symbol/timeframe and write it to parquet. Returns the path."""
        if not self._api_key:
            raise RuntimeError("QTE_TIINGO__API_KEY is not set; cannot download history")

        timeframe = normalize_timeframe(request.timeframe)
        end = request.end or datetime.now(UTC).date()
        start = request.start or (end - timedelta(days=365 * 3))
        market = request.resolved_market()

        frame = await self._fetch(request.symbol, timeframe, start, end, market)
        if frame.empty:
            raise RuntimeError(
                f"Tiingo returned no rows for {request.symbol} {timeframe} "
                f"{start}..{end} — check the ticker spelling and your plan's history depth"
            )
        path = self.path_for(request.symbol, timeframe)
        frame.to_parquet(path, engine="pyarrow", compression="snappy")
        log.info(
            "Saved %s rows=%d span=%s..%s",
            path.name,
            len(frame),
            frame.index[0].isoformat(),
            frame.index[-1].isoformat(),
        )
        return path

    async def download_many(self, requests: list[DownloadRequest]) -> list[Path]:
        return [await self.download(request) for request in requests]

    # ── HTTP ──────────────────────────────────────────────────────────

    async def _fetch(
        self, symbol: str, timeframe: str, start: date, end: date, market: str
    ) -> pd.DataFrame:
        frequency = _FREQUENCY[timeframe]
        ticker = symbol.lower()
        base = settings.tiingo.rest_url.rstrip("/")

        if market == "crypto":
            url = f"{base}/tiingo/crypto/prices"
            params: dict[str, Any] = {
                "tickers": ticker,
                "startDate": start.isoformat(),
                "endDate": end.isoformat(),
                "resampleFreq": frequency,
            }
        else:
            url = f"{base}/tiingo/fx/{ticker}/prices"
            params = {
                "startDate": start.isoformat(),
                "endDate": end.isoformat(),
                "resampleFreq": frequency,
            }

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Token {self._api_key}",
        }
        async with httpx.AsyncClient(timeout=settings.tiingo.request_timeout) as client:
            response = await client.get(url, params=params, headers=headers)
            response.raise_for_status()
            body = response.json()

        rows = _crypto_rows(body) if market == "crypto" else body
        return normalize_frame(rows, timeframe)


def _crypto_rows(body: Any) -> list[dict[str, Any]]:
    """Unwrap the crypto response, which nests bars under a per-ticker object."""
    if isinstance(body, list) and body and isinstance(body[0], dict) and "priceData" in body[0]:
        return body[0]["priceData"]
    return body if isinstance(body, list) else []


def normalize_frame(rows: list[dict[str, Any]], timeframe: str) -> pd.DataFrame:
    """Coerce raw Tiingo rows into the canonical OHLCV frame.

    Bars are keyed by **open** time in UTC and sorted ascending, with duplicate
    timestamps collapsed — Tiingo occasionally repeats a bar at a page boundary,
    and a duplicated bar silently double-counts in any cumulative metric.
    """
    if not rows:
        return pd.DataFrame(columns=OHLCV, index=pd.DatetimeIndex([], tz="UTC", name="open_time"))

    frame = pd.DataFrame(rows)
    timestamp_column = next((c for c in ("date", "timestamp", "datetime") if c in frame), None)
    if timestamp_column is None:
        raise ValueError(f"Tiingo rows carry no recognisable timestamp column: {list(frame)}")

    frame["open_time"] = pd.to_datetime(frame[timestamp_column], utc=True)
    if "volume" not in frame:
        # FX bars have no volume at all; a zero column keeps the schema uniform
        # so a strategy can read df["volume"] without branching per asset class.
        frame["volume"] = 0.0

    frame = frame.set_index("open_time")[OHLCV].astype(float).sort_index()
    frame = frame[~frame.index.duplicated(keep="last")]
    frame.attrs["timeframe"] = normalize_timeframe(timeframe)
    frame.attrs["timeframe_seconds"] = timeframe_seconds(timeframe)
    return frame
