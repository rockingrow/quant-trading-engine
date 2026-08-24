"""Tiingo REST history -> the canonical OHLCV frame.

Tiingo splits history across two APIs with different shapes -- ``/tiingo/fx``
and ``/tiingo/crypto`` -- and this module is where that difference stops. What
leaves it is the frame described by
:meth:`~qte_shared.interfaces.market_data.HistorySource.fetch`, identical for
both markets and for every other vendor.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import httpx
import pandas as pd

from qte_shared.interfaces.market_data import (
    HistoryRequest,
    HistorySource,
    ProviderNotConfigured,
    normalize_ohlcv,
)
from qte_shared.providers.tiingo.settings import TiingoSettings

#: Tiingo's own resample-frequency spelling, per QTE timeframe.
FREQUENCY = {
    "M1": "1min",
    "M5": "5min",
    "M15": "15min",
    "M30": "30min",
    "H1": "1hour",
    "H4": "4hour",
    "D1": "1day",
}


class TiingoHistorySource(HistorySource):
    """Fetches completed bars over Tiingo's REST API."""

    def __init__(self, config: TiingoSettings) -> None:
        if not config.api_key:
            raise ProviderNotConfigured(
                "QTE_TIINGO__API_KEY is not set; Tiingo cannot serve historical bars"
            )
        self._config = config

    def supports_timeframe(self, timeframe: str) -> bool:
        return timeframe in FREQUENCY

    async def fetch(self, request: HistoryRequest) -> pd.DataFrame:
        request = request.normalized()
        if request.timeframe not in FREQUENCY:
            raise ValueError(f"Tiingo has no resample frequency for {request.timeframe}")

        url, params = self._endpoint(request)
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Token {self._config.api_key}",
        }
        async with httpx.AsyncClient(timeout=self._config.request_timeout) as client:
            response = await client.get(url, params=params, headers=headers)
            response.raise_for_status()
            body = response.json()

        rows = _crypto_rows(body) if request.market == "crypto" else body
        return normalize_ohlcv(rows if isinstance(rows, list) else [], request.timeframe)

    # -- Wire details ------------------------------------------------------

    def _endpoint(self, request: HistoryRequest) -> tuple[str, dict[str, Any]]:
        base = self._config.rest_url.rstrip("/")
        ticker = request.symbol.lower()
        window = _window(request.start, request.end)
        frequency = FREQUENCY[request.timeframe]

        if request.market == "crypto":
            return (
                f"{base}/tiingo/crypto/prices",
                {"tickers": ticker, "resampleFreq": frequency, **window},
            )
        return (
            f"{base}/tiingo/fx/{ticker}/prices",
            {"resampleFreq": frequency, **window},
        )


def _window(start: date, end: date) -> dict[str, str]:
    return {"startDate": start.isoformat(), "endDate": end.isoformat()}


def _crypto_rows(body: Any) -> list[dict[str, Any]]:
    """Unwrap the crypto response, which nests bars under a per-ticker object."""
    if isinstance(body, list) and body and isinstance(body[0], dict) and "priceData" in body[0]:
        return body[0]["priceData"]
    return body if isinstance(body, list) else []
