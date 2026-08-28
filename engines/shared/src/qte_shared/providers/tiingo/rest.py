"""Tiingo REST history -> the canonical OHLCV frame.

Tiingo splits history across two APIs with different shapes -- ``/tiingo/fx``
and ``/tiingo/crypto`` -- and this module is where that difference stops. What
leaves it is the frame described by
:meth:`~qte_shared.interfaces.market_data.HistorySource.fetch`, identical for
both markets and for every other vendor.

**A wide range is paged, because the vendor truncates silently.** An intraday
request whose answer exceeds Tiingo's per-response row cap comes back ``200
OK`` with the *end* of the range missing and nothing to say so: a year of M15
answered with four months of it, a caller none the wiser. Measured on a free
plan, one call returned 6859 bars of a year of M15 and 7919 bars of sixty days
of M5 -- a row ceiling, not a date one, which is why the page span here is
computed from bars rather than from days. So :meth:`TiingoHistorySource.fetch`
walks the range in windows sized to stay under
``QTE_TIINGO__MAX_ROWS_PER_REQUEST``, and treats a window that still comes back
short as a truncation to resume from rather than as the end of the data.
Duplicate bars across a page boundary are collapsed by
:func:`~qte_shared.interfaces.market_data.normalize_ohlcv`.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import httpx
import pandas as pd

from qte_shared.interfaces.market_data import (
    HistoryRequest,
    HistorySource,
    ProviderError,
    ProviderNotConfigured,
    empty_ohlcv_frame,
    normalize_ohlcv,
)
from qte_shared.logging_setup import get_logger
from qte_shared.providers.tiingo.settings import TiingoSettings
from qte_shared.timeframes import timeframe_seconds

log = get_logger(__name__)

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

#: Seconds in a day, used to turn a row budget into a window of dates.
_SECONDS_PER_DAY = 86_400


class TiingoHistorySource(HistorySource):
    """Fetches completed bars over Tiingo's REST API, paging a wide range."""

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

        span_days = self._page_span_days(request.timeframe)
        pages: list[pd.DataFrame] = []
        cursor = request.start
        page_count = 0

        async with httpx.AsyncClient(timeout=self._config.request_timeout) as client:
            while cursor <= request.end and page_count < self._config.max_pages:
                window_end = min(cursor + timedelta(days=span_days - 1), request.end)
                try:
                    frame = await self._fetch_page(client, request, cursor, window_end)
                except httpx.HTTPStatusError as error:
                    raise self._explain(error, request, page_count, cursor) from error
                page_count += 1

                if frame.empty:
                    # A closed market, or a gap the vendor has nothing for.
                    # Neither means the range is over, so keep walking.
                    cursor = window_end + timedelta(days=1)
                    continue

                pages.append(frame)
                cursor = self._next_cursor(frame, cursor, window_end, request.end)

            if page_count >= self._config.max_pages and cursor <= request.end:
                log.warning(
                    "Tiingo paging stopped at the %d-page ceiling for %s %s with %s..%s "
                    "still unfetched. Raise QTE_TIINGO__MAX_PAGES or narrow the range.",
                    self._config.max_pages,
                    request.symbol,
                    request.timeframe,
                    cursor,
                    request.end,
                )

        return self._combine(pages, request)

    # -- Paging ------------------------------------------------------------

    def _page_span_days(self, timeframe: str) -> int:
        """Days per request that keep the answer under the vendor's row cap.

        Deliberately pessimistic: it prices a day at its full 24 hours of bars
        even though FX closes at the weekend, so the real answer lands under
        the budget rather than on it.
        """
        bars_per_day = _SECONDS_PER_DAY / timeframe_seconds(timeframe)
        return max(1, int(self._config.max_rows_per_request // bars_per_day))

    def _next_cursor(
        self, frame: pd.DataFrame, cursor: date, window_end: date, range_end: date
    ) -> date:
        """Where the next page starts, given what this one actually returned.

        A page that stopped before its window was truncated, and the next one
        resumes *on* that last day rather than after it, because the rest of
        that day is still missing.

        A page that reached its window can still have been capped part-way
        through the final day, and the dates alone cannot tell the two apart.
        The row count can: the window was sized to come in under
        ``max_rows_per_request``, so an answer that reaches the budget is one
        the vendor may have cut. Re-reading that day is a day of overlap, not
        an extra request -- except at the end of the whole range, where asking
        again would return the same capped answer and buy nothing, so the walk
        stops there instead.
        """
        last_returned = frame.index[-1].date()
        reached_budget = len(frame) >= self._config.max_rows_per_request
        incomplete = last_returned < window_end or (reached_budget and window_end < range_end)
        if incomplete and last_returned > cursor:
            return last_returned
        if not incomplete:
            return window_end + timedelta(days=1)
        # One day on its own overflowed the row cap. Nothing smaller can be
        # asked for -- the endpoint's window is date-granular -- so record the
        # hole rather than spinning on it.
        log.warning(
            "Tiingo capped a single day (%s) below one page; bars after %s are "
            "unreachable at this timeframe. Lower QTE_TIINGO__MAX_ROWS_PER_REQUEST "
            "or use a longer timeframe.",
            cursor,
            frame.index[-1].isoformat(),
        )
        return cursor + timedelta(days=1)

    def _combine(self, pages: list[pd.DataFrame], request: HistoryRequest) -> pd.DataFrame:
        """One frame from every page, duplicates at the seams collapsed."""
        if not pages:
            return empty_ohlcv_frame()
        if len(pages) == 1:
            return pages[0]
        merged = pd.concat(pages)
        merged = merged[~merged.index.duplicated(keep="last")].sort_index()
        merged.attrs.update(pages[0].attrs)
        log.debug(
            "Tiingo paged %s %s into %d requests, %d bars",
            request.symbol,
            request.timeframe,
            len(pages),
            len(merged),
        )
        return merged

    @staticmethod
    def _explain(
        error: httpx.HTTPStatusError,
        request: HistoryRequest,
        pages_done: int,
        cursor: date,
    ) -> ProviderError:
        """Turn a transport error into one that names the plan, not the status.

        Paging multiplied one call into as many as the range needs, which makes
        the per-hour cap a routine outcome rather than an exotic one. A bare
        ``HTTPStatusError: 429`` says nothing about which plan, how far the walk
        got, or what to change.
        """
        if error.response.status_code == 429:
            return ProviderError(
                f"Tiingo rate-limited the request after {pages_done} page(s) of "
                f"{request.symbol} {request.timeframe}; history from {cursor} to "
                f"{request.end} was not fetched. The range needed more calls than the "
                "plan allows in one window — wait for the quota to reset, narrow the "
                "range, use a longer timeframe, or raise "
                "QTE_TIINGO__MAX_ROWS_PER_REQUEST on a paid plan to need fewer calls."
            )
        return ProviderError(
            f"Tiingo returned {error.response.status_code} for {request.symbol} "
            f"{request.timeframe} after {pages_done} page(s); history from {cursor} to "
            f"{request.end} was not fetched."
        )

    # -- Wire details ------------------------------------------------------

    async def _fetch_page(
        self,
        client: httpx.AsyncClient,
        request: HistoryRequest,
        start: date,
        end: date,
    ) -> pd.DataFrame:
        url, params = self._endpoint(request, start, end)
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Token {self._config.api_key}",
        }
        response = await client.get(url, params=params, headers=headers)
        response.raise_for_status()
        body = response.json()

        rows = _crypto_rows(body) if request.market == "crypto" else body
        return normalize_ohlcv(rows if isinstance(rows, list) else [], request.timeframe)

    def _endpoint(
        self, request: HistoryRequest, start: date, end: date
    ) -> tuple[str, dict[str, Any]]:
        base = self._config.rest_url.rstrip("/")
        ticker = request.symbol.lower()
        window = _window(start, end)
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
