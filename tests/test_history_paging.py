"""Tiingo pages a wide range, and a short answer is never taken at face value.

The vendor caps an intraday response and reports the cap by returning *fewer
bars with a 200*. Everything here is built around that one fact: the fake
endpoint below truncates exactly the way the real one was measured to, and the
tests assert the engine notices.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import httpx
import pandas as pd
import pytest
from qte_shared.history_cache import HistoryCache, covers, merge_frames
from qte_shared.interfaces import HistoryRequest
from qte_shared.providers.tiingo import TiingoSettings
from qte_shared.providers.tiingo.rest import TiingoHistorySource

BAR_SECONDS = {"15min": 900, "5min": 300, "1hour": 3600}


class CappedTiingo:
    """A Tiingo that answers ``200 OK`` with at most *cap* bars, silently.

    Bars run continuously from ``startDate``; the weekend is deliberately not
    modelled, because the truncation this guards against has nothing to do with
    which days trade.
    """

    def __init__(self, cap: int) -> None:
        self.cap = cap
        self.requests: list[tuple[date, date]] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        params = request.url.params
        start = date.fromisoformat(params["startDate"])
        end = date.fromisoformat(params["endDate"])
        self.requests.append((start, end))

        step = timedelta(seconds=BAR_SECONDS[params["resampleFreq"]])
        cursor = datetime(start.year, start.month, start.day)
        limit = datetime(end.year, end.month, end.day) + timedelta(days=1)

        rows = []
        while cursor < limit and len(rows) < self.cap:
            rows.append(
                {
                    "date": cursor.isoformat() + "Z",
                    "open": 2000.0,
                    "high": 2001.0,
                    "low": 1999.0,
                    "close": 2000.5,
                }
            )
            cursor += step
        return httpx.Response(200, json=rows)


@pytest.fixture
def capped(monkeypatch):
    """Point the history source at a capped fake instead of the network."""
    # Captured before patching: the replacement builds real clients, and
    # reading the name back through the module would recurse into itself.
    real_client = httpx.AsyncClient

    def install(cap: int = 500, vendor: CappedTiingo | None = None) -> CappedTiingo:
        vendor = vendor or CappedTiingo(cap)
        transport = httpx.MockTransport(vendor.handler)

        def build_client(*args, **kwargs):
            kwargs.pop("timeout", None)
            return real_client(transport=transport, **kwargs)

        monkeypatch.setattr(httpx, "AsyncClient", build_client)
        return vendor

    return install


def source(**overrides) -> TiingoHistorySource:
    config = TiingoSettings(api_key="test-key", **overrides)
    return TiingoHistorySource(config)


def request_for(days: int, timeframe: str = "M15") -> HistoryRequest:
    end = date(2026, 3, 1)
    return HistoryRequest(
        symbol="XAUUSD", timeframe=timeframe, start=end - timedelta(days=days), end=end
    ).normalized()


# ── Paging ────────────────────────────────────────────────────────────────


async def test_wide_range_is_paged_and_complete(capped):
    """A year of M15 comes back whole, not stopped at the vendor's cap."""
    vendor = capped(cap=2000)
    frame = await source(max_rows_per_request=1000).fetch(request_for(days=365))

    assert len(vendor.requests) > 1, "a range past the cap must be split"
    request = request_for(days=365)
    assert frame.index[0].date() <= request.start + timedelta(days=1)
    assert frame.index[-1].date() >= request.end - timedelta(days=1)


async def test_single_request_when_the_range_fits(capped):
    vendor = capped(cap=10_000)
    await source(max_rows_per_request=5000).fetch(request_for(days=10))
    assert len(vendor.requests) == 1


async def test_truncated_page_resumes_where_it_stopped(capped):
    """The cap below is *tighter* than the page budget, so pages come back short.

    This is the regression that matters: before paging, one such answer was
    returned as if it were the whole range.
    """
    vendor = capped(cap=200)
    frame = await source(max_rows_per_request=5000).fetch(request_for(days=60))

    assert len(vendor.requests) > 1
    # Every page after the first starts on or after the previous page's last day.
    starts = [start for start, _ in vendor.requests]
    assert starts == sorted(starts)
    assert frame.index[-1].date() >= date(2026, 2, 20)


async def test_pages_do_not_duplicate_bars_at_the_seam(capped):
    capped(cap=300)
    frame = await source(max_rows_per_request=5000).fetch(request_for(days=45))
    assert frame.index.is_unique
    assert frame.index.is_monotonic_increasing


async def test_page_ceiling_stops_the_walk(capped):
    """A vendor making no progress must not spin forever."""
    vendor = capped(cap=1)
    await source(max_rows_per_request=5000, max_pages=5).fetch(request_for(days=365))
    assert len(vendor.requests) <= 5


async def test_empty_window_does_not_end_the_range(capped):
    """A closed market mid-range is a gap, not the end of the data."""

    class Holiday(CappedTiingo):
        def handler(self, request: httpx.Request) -> httpx.Response:
            params = request.url.params
            start = date.fromisoformat(params["startDate"])
            self.requests.append((start, date.fromisoformat(params["endDate"])))
            if start.month == 2:
                return httpx.Response(200, json=[])
            return super().handler(request)

    vendor = capped(vendor=Holiday(cap=500))
    frame = await source(max_rows_per_request=500).fetch(request_for(days=90))

    assert vendor.requests, "the fake vendor was never called"
    assert not frame.empty
    assert frame.index[-1].date() >= date(2026, 2, 25)


# ── Coverage checks ───────────────────────────────────────────────────────


def frame_over(start: date, end: date) -> pd.DataFrame:
    index = pd.DatetimeIndex(
        pd.date_range(start=start, end=end, freq="15min", tz="UTC"), name="open_time"
    )
    return pd.DataFrame(
        {"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 0.0}, index=index
    )


def test_covers_accepts_a_weekend_shaped_shortfall():
    request = request_for(days=30)
    frame = frame_over(request.start, request.end - timedelta(days=2))
    assert covers(frame, request)


def test_covers_rejects_a_truncated_range():
    request = request_for(days=365)
    frame = frame_over(request.start, request.start + timedelta(days=100))
    assert not covers(frame, request)


def test_covers_rejects_history_that_starts_late():
    request = request_for(days=60)
    frame = frame_over(request.start + timedelta(days=20), request.end)
    assert not covers(frame, request)


def test_merge_keeps_the_union_and_prefers_the_newer_bar():
    older = frame_over(date(2026, 1, 1), date(2026, 1, 10))
    newer = frame_over(date(2026, 1, 5), date(2026, 1, 20)).assign(close=99.0)
    merged = merge_frames(older, newer)

    assert merged.index.is_unique
    assert merged.index[0] == older.index[0]
    assert merged.index[-1] == newer.index[-1]
    assert merged.loc[newer.index[0], "close"] == 99.0


def test_cache_round_trips_a_frame(tmp_path):
    cache = HistoryCache("tiingo", tmp_path)
    if not cache.available:
        pytest.skip("pyarrow is not installed in this environment")

    first = frame_over(date(2026, 1, 1), date(2026, 1, 10))
    cache.store(first, "XAUUSD", "M15")
    second = frame_over(date(2026, 1, 8), date(2026, 1, 20))
    cache.store(second, "XAUUSD", "M15")

    loaded = cache.load("XAUUSD", "M15")
    assert loaded.index[0] == first.index[0]
    assert loaded.index[-1] == second.index[-1]
    assert loaded.index.is_unique
    assert cache.path_for("XAUUSD", "M15").parent.name == "tiingo"


# ── Regressions from the first review ─────────────────────────────────────


def test_covers_accepts_a_start_on_a_closed_day():
    """A window opening on a Saturday has its first bar on the Sunday open.

    Demanding an exact match there made the cache miss roughly two days in
    seven, which is the traffic it exists to prevent.
    """
    request = request_for(days=90)
    frame = frame_over(request.start + timedelta(days=2), request.end)
    assert covers(frame, request)


def test_covers_still_rejects_a_materially_late_start():
    request = request_for(days=90)
    frame = frame_over(request.start + timedelta(days=30), request.end)
    assert not covers(frame, request)


def test_cursor_rereads_a_day_that_may_have_been_capped():
    """The row cap can land inside the window's own final day.

    "Ended where I asked" and "ran out" look identical by date, so a page that
    reached its row budget re-reads its last day instead of assuming the first.
    """
    window_end = date(2026, 2, 20)
    frame = frame_over(date(2026, 2, 1), window_end)
    instance = source(max_rows_per_request=len(frame))
    assert instance._next_cursor(frame, date(2026, 2, 1), window_end, date(2026, 3, 1)) == (
        window_end
    )


def test_cursor_advances_past_a_page_that_came_in_under_budget():
    """The ordinary case must not cost a second look at the same day."""
    window_end = date(2026, 2, 20)
    frame = frame_over(date(2026, 2, 1), window_end)
    instance = source(max_rows_per_request=len(frame) * 10)
    assert instance._next_cursor(frame, date(2026, 2, 1), window_end, date(2026, 3, 1)) == (
        window_end + timedelta(days=1)
    )


def test_cursor_stops_at_the_end_of_the_range():
    """Re-asking at the range end returns the same capped answer, so do not."""
    window_end = date(2026, 3, 1)
    frame = frame_over(date(2026, 2, 1), window_end)
    instance = source(max_rows_per_request=len(frame))
    assert instance._next_cursor(frame, date(2026, 2, 1), window_end, window_end) == (
        window_end + timedelta(days=1)
    )


def test_cursor_forces_progress_when_one_day_fills_a_page():
    instance = source(max_rows_per_request=1)
    day = date(2026, 2, 1)
    frame = frame_over(day, day)
    assert instance._next_cursor(frame, day, date(2026, 3, 1), date(2026, 3, 1)) == day + timedelta(
        days=1
    )


def test_the_page_ceiling_clears_a_three_year_m1_download():
    """`qte-backtest download` defaults to three years; M1 pages three days.

    A ceiling below that turns the silent truncation this module removes back
    on at a different layer.
    """
    instance = source()
    span = instance._page_span_days("M1")
    needed = (365 * 3) / span
    assert TiingoSettings(api_key="k").max_pages >= needed


def test_cache_replace_discards_what_it_held(tmp_path):
    cache = HistoryCache("tiingo", tmp_path)
    if not cache.available:
        pytest.skip("pyarrow is not installed in this environment")

    cache.store(frame_over(date(2026, 1, 1), date(2026, 1, 20)), "XAUUSD", "M15")
    kept = frame_over(date(2026, 1, 15), date(2026, 1, 20))
    cache.replace(kept, "XAUUSD", "M15")

    loaded = cache.load("XAUUSD", "M15")
    assert loaded.index[0] == kept.index[0], "replace must not merge the old range back in"
    assert len(loaded) == len(kept)


async def test_a_rate_limit_names_the_plan_not_the_status_code(monkeypatch):
    """Paging makes 429 routine, so the error has to be actionable."""
    from qte_shared.interfaces import ProviderError

    real_client = httpx.AsyncClient
    vendor = CappedTiingo(cap=400)
    calls = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["count"] += 1
        if calls["count"] > 2:
            return httpx.Response(429, json={"detail": "too many requests"})
        return vendor.handler(request)

    transport = httpx.MockTransport(handler)
    monkeypatch.setattr(httpx, "AsyncClient", lambda *a, **k: real_client(transport=transport))

    with pytest.raises(ProviderError) as raised:
        await source(max_rows_per_request=400).fetch(request_for(days=365))

    message = str(raised.value)
    assert "rate-limited" in message
    assert "page(s)" in message
    assert "QTE_TIINGO__MAX_ROWS_PER_REQUEST" in message


async def test_a_non_rate_limit_error_still_names_the_range(monkeypatch):
    from qte_shared.interfaces import ProviderError

    real_client = httpx.AsyncClient
    transport = httpx.MockTransport(lambda request: httpx.Response(500, json={}))
    monkeypatch.setattr(httpx, "AsyncClient", lambda *a, **k: real_client(transport=transport))

    with pytest.raises(ProviderError) as raised:
        await source().fetch(request_for(days=30))

    assert "500" in str(raised.value)
    assert "XAUUSD" in str(raised.value)
