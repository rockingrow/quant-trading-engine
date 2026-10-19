"""A bar ingestion joined late is completed from the vendor, or sent as built.

The merge rule is the point. The vendor supplies what the engine cannot have
seen — the open, and any extreme that printed before it joined — while the
engine keeps its own close and counts, which are fresher than a vendor bar
fetched a second after the bucket ended.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pandas as pd

from qte_ingestion.repair import PartialBarRepairer, merge_partial_bar
from qte_shared.interfaces import HistoryRequest, HistorySource
from qte_shared.models import Candle

OPEN_TIME = datetime(2026, 9, 10, 9, 45, tzinfo=UTC)


def partial_bar() -> Candle:
    """The 09:45 bar the audit's restart published after 1.5 minutes of ticks."""
    return Candle(
        symbol="XAUUSD",
        timeframe="M15",
        open_time=OPEN_TIME,
        open=4393.26,
        high=4394.52,
        low=4392.68,
        close=4394.49,
        volume=0.0,
        tick_count=137,
    )


def vendor_frame(open_time: datetime = OPEN_TIME) -> pd.DataFrame:
    """Tiingo's final bar for that bucket, as the history source returns it."""
    bar_index = pd.DatetimeIndex([pd.Timestamp(open_time)], name="open_time")
    return pd.DataFrame(
        {
            "open": [4393.26],
            "high": [4396.03],
            "low": [4392.65],
            "close": [4394.17],
            "volume": [0.0],
        },
        index=bar_index,
    )


class VendorSource(HistorySource):
    def __init__(
        self, history: pd.DataFrame | None = None, failure: Exception | None = None
    ) -> None:
        self.history = history if history is not None else vendor_frame()
        self.failure = failure
        self.requests: list[HistoryRequest] = []

    async def fetch(self, request: HistoryRequest) -> pd.DataFrame:
        self.requests.append(request)
        if self.failure is not None:
            raise self.failure
        return self.history


class SlowVendorSource(VendorSource):
    async def fetch(self, request: HistoryRequest) -> pd.DataFrame:
        await asyncio.sleep(1)
        return await super().fetch(request)


def test_the_merge_takes_the_vendor_open_the_widest_range_and_the_engine_close():
    vendor_bar = partial_bar().model_copy(
        update={"open": 4390.0, "high": 4396.03, "low": 4393.0, "close": 4394.17, "tick_count": 0}
    )

    merged = merge_partial_bar(partial_bar(), vendor_bar)

    assert merged.open == 4390.0
    assert merged.high == 4396.03, "the vendor saw a high the engine missed"
    assert merged.low == 4392.68, "the engine saw a low the vendor's bar does not have"
    assert merged.close == 4394.49
    assert merged.tick_count == 137


async def test_a_partial_bar_is_completed_from_the_vendors_bar():
    source = VendorSource()

    repaired = await PartialBarRepairer(source, {"XAUUSD": "fx"}).repair(partial_bar())

    assert (repaired.open, repaired.high, repaired.low, repaired.close) == (
        4393.26,
        4396.03,
        4392.65,
        4394.49,
    )
    request = source.requests[0]
    assert (request.start, request.end, request.market) == (
        OPEN_TIME.date(),
        OPEN_TIME.date(),
        "fx",
    )


async def test_a_vendor_failure_publishes_the_bar_as_built(caplog):
    source = VendorSource(failure=ConnectionError("Tiingo is down"))

    with caplog.at_level("WARNING"):
        repaired = await PartialBarRepairer(source, {"XAUUSD": "fx"}).repair(partial_bar())

    assert repaired == partial_bar()
    assert any("unrepaired" in record.getMessage() for record in caplog.records)


async def test_a_vendor_slower_than_the_timeout_publishes_the_bar_as_built(caplog):
    """The repair sits on the path that reads the socket; a slow vendor must not."""
    repairer = PartialBarRepairer(SlowVendorSource(), {"XAUUSD": "fx"}, timeout_seconds=0.05)

    with caplog.at_level("WARNING"):
        repaired = await repairer.repair(partial_bar())

    assert repaired == partial_bar()
    assert any("TimeoutError" in record.getMessage() for record in caplog.records)


async def test_a_bucket_the_vendor_has_no_bar_for_is_published_as_built(caplog):
    source = VendorSource(history=vendor_frame(OPEN_TIME.replace(minute=30)))

    with caplog.at_level("WARNING"):
        repaired = await PartialBarRepairer(source, {"XAUUSD": "fx"}).repair(partial_bar())

    assert repaired == partial_bar()
    assert any("no bar for that bucket" in record.getMessage() for record in caplog.records)


async def test_a_provider_without_history_publishes_without_a_warning(caplog):
    with caplog.at_level("WARNING"):
        repaired = await PartialBarRepairer(None, {"XAUUSD": "fx"}).repair(partial_bar())

    assert repaired == partial_bar()
    assert not [record for record in caplog.records if record.levelname == "WARNING"]


async def test_a_symbol_with_no_configured_market_is_not_guessed():
    source = VendorSource()

    repaired = await PartialBarRepairer(source, {}).repair(partial_bar())

    assert repaired == partial_bar()
    assert source.requests == [], "the market is never guessed from the symbol"


async def test_a_bar_closed_after_an_outage_takes_the_vendors_close():
    """Found by the fix verification: a bar restored after a restart that outlasted its
    bucket heard its last tick before the outage, so its own close was minutes stale."""
    restored_bar = partial_bar().model_copy(update={"close": 4393.9, "tick_count": 36})

    whole_bar = await PartialBarRepairer(VendorSource(), {"XAUUSD": "fx"}).repair(
        restored_bar, close_is_current=False
    )

    assert whole_bar.close == 4394.17, "the vendor heard the bucket's last price"
    assert (whole_bar.open, whole_bar.high, whole_bar.low) == (4393.26, 4396.03, 4392.65)
    assert whole_bar.tick_count == 36
