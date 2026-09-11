"""Refuse to build on candle state this feed did not write.

Redis outlives a change of provider. A dev stack rehearsed on the simulator
leaves forward-anchored bars — timestamps days ahead of the clock — and an open
bar in a bucket that has not begun yet. Pointed at a real vendor afterwards,
ingestion restored that open bar and dropped every real tick as "behind" it,
backfill merged the invented history in ahead of the vendor's, and the runner
warmed on it. Nothing failed; the only symptom was a stream of late-tick
warnings.

So before ingestion reads anything back, this module decides whether the candle
state belongs to the market it is about to feed. It does not when:

* **another provider wrote it** — the name is recorded under
  ``qte:history:provider`` on every start, so a switch shows on the next one;
* **nobody recorded who wrote it** — state from before that key existed has no
  provenance at all, and a simulator replay placed on past buckets looks exactly
  like vendor history, so unmarked state is discarded once rather than trusted;
  or
* **it is dated in the future** — a closed bar whose bucket has not ended, an
  open bar that has not begun, or such a close still waiting in the outbox. A
  live vendor cannot produce one. The simulator can, by design, which is why a
  synthetic provider is exempt from this test and only from this one.

Foreign state is discarded: each subscribed pair's candle list and open bar, and
the candle outbox. That is a cache, which backfill rebuilds from the vendor in
seconds. Open positions are **not** touched: a cycle is trading state rather
than market history, and deleting one could orphan a position the broker still
holds.
"""

from __future__ import annotations

from datetime import UTC, datetime

from qte_shared.logging_setup import get_logger
from qte_shared.market_data_plan import SymbolFeed
from qte_shared.timeframes import CLOCK_TOLERANCE, bucket_close

log = get_logger(__name__)


async def discard_foreign_candle_state(
    candle_state,
    subscriptions: list[SymbolFeed],
    *,
    provider_name: str,
    synthetic: bool,
    moment: datetime | None = None,
) -> bool:
    """Discard candle state *provider_name* did not write; return whether it did.

    The provider is recorded as the writer afterwards in every case, so the next
    start can tell a switch from a restart.
    """
    horizon = (moment or datetime.now(UTC)) + CLOCK_TOLERANCE
    reasons: list[str] = []

    recorded = await candle_state.get_history_provider()
    if recorded is None:
        if await _holds_candle_state(candle_state, subscriptions):
            reasons.append("no provider was ever recorded as its writer")
    elif recorded != provider_name:
        reasons.append(f"it was written by provider {recorded!r}")
    if not synthetic:
        future_bars = await _future_dated_bars(candle_state, subscriptions, horizon)
        if future_bars:
            reasons.append("it holds bars dated after now (" + "; ".join(future_bars) + ")")

    if reasons:
        log.warning(
            "Discarding candle state before ingestion starts on provider %r, because %s. "
            "The candle lists, open bars and candle outbox are rebuilt from the feed and its "
            "history; open positions are left untouched.",
            provider_name,
            " and ".join(reasons),
        )
        for symbol_feed in subscriptions:
            for timeframe in symbol_feed.timeframes:
                await candle_state.discard_candle_state(symbol_feed.symbol, timeframe)
        await candle_state.discard_candle_outbox()

    await candle_state.set_history_provider(provider_name)
    return bool(reasons)


async def _holds_candle_state(candle_state, subscriptions: list[SymbolFeed]) -> bool:
    """Whether any subscribed pair has history or an open bar, or a close is staged."""
    for symbol_feed in subscriptions:
        for timeframe in symbol_feed.timeframes:
            if await candle_state.get_candles(symbol_feed.symbol, timeframe, count=1):
                return True
            if await candle_state.get_open_candle(symbol_feed.symbol, timeframe) is not None:
                return True
    return bool(await candle_state.pending_candles())


async def _future_dated_bars(
    candle_state, subscriptions: list[SymbolFeed], horizon: datetime
) -> list[str]:
    """Describe each bar that no live feed could have produced by *horizon*."""
    described: list[str] = []
    for symbol_feed in subscriptions:
        for timeframe in symbol_feed.timeframes:
            newest = await candle_state.get_candles(symbol_feed.symbol, timeframe, count=1)
            if newest and bucket_close(newest[-1].open_time, timeframe) > horizon:
                described.append(
                    f"{symbol_feed.symbol} {timeframe} closed bar at "
                    f"{newest[-1].open_time.isoformat()}"
                )
            open_bar = await candle_state.get_open_candle(symbol_feed.symbol, timeframe)
            if open_bar is not None and open_bar.open_time > horizon:
                described.append(
                    f"{symbol_feed.symbol} {timeframe} open bar at {open_bar.open_time.isoformat()}"
                )
    for candle in await candle_state.pending_candles():
        if bucket_close(candle.open_time, candle.timeframe) > horizon:
            described.append(
                f"{candle.symbol} {candle.timeframe} staged close at {candle.open_time.isoformat()}"
            )
    return described


__all__ = ["discard_foreign_candle_state"]
