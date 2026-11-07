"""The runner's candle window: warm it from Redis, and feed what it missed.

Core NATS does not replay, so a close published while the runner was down, or
whose message was lost, never arrives on the subscription. Redis holds every
close ingestion staged, so this class reads it back:

* at start, the window each slot warms on (:meth:`HistorySync.stored_history`,
  cut at the newest bar the slot was fed, :meth:`HistorySync.decided_open_time`);
* right after the subscriptions go live, every newer close, fed as a live close
  while it is at most ``QTE_RUNNER__CATCH_UP_MAX_AGE`` old and kept as history
  otherwise (:meth:`HistorySync.catch_up`);
* every ``QTE_RUNNER__HISTORY_SYNC_INTERVAL`` afterwards, to repair a lost
  message or a late backfill (:meth:`HistorySync.run_loop`).

Deciding on a bar stays the runner's: a close that is current is handed to
:meth:`RunnerHooks.feed_candle_serialized`.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from qte_shared.config import settings
from qte_shared.logging_setup import get_logger
from qte_shared.models import Candle
from qte_shared.timeframes import CLOCK_TOLERANCE, bucket_close, timeframe_seconds
from qte_strategy_engine.runner_context import RunnerContext, RunnerHooks, provider_is_synthetic
from qte_strategy_engine.settings import RunnerSettings, runner_settings
from qte_strategy_engine.slot import StrategySlot

log = get_logger(__name__)


class HistorySync:
    """Keeps every slot's window in step with what Redis holds."""

    def __init__(
        self,
        context: RunnerContext,
        hooks: RunnerHooks,
        *,
        settings: RunnerSettings | None = None,
    ) -> None:
        self._context = context
        self._hooks = hooks
        self._settings = settings or runner_settings

    async def stored_history(
        self, strategy_slot: StrategySlot, *, allow_future: bool
    ) -> list[Candle]:
        """The slot's window as Redis holds it, cleaned of what cannot be history."""
        stored = await self._context.state.get_candles(
            strategy_slot.symbol, strategy_slot.timeframe, strategy_slot.buffer.maxlen or 0
        )
        trusted = [candle for candle in stored if self._context.scope.accepts(candle.origin)]
        return _clean_history(trusted, strategy_slot, allow_future=allow_future)

    async def decided_open_time(
        self, strategy_slot: StrategySlot, *, allow_future: bool
    ) -> datetime | None:
        """The newest bar this slot was fed, or where to draw that line instead.

        A mark dated after now was written while a synthetic feed ran ahead of
        the clock, and because every live feed rewrites it, it also means no bar
        of the current feed has been fed since. Taken at face value it would file
        every close this runner missed as already decided. So the line is drawn
        at ``QTE_RUNNER__CATCH_UP_MAX_AGE`` instead: older bars warm the window,
        newer ones are caught up.
        """
        decided = await self._context.state.get_decided_open_time(
            strategy_slot.strategy.name, strategy_slot.symbol, strategy_slot.timeframe
        )
        moment = datetime.now(UTC)
        if decided is None or allow_future or decided <= moment + CLOCK_TOLERANCE:
            return decided
        oldest_catch_up_close = moment - timedelta(seconds=self._settings.catch_up_max_age)
        log.warning(
            "Ignoring the decided-bar mark %s of %s/%s %s: it is dated after now, so a feed "
            "running ahead of the clock wrote it. Bars that closed before %s warm the window "
            "and later ones are caught up.",
            decided.isoformat(),
            strategy_slot.strategy.name,
            strategy_slot.symbol,
            strategy_slot.timeframe,
            oldest_catch_up_close.isoformat(),
        )
        return oldest_catch_up_close - timedelta(seconds=timeframe_seconds(strategy_slot.timeframe))

    async def catch_up(self) -> None:
        """Feed every close Redis holds that this runner was not listening for.

        Called with every slot lock held and the subscriptions already live. A
        bar that closed within ``QTE_RUNNER__CATCH_UP_MAX_AGE`` is fed like a
        live close, strategy decision included. An older one joins the window as
        history only: deciding on it now would send an entry at a price the
        strategy never saw, which is not the trade a backtest would record.
        """
        for strategy_slot in self._context.slots:
            await self.sync_history(strategy_slot)

    async def sync_history(
        self, strategy_slot: StrategySlot, *, before_open: datetime | None = None
    ) -> None:
        """Merge cached history and replay unseen closes with the slot lock held.

        Redis is authoritative about which buckets traded. We never invent
        candles across a session break. A live event bounds the read so no
        later cached bar can overtake it or leak into its indicator window.
        """
        await self._hooks.check_ownership()
        allow_future = provider_is_synthetic()
        stored = await self.stored_history(strategy_slot, allow_future=allow_future)
        if before_open is not None:
            stored = [candle for candle in stored if candle.open_time < before_open]
        newest_fed = strategy_slot.buffer[-1].open_time if strategy_slot.buffer else None
        if newest_fed is None:
            newest_fed = await self.decided_open_time(strategy_slot, allow_future=allow_future)
            if newest_fed is None:
                # Initial history may arrive after start() returned. Warm it
                # without manufacturing past decisions. A close produced after
                # this process started is still a live event even when its
                # first NATS notification was lost.
                for candle in stored:
                    if (
                        bucket_close(candle.open_time, strategy_slot.timeframe)
                        <= self._context.history_started_at
                    ):
                        strategy_slot.buffer.append(candle)
                    else:
                        await self.feed_if_current(strategy_slot, candle)
                return
        historical = {candle.open_time: candle for candle in strategy_slot.buffer}
        historical.update(
            (candle.open_time, candle) for candle in stored if candle.open_time <= newest_fed
        )
        strategy_slot.buffer.clear()
        strategy_slot.buffer.extend(historical[moment] for moment in sorted(historical))
        for candle in stored:
            if candle.open_time > newest_fed:
                await self.feed_if_current(strategy_slot, candle)

    async def feed_if_current(self, strategy_slot: StrategySlot, candle: Candle) -> None:
        closed_ago = datetime.now(UTC) - bucket_close(candle.open_time, strategy_slot.timeframe)
        if closed_ago <= timedelta(seconds=self._settings.catch_up_max_age):
            await self._hooks.feed_candle_serialized(strategy_slot, candle)
        else:
            await self._hooks.check_ownership()
            strategy_slot.buffer.append(candle)
            await self.record_decided(strategy_slot, candle)
            log.warning(
                "Skipped the decision on missed close of %s/%s at %s: older than "
                "QTE_RUNNER__CATCH_UP_MAX_AGE; retained as history",
                strategy_slot.strategy.name,
                strategy_slot.symbol,
                candle.open_time,
            )

    async def run_loop(self) -> None:
        """Repair lost messages and late backfills without waiting for another tick."""
        while not self._context.stopping.is_set():
            await asyncio.sleep(self._settings.history_sync_interval)
            try:
                await self._hooks.refresh_shadow_mode()
                for strategy_slot in self._context.slots:
                    async with strategy_slot.lock:
                        await self.sync_history(strategy_slot)
            except asyncio.CancelledError:
                raise
            except (RedisConnectionError, RedisTimeoutError) as transient:
                # A dropped pooled connection or a read that outran its deadline
                # costs this tick and nothing else: the next pass five seconds
                # later re-reads the flag and re-syncs every slot. A traceback
                # here is noise, and ERROR forwards it to Telegram.
                #
                # Only Redis's own exceptions, deliberately. The bare builtin
                # TimeoutError would also catch a history request/reply that the
                # MT5 ingester never answered — nats-py raises
                # asyncio.TimeoutError, which *is* the builtin on 3.13 — and a
                # dead history provider reported as "Redis was unreachable" at
                # WARNING is an outage nobody is told about. redis-py converts
                # its own socket timeouts into RedisTimeoutError, so nothing
                # this branch is meant for is lost.
                log.warning(
                    "History synchronization skipped a pass; Redis was unreachable (%s: %s)",
                    type(transient).__name__,
                    transient,
                )
            except Exception:
                log.exception("History synchronization failed; retrying at the next interval")

    async def record_decided(self, strategy_slot: StrategySlot, candle: Candle) -> None:
        """Remember *candle* as the newest bar this slot was fed. Never raises.

        A lost write can let a restart within ``QTE_RUNNER__CATCH_UP_MAX_AGE``
        feed the same bar again. That is the lesser failure next to letting a
        Redis error cost the strategy the decision it is about to make.
        """
        try:
            await self._context.state.set_decided_open_time(
                strategy_slot.strategy.name,
                strategy_slot.symbol,
                strategy_slot.timeframe,
                candle.open_time,
            )
        except Exception:
            log.exception(
                "Could not record the decided bar %s for %s/%s %s",
                candle.open_time.isoformat(),
                strategy_slot.strategy.name,
                strategy_slot.symbol,
                strategy_slot.timeframe,
            )

    @staticmethod
    def warn_if_history_exceeds_redis(slot: StrategySlot) -> None:
        """Say so when live can never give the strategy its backtest window.

        The backtest reads the whole parquet file, so it can always satisfy the
        window. A restarted runner refills from Redis, which keeps only
        ``QTE_REDIS__CANDLE_HISTORY`` bars — ask for more than that and the two
        drivers feed the same strategy different amounts of history, silently.
        """
        wanted = slot.strategy.history_window()
        retained = settings.redis.candle_history
        if wanted is None:
            log.warning(
                "Strategy %s sets max_history=0 (unbounded). Live it will see at most "
                "%d candles — whatever Redis retained — while a backtest sees the whole "
                "file. Set an explicit max_history to make the two agree.",
                slot.strategy.name,
                retained,
            )
        elif wanted > retained:
            log.warning(
                "Strategy %s wants %d candles but Redis retains %d. After a restart it "
                "will run on a shorter window than it was backtested on; raise "
                "QTE_REDIS__CANDLE_HISTORY to at least %d.",
                slot.strategy.name,
                wanted,
                retained,
                wanted,
            )


def _clean_history(
    candles: list[Candle], strategy_slot: StrategySlot, *, allow_future: bool
) -> list[Candle]:
    """Redis history as a window: one bar per open time, and none from the future.

    A duplicate open time is what an earlier backfill left behind when it stored
    a bucket still forming and ingestion later closed the same bucket. The later
    entry is the live close, so it wins. A bar whose bucket has not ended cannot
    be market history either -- unless the configured feed is the simulator,
    which anchors its bars ahead of the clock on purpose.
    """
    by_open_time: dict[datetime, Candle] = {}
    for candle in candles:
        by_open_time[candle.open_time] = candle
    ordered = [by_open_time[open_time] for open_time in sorted(by_open_time)]
    duplicate_count = len(candles) - len(ordered)
    if duplicate_count:
        log.warning(
            "Dropped %d duplicate bar(s) from the %s/%s %s history in Redis",
            duplicate_count,
            strategy_slot.strategy.name,
            strategy_slot.symbol,
            strategy_slot.timeframe,
        )
    if allow_future:
        return ordered
    horizon = datetime.now(UTC) + CLOCK_TOLERANCE
    finished = [
        candle
        for candle in ordered
        if bucket_close(candle.open_time, strategy_slot.timeframe) <= horizon
    ]
    if len(finished) < len(ordered):
        log.error(
            "Ignored %d bar(s) dated after now in the %s/%s %s history, newest %s. "
            "Ingestion discards such state on its next start.",
            len(ordered) - len(finished),
            strategy_slot.strategy.name,
            strategy_slot.symbol,
            strategy_slot.timeframe,
            ordered[-1].open_time.isoformat(),
        )
    return finished


__all__ = ["HistorySync"]
