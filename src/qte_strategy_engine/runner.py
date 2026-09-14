"""The live loop: NATS candle closes in, broker signals out.

    NATS QTE.candle.closed.* → strategy.on_candle_closed → SignalFactory
        → BrokerSink (JetStream SIGNALS.<strategy> | HTTP webhook)
        → Postgres audit + QTE.signal.emitted mirror

The runner is the only component that knows this is *live*. Strategies see the
same frame and the same context object the backtest hands them, which is what
makes a backtested edge and a traded edge the same code path.

Three ordering rules matter here:

* **Stage, publish, then commit.** A durable outbox row is written before the
  broker call. Its UUID is the stable delivery id, so an ambiguous timeout can
  be retried without manufacturing a second command or a ghost local cycle.
* **Warm from Redis, not from the feed.** On boot the runner pulls its
  indicator window out of Redis rather than waiting hours for live candles to
  accumulate, so a restart resumes trading on the next close.
* **Catch up before listening.** Core NATS does not replay, so a close
  published while the runner was down or still starting never arrives. Each
  pair records the newest bar it was fed, and on boot every newer bar in Redis
  is fed as a close — decided on while at most ``QTE_RUNNER__CATCH_UP_MAX_AGE``
  old, kept as history otherwise — with every slot held, so a live close cannot
  overtake it.

Position state is written twice on purpose. The cycle a pair is holding goes to
Redis (hot, read on every bar) *and* to Postgres (durable), and boot prefers
Redis and falls back to the table. A re-provisioned cache would otherwise be
indistinguishable from "flat", and the runner would mint a second cycle against
a position the broker still has open.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections import defaultdict, deque
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from nats.aio.msg import Msg

from qte_shared.bus import NatsBus, Subjects
from qte_shared.cache import RedisState
from qte_shared.config import settings
from qte_shared.db import EventRepository
from qte_shared.interfaces.market_data import ProviderError
from qte_shared.logging_setup import get_logger
from qte_shared.models import BrokerSignal, Candle, CandleClosedEvent, OpenPosition, TickEvent
from qte_shared.providers import get_provider_class
from qte_shared.strategies.mapping import SymbolMapping
from qte_shared.strategies.plugin_loader import load_strategies
from qte_shared.strategies.signal_factory import BracketPolicy, SignalFactory
from qte_shared.strategies.signal_serialization import signal_record
from qte_shared.strategies.sizing import PositionSizer
from qte_shared.strategies.strategy_base import (
    SignalIntent,
    StrategyContext,
    StrategyLike,
    as_intents,
    candles_to_frame,
    overrides_on_tick,
)
from qte_shared.timeframes import (
    CLOCK_TOLERANCE,
    bucket_close,
    normalize_timeframe,
    timeframe_seconds,
)
from qte_strategy_engine.broker_sink import BrokerSink, DeliveryResult
from qte_strategy_engine.db import OpenPositionRepository, SignalRepository
from qte_strategy_engine.preflight import run_preflight_audit
from qte_strategy_engine.settings import runner_settings

log = get_logger(__name__)

SERVICE_NAME = "strategy-runner"


class StrategySlot:
    """One strategy bound to one symbol, with its own candle buffer and cycle."""

    def __init__(self, strategy: StrategyLike, symbol: str, factory: SignalFactory) -> None:
        self.strategy = strategy
        self.symbol = symbol
        self.factory = factory
        self.timeframe = normalize_timeframe(strategy.timeframe)
        # Exactly the window the backtest hands the same strategy — the bound
        # lives on the strategy contract so the two drivers cannot drift apart.
        self.buffer: deque[Candle] = deque(maxlen=strategy.history_window())
        self.started = False
        # Candle and tick subscriptions are independent NATS callbacks. Keep a
        # strategy instance and its position cycle a single-writer aggregate so
        # both callbacks cannot decide they are flat and publish two entries.
        self.lock = asyncio.Lock()

    @property
    def key(self) -> tuple[str, str]:
        return (self.strategy.name, self.symbol)

    @property
    def is_warm(self) -> bool:
        return len(self.buffer) >= self.strategy.warmup


class StrategyRunner:
    """Loads the plugins, subscribes, and drives them for the process lifetime."""

    def __init__(self, sink: BrokerSink | None = None) -> None:
        self.bus = NatsBus(name="qte-strategy-runner")
        self.state = RedisState()
        self.subjects = Subjects()
        self.events = EventRepository()
        self.signals = SignalRepository()
        self.positions = OpenPositionRepository()
        self.sink = sink or BrokerSink()
        self._configured_shadow_mode = self.sink.shadow_mode
        self._history_started_at = datetime.now(UTC)
        self._owner_id = str(uuid4())
        self._ownership_acquired = False
        self.slots: list[StrategySlot] = []
        self._by_subject: dict[tuple[str, str], list[StrategySlot]] = defaultdict(list)
        self._stopping = asyncio.Event()
        self._tasks: list[asyncio.Task[Any]] = []
        self._uncertain_pairs: set[tuple[str, str]] = set()
        self._unconfirmed_staging: set[tuple[str, str]] = set()
        self._pending_results: dict[str, DeliveryResult] = {}
        self._cleaned = False

    # ── Startup ───────────────────────────────────────────────────────

    async def start(self) -> None:
        # First, before anything is connected. The pre-flight audit may refuse
        # to trade this book, and a refusal should cost nothing to unwind —
        # there is no sense opening NATS, Redis and the broker for strategies
        # we are about to reject. Off by configuration is the same call.
        run_preflight_audit()
        self._cleaned = False
        self._history_started_at = datetime.now(UTC)

        try:
            await self.bus.connect()
            await self.state.connect()
            if not await self.state.claim_runner(self._owner_id):
                raise RuntimeError(
                    "Runner ownership is already held in this Redis namespace. Stop the "
                    "other runner; after an unclean exit verify it is gone before clearing "
                    "the runner:owner key. Automatic failover is disabled."
                )
            self._ownership_acquired = True
            await self._refresh_shadow_mode()
            await self.sink.start()

            self._build_slots()
            if not self.slots:
                raise RuntimeError(
                    f"No strategies loaded from {settings.engine.strategies_dir}. Clone your "
                    "private strategy repo into __strategies__/ (see README, phase 6)."
                )

            await self._restore_state()
            await self._recover_pending_deliveries()
            # Every slot is held while the subscriptions go live and the missed
            # closes are replayed. A close that arrives meanwhile waits for the
            # replay, then finds its bar already fed and is ignored, so no bar is
            # decided on twice or behind a newer one.
            async with contextlib.AsyncExitStack() as held_slots:
                for strategy_slot in self.slots:
                    await held_slots.enter_async_context(strategy_slot.lock)
                await self._subscribe()
                await self._catch_up()
            self._tasks.append(
                asyncio.create_task(self._delivery_retry_loop(), name="signal-outbox-retry")
            )
            self._tasks.append(
                asyncio.create_task(self._history_sync_loop(), name="runner-history-sync")
            )

            await self.events.record_event(
                service=SERVICE_NAME,
                event="started",
                payload={
                    "strategies": [slot.strategy.describe() for slot in self.slots],
                    "shadow_mode": self.sink.shadow_mode,
                    "transport": self.sink.transport,
                },
            )
            log.info(
                "Runner started slots=%d shadow_mode=%s transport=%s",
                len(self.slots),
                self.sink.shadow_mode,
                self.sink.transport,
            )
        except BaseException:
            await self._close_resources(record_event=False)
            raise

    async def _refresh_shadow_mode(self) -> None:
        """Read the durable control before recovery and every new delivery.

        Missing state uses the configured default. Invalid/unreadable state
        fails closed, including when a control broadcast never reached us.
        """
        try:
            stored_mode = await self.state.get_flag("shadow_mode", None)
            if stored_mode is not None and not isinstance(stored_mode, bool):
                raise ValueError("Persisted shadow_mode must be a boolean")
        except Exception:
            self.sink.set_shadow_mode(True)
            raise
        effective_mode = settings.broker.force_shadow_mode or (
            self._configured_shadow_mode if stored_mode is None else stored_mode
        )
        if self.sink.shadow_mode != effective_mode:
            self.sink.set_shadow_mode(effective_mode)

    async def _check_ownership(self) -> None:
        """Refuse all decisions and effects after shutdown or ownership loss."""
        try:
            owns_state = self._ownership_acquired and await self.state.owns_runner(self._owner_id)
            if self._stopping.is_set() or not owns_state:
                raise RuntimeError("Runner no longer owns the trading state")
        except Exception:
            self.sink.set_shadow_mode(True)
            self.request_stop()
            raise

    def _build_slots(self) -> None:
        """Instantiate one slot per (strategy, symbol) pair we are to trade.

        The mapping table decides the pairs when there is one. Without it each
        strategy keeps the symbols it declares on itself, which is what
        happened before the table existed — see :mod:`qte_shared.strategies.mapping`.
        """
        mapping = SymbolMapping.load(settings.engine.mapping_file)
        discovered = load_strategies(settings.engine.strategies_dir)
        if mapping:
            loaded_names = [entry.name for entry in discovered]
            self._warn_on_unmapped(mapping, loaded_names)
            self._warn_on_unused_strategy_defaults(mapping, loaded_names)

        for entry in discovered:
            defaults = mapping.defaults_for(entry.name)
            if mapping:
                symbols = mapping.symbols_for(entry.name)
                if not symbols:
                    log.info(
                        "Strategy %s is loaded but mapped to no symbol in %s — not running it",
                        entry.name,
                        mapping.source,
                    )
                    continue
            else:
                declared = entry.cls.symbols or settings.engine.symbols
                symbols = [symbol.upper() for symbol in declared]

            for symbol in symbols:
                # One instance per pair: a strategy carries per-symbol state
                # between bars, and sharing it across symbols would let gold's
                # last bar decide what happens on bitcoin's next one.
                # Params layer: the strategy's [strategies.<name>] defaults,
                # then this pair's [symbols.<symbol>.params.<name>] on top.
                params = {**defaults, **mapping.params_for(symbol, entry.name)}
                strategy = entry.instantiate(params)
                # Size against the account, at the risk this pair is mapped at.
                # The strategy is never told either — see qte_shared.strategies.sizing.
                sizer = PositionSizer.from_settings(params)
                factory = SignalFactory(
                    strategy.name,
                    timeframe=strategy.timeframe,
                    bracket=BracketPolicy(),
                    inputs=strategy.params,
                    sizer=sizer,
                    default_quantity=runner_settings.default_quantity,
                )
                slot = StrategySlot(strategy, symbol, factory)
                self._warn_if_history_exceeds_redis(slot)
                self.slots.append(slot)
                self._by_subject[(symbol, slot.timeframe)].append(slot)
                log.info(
                    "Slot ready strategy=%s symbol=%s tf=%s warmup=%d risk=%.3f%% of %.2f",
                    strategy.name,
                    symbol,
                    slot.timeframe,
                    strategy.warmup,
                    sizer.risk_percent,
                    sizer.capital,
                )

    @staticmethod
    def _warn_on_unmapped(mapping: SymbolMapping, loaded: list[str]) -> None:
        """Say so when the table names a strategy the loader never found.

        Almost always a typo or a stale name after a rename, and the symptom
        without this line is a symbol that quietly trades nothing — which reads
        exactly like a strategy that found no setups.
        """
        unknown = [name for name in mapping.strategies if name not in set(loaded)]
        if unknown:
            log.error(
                "Mapping table %s names %s, which %s did not publish. Those symbols will "
                "trade nothing. Available: %s",
                mapping.source,
                ", ".join(sorted(unknown)),
                settings.engine.strategies_dir,
                ", ".join(sorted(loaded)) or "none",
            )

    @staticmethod
    def _warn_on_unused_strategy_defaults(mapping: SymbolMapping, loaded: list[str]) -> None:
        """Say so when ``[strategies.<name>]`` names a strategy nobody publishes.

        Unlike an unmapped pairing this trades nothing wrong — the defaults are
        simply never read — but a typo here means an intended ``risk_percent``
        override silently does not apply, so it is worth a line.
        """
        unknown = [name for name in mapping.strategy_defaults if name not in set(loaded)]
        if unknown:
            log.warning(
                "Mapping table %s has [strategies.*] defaults for %s, which %s did not "
                "publish — those defaults will not apply anywhere.",
                mapping.source,
                ", ".join(sorted(unknown)),
                settings.engine.strategies_dir,
            )

    @staticmethod
    def _warn_if_history_exceeds_redis(slot: StrategySlot) -> None:
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

    async def _restore_state(self) -> None:
        """Refill candle buffers and the open cycle each slot was holding.

        A slot that has been fed before takes only the bars up to the newest one
        it was fed. Anything Redis holds beyond that closed while this runner was
        not listening, and :meth:`_catch_up` feeds it as a close rather than
        leaving it in the window as history nobody decided on.
        """
        allow_future = self._provider_is_synthetic()
        for strategy_slot in self.slots:
            candles = await self._stored_history(strategy_slot, allow_future=allow_future)
            decided = await self._decided_open_time(strategy_slot, allow_future=allow_future)
            if decided is not None:
                candles = [candle for candle in candles if candle.open_time <= decided]
            strategy_slot.buffer.extend(candles)
            await self._restore_position(strategy_slot)
            log.info(
                "Warm-up %s/%s %s: %d/%d candles from Redis",
                strategy_slot.strategy.name,
                strategy_slot.symbol,
                strategy_slot.timeframe,
                len(strategy_slot.buffer),
                strategy_slot.strategy.warmup,
            )

    async def _stored_history(
        self, strategy_slot: StrategySlot, *, allow_future: bool
    ) -> list[Candle]:
        """The slot's window as Redis holds it, cleaned of what cannot be history."""
        stored = await self.state.get_candles(
            strategy_slot.symbol, strategy_slot.timeframe, strategy_slot.buffer.maxlen or 0
        )
        return _clean_history(stored, strategy_slot, allow_future=allow_future)

    async def _decided_open_time(
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
        decided = await self.state.get_decided_open_time(
            strategy_slot.strategy.name, strategy_slot.symbol, strategy_slot.timeframe
        )
        moment = datetime.now(UTC)
        if decided is None or allow_future or decided <= moment + CLOCK_TOLERANCE:
            return decided
        oldest_catch_up_close = moment - timedelta(seconds=runner_settings.catch_up_max_age)
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

    async def _catch_up(self) -> None:
        """Feed every close Redis holds that this runner was not listening for.

        Called with every slot lock held and the subscriptions already live. A
        bar that closed within ``QTE_RUNNER__CATCH_UP_MAX_AGE`` is fed like a
        live close, strategy decision included. An older one joins the window as
        history only: deciding on it now would send an entry at a price the
        strategy never saw, which is not the trade a backtest would record.
        """
        for strategy_slot in self.slots:
            await self._sync_history(strategy_slot)

    async def _sync_history(
        self, strategy_slot: StrategySlot, *, before_open: datetime | None = None
    ) -> None:
        """Merge cached history and replay unseen closes with the slot lock held.

        Redis is authoritative about which buckets traded. We never invent
        candles across a session break. A live event bounds the read so no
        later cached bar can overtake it or leak into its indicator window.
        """
        await self._check_ownership()
        allow_future = self._provider_is_synthetic()
        stored = await self._stored_history(strategy_slot, allow_future=allow_future)
        if before_open is not None:
            stored = [candle for candle in stored if candle.open_time < before_open]
        newest_fed = strategy_slot.buffer[-1].open_time if strategy_slot.buffer else None
        if newest_fed is None:
            newest_fed = await self._decided_open_time(strategy_slot, allow_future=allow_future)
            if newest_fed is None:
                # Initial history may arrive after start() returned. Warm it
                # without manufacturing past decisions. A close produced after
                # this process started is still a live event even when its
                # first NATS notification was lost.
                for candle in stored:
                    if (
                        bucket_close(candle.open_time, strategy_slot.timeframe)
                        <= self._history_started_at
                    ):
                        strategy_slot.buffer.append(candle)
                    else:
                        await self._feed_if_current(strategy_slot, candle)
                return
        historical = {candle.open_time: candle for candle in strategy_slot.buffer}
        historical.update(
            (candle.open_time, candle) for candle in stored if candle.open_time <= newest_fed
        )
        strategy_slot.buffer.clear()
        strategy_slot.buffer.extend(historical[moment] for moment in sorted(historical))
        for candle in stored:
            if candle.open_time > newest_fed:
                await self._feed_if_current(strategy_slot, candle)

    async def _feed_if_current(self, strategy_slot: StrategySlot, candle: Candle) -> None:
        closed_ago = datetime.now(UTC) - bucket_close(candle.open_time, strategy_slot.timeframe)
        if closed_ago <= timedelta(seconds=runner_settings.catch_up_max_age):
            await self._feed_candle_serialized(strategy_slot, candle)
        else:
            await self._check_ownership()
            strategy_slot.buffer.append(candle)
            await self._record_decided(strategy_slot, candle)
            log.warning(
                "Skipped the decision on missed close of %s/%s at %s: older than "
                "QTE_RUNNER__CATCH_UP_MAX_AGE; retained as history",
                strategy_slot.strategy.name,
                strategy_slot.symbol,
                candle.open_time,
            )

    async def _history_sync_loop(self) -> None:
        """Repair lost messages and late backfills without waiting for another tick."""
        while not self._stopping.is_set():
            await asyncio.sleep(runner_settings.history_sync_interval)
            try:
                await self._refresh_shadow_mode()
                for strategy_slot in self.slots:
                    async with strategy_slot.lock:
                        await self._sync_history(strategy_slot)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("History synchronization failed; retrying at the next interval")

    async def _record_decided(self, strategy_slot: StrategySlot, candle: Candle) -> None:
        """Remember *candle* as the newest bar this slot was fed. Never raises.

        A lost write can let a restart within ``QTE_RUNNER__CATCH_UP_MAX_AGE``
        feed the same bar again. That is the lesser failure next to letting a
        Redis error cost the strategy the decision it is about to make.
        """
        try:
            await self.state.set_decided_open_time(
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
    def _provider_is_synthetic() -> bool:
        """Whether the configured feed invents its prices, as the dev simulator does."""
        try:
            return get_provider_class(settings.market_data.provider).synthetic
        except (ProviderError, ImportError):
            return False

    async def _restore_position(self, slot: StrategySlot) -> None:
        """Reload the cycle this slot holds — Redis first, Postgres behind it.

        The fallback is the point. Redis coming up empty is ambiguous: it means
        "flat" and it means "someone re-provisioned the cache", and acting on
        the wrong reading opens a second cycle against a position the broker is
        still carrying. The table settles it, and re-seeds the cache so the
        next boot is a cache hit again.
        """
        strategy, symbol = slot.strategy.name, slot.symbol
        position = await self.state.get_open_position(strategy, symbol)
        source = "redis"
        if position is None:
            position = await self.positions.get(strategy, symbol)
            source = "postgres"
            if position is not None:
                await self.state.set_open_position(position)

        if position is None:
            return
        slot.factory.restore_position(position, symbol=symbol)
        log.info(
            "Restored open cycle from %s strategy=%s symbol=%s uxid=%s qty=%s remaining=%s",
            source,
            strategy,
            symbol,
            position.signal_uxid,
            position.quantity,
            position.remaining,
        )

    async def _subscribe(self) -> None:
        for symbol, timeframe in sorted(self._by_subject):
            await self.bus.subscribe(
                self.subjects.candle_closed(symbol, timeframe),
                self._on_candle_message,
                queue=runner_settings.queue_group,
            )
        await self.bus.subscribe(self.subjects.engine_control(), self._on_control_message)

        wants_ticks = runner_settings.subscribe_ticks or any(
            overrides_on_tick(slot.strategy) for slot in self.slots
        )
        if wants_ticks:
            await self.bus.subscribe(self.subjects.tick_wildcard(), self._on_tick_message)
            log.info("Tick subscription active — a strategy overrides on_tick")

    # ── Message handlers ──────────────────────────────────────────────

    async def _on_candle_message(self, msg: Msg) -> None:
        event = CandleClosedEvent.model_validate_json(msg.data)
        slots = self._by_subject.get((event.symbol, normalize_timeframe(event.timeframe)), [])
        for slot in slots:
            await self._feed_candle(slot, event.candle)

    async def _feed_candle(self, slot: StrategySlot, candle: Candle) -> None:
        async with slot.lock:
            if slot.buffer and candle.open_time <= slot.buffer[-1].open_time:
                return
            await self._sync_history(slot, before_open=candle.open_time)
            await self._feed_if_current(slot, candle)

    async def _feed_candle_serialized(self, slot: StrategySlot, candle: Candle) -> None:
        await self._check_ownership()
        if slot.buffer and candle.open_time <= slot.buffer[-1].open_time:
            # A redelivery or a duplicate close. Acting on it twice would open a
            # second position on a signal the strategy already made once.
            log.debug(
                "Ignoring non-advancing candle %s %s at %s",
                slot.strategy.name,
                slot.symbol,
                candle.open_time,
            )
            return
        slot.buffer.append(candle)
        # Recorded before the strategy runs, not after it: a crash in between
        # costs this one decision, where recording afterwards would let the next
        # start's catch-up decide the same bar a second time.
        await self._record_decided(slot, candle)

        if not slot.is_warm:
            log.debug(
                "Still warming %s/%s: %d/%d",
                slot.strategy.name,
                slot.symbol,
                len(slot.buffer),
                slot.strategy.warmup,
            )
            return

        if slot.key in self._uncertain_pairs:
            return

        context = StrategyContext(
            symbol=slot.symbol,
            timeframe=slot.timeframe,
            now=candle.open_time,
            mode="live",
            params=slot.strategy.params,
            open_uxid=slot.factory.open_cycle(slot.symbol),
        )
        if not slot.started:
            slot.strategy.on_start(context)
            slot.started = True

        frame = candles_to_frame(list(slot.buffer))
        try:
            result = slot.strategy.on_candle_closed(frame, context)
        except Exception:
            # A crashing plugin must not take the runner down with it — the
            # other strategies are still trading.
            log.exception(
                "Strategy %s raised on %s %s", slot.strategy.name, slot.symbol, candle.open_time
            )
            await self.events.record_event(
                service=SERVICE_NAME,
                event="strategy_error",
                level="ERROR",
                payload={"strategy": slot.strategy.name, "symbol": slot.symbol},
            )
            return

        for intent in as_intents(result):
            await self._emit(slot, intent, candle.close, context.now)

    async def _on_tick_message(self, msg: Msg) -> None:
        event = TickEvent.model_validate_json(msg.data)
        price = event.tick.price
        for slot in self.slots:
            if slot.symbol != event.symbol or not slot.started or slot.key in self._uncertain_pairs:
                continue
            async with slot.lock:
                await self._check_ownership()
                context = StrategyContext(
                    symbol=slot.symbol,
                    timeframe=slot.timeframe,
                    now=event.tick.ts,
                    mode="live",
                    params=slot.strategy.params,
                    open_uxid=slot.factory.open_cycle(slot.symbol),
                )
                try:
                    result = slot.strategy.on_tick(price, context)
                except Exception:
                    log.exception(
                        "Strategy %s raised on tick for %s", slot.strategy.name, slot.symbol
                    )
                    continue
                for intent in as_intents(result):
                    await self._emit(slot, intent, price, event.tick.ts)

    async def _on_control_message(self, msg: Msg) -> None:
        """Control plane: today, the shadow-mode switch from the API."""
        try:
            command = json.loads(msg.data)
        except json.JSONDecodeError:
            log.warning("Unparseable control message: %.120r", msg.data)
            return

        action = command.get("action")
        if action == "set_shadow_mode":
            # The CLI already persisted the command. Re-read it instead of
            # allowing an older, delayed broadcast to overwrite a newer mode.
            await self._refresh_shadow_mode()
            await self.events.record_event(
                service=SERVICE_NAME,
                event="shadow_mode_changed",
                level="WARNING",
                payload={"enabled": self.sink.shadow_mode},
            )
        elif action == "ping":
            if msg.reply:
                await self.bus.nc.publish(
                    msg.reply,
                    _encode(
                        {
                            "service": SERVICE_NAME,
                            "slots": len(self.slots),
                            "shadow_mode": self.sink.shadow_mode,
                            "ready": all(slot.is_warm for slot in self.slots)
                            and not self._uncertain_pairs,
                        }
                    ),
                )
        else:
            log.warning("Unknown control action: %r", action)

    # ── Emission ──────────────────────────────────────────────────────

    async def _emit(
        self, slot: StrategySlot, intent: SignalIntent, fallback_price: float, moment: datetime
    ) -> None:
        await self._check_ownership()
        await self._refresh_shadow_mode()
        if slot.key in self._uncertain_pairs:
            log.error(
                "Dropped intent for uncertain delivery strategy=%s symbol=%s; "
                "the durable outbox must reconcile first",
                slot.strategy.name,
                slot.symbol,
            )
            return
        if intent.price is None:
            intent.price = fallback_price

        # Size is not filled in here: `build()` risk-sizes the entry against
        # the account and rescales the strategy's closes to match, so the
        # backtest and this loop put the same number on the wire.
        try:
            signal = slot.factory.build(
                intent,
                symbol=slot.symbol,
                moment=moment,
                commit=False,
            )
        except ValueError as exc:
            log.warning("Dropped intent from %s: %s", slot.strategy.name, exc)
            return

        original_shadow = self.sink.shadow_mode
        delivery_id = await self.signals.stage_signal(
            signal,
            transport=self.sink.transport,
            shadow=original_shadow,
            recovery_context=slot.factory.pending_delivery_context(slot.symbol),
        )
        if delivery_id is None:
            # The insert may have committed before its connection failed.
            # Do not create a second command until a successful outbox scan
            # proves whether this pair has an unfinished row.
            self._uncertain_pairs.add(slot.key)
            self._unconfirmed_staging.add(slot.key)
            slot.factory.discard_pending_delivery_context(slot.symbol)
            log.error(
                "Signal not sent because its outbox row could not be persisted "
                "strategy=%s symbol=%s uxid=%s",
                slot.strategy.name,
                slot.symbol,
                signal.signal_uxid,
            )
            return

        self._uncertain_pairs.add(slot.key)
        result = await self._send_staged_signal(signal, delivery_id, shadow=original_shadow)
        if result is None:
            return
        if await self._finish_delivery(slot, signal, delivery_id, result):
            self._uncertain_pairs.discard(slot.key)

        await self.bus.publish(
            self.subjects.signal_emitted(),
            {
                "signal": signal_record(signal),
                "delivery": {
                    "id": delivery_id,
                    "status": result.status,
                    "transport": result.transport,
                },
                "reason": intent.reason,
                "emitted_at": datetime.now(UTC).isoformat(),
            },
        )

    async def _send_staged_signal(
        self, signal: BrokerSignal, delivery_id: str, *, shadow: bool
    ) -> DeliveryResult | None:
        """Record uncertainty before crossing the broker boundary."""
        await self._check_ownership()
        await self._refresh_shadow_mode()
        if shadow:
            return DeliveryResult(status="shadow", transport=self.sink.transport)
        if self.sink.shadow_mode:
            log.error(
                "Cannot recover live delivery id=%s while shadow mode is enabled", delivery_id
            )
            return None
        if not await self.signals.mark_delivery(delivery_id, status="unknown"):
            return None
        await self._check_ownership()
        await self._refresh_shadow_mode()
        if self.sink.shadow_mode:
            return None
        try:
            return await self.sink.send(signal, delivery_id=delivery_id)
        except Exception:
            log.exception("Broker call raised after staging delivery id=%s", delivery_id)
            return DeliveryResult(status="unknown", transport=self.sink.transport)

    async def _finish_delivery(
        self,
        strategy_slot: StrategySlot,
        signal: BrokerSignal,
        delivery_id: str,
        outcome: DeliveryResult,
    ) -> bool:
        """Checkpoint broker acceptance separately from local state persistence.

        A known result stays in memory if its checkpoint fails. After a crash,
        the pre-send unknown marker prevents blind resends outside a configured
        deduplication horizon. Accepted checkpoints only retry local writes.
        """
        if outcome.status != "unknown":
            self._pending_results[delivery_id] = outcome
        try:
            if outcome.status in {"sent", "shadow"}:
                if not await self.signals.mark_delivery(
                    delivery_id, status=f"{outcome.status}_pending"
                ):
                    return False
                strategy_slot.factory.commit(signal, delivery_id=delivery_id)
                if not await self._track_cycle(strategy_slot):
                    return False
            finalized = await self.signals.mark_delivery(
                delivery_id, status=outcome.status, error=outcome.detail or None
            )
            if not finalized or outcome.status == "unknown":
                return False
        except Exception:
            log.exception("Could not reconcile delivery id=%s; pair remains blocked", delivery_id)
            return False
        self._pending_results.pop(delivery_id, None)
        strategy_slot.factory.discard_pending_delivery_context(strategy_slot.symbol)
        return True

    async def _recover_pending_deliveries(self) -> None:
        """Reconcile every unfinished phase without starving rows beyond page one."""
        await self._check_ownership()
        await self._refresh_shadow_mode()
        active_slots = {strategy_slot.key: strategy_slot for strategy_slot in self.slots}
        blocked_pairs: set[tuple[str, str]] = set()
        resolved_pairs: set[tuple[str, str]] = set()
        after_cursor = None
        while True:
            pending_rows = await self.signals.pending_deliveries(
                after_cursor=after_cursor, include_ids=tuple(self._pending_results)
            )
            if not pending_rows:
                break
            after_cursor = (pending_rows[-1].created_at, pending_rows[-1].id)
            for pending_row in pending_rows:
                pairing = (pending_row.strategy, pending_row.symbol.upper())
                self._uncertain_pairs.add(pairing)
                strategy_slot = active_slots.get(pairing)
                if strategy_slot is None or pairing in blocked_pairs:
                    blocked_pairs.add(pairing)
                    continue
                try:
                    async with strategy_slot.lock:
                        resolved = await self._recover_delivery(strategy_slot, pending_row)
                except Exception:
                    log.exception("Could not recover delivery id=%s", pending_row.id)
                    resolved = False
                if resolved:
                    resolved_pairs.add(pairing)
                else:
                    blocked_pairs.add(pairing)
        # Do not unblock between rows: an older unresolved row blocks every
        # later row of its pair, including those on another page.
        self._uncertain_pairs.difference_update(
            (resolved_pairs | self._unconfirmed_staging) - blocked_pairs
        )
        self._unconfirmed_staging.clear()

    async def _recover_delivery(self, strategy_slot: StrategySlot, pending_row: Any) -> bool:
        await self._check_ownership()
        delivery_id = str(pending_row.id)
        # A scan may see a prepared row while its original sender still holds
        # this lock. Never resend that stale snapshot after the sender finishes.
        pending_row = await self.signals.get_delivery(delivery_id)
        if pending_row is None:
            raise ValueError("Outbox row disappeared during recovery")
        if pending_row.delivery_status in {"sent", "shadow", "failed"}:
            if delivery_id not in self._pending_results:
                return True
        signal = BrokerSignal.model_validate(pending_row.payload["payload"])
        if (signal.strategy, signal.symbol.upper()) != strategy_slot.key:
            raise ValueError("Outbox payload does not match its stored strategy and symbol")
        strategy_slot.factory.restore_pending_delivery_context(
            strategy_slot.symbol, self.signals.recovery_context(pending_row)
        )
        outcome = self._pending_results.get(delivery_id)
        if outcome is None and pending_row.delivery_status in {"sent_pending", "shadow_pending"}:
            outcome = DeliveryResult(
                status=pending_row.delivery_status.removesuffix("_pending"),
                transport=pending_row.transport,
            )
        if outcome is None:
            if pending_row.transport != self.sink.transport:
                log.error("Transport changed for unfinished delivery id=%s", delivery_id)
                return False
            if not pending_row.shadow and pending_row.delivery_status != "prepared":
                elapsed = (datetime.now(UTC) - pending_row.created_at).total_seconds()
                retry_horizon = runner_settings.delivery_retry_max_age
                if retry_horizon <= 0 or not 0 <= elapsed < retry_horizon:
                    log.error(
                        "Delivery id=%s requires broker reconciliation: outside the configured "
                        "deduplication horizon; no automatic resend",
                        delivery_id,
                    )
                    return False
            was_ambiguous = pending_row.delivery_status in {"pending", "unknown"}
            outcome = await self._send_staged_signal(signal, delivery_id, shadow=pending_row.shadow)
            if outcome is not None and outcome.status == "failed" and was_ambiguous:
                # Rejecting a retry does not disprove acceptance of the first
                # ambiguous attempt. Keep the pair blocked for reconciliation.
                outcome = DeliveryResult(
                    status="unknown", transport=outcome.transport, detail=outcome.detail
                )
        if outcome is None:
            return False
        return await self._finish_delivery(strategy_slot, signal, delivery_id, outcome)

    async def _delivery_retry_loop(self) -> None:
        """Continuously reconcile ambiguous sends without requiring a restart."""
        while not self._stopping.is_set():
            await asyncio.sleep(runner_settings.delivery_retry_interval)
            try:
                await self._recover_pending_deliveries()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Signal outbox retry failed — retrying at the next interval")

    async def _track_cycle(self, slot: StrategySlot) -> bool:
        """Mirror the slot's cycle into Redis and Postgres, or clear both.

        Written from what the factory now believes rather than from the action
        that was just sent, so the "a TP1 taking the whole entry ends the
        cycle" rule is decided once — in :class:`OpenPosition` — instead of
        being restated by everything that persists a transition.
        """
        position = slot.factory.open_position(slot.symbol)
        if position is None:
            await self.state.clear_open_cycle(slot.strategy.name, slot.symbol)
            return await self.positions.clear(slot.strategy.name, slot.symbol)
        return await self._persist_position(position)

    async def _persist_position(self, position: OpenPosition) -> bool:
        await self.state.set_open_position(position)
        return await self.positions.upsert(position)

    # ── Shutdown ──────────────────────────────────────────────────────

    def request_stop(self) -> None:
        self._stopping.set()

    async def run_forever(self) -> None:
        try:
            await self.start()
            await self._stopping.wait()
        finally:
            await self.stop()

    async def stop(self) -> None:
        await self._close_resources(record_event=True)

    async def _close_resources(self, *, record_event: bool) -> None:
        """Release acquired resources in reverse order; safe after partial start."""
        if getattr(self, "_cleaned", False):
            return
        cleanup_failed = False
        self._stopping.set()
        for task in self._tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                cleanup_failed = True
                log.exception("Task cleanup failed during runner shutdown")
        self._tasks.clear()
        for slot in self.slots:
            with contextlib.suppress(Exception):
                slot.strategy.on_stop()
        if record_event:
            with contextlib.suppress(Exception):
                await self.events.record_event(service=SERVICE_NAME, event="stopped")
        # Drain callbacks before releasing ownership: a queued close must not
        # run alongside the next owner. Uncertain cleanup retains the claim.
        for close in (self.bus.close, self.sink.stop):
            try:
                await close()
            except Exception:
                cleanup_failed = True
                log.exception("Resource cleanup failed during runner shutdown")
        if self._ownership_acquired and not cleanup_failed:
            try:
                await self.state.release_runner(self._owner_id)
                self._ownership_acquired = False
            except Exception:
                cleanup_failed = True
                log.exception("Could not release runner ownership")
        try:
            await self.state.close()
        except Exception:
            cleanup_failed = True
            log.exception("Redis cleanup failed during runner shutdown")
        self._cleaned = not cleanup_failed
        log.info("Runner stopped")


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


def _encode(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, default=str).encode()


__all__ = ["StrategyRunner", "StrategySlot"]
