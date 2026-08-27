"""The live loop: NATS candle closes in, broker signals out.

    NATS QTE.candle.closed.* → strategy.on_candle_closed → SignalFactory
        → BrokerSink (JetStream SIGNALS.<strategy> | HTTP webhook)
        → Postgres audit + QTE.signal.emitted mirror

The runner is the only component that knows this is *live*. Strategies see the
same frame and the same context object the backtest hands them, which is what
makes a backtested edge and a traded edge the same code path.

Two ordering rules matter here:

* **Stage, publish, then commit.** A durable outbox row is written before the
  broker call. Its UUID is the stable delivery id, so an ambiguous timeout can
  be retried without manufacturing a second command or a ghost local cycle.
* **Warm from Redis, not from the feed.** On boot the runner pulls its
  indicator window out of Redis rather than waiting hours for live candles to
  accumulate, so a restart resumes trading on the next close.

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
from datetime import UTC, datetime
from typing import Any

from nats.aio.msg import Msg
from qte_shared.bus import NatsBus, Subjects
from qte_shared.cache import RedisState
from qte_shared.config import settings
from qte_shared.db import EventRepository
from qte_shared.logging_setup import get_logger
from qte_shared.models import BrokerSignal, Candle, CandleClosedEvent, OpenPosition, TickEvent
from qte_shared.plugin_loader import load_strategies
from qte_shared.routing import SymbolRouting
from qte_shared.signal_factory import BracketPolicy, SignalFactory
from qte_shared.sizing import PositionSizer
from qte_shared.strategy_base import (
    SignalIntent,
    StrategyContext,
    StrategyLike,
    as_intents,
    candles_to_frame,
    overrides_on_tick,
)
from qte_shared.timeframes import normalize_timeframe

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
        self.slots: list[StrategySlot] = []
        self._by_subject: dict[tuple[str, str], list[StrategySlot]] = defaultdict(list)
        self._stopping = asyncio.Event()
        self._tasks: list[asyncio.Task[Any]] = []
        self._uncertain_pairs: set[tuple[str, str]] = set()
        self._cleaned = False

    # ── Startup ───────────────────────────────────────────────────────

    async def start(self) -> None:
        # First, before anything is connected. The pre-flight audit may refuse
        # to trade this book, and a refusal should cost nothing to unwind —
        # there is no sense opening NATS, Redis and the broker for strategies
        # we are about to reject. Off by configuration is the same call.
        run_preflight_audit()
        self._cleaned = False

        try:
            await self.bus.connect()
            await self.state.connect()
            await self.sink.start()

            self._build_slots()
            if not self.slots:
                raise RuntimeError(
                    f"No strategies loaded from {settings.engine.strategies_dir}. Clone your "
                    "private strategy repo into __strategies__/ (see README, phase 6)."
                )

            await self._restore_state()
            await self._recover_pending_deliveries()
            await self._subscribe()
            self._tasks.append(
                asyncio.create_task(self._delivery_retry_loop(), name="signal-outbox-retry")
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

    def _build_slots(self) -> None:
        """Instantiate one slot per (strategy, symbol) pair we are to trade.

        The routing table decides the pairs when there is one. Without it each
        strategy keeps the symbols it declares on itself, which is what
        happened before the table existed — see :mod:`qte_shared.routing`.
        """
        routing = SymbolRouting.load(settings.engine.routing_file)
        discovered = load_strategies(
            settings.engine.strategies_dir, runner_settings.enabled_strategies or None
        )
        if routing:
            self._warn_on_unrouted(routing, [entry.name for entry in discovered])

        for entry in discovered:
            defaults = runner_settings.strategy_params.get(entry.name, {})
            if routing:
                symbols = routing.symbols_for(entry.name)
                if not symbols:
                    log.info(
                        "Strategy %s is loaded but routed to no symbol in %s — not running it",
                        entry.name,
                        routing.source,
                    )
                    continue
            else:
                declared = entry.cls.symbols or settings.engine.symbols
                symbols = [symbol.upper() for symbol in declared]

            for symbol in symbols:
                # One instance per pair: a strategy carries per-symbol state
                # between bars, and sharing it across symbols would let gold's
                # last bar decide what happens on bitcoin's next one.
                params = {**defaults, **routing.params_for(symbol, entry.name)}
                strategy = entry.instantiate(params)
                # Size against the account, at the risk this pair is routed at.
                # The strategy is never told either — see qte_shared.sizing.
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
    def _warn_on_unrouted(routing: SymbolRouting, loaded: list[str]) -> None:
        """Say so when the table names a strategy the loader never found.

        Almost always a typo or a stale name after a rename, and the symptom
        without this line is a symbol that quietly trades nothing — which reads
        exactly like a strategy that found no setups.
        """
        unknown = [name for name in routing.strategies if name not in set(loaded)]
        if unknown:
            log.error(
                "Routing table %s names %s, which %s did not publish. Those symbols will "
                "trade nothing. Available: %s",
                routing.source,
                ", ".join(sorted(unknown)),
                settings.engine.strategies_dir,
                ", ".join(sorted(loaded)) or "none",
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
        """Refill candle buffers and the open cycle each slot was holding."""
        for slot in self.slots:
            candles = await self.state.get_candles(
                slot.symbol, slot.timeframe, slot.buffer.maxlen or 0
            )
            slot.buffer.extend(candles)
            await self._restore_position(slot)
            log.info(
                "Warm-up %s/%s %s: %d/%d candles from Redis",
                slot.strategy.name,
                slot.symbol,
                slot.timeframe,
                len(slot.buffer),
                slot.strategy.warmup,
            )

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
            await self._feed_candle_serialized(slot, candle)

    async def _feed_candle_serialized(self, slot: StrategySlot, candle: Candle) -> None:
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

        if not slot.is_warm:
            log.debug(
                "Still warming %s/%s: %d/%d",
                slot.strategy.name,
                slot.symbol,
                len(slot.buffer),
                slot.strategy.warmup,
            )
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
            if slot.symbol != event.symbol or not slot.started:
                continue
            async with slot.lock:
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
            enabled = bool(command.get("enabled", True))
            self.sink.set_shadow_mode(enabled)
            await self.state.set_flag("shadow_mode", enabled)
            await self.events.record_event(
                service=SERVICE_NAME,
                event="shadow_mode_changed",
                level="WARNING",
                payload={"enabled": enabled},
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
                        }
                    ),
                )
        else:
            log.warning("Unknown control action: %r", action)

    # ── Emission ──────────────────────────────────────────────────────

    async def _emit(
        self, slot: StrategySlot, intent: SignalIntent, fallback_price: float, moment: datetime
    ) -> None:
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

        delivery_id = await self.signals.stage_signal(
            signal,
            transport=self.sink.transport,
            shadow=self.sink.shadow_mode,
            recovery_context=slot.factory.pending_delivery_context(slot.symbol),
        )
        if delivery_id is None:
            slot.factory.discard_pending_delivery_context(slot.symbol)
            log.error(
                "Signal not sent because its outbox row could not be persisted "
                "strategy=%s symbol=%s uxid=%s",
                slot.strategy.name,
                slot.symbol,
                signal.signal_uxid,
            )
            return

        result = await self.sink.send(signal, delivery_id=delivery_id)

        # Only after a successful send (or a shadow run) does the cycle become
        # "the position we hold". Both the in-process factory and Redis are
        # committed here; build() deliberately left them unchanged.
        if result.status in {"sent", "shadow"}:
            slot.factory.commit(signal, delivery_id=delivery_id)
            try:
                persisted = await self._track_cycle(slot)
            except Exception:
                # The broker already accepted the signal and the outbox row is
                # still pending. Keep the pair blocked so startup recovery can
                # reconcile it before another decision changes the cycle.
                log.exception(
                    "Failed to persist cycle strategy=%s symbol=%s uxid=%s",
                    slot.strategy.name,
                    slot.symbol,
                    signal.signal_uxid,
                )
                persisted = False
            if persisted:
                await self.signals.mark_delivery(delivery_id, status=result.status)
            else:
                self._uncertain_pairs.add(slot.key)
        else:
            await self.signals.mark_delivery(
                delivery_id,
                status=result.status,
                error=result.detail,
            )
            if result.status == "unknown":
                self._uncertain_pairs.add(slot.key)
            else:
                slot.factory.discard_pending_delivery_context(slot.symbol)

        await self.bus.publish(
            self.subjects.signal_emitted(),
            {
                "signal": signal.model_dump(mode="json"),
                "delivery": {
                    "id": delivery_id,
                    "status": result.status,
                    "transport": result.transport,
                },
                "reason": intent.reason,
                "emitted_at": datetime.now(UTC).isoformat(),
            },
        )

    async def _recover_pending_deliveries(self, *, unknown_only: bool = False) -> None:
        """Replay durable outbox rows with their original de-duplication ids."""
        slots = {slot.key: slot for slot in self.slots}
        rows = (
            await self.signals.pending_deliveries(statuses=("unknown",))
            if unknown_only
            else await self.signals.pending_deliveries()
        )
        for row in rows:
            signal = BrokerSignal.model_validate(row.payload["payload"])
            key = (signal.strategy, signal.symbol.upper())
            slot = slots.get(key)
            if slot is None:
                self._uncertain_pairs.add(key)
                log.error(
                    "Cannot recover signal delivery id=%s: no active slot for %s/%s",
                    row.id,
                    *key,
                )
                continue

            async with slot.lock:
                delivery_id = str(row.id)
                slot.factory.restore_pending_delivery_context(
                    slot.symbol, self.signals.recovery_context(row)
                )
                original_transport = getattr(row, "transport", self.sink.transport)
                original_shadow = getattr(row, "shadow", self.sink.shadow_mode)
                if original_transport != self.sink.transport:
                    self._uncertain_pairs.add(key)
                    log.error(
                        "Cannot recover delivery id=%s over %s: it was staged for %s",
                        delivery_id,
                        self.sink.transport,
                        original_transport,
                    )
                    continue
                if original_shadow:
                    # A staged paper trade must never become live because the
                    # operator changed configuration before recovery.
                    result = DeliveryResult(status="shadow", transport=original_transport)
                elif self.sink.shadow_mode:
                    # The inverse transition is unsafe too: calling send would
                    # label a previously-live command as shadow without ever
                    # reconciling it with the broker.
                    self._uncertain_pairs.add(key)
                    log.error(
                        "Cannot recover live delivery id=%s while shadow mode is enabled",
                        delivery_id,
                    )
                    continue
                else:
                    result = await self.sink.send(signal, delivery_id=delivery_id)
                if result.status in {"sent", "shadow"}:
                    slot.factory.commit(signal, delivery_id=delivery_id)
                    try:
                        persisted = await self._track_cycle(slot)
                    except Exception:
                        log.exception(
                            "Failed to persist recovered cycle delivery_id=%s", delivery_id
                        )
                        persisted = False
                    if persisted:
                        await self.signals.mark_delivery(delivery_id, status=result.status)
                        self._uncertain_pairs.discard(key)
                    else:
                        self._uncertain_pairs.add(key)
                else:
                    await self.signals.mark_delivery(
                        delivery_id,
                        status=result.status,
                        error=result.detail,
                    )
                    if result.status == "unknown":
                        self._uncertain_pairs.add(key)
                    else:
                        self._uncertain_pairs.discard(key)
                        slot.factory.discard_pending_delivery_context(slot.symbol)

    async def _delivery_retry_loop(self) -> None:
        """Continuously reconcile ambiguous sends without requiring a restart."""
        while not self._stopping.is_set():
            await asyncio.sleep(runner_settings.delivery_retry_interval)
            try:
                await self._recover_pending_deliveries(unknown_only=True)
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
        for close in (self.sink.stop, self.state.close, self.bus.close):
            try:
                await close()
            except Exception:
                cleanup_failed = True
                log.exception("Resource cleanup failed during runner shutdown")
        self._cleaned = not cleanup_failed
        log.info("Runner stopped")


def _encode(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, default=str).encode()


__all__ = ["StrategyRunner", "StrategySlot"]
