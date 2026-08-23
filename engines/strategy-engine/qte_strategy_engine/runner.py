"""The live loop: NATS candle closes in, broker signals out.

    NATS QTE.candle.closed.* → strategy.on_candle_closed → SignalFactory
        → BrokerSink (JetStream SIGNALS.<strategy> | HTTP webhook)
        → Postgres audit + QTE.signal.emitted mirror

The runner is the only component that knows this is *live*. Strategies see the
same frame and the same context object the backtest hands them, which is what
makes a backtested edge and a traded edge the same code path.

Two ordering rules matter here:

* **Publish, then audit.** A slow Postgres must never delay a trade, so the
  signal goes to the broker first and the audit row is written after — and an
  audit failure is logged, never raised.
* **Warm from Redis, not from the feed.** On boot the runner pulls its
  indicator window out of Redis rather than waiting hours for live candles to
  accumulate, so a restart resumes trading on the next close.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections import defaultdict, deque
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

from nats.aio.msg import Msg
from qte_shared.bus import NatsBus, Subjects
from qte_shared.cache import RedisState
from qte_shared.config import settings
from qte_shared.db import EventRepository
from qte_shared.logging_setup import get_logger
from qte_shared.models import Candle, CandleClosedEvent, SignalAction, TickEvent
from qte_shared.plugin_loader import load_strategies
from qte_shared.signal_factory import BracketPolicy, SignalFactory
from qte_shared.strategy_base import (
    SignalIntent,
    StrategyBase,
    StrategyContext,
    candles_to_frame,
)
from qte_shared.timeframes import normalize_timeframe

from qte_strategy_engine.broker_sink import BrokerSink
from qte_strategy_engine.db import SignalRepository
from qte_strategy_engine.settings import runner_settings

log = get_logger(__name__)

SERVICE_NAME = "strategy-runner"


class StrategySlot:
    """One strategy bound to one symbol, with its own candle buffer and cycle."""

    def __init__(self, strategy: StrategyBase, symbol: str, factory: SignalFactory) -> None:
        self.strategy = strategy
        self.symbol = symbol
        self.factory = factory
        self.timeframe = normalize_timeframe(strategy.timeframe)
        # Keep a little more than the strategy asked for: an indicator that
        # needs N bars needs N *valid* bars, and the oldest few are consumed by
        # its own warm-up before it produces a number at all.
        self.buffer: deque[Candle] = deque(maxlen=max(strategy.warmup * 2, 400))
        self.started = False

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
        self.sink = sink or BrokerSink()
        self.slots: list[StrategySlot] = []
        self._by_subject: dict[tuple[str, str], list[StrategySlot]] = defaultdict(list)
        self._stopping = asyncio.Event()
        self._tasks: list[asyncio.Task[Any]] = []

    # ── Startup ───────────────────────────────────────────────────────

    async def start(self) -> None:
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
        await self._subscribe()

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

    def _build_slots(self) -> None:
        """Instantiate one slot per (strategy, symbol) pair we are to trade."""
        discovered = load_strategies(
            settings.engine.strategies_dir, runner_settings.enabled_strategies or None
        )
        for entry in discovered:
            params = runner_settings.strategy_params.get(entry.name, {})
            strategy = entry.instantiate(params)
            symbols = [s.upper() for s in (strategy.symbols or settings.engine.symbols)]
            for symbol in symbols:
                factory = SignalFactory(
                    strategy.name,
                    timeframe=strategy.timeframe,
                    bracket=BracketPolicy(),
                    inputs=strategy.params,
                )
                slot = StrategySlot(strategy, symbol, factory)
                self.slots.append(slot)
                self._by_subject[(symbol, slot.timeframe)].append(slot)
                log.info(
                    "Slot ready strategy=%s symbol=%s tf=%s warmup=%d",
                    strategy.name,
                    symbol,
                    slot.timeframe,
                    strategy.warmup,
                )

    async def _restore_state(self) -> None:
        """Refill candle buffers and open-cycle ids from Redis."""
        for slot in self.slots:
            candles = await self.state.get_candles(
                slot.symbol, slot.timeframe, slot.buffer.maxlen or 0
            )
            slot.buffer.extend(candles)
            cycle = await self.state.get_open_cycle(slot.strategy.name, slot.symbol)
            if cycle:
                slot.factory.restore_cycles({slot.symbol: cycle})
                log.info(
                    "Restored open cycle strategy=%s symbol=%s uxid=%s",
                    slot.strategy.name,
                    slot.symbol,
                    cycle,
                )
            log.info(
                "Warm-up %s/%s %s: %d/%d candles from Redis",
                slot.strategy.name,
                slot.symbol,
                slot.timeframe,
                len(slot.buffer),
                slot.strategy.warmup,
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
            type(slot.strategy).on_tick is not StrategyBase.on_tick for slot in self.slots
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

        for intent in _as_intents(result):
            await self._emit(slot, intent, candle.close, context.now)

    async def _on_tick_message(self, msg: Msg) -> None:
        event = TickEvent.model_validate_json(msg.data)
        price = event.tick.price
        for slot in self.slots:
            if slot.symbol != event.symbol or not slot.started:
                continue
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
                log.exception("Strategy %s raised on tick for %s", slot.strategy.name, slot.symbol)
                continue
            for intent in _as_intents(result):
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
        if intent.price is None:
            intent.price = fallback_price
        if intent.action.is_entry and intent.quantity is None:
            intent.quantity = runner_settings.default_quantity

        try:
            signal = slot.factory.build(intent, symbol=slot.symbol, moment=moment)
        except ValueError as exc:
            log.warning("Dropped intent from %s: %s", slot.strategy.name, exc)
            return

        result = await self.sink.send(signal)

        # Only after a successful send (or a shadow run) does the cycle become
        # "the position we hold". Recording it after a failed publish would
        # leave the runner tracking a trade the broker never received.
        if result.status != "failed":
            await self._track_cycle(slot, signal.position.action, signal.signal_uxid)

        await self.signals.record_signal(
            signal,
            transport=result.transport,
            delivery_status=result.status,
            shadow=self.sink.shadow_mode,
            delivery_error=result.detail if result.status == "failed" else None,
        )
        await self.bus.publish(
            self.subjects.signal_emitted(),
            {
                "signal": signal.model_dump(mode="json"),
                "delivery": {"status": result.status, "transport": result.transport},
                "reason": intent.reason,
                "emitted_at": datetime.now(UTC).isoformat(),
            },
        )

    async def _track_cycle(self, slot: StrategySlot, action: SignalAction, uxid: str) -> None:
        if action.is_entry:
            await self.state.set_open_cycle(slot.strategy.name, slot.symbol, uxid)
        elif action in (SignalAction.TP2, SignalAction.SL, SignalAction.R_SL, SignalAction.FLAT):
            await self.state.clear_open_cycle(slot.strategy.name, slot.symbol)

    # ── Shutdown ──────────────────────────────────────────────────────

    def request_stop(self) -> None:
        self._stopping.set()

    async def run_forever(self) -> None:
        await self.start()
        try:
            await self._stopping.wait()
        finally:
            await self.stop()

    async def stop(self) -> None:
        self._stopping.set()
        for task in self._tasks:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        for slot in self.slots:
            with contextlib.suppress(Exception):
                slot.strategy.on_stop()
        await self.events.record_event(service=SERVICE_NAME, event="stopped")
        await self.sink.stop()
        await self.bus.close()
        await self.state.close()
        log.info("Runner stopped")


def _as_intents(result: Any) -> Sequence[SignalIntent]:
    if result is None:
        return ()
    if isinstance(result, SignalIntent):
        return (result,)
    return tuple(result)


def _encode(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, default=str).encode()


__all__ = ["StrategyRunner", "StrategySlot"]
