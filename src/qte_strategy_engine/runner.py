"""The live loop: NATS candle closes in, broker signals out.

    NATS QTE.candle.closed.* → strategy.on_candle_closed → SignalFactory
        → BrokerSink (JetStream SIGNALS.<strategy> | HTTP webhook)
        → Postgres audit + QTE.signal.emitted mirror

The runner is the only component that knows this is *live*. Strategies see the
same frame and the same context object the backtest hands them, which is what
makes a backtested edge and a traded edge the same code path.

This class is the coordinator: it owns the lifecycle, the ownership claim, the
slots and the decision path, and hands the rest to four collaborators that share
one :class:`~qte_strategy_engine.runner_context.RunnerContext` and reach back
only through the :class:`~qte_strategy_engine.runner_context.RunnerHooks` it
implements:

* :class:`~qte_strategy_engine.delivery.DeliveryPipeline` — the signal outbox;
* :class:`~qte_strategy_engine.recovery.PositionRecovery` — restore, stale flush,
  orphan settlement;
* :class:`~qte_strategy_engine.history_sync.HistorySync` — warm-up and catch-up;
* :class:`~qte_strategy_engine.control_plane.ControlPlane` — ``qte-control`` and
  the bot.

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

One decision here is the runner's own rather than a strategy's. While a
strategy's declared weekend window is open — see
:mod:`qte_shared.strategies.strategy_settings` — its entries are dropped and any
open cycle is closed with a ``FLAT``, because a market that is shut cannot be
stopped out of. The backtest replay applies the identical rule, so the two still
decide the same things; the periodic sweep that also flattens between bars is
the one part with no replay counterpart, and it only fires at moments a backtest
has no bar for.

``QTE_ACCOUNT__MAX_OPEN_POSITIONS`` is the runner's other own decision, and this
one has no replay counterpart at all: a backtest replays one strategy and the
ceiling counts every strategy and symbol on the account together. It is off by
default for that reason — a configured ceiling makes the traded book a subset of
the replayed one, which is a divergence an operator chooses knowingly because
the account enforces its own limit whatever the engine believes.

Position state is written twice on purpose. The cycle a pair is holding goes to
Redis (hot, read on every bar) *and* to Postgres (durable), and boot prefers
Redis and falls back to the table. A re-provisioned cache would otherwise be
indistinguishable from "flat", and the runner would mint a second cycle against
a position the broker still has open.

Restoring a cycle is the right answer for a restart. Age alone closes nothing:
a strategy can be right to hold a cycle for days, so the stale flush
(``QTE_RUNNER__FLUSH_STALE_POSITIONS``) is ``off`` unless an operator asks for
it. What *is* closed on start is a cycle the mapping table no longer claims —
nothing would decide on it — see :mod:`qte_strategy_engine.recovery`.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections import defaultdict
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from nats.aio.msg import Msg

from qte_shared.bar_gate import BAR_GATE_FLAG, BarGate
from qte_shared.bus import NatsBus, Subjects
from qte_shared.cache import RedisState
from qte_shared.config import AccountSettings, settings
from qte_shared.db import EventRepository
from qte_shared.logging_setup import get_logger
from qte_shared.models import (
    Candle,
    CandleClosedEvent,
    SignalAction,
    TickEvent,
)
from qte_shared.notifications import ServiceStatusNotifier, TelegramErrorNotifier
from qte_shared.strategies.mapping import SymbolMapping
from qte_shared.strategies.plugin_loader import DISABLED_HOOK, read_strategy_directory
from qte_shared.strategies.signal_factory import BracketPolicy, SignalFactory
from qte_shared.strategies.sizing import PositionSizer
from qte_shared.strategies.strategy_base import (
    SignalIntent,
    StrategyContext,
    as_intents,
    candles_to_frame,
    overrides_on_tick,
)
from qte_shared.strategies.strategy_settings import (
    describe_warmup,
    gate_entries,
    resolve_warmup,
    resolve_weekend_flat,
)
from qte_shared.timeframes import (
    CLOCK_TOLERANCE,
    normalize_timeframe,
)
from qte_strategy_engine.broker_sink import BrokerSink
from qte_strategy_engine.control_plane import ControlPlane, RunnerIdentity
from qte_strategy_engine.db import OpenPositionRepository, SignalRepository
from qte_strategy_engine.delivery import DeliveryPipeline
from qte_strategy_engine.history_sync import HistorySync
from qte_strategy_engine.instance import container_instance_token
from qte_strategy_engine.preflight import run_preflight_audit
from qte_strategy_engine.recovery import PositionRecovery
from qte_strategy_engine.runner_context import (
    SERVICE_NAME,
    RunnerContext,
    provider_is_synthetic,
)
from qte_strategy_engine.settings import RunnerSettings, runner_settings
from qte_strategy_engine.slot import StrategySlot
from qte_strategy_engine.telegram_notify import TelegramLifecycleNotifier

log = get_logger(__name__)


def _context_attribute(name: str) -> property:
    """A runner attribute that reads and writes the shared :class:`RunnerContext`.

    So ``runner.signals = repository`` — a test, or a driver swapping a
    collaborator — reaches every collaborator holding the context, instead of
    leaving them on the old one.
    """

    def read(runner: StrategyRunner) -> Any:
        return getattr(runner.context, name)

    def write(runner: StrategyRunner, value: Any) -> None:
        setattr(runner.context, name, value)

    return property(read, write, doc=f"The shared context's ``{name}``.")


class StrategyRunner:
    """Loads the plugins, subscribes, and drives them for the process lifetime."""

    def __init__(
        self,
        sink: BrokerSink | None = None,
        *,
        account_settings: AccountSettings | None = None,
        settings_for_runner: RunnerSettings | None = None,
    ) -> None:
        scope = settings.state_scope
        #: Connections, repositories and the shared book, held by every
        #: collaborator by reference. The properties below write through to it.
        self.context = RunnerContext(
            bus=NatsBus(name="qte-strategy-runner"),
            state=RedisState(),
            subjects=Subjects(),
            events=EventRepository(),
            signals=SignalRepository(),
            positions=OpenPositionRepository(),
            sink=sink or BrokerSink(),
            #: Position-lifecycle notifications. A no-op unless a bot token and
            #: at least one chat id are configured, and never on the trade path:
            #: the emit path only queues an update for its background worker.
            telegram=TelegramLifecycleNotifier(),
            scope=scope,
        )
        #: Read at call time rather than copied, so a changed ceiling or a
        #: test's monkeypatch reaches the next decision.
        self._account = account_settings or settings.account
        self._settings = settings_for_runner or runner_settings
        #: ERROR logs forwarded to the same bot, de-duplicated. Also a no-op
        #: unless QTE_TELEGRAM__LOG_ERRORS_ENABLED is on.
        self.telegram_errors = TelegramErrorNotifier()
        #: The 'UP' / 'DOWN' message `make start` and `make stop` produce.
        self.status = ServiceStatusNotifier(SERVICE_NAME)
        # Refuses a synthetic feed outside dev state before anything connects.
        for provider_name in scope.providers:
            scope.origin(provider=provider_name, synthetic=provider_is_synthetic(provider_name))
        #: The zone every slot's weekend window is read in. One value for the
        #: process, resolved once — the backtest reads the same setting.
        self._market_zone = settings.engine.market_zone
        self._configured_shadow_mode = self.sink.shadow_mode
        #: Set only for a container's PID 1, whose stale claim is provably dead.
        self._instance_token = (
            container_instance_token(self._settings.instance_file)
            if self._settings.reclaim_own_claim
            else None
        )
        self._owner_id = (
            f"{self._instance_token}:{uuid4()}" if self._instance_token else str(uuid4())
        )
        self._ownership_acquired = False
        self._by_subject: dict[tuple[str, str], list[StrategySlot]] = defaultdict(list)
        self._tasks: list[asyncio.Task[Any]] = []
        #: Entries past the account ceiling but not yet in any slot's book.
        #: See :meth:`emit`; without it two pairs closing a bar on the same
        #: boundary would each read the book before the other had grown.
        self._entries_in_flight = 0
        self._started_at = datetime.now(UTC)
        self._cleaned = False

        # The collaborators. Each depends on the context and on the narrow
        # RunnerHooks protocol this class implements, never on the runner.
        self.delivery = DeliveryPipeline(self.context, self, settings=self._settings)
        self.recovery = PositionRecovery(self.context, self, self.delivery, settings=self._settings)
        self.history = HistorySync(self.context, self, settings=self._settings)
        self.control = ControlPlane(
            self.context,
            self,
            self.delivery,
            self.recovery,
            RunnerIdentity(owner_id=self._owner_id, started_at=self._started_at),
        )

    # ── The shared context, as attributes ─────────────────────────────

    bus = _context_attribute("bus")
    state = _context_attribute("state")
    subjects = _context_attribute("subjects")
    events = _context_attribute("events")
    signals = _context_attribute("signals")
    positions = _context_attribute("positions")
    sink = _context_attribute("sink")
    telegram = _context_attribute("telegram")
    slots = _context_attribute("slots")
    _scope = _context_attribute("scope")
    _bar_gate = _context_attribute("bar_gate")
    _stopping = _context_attribute("stopping")

    # ── Startup ───────────────────────────────────────────────────────

    async def start(self) -> None:
        # First, before anything is connected. The pre-flight audit may refuse
        # to trade this book, and a refusal should cost nothing to unwind —
        # there is no sense opening NATS, Redis and the broker for strategies
        # we are about to reject. Off by configuration is the same call.
        run_preflight_audit()
        self._cleaned = False
        self.context.history_started_at = datetime.now(UTC)

        try:
            # First of all, so everything below is covered: a refused ownership
            # claim or an unreachable Redis is exactly the error worth a message,
            # and it is logged long before this method returns.
            await self.telegram_errors.start(SERVICE_NAME)
            await self.bus.connect()
            await self.state.connect()
            await self._acquire_ownership()
            await self.refresh_shadow_mode()
            await self.refresh_bar_gate()
            await self.sink.start()
            await self.telegram.start()

            self._build_slots()
            if not self.slots:
                raise RuntimeError(
                    f"No strategies loaded from {settings.engine.strategies_dir}. Clone your "
                    "private strategy repo into __strategies__/ (see docs/strategies.md)."
                )

            self._validate_tick_publication()
            await self._restore_state()
            await self.delivery.recover_pending_deliveries()
            # After recovery, so the outbox has reconciled every ambiguous
            # delivery and the book is what it says it is. Before the
            # subscriptions below, so no live close races the flush and no
            # strategy is ever asked to decide a bar against a stale position.
            await self.recovery.flush_stale_positions()
            # Independent of the flush above and of its mode: one closes what is
            # merely old, this closes what the mapping table no longer claims.
            await self.recovery.settle_orphan_positions()
            # Every slot is held while the subscriptions go live and the missed
            # closes are replayed. A close that arrives meanwhile waits for the
            # replay, then finds its bar already fed and is ignored, so no bar is
            # decided on twice or behind a newer one.
            async with contextlib.AsyncExitStack() as held_slots:
                for strategy_slot in self.slots:
                    await held_slots.enter_async_context(strategy_slot.lock)
                await self._subscribe()
                await self.history.catch_up()
            self._tasks.append(
                asyncio.create_task(self.delivery.retry_loop(), name="signal-outbox-retry")
            )
            self._tasks.append(
                asyncio.create_task(self.history.run_loop(), name="runner-history-sync")
            )
            if self._settings.weekend_flat_sweep_interval > 0 and any(
                strategy_slot.weekend_flat.enabled for strategy_slot in self.slots
            ):
                self._tasks.append(
                    asyncio.create_task(self._weekend_flat_loop(), name="runner-weekend-flat")
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
                "Runner started slots=%d shadow_mode=%s transport=%s telegram status=%s",
                len(self.slots),
                self.sink.shadow_mode,
                self.sink.transport,
                self.status.describe(),
            )
            # Last, so "UP" means every dependency answered and the book is
            # loaded — the announcement is worth exactly what it implies.
            await self.status.announce_started(
                {
                    "Book": self._scope.namespace,
                    "Strategies": [
                        f"{slot.symbol} {slot.timeframe} {slot.strategy.name}"
                        for slot in self.slots
                    ],
                    "Transport": self.sink.transport,
                    "Shadow mode": "ON" if self.sink.shadow_mode else "OFF — trading live",
                }
            )
        except BaseException:
            await self._close_resources(record_event=False)
            raise

    async def _acquire_ownership(self) -> None:
        """Claim the namespace, or take back this container's own stale claim.

        A held claim is refused, with one exception: a holder that carries this
        container's instance token is an earlier process of the container we
        are PID 1 of, so it is gone. Recovery after that runs exactly as on any
        other start — an ambiguous delivery still blocks its pair.
        """
        if await self.state.claim_runner(self._owner_id):
            self._ownership_acquired = True
            return
        if self._instance_token is not None and await self.state.reclaim_runner(
            self._instance_token, self._owner_id
        ):
            self._ownership_acquired = True
            log.warning(
                "Reclaimed the runner ownership left by an earlier process of this "
                "container owner_id=%s",
                self._owner_id,
            )
            with contextlib.suppress(Exception):
                await self.events.record_event(
                    service=SERVICE_NAME,
                    event="ownership_reclaimed",
                    level="WARNING",
                    payload={"owner_id": self._owner_id},
                )
            return
        holder = None
        with contextlib.suppress(Exception):
            holder = await self.state.runner_owner()
        # The audit trail is what an operator can alert on; never let it mask
        # the refusal itself.
        with contextlib.suppress(Exception):
            await self.events.record_event(
                service=SERVICE_NAME,
                event="ownership_refused",
                level="ERROR",
                payload={"holder": holder, "namespace": self._scope.namespace},
            )
        raise RuntimeError(
            "Runner ownership is already held in this Redis namespace "
            f"(namespace={self._scope.namespace}, holder={holder}). Stop the other runner; "
            "after an unclean exit verify it is gone, then run `qte-control owner clear` "
            "(make owner-clear) to remove the stale runner:owner key. Automatic failover "
            "is disabled."
        )

    async def refresh_shadow_mode(self) -> None:
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
        effective_mode = (
            self._scope.is_paper
            or settings.broker.force_shadow_mode
            or (self._configured_shadow_mode if stored_mode is None else stored_mode)
        )
        if self.sink.shadow_mode != effective_mode:
            self.sink.set_shadow_mode(effective_mode)

    async def live_delivery_paused(self) -> bool:
        await self.refresh_shadow_mode()
        return not self._scope.is_paper and self.sink.shadow_mode

    async def check_ownership(self) -> None:
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
        directory = read_strategy_directory(settings.engine.strategies_dir)
        discovered = directory.loaded
        # Read from the pairings rather than from the file, so a symbol carrying
        # `enabled = false` is already absent: the parser drops those, and a
        # disabled symbol is exactly a pair the operator retired.
        self.context.declared_pairs = (
            {(pairing.strategy, pairing.symbol.upper()) for pairing in mapping.pairings}
            if mapping
            else None
        )
        if mapping:
            loaded_names = [entry.name for entry in discovered]
            self._warn_on_unmapped(mapping, loaded_names, directory.disabled)
            self._warn_on_unused_strategy_defaults(mapping, loaded_names, directory.disabled)

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
                # The repo declares the window; this pair's `use_weekend_flat`
                # decides whether it is enforced — the same call the backtest makes.
                # A value that cannot be honoured stops the start, never reads as off.
                try:
                    weekend_flat = resolve_weekend_flat(entry.settings.weekend_flat, params)
                except ValueError as error:
                    raise ValueError(f"{entry.name} on {symbol}: {error}") from None
                strategy = entry.instantiate(params)
                # The strategy declares the bars its indicators need; this pair's
                # `warmup` may ask for a different count. Resolved from the same
                # params the backtest resolves it from, so a replay starts
                # deciding on the same bar this slot will.
                try:
                    warmup = resolve_warmup(
                        strategy.warmup,
                        strategy.history_window(),
                        params,
                        subject=f"{entry.name} on {symbol}",
                    )
                except ValueError as error:
                    raise ValueError(f"{entry.name} on {symbol}: {error}") from None
                # Size against the account, at the risk this pair is mapped at.
                # The strategy is never told either — see qte_shared.strategies.sizing.
                sizer = PositionSizer.from_settings(params)
                factory = SignalFactory(
                    strategy.name,
                    timeframe=strategy.timeframe,
                    bracket=BracketPolicy(),
                    inputs=strategy.params,
                    sizer=sizer,
                    default_quantity=self._settings.default_quantity,
                )
                slot = StrategySlot(
                    strategy, symbol, factory, weekend_flat=weekend_flat, warmup=warmup
                )
                self.history.warn_if_history_exceeds_redis(slot)
                self.slots.append(slot)
                self._by_subject[(symbol, slot.timeframe)].append(slot)
                log.info(
                    "Slot ready strategy=%s symbol=%s tf=%s warmup=%s risk=%.3f%% of %.2f "
                    "weekend_flat=%s (%s)",
                    strategy.name,
                    symbol,
                    slot.timeframe,
                    describe_warmup(slot.warmup, strategy.warmup),
                    sizer.risk_percent,
                    sizer.capital,
                    slot.weekend_flat.describe(),
                    self._market_zone.key,
                )

    @staticmethod
    def _warn_on_unmapped(
        mapping: SymbolMapping, loaded: list[str], disabled: Mapping[str, Path]
    ) -> None:
        """Say so when the table names a strategy the loader never found.

        Almost always a typo or a stale name after a rename, and the symptom
        without this line is a symbol that quietly trades nothing — which reads
        exactly like a strategy that found no setups.

        A strategy its repository disabled is still an error here — the table
        and the repository disagree about whether it trades — but it is named
        as such, because the fix is a deliberate edit to one of the two rather
        than a hunt for the misspelling.
        """
        unknown = [name for name in mapping.strategies if name not in set(loaded)]
        switched_off = sorted(name for name in unknown if name in disabled)
        missing = sorted(name for name in unknown if name not in disabled)
        if switched_off:
            log.error(
                "Mapping table %s names %s, which %s keeps disabled (%s). Those symbols will "
                "not run it: remove it from the table, or move it back into that "
                "repository's ALIASES.",
                mapping.source,
                ", ".join(switched_off),
                ", ".join(sorted({str(disabled[name]) for name in switched_off})),
                DISABLED_HOOK,
            )
        if missing:
            log.error(
                "Mapping table %s names %s, which %s did not publish. Those symbols will "
                "trade nothing. Available: %s",
                mapping.source,
                ", ".join(missing),
                settings.engine.strategies_dir,
                ", ".join(sorted(loaded)) or "none",
            )

    @staticmethod
    def _warn_on_unused_strategy_defaults(
        mapping: SymbolMapping, loaded: list[str], disabled: Mapping[str, Path]
    ) -> None:
        """Say so when ``[strategies.<name>]`` names a strategy nobody publishes.

        Unlike an unmapped pairing this trades nothing wrong — the defaults are
        simply never read — but a typo here means an intended ``risk_percent``
        override silently does not apply, so it is worth a line. One for a
        strategy its repository disabled waits, unread, for it to come back.
        """
        unknown = [name for name in mapping.strategy_defaults if name not in set(loaded)]
        switched_off = sorted(name for name in unknown if name in disabled)
        missing = sorted(name for name in unknown if name not in disabled)
        if switched_off:
            log.warning(
                "Mapping table %s has [strategies.*] defaults for %s, which its repository "
                "keeps disabled (%s) — they apply to nothing until it is back in ALIASES.",
                mapping.source,
                ", ".join(switched_off),
                DISABLED_HOOK,
            )
        if missing:
            log.warning(
                "Mapping table %s has [strategies.*] defaults for %s, which %s did not "
                "publish — those defaults will not apply anywhere.",
                mapping.source,
                ", ".join(missing),
                settings.engine.strategies_dir,
            )

    async def _restore_state(self) -> None:
        """Refill candle buffers and the open cycle each slot was holding.

        A slot that has been fed before takes only the bars up to the newest one
        it was fed. Anything Redis holds beyond that closed while this runner was
        not listening, and :meth:`_catch_up` feeds it as a close rather than
        leaving it in the window as history nobody decided on.
        """
        allow_future = provider_is_synthetic()
        for strategy_slot in self.slots:
            candles = await self.history.stored_history(strategy_slot, allow_future=allow_future)
            decided = await self.history.decided_open_time(strategy_slot, allow_future=allow_future)
            if decided is not None:
                candles = [candle for candle in candles if candle.open_time <= decided]
            strategy_slot.buffer.extend(candles)
            await self.recovery.restore_position(strategy_slot)
            log.info(
                "Warm-up %s/%s %s: %d/%d candles from Redis",
                strategy_slot.strategy.name,
                strategy_slot.symbol,
                strategy_slot.timeframe,
                len(strategy_slot.buffer),
                strategy_slot.warmup,
            )

    # ── Stale positions ───────────────────────────────────────────────

    def _wants_ticks(self) -> bool:
        return self._settings.subscribe_ticks or any(
            overrides_on_tick(strategy_slot.strategy) for strategy_slot in self.slots
        )

    def _validate_tick_publication(self) -> None:
        """Reject a book whose tick callbacks would never receive market data."""
        if self._wants_ticks() and not settings.market_stream.publish_ticks:
            raise RuntimeError(
                "Runner requires ticks, but QTE_INGESTION__PUBLISH_TICKS is false. "
                "Set it to true for both ingestion and runner before starting this book."
            )

    async def _subscribe(self) -> None:
        for symbol, timeframe in sorted(self._by_subject):
            await self.bus.subscribe(
                self.subjects.candle_closed(symbol, timeframe),
                self._on_candle_message,
                queue=self._settings.queue_group,
            )
        await self.bus.subscribe(self.subjects.engine_control(), self._on_control_message)

        if self._wants_ticks():
            await self.bus.subscribe(self.subjects.tick_wildcard(), self._on_tick_message)
            log.info("Tick subscription active — tick publication is required")

    # ── Message handlers ──────────────────────────────────────────────

    async def _on_candle_message(self, msg: Msg) -> None:
        event = CandleClosedEvent.model_validate_json(msg.data)
        if (event.symbol, event.timeframe) != (event.candle.symbol, event.candle.timeframe):
            raise ValueError("Candle envelope does not match its market data")
        slots = self._by_subject.get((event.symbol, normalize_timeframe(event.timeframe)), [])
        for slot in slots:
            await self._feed_candle(slot, event.candle)

    async def _feed_candle(self, slot: StrategySlot, candle: Candle) -> None:
        if not self._scope.accepts(candle.origin):
            log.warning("Ignored candle with missing or foreign provenance")
            return
        async with slot.lock:
            if slot.buffer and candle.open_time <= slot.buffer[-1].open_time:
                return
            await self.history.sync_history(slot, before_open=candle.open_time)
            await self.history.feed_if_current(slot, candle)

    async def feed_candle_serialized(self, slot: StrategySlot, candle: Candle) -> None:
        if not self._scope.accepts(candle.origin):
            return
        await self.check_ownership()
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
        await self.history.record_decided(slot, candle)

        if not slot.is_warm:
            log.debug(
                "Still warming %s/%s: %d/%d",
                slot.strategy.name,
                slot.symbol,
                len(slot.buffer),
                slot.warmup,
            )
            return

        # After the window was stored and the bar recorded, before the strategy
        # is consulted: a paused pair keeps a window it can trade on the moment
        # it is allowed again. See :mod:`qte_strategy_engine.bar_gate`.
        if await self._bar_is_gated(slot):
            return

        if slot.key in self.delivery.uncertain_pairs or await self.live_delivery_paused():
            return

        context = self._strategy_context(slot, candle.open_time)
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

        # The market this pair trades shuts before the next bar the strategy
        # could manage a position on, so its entries are dropped here rather
        # than in the strategy: the gate belongs to the calendar, and without
        # it the FLAT below and the next entry would take turns every bar until
        # the close. The replay calls the same function.
        gate = gate_entries(as_intents(result), slot.weekend_flat, context.now, self._market_zone)
        for blocked in gate.blocked:
            log.info(
                "Blocked %s for %s %s at %s: inside the weekend-flat window %s (%s)",
                blocked.action.value,
                slot.strategy.name,
                slot.symbol,
                context.now,
                slot.weekend_flat.describe(),
                self._market_zone.key,
            )
        for intent in gate.allowed:
            await self.emit(slot, intent, candle.close, context.now)

        if gate.market_is_shut:
            # After the strategy's own exits, not before them: a bar that hit
            # the stop stopped out, and the flat is what is left over.
            await self._flatten_for_the_weekend(slot, candle.close, context.now)

    async def _bar_is_gated(self, slot: StrategySlot) -> bool:
        """Whether this pair is paused, re-read from Redis for this bar.

        One GET per closed bar per pair, which on an M15 book is a handful an
        hour — cheap enough to buy the property that matters: a ``/prevent``
        takes effect on the next bar even if the NATS broadcast was missed.
        """
        await self.refresh_bar_gate()
        if not self._bar_gate.blocks(symbol=slot.symbol, strategy=slot.strategy.name):
            return False
        log.info(
            "Paused: not deciding %s on %s %s (gate: %s)",
            slot.strategy.name,
            slot.symbol,
            slot.timeframe,
            self._bar_gate.describe(),
        )
        return True

    async def refresh_bar_gate(self) -> None:
        """Re-read the stored gate. A Redis failure keeps the last known one."""
        try:
            stored = await self.state.get_flag(BAR_GATE_FLAG, None)
        except Exception:  # noqa: BLE001 - an unreadable gate keeps the last one
            log.warning("Could not read the bar gate; keeping %s", self._bar_gate.describe())
            return
        self._bar_gate = BarGate.from_payload(stored)

    @staticmethod
    def _strategy_context(slot: StrategySlot, moment: datetime) -> StrategyContext:
        """What the strategy is told on one decision: the pair, the clock and its cycles."""
        open_uxids = slot.factory.open_cycles(slot.symbol)
        return StrategyContext(
            symbol=slot.symbol,
            timeframe=slot.timeframe,
            now=moment,
            mode="live",
            params=slot.strategy.params,
            open_uxid=open_uxids[-1] if open_uxids else None,
            open_uxids=open_uxids,
        )

    async def _flatten_for_the_weekend(
        self, slot: StrategySlot, price: float, moment: datetime
    ) -> None:
        """Close whatever is open on *slot*, because its market is shutting."""
        for uxid in slot.factory.open_cycles(slot.symbol):
            log.warning(
                "Weekend flat: closing %s on %s uxid=%s at %s — window %s (%s)",
                slot.strategy.name,
                slot.symbol,
                uxid,
                moment,
                slot.weekend_flat.describe(),
                self._market_zone.key,
            )
            await self.flatten_cycle(slot, uxid, price, moment, reason="WEEKEND_FLAT")

    async def flatten_cycle(
        self, slot: StrategySlot, uxid: str, price: float, moment: datetime, *, reason: str
    ) -> None:
        """Close one open cycle with a ``FLAT``, whatever asked for it.

        Emitted as an ordinary intent on the ordinary path, so it is staged in
        the outbox, sized, delivered and audited exactly like a strategy's own
        exit — there is no second delivery route to keep correct. One FLAT per
        cycle, each naming its own: with several open, an unnamed close would be
        ambiguous and the factory refuses it.

        A pair that is already flat produces nothing, which is what makes the
        callers safe to run on every bar, from the sweep and from an operator's
        ``/flat``.
        """
        await self.emit(
            slot,
            SignalIntent(
                action=SignalAction.FLAT,
                symbol=slot.symbol,
                price=price,
                reason=reason,
                signal_uxid=uxid,
            ),
            price,
            moment,
        )

    async def _weekend_flat_loop(self) -> None:
        """Flatten a shut market even when no candle closes to prompt it.

        The bar-driven path in :meth:`feed_candle_serialized` is the one the
        backtest also takes, so the two drivers decide alike on every bar. It is
        not enough on its own: a feed that stalls at 16:50 on a Friday delivers
        no further close, and the position rides through the weekend. This loop
        covers exactly the moments a backtest has no bar for, which is why it
        can exist without the replay drifting from the runner.

        ``QTE_RUNNER__WEEKEND_FLAT_SWEEP_INTERVAL=0`` turns it off and leaves
        only the bar-driven path.
        """
        while not self._stopping.is_set():
            await asyncio.sleep(self._settings.weekend_flat_sweep_interval)
            for strategy_slot in self.slots:
                if not strategy_slot.weekend_flat.enabled or not strategy_slot.buffer:
                    continue
                try:
                    async with strategy_slot.lock:
                        await self._sweep_one_slot(strategy_slot)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    # One pair whose ownership check or delivery failed must not
                    # stop the sweep reaching the others; the next interval
                    # retries it, and the window is open for hours.
                    log.exception(
                        "Weekend-flat sweep failed for %s %s; retrying at the next interval",
                        strategy_slot.strategy.name,
                        strategy_slot.symbol,
                    )

    async def _sweep_one_slot(self, strategy_slot: StrategySlot) -> None:
        """One slot's share of :meth:`_weekend_flat_loop`, under its lock.

        The moment tested is the wall clock and not a bar time: the whole point
        is that no bar arrived. The price on the intent is the last close this
        pair saw, which is the only price the runner has when the feed is the
        thing that stopped — a ``FLAT`` carries no size or level to the broker,
        so it is the audit trail that reads it rather than the fill.
        """
        moment = datetime.now(UTC)
        if not strategy_slot.weekend_flat.covers(moment, self._market_zone):
            return
        if strategy_slot.key in self.delivery.uncertain_pairs:
            return
        await self.check_ownership()
        if await self.live_delivery_paused():
            return
        await self._flatten_for_the_weekend(strategy_slot, strategy_slot.buffer[-1].close, moment)

    async def _on_control_message(self, msg: Msg) -> None:
        """The control subject's handler; :class:`ControlPlane` answers it."""
        await self.control.dispatch(msg)

    async def _on_tick_message(self, msg: Msg) -> None:
        event = TickEvent.model_validate_json(msg.data)
        if event.symbol != event.tick.symbol or not self._scope.accepts(event.tick.origin):
            log.warning("Ignored tick with missing or foreign provenance")
            return
        if event.tick.ts.tzinfo is None or (
            not event.tick.origin.synthetic and event.tick.ts > datetime.now(UTC) + CLOCK_TOLERANCE
        ):
            return
        price = event.tick.price
        for slot in self.slots:
            if (
                slot.symbol != event.symbol
                or not slot.started
                or slot.key in self.delivery.uncertain_pairs
            ):
                continue
            if self._bar_gate.blocks(symbol=slot.symbol, strategy=slot.strategy.name):
                # `/prevent` pauses the pair, not only its closed bars: a
                # tick-driven strategy must not go on trading behind it. The
                # gate held in memory is read, refreshed on every bar and on
                # the control broadcast — a Redis GET per tick would cost more
                # than everything else this handler does.
                continue
            async with slot.lock:
                await self.check_ownership()
                if await self.live_delivery_paused():
                    continue
                context = self._strategy_context(slot, event.tick.ts)
                try:
                    result = slot.strategy.on_tick(price, context)
                except Exception:
                    log.exception(
                        "Strategy %s raised on tick for %s", slot.strategy.name, slot.symbol
                    )
                    continue
                # The same calendar gate the candle path applies. A tick-driven
                # entry into a market that is shutting is the same mistake, and
                # exits still pass so a position can always be got out of.
                gate = gate_entries(
                    as_intents(result), slot.weekend_flat, event.tick.ts, self._market_zone
                )
                for blocked in gate.blocked:
                    log.info(
                        "Blocked tick %s for %s %s: inside the weekend-flat window %s",
                        blocked.action.value,
                        slot.strategy.name,
                        slot.symbol,
                        slot.weekend_flat.describe(),
                    )
                for intent in gate.allowed:
                    await self.emit(slot, intent, price, event.tick.ts)

    # ── Emission ──────────────────────────────────────────────────────

    async def emit(
        self,
        slot: StrategySlot,
        intent: SignalIntent,
        fallback_price: float | None,
        moment: datetime,
    ) -> None:
        """Deliver one intent, holding a slot against the account ceiling.

        Entries only. A close is never held up by a full book: a position that
        cannot be got out of is a worse failure than one more than the account
        wanted, and every close here is closing something the book already has.

        The reservation is what makes the ceiling hold. Two pairs whose bars
        close on the same boundary are two tasks inside this method at once,
        and neither one's entry reaches a book until several awaits later —
        both would read the same count and both would open. Counting the
        entries in flight closes that window, and ``finally`` gives the slot
        straight back to an intent the factory refused or a delivery that never
        landed. A committed entry is briefly counted twice, in its book and in
        flight, which errs towards refusing rather than overfilling.

        Ownership is checked here once for every intent, entry or close — the
        pipeline does not repeat it, and keeps only the two checks that sit
        right against the broker boundary.
        """
        # Before the ceiling, not after it: `check_ownership` is the kill
        # switch, and a refusal is itself an effect — it writes an audit row. A
        # runner that has lost its claim must find that out here rather than go
        # quiet behind a full book.
        await self.check_ownership()
        if intent.action.is_entry:
            if await self._account_is_full(slot, intent):
                return
            self._entries_in_flight += 1
            try:
                await self.delivery.emit_intent(slot, intent, fallback_price, moment)
            finally:
                self._entries_in_flight -= 1
            return
        await self.delivery.emit_intent(slot, intent, fallback_price, moment)

    async def _account_is_full(self, slot: StrategySlot, intent: SignalIntent) -> bool:
        """Whether one more position would exceed what the account will hold.

        ``QTE_ACCOUNT__MAX_OPEN_POSITIONS`` is the engine's copy of a limit the
        broker account enforces across every strategy and symbol *together* —
        the worker that places the orders counts them all as one budget. Zero,
        the default, means the engine was never told the number and does not
        invent one.

        It is a ceiling on the account and not on a pair. A per-pair cap would
        not help: the pairs in a mapping each stay inside their own book and
        overrun the account between them.

        The count is this process's own book, summed over every slot in memory
        — the same source :meth:`_flush_stale_positions` treats as
        authoritative, and no database round trip on the trade path. So it
        counts what *this engine* opened, not what the account holds: a
        position placed by hand, or an entry the worker refused for some other
        reason, is invisible here. This keeps the engine inside its own budget;
        it does not prove the two books equal. Only execution feedback could,
        and the engine deliberately does not subscribe to it — see
        ``docs/broker-contract.md``.

        Refused out loud, deliberately. The per-pair cap that used to live
        upstream of here skipped its bar in silence, which made a book that had
        quietly stopped trading unreadable in the logs.
        """
        ceiling = self._account.max_open_positions
        if ceiling <= 0:
            return False
        held = (
            sum(len(other.factory.open_cycles(other.symbol)) for other in self.slots)
            + self._entries_in_flight
        )
        if held < ceiling:
            return False
        log.warning(
            "Refused %s for %s %s: the account already holds %d position(s) against a "
            "ceiling of %d (QTE_ACCOUNT__MAX_OPEN_POSITIONS), counted across every "
            "strategy and symbol together",
            intent.action.value,
            slot.strategy.name,
            slot.symbol,
            held,
            ceiling,
        )
        await self.events.record_event(
            service=SERVICE_NAME,
            event="entry_refused_account_full",
            level="WARNING",
            payload={
                "strategy": slot.strategy.name,
                "symbol": slot.symbol,
                "action": intent.action.value,
                "open_positions": held,
                "ceiling": ceiling,
            },
        )
        return True

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
        # Announced here rather than in `stop`, so a shutdown that began with a
        # failed start is reported too — that is the one an operator is least
        # likely to be watching a terminal for. Before the notifiers below
        # close their clients, and awaited rather than queued: a worker that is
        # about to be cancelled would never send it.
        with contextlib.suppress(Exception):
            await self.status.announce_stopped()
        # Drain callbacks before releasing ownership: a queued close must not
        # run alongside the next owner. Uncertain cleanup retains the claim.
        for close in (
            self.bus.close,
            self.telegram.stop,
            self.telegram_errors.stop,
            self.status.aclose,
            self.sink.stop,
        ):
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
        elif self._ownership_acquired:
            # Retaining the claim is the decision above, but it was silent: the
            # next start reports only that a claim is held, and by then this
            # container — and the cleanup failure that explains it — is gone
            # with its logs. Name it here, and record it where it outlives the
            # container.
            log.warning(
                "Retaining the runner ownership claim because shutdown cleanup failed "
                "owner_id=%s namespace=%s — the next container is refused until "
                "`qte-control owner clear` removes it",
                self._owner_id,
                self._scope.namespace,
            )
            with contextlib.suppress(Exception):
                await self.events.record_event(
                    service=SERVICE_NAME,
                    event="ownership_retained",
                    level="WARNING",
                    payload={"owner_id": self._owner_id, "namespace": self._scope.namespace},
                )
        try:
            await self.state.close()
        except Exception:
            cleanup_failed = True
            log.exception("Redis cleanup failed during runner shutdown")
        self._cleaned = not cleanup_failed
        log.info("Runner stopped")


__all__ = ["SERVICE_NAME", "StrategyRunner", "StrategySlot"]
