"""Everything the bot asks the engine, in one place.

Four sources, picked by what the question actually is:

* **NATS request/reply** for anything only a *running* process knows or can do
  — how full a strategy's in-memory window is, closing a position, re-asking a
  vendor for history. The same actions ``qte-control`` uses; there is no HTTP
  control plane in this repository and this does not add one.
* **Redis** for the candle windows, and for the bar gate, which is stored
  rather than broadcast so that a runner restarting during a pause comes back
  paused. Written here and announced over NATS second, exactly as
  ``qte-control`` does it for shadow mode.
* **Postgres** for the book: open positions, and the closed cycles behind
  ``/closed``. Both answer without a runner, which is deliberate — "what am I
  holding" is the question you most want answered when the runner is down.
* **The ``__strategies__/`` mount** for which build of each strategy
  repository is deployed — its ``BUNDLE_VERSION.json`` — and what it
  publishes. Read from disk, so it answers without a runner and without
  importing a line of private code.
* **The configuration files** for the symbol → strategy table and the
  data-provider plans, which are what an operator edits and so what they
  expect a command to read back.

The drill behind ``/demo`` is the one thing here that *runs* rather than asks:
it drives the mounted strategies itself, in a worker thread, exactly as
``make demo-signal`` does. It belongs in the bot and not in the runner because
it is seconds of CPU per pair and the live loop must not spend them — and it is
safe beside a running runner because it writes no engine state.

Every method is best-effort and returns ``None`` (or an empty page) when the
engine cannot be reached, so a handler renders "could not reach the runner"
instead of a traceback in the chat.
"""

from __future__ import annotations

import asyncio
from typing import Any

from qte_bot.mounts import MountedRepository, scan_mounted_repositories
from qte_shared.bar_gate import BAR_GATE_FLAG, BarGate
from qte_shared.bus import NatsBus, Subjects
from qte_shared.cache import RedisState
from qte_shared.config import market_data_plan, provider_market_data_plan, settings
from qte_shared.logging_setup import get_logger
from qte_shared.market_data_plan import MarketDataPlan, SymbolFeed
from qte_shared.strategies.mapping import SymbolMapping
from qte_strategy_engine.db import (
    ClosedCycle,
    OpenPositionRepository,
    SignalRepository,
)
from qte_strategy_engine.demo_signal import (
    DemoDelivery,
    DemoPair,
    DemoSignal,
    build_demo_pairs,
    build_demo_signal,
    publish_demo_signals,
)

log = get_logger(__name__)


class EngineClient:
    """The engine, as the bot sees it."""

    def __init__(
        self,
        *,
        bus: NatsBus | None = None,
        state: RedisState | None = None,
        signals: SignalRepository | None = None,
        positions: OpenPositionRepository | None = None,
        request_timeout: float = 5.0,
        flat_timeout: float = 30.0,
    ) -> None:
        self._bus = bus or NatsBus(name="qte-bot")
        self._state = state or RedisState()
        self._signals = signals or SignalRepository()
        self._positions = positions or OpenPositionRepository()
        self._subjects = Subjects()
        self._request_timeout = request_timeout
        self._flat_timeout = flat_timeout
        # One drill at a time per bot process. Two concurrent runs would drive
        # the same strategies over the same synthetic bars for no extra
        # coverage and put every card into the channel twice.
        self._demo_lock = asyncio.Lock()

    @property
    def namespace(self) -> str:
        """The book this bot is wired to — in every message, so it is never a guess."""
        return settings.state_scope.namespace

    async def start(self) -> None:
        await self._bus.connect()
        await self._state.connect()

    async def aclose(self) -> None:
        await self._bus.close()
        await self._state.close()

    # ── The plan ──────────────────────────────────────────────────────

    def planned_feeds(self) -> list[SymbolFeed]:
        """What the data-provider settings say is fed, symbol by symbol.

        The source for ``/warmup`` and ``/flush``'s symbol lists, and for
        ``/redis``'s rows: ``config/data_providers/<provider>.toml`` is the file
        an operator edits, so it is the list they expect to be offered. What is
        actually subscribed is ingestion's answer, and it is checked against
        this when a command reaches it.
        """
        return settings.engine.resolve_subscriptions(
            market_data_plan(), settings.market_stream.market_overrides
        )

    def planned_symbols(self) -> list[str]:
        return sorted({feed.symbol for feed in self.planned_feeds()})

    def provider_plans(self) -> dict[str, MarketDataPlan]:
        """Each configured provider's own plan, keyed by provider name.

        The per-provider plans rather than the merged one, because ``[provider]``
        — the vendor's own knobs — survives the merge only when a single
        provider is configured, and the knobs are half of what ``/provider``
        exists to show. Credentials cannot appear: the parser drops every
        credential-shaped key out of ``[provider]`` as it reads the file.
        """
        return {name: provider_market_data_plan(name) for name in settings.market_data.providers}

    async def strategy_mapping(self) -> SymbolMapping:
        """The symbol → strategy table as the runner reads it, parsed off-thread.

        Read from the file rather than asked of the runner on purpose: this is
        the configuration an operator edits, and comparing it with what
        ``/runner`` says is loaded is how a deploy that has not been restarted
        yet shows itself.
        """
        return await asyncio.to_thread(SymbolMapping.load, settings.engine.mapping_file)

    # ── Redis ─────────────────────────────────────────────────────────

    async def candle_windows(self) -> list[dict[str, Any]]:
        """Bars held per planned series, with the newest open time."""
        windows: list[dict[str, Any]] = []
        for feed in self.planned_feeds():
            for timeframe in feed.timeframes:
                try:
                    held = await self._state.count_candles(feed.symbol, timeframe)
                    newest = await self._state.get_candles(feed.symbol, timeframe, count=1)
                except Exception as failure:  # noqa: BLE001 - the bot reports an unreachable service
                    log.warning(
                        "Could not read the %s %s window: %s", feed.symbol, timeframe, failure
                    )
                    windows.append({"symbol": feed.symbol, "timeframe": timeframe, "error": True})
                    continue
                windows.append(
                    {
                        "symbol": feed.symbol,
                        "timeframe": timeframe,
                        "provider": feed.provider or settings.market_data.provider_key,
                        "bars": held,
                        "newest_bar": newest[-1].open_time if newest else None,
                        "error": False,
                    }
                )
        return windows

    async def history_provider(self) -> str | None:
        try:
            return await self._state.get_history_provider()
        except Exception:  # noqa: BLE001 - the bot reports an unreachable service
            return None

    # ── The mount ─────────────────────────────────────────────────────

    async def mounted_repositories(self) -> list[MountedRepository]:
        """The strategy repositories on disk, with the build each one carries.

        Read from the ``__strategies__/`` mount every service container
        shares, not asked of the runner: the question is which build was
        deployed, and a runner that is down — or still holding the build it
        imported before the deploy — would answer a different one.
        """
        return await asyncio.to_thread(scan_mounted_repositories, settings.engine.strategies_dir)

    async def mounted_strategy_names(self) -> list[str]:
        """Every strategy the mount record publishes — what a drill can fire.

        The mount, not the runner: :func:`build_demo_pairs` loads the
        strategies off disk itself, so offering the runner's list would offer
        names this bot cannot drill and hide ones it can.
        """
        repositories = await self.mounted_repositories()
        return sorted({name for repository in repositories for name in repository.strategies})

    # ── The drill ─────────────────────────────────────────────────────

    @property
    def demo_running(self) -> bool:
        return self._demo_lock.locked()

    async def fire_demo_signals(
        self, strategy: str | None = None
    ) -> tuple[list[DemoDelivery], list[str]]:
        """Run the demo drill here and publish it; returns deliveries and failures.

        The same code path as ``make demo-signal``, and for the same reason it
        does not go through the runner: driving a strategy over four synthetic
        regimes is seconds of CPU per pair, and the live loop must not spend
        them. It is safe to run beside a live runner because the drill writes
        no engine state — a throwaway ``SignalFactory``, nothing into Redis,
        Postgres or the outbox — and publishes on a subject the broker answers
        with a Telegram card and nothing else.

        Building is synchronous and runs in a worker thread, so the bot keeps
        answering while a drill over a whole mount is in flight. *failures* are
        the pairs whose payload could not be built at all, as readable lines.
        """
        async with self._demo_lock:
            pairs, demos, failures = await asyncio.to_thread(_build_demos, strategy)
            if not pairs:
                return [], failures
            return await publish_demo_signals(demos), failures

    # ── Postgres ──────────────────────────────────────────────────────

    async def open_positions(self) -> list[dict[str, Any]]:
        """The whole book as table rows, each with the timeframe it trades.

        The timeframe is not on the position — a cycle belongs to a (strategy,
        symbol) pair — so it is read back from the signals that opened the
        cycles. One extra query for a column the table is unreadable without.
        """
        positions = await self._positions.list_open()
        timeframes = await self._signals.cycle_timeframes(
            [position.signal_uxid for position in positions]
        )
        return [
            {
                "symbol": position.symbol,
                "timeframe": timeframes.get(position.signal_uxid, ""),
                "strategy": position.strategy,
                "signal_uxid": position.signal_uxid,
                "opened_at": position.opened_at,
                "action": position.action.value,
                "price": position.price,
                "remaining": position.remaining,
            }
            for position in positions
        ]

    async def closed_cycles(self, *, limit: int, offset: int) -> tuple[list[ClosedCycle], int]:
        return await self._signals.closed_cycles(limit=limit, offset=offset)

    # ── The runner ────────────────────────────────────────────────────

    async def runner_status(self) -> dict[str, Any] | None:
        return await self._request(self._subjects.engine_control(), {"action": "status"})

    async def flat(
        self, *, everything: bool = False, symbol: str | None = None, strategy: str | None = None
    ) -> dict[str, Any] | None:
        """Ask the runner to close positions. Its reply says what it managed."""
        return await self._request(
            self._subjects.engine_control(),
            {
                "action": "flat",
                "scope": {"everything": everything, "symbol": symbol, "strategy": strategy},
            },
            timeout=self._flat_timeout,
        )

    # ── The bar gate ──────────────────────────────────────────────────

    async def read_gate(self) -> BarGate:
        try:
            return BarGate.from_payload(await self._state.get_flag(BAR_GATE_FLAG, None))
        except Exception as failure:  # noqa: BLE001 - the bot reports an unreachable service
            log.warning("Could not read the bar gate: %s", failure)
            return BarGate()

    async def change_gate(
        self,
        *,
        blocking: bool,
        everything: bool = False,
        symbol: str | None = None,
        strategy: str | None = None,
    ) -> tuple[BarGate | None, bool]:
        """Store the new gate, then tell the running runner to re-read it.

        Returns the stored gate and whether the broadcast got through. Storing
        first is what makes the pause survive a restart; the broadcast only
        decides whether it takes effect now or on the runner's next bar, which
        is why a failed broadcast is reported rather than raised.
        """
        gate = await self.read_gate()
        if blocking:
            gate.prevent(everything=everything, symbol=symbol, strategy=strategy)
        else:
            gate.allow(everything=everything, symbol=symbol, strategy=strategy)
        try:
            await self._state.set_flag(BAR_GATE_FLAG, gate.to_payload())
        except Exception as failure:  # noqa: BLE001 - the bot reports an unreachable service
            # Nothing was changed: a gate that cannot be stored would be lost
            # by the next restart, and reporting it as applied would be a lie
            # about whether a pair is trading.
            log.error("Could not store the bar gate: %s", failure)
            return None, False
        announced = await self._request(self._subjects.engine_control(), {"action": "set_bar_gate"})
        return gate, announced is not None

    # ── Ingestion ─────────────────────────────────────────────────────

    async def ingestion_status(self) -> dict[str, Any] | None:
        return await self._request(self._subjects.ingestion_control(), {"action": "status"})

    async def warmup(self, symbols: list[str] | None) -> dict[str, Any] | None:
        """Re-request the warm-up windows. ``None`` means every planned symbol."""
        return await self._request(
            self._subjects.ingestion_control(),
            {"action": "warmup", "symbols": symbols},
            timeout=self._flat_timeout,
        )

    async def flush(self, symbols: list[str] | None) -> dict[str, Any] | None:
        """Drop the warm-up windows. ``None`` means every planned symbol."""
        return await self._request(
            self._subjects.ingestion_control(),
            {"action": "flush", "symbols": symbols},
            timeout=self._flat_timeout,
        )

    # ── Internals ─────────────────────────────────────────────────────

    async def _request(
        self, subject: str, payload: dict[str, Any], *, timeout: float | None = None
    ) -> dict[str, Any] | None:
        """One control request. ``None`` means nobody answered in time."""
        try:
            reply = await self._bus.request(
                subject, payload, timeout=timeout or self._request_timeout
            )
        except Exception as failure:  # noqa: BLE001 - the bot reports an unreachable service
            log.warning("No answer on %s for %s: %s", subject, payload.get("action"), failure)
            return None
        return reply if isinstance(reply, dict) else None


def _build_demos(
    strategy: str | None,
) -> tuple[list[DemoPair], list[DemoSignal], list[str]]:
    """Build every pair's payload in one thread, keeping the ones that worked.

    A pair whose payload cannot be built is the drill's finding, not a reason
    to drop the pairs that can: an operator firing the whole mount wants the
    working ones fired and the broken ones named.
    """
    pairs = build_demo_pairs(strategy)
    demos: list[DemoSignal] = []
    failures: list[str] = []
    for pair in pairs:
        try:
            demos.append(build_demo_signal(pair))
        except Exception as failure:
            log.exception(
                "Demo drill could not build a payload for %s on %s",
                pair.strategy_name,
                pair.symbol,
            )
            failures.append(f"{pair.strategy_name} on {pair.symbol}: {failure}")
    return pairs, demos, failures
