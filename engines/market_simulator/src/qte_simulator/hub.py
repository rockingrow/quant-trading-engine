"""Who is listening, what has been sent, and where each symbol's clock stands.

The hub is the server's only mutable state. Everything else — the command
handlers, the generators — is a function of it.

The part that is not obvious is the **cursor**: the open time of the last bar
emitted for a ``(symbol, timeframe)`` pair. It is what lets successive commands
form one continuous series instead of each landing wherever the wall clock
happens to be. Without it, replaying 300 warm-up bars and then sending the bar
that should trigger a signal would put the trigger bar *before* the warm-up
ended, and ingestion would discard it as a late tick.

Sealing moves the cursor onto the sealed bucket, not past it. The seal tick has
already opened a bar there; the next command must not land in the same bucket
or it would inherit the seal's price as its open.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime

from qte_shared.logging_setup import get_logger
from qte_shared.models import Tick
from qte_shared.providers.simulator.protocol import dumps, encode_tick
from qte_shared.timeframes import normalize_timeframe

log = get_logger(__name__)


@dataclass
class Subscriber:
    """One attached feed client and the symbols it asked for."""

    id: int
    remote: str
    send: object  # Callable[[str], Awaitable[None]] — the socket's send
    symbols: set[str] = field(default_factory=set)
    sent: int = 0

    def wants(self, symbol: str) -> bool:
        """An empty subscription means everything — see ``subscribe_frame``."""
        return not self.symbols or symbol in self.symbols


class SimulatorHub:
    """Fan-out to the attached feeds, plus the bookkeeping the CLI reports on."""

    def __init__(self) -> None:
        self.started_at = datetime.now(UTC)
        self.ticks_sent = 0
        self.bars_sent = 0
        #: Last price sent per symbol. Consecutive commands are meant to form
        #: one series, and a `--generate` run that restarted from a reference
        #: price each time would put a 20% gap in the middle of the chart the
        #: strategy is reading.
        self.last_prices: dict[str, float] = {}
        #: Timestamp of the last tick sent per symbol. A walk that continues a
        #: replay has to start from where the series got to, not from the wall
        #: clock — the resampler drops anything behind the bar it holds open.
        self.last_tick_ts: dict[str, datetime] = {}
        self._subscribers: dict[int, Subscriber] = {}
        self._next_id = 0
        self._cursors: dict[tuple[str, str], datetime] = {}
        self._generators: dict[str, asyncio.Task[None]] = {}

    # ── Subscribers ───────────────────────────────────────────────────

    def attach(self, remote: str, send) -> Subscriber:
        self._next_id += 1
        subscriber = Subscriber(id=self._next_id, remote=remote, send=send)
        self._subscribers[subscriber.id] = subscriber
        log.info("Feed client attached id=%d remote=%s", subscriber.id, remote)
        return subscriber

    def detach(self, subscriber: Subscriber) -> None:
        self._subscribers.pop(subscriber.id, None)
        log.info("Feed client detached id=%d after %d ticks", subscriber.id, subscriber.sent)

    @property
    def subscribers(self) -> list[Subscriber]:
        return list(self._subscribers.values())

    # ── Publishing ────────────────────────────────────────────────────

    async def publish(self, tick: Tick) -> int:
        """Send *tick* to every client that wants it; return how many got it.

        A client whose socket has failed is dropped rather than retried: it has
        gone, and holding its place would make the next `status` lie about who
        is listening.
        """
        self.ticks_sent += 1
        self.last_prices[tick.symbol] = tick.price
        self.last_tick_ts[tick.symbol] = tick.ts
        frame = dumps(encode_tick(tick, seq=self.ticks_sent))
        delivered = 0
        for subscriber in list(self._subscribers.values()):
            if not subscriber.wants(tick.symbol):
                continue
            try:
                await subscriber.send(frame)
            except Exception as exc:
                log.warning("Dropping feed client id=%d: %s", subscriber.id, exc)
                self.detach(subscriber)
                continue
            subscriber.sent += 1
            delivered += 1
        return delivered

    async def publish_all(self, ticks: list[Tick]) -> int:
        """Publish a run of ticks in order. Returns the deliveries of the last one.

        Order is the guarantee that matters: the resampler reads bucket
        advances, so a tick arriving out of sequence is dropped as late.
        """
        delivered = 0
        for tick in ticks:
            delivered = await self.publish(tick)
        return delivered

    # ── Cursors ───────────────────────────────────────────────────────

    def cursor(self, symbol: str, timeframe: str) -> datetime | None:
        return self._cursors.get((symbol.upper(), normalize_timeframe(timeframe)))

    def advance_cursor(self, symbol: str, timeframe: str, open_time: datetime) -> None:
        """Record the newest bar's open time, never moving backwards.

        A command may deliberately place a bar in the past (``--anchor past``)
        while the series has already moved on; that is allowed, but it must not
        rewind the point the *next* ``--anchor next`` continues from.
        """
        key = (symbol.upper(), normalize_timeframe(timeframe))
        current = self._cursors.get(key)
        if current is None or open_time > current:
            self._cursors[key] = open_time

    def cursors(self) -> dict[str, dict[str, str]]:
        out: dict[str, dict[str, str]] = defaultdict(dict)
        for (symbol, timeframe), moment in sorted(self._cursors.items()):
            out[symbol][timeframe] = moment.isoformat()
        return dict(out)

    def reset(self) -> None:
        """Forget the cursors and the counters. Attached clients stay attached."""
        self._cursors.clear()
        self.last_prices.clear()
        self.last_tick_ts.clear()
        self.ticks_sent = 0
        self.bars_sent = 0

    # ── Background generators ─────────────────────────────────────────

    def register_generator(self, name: str, task: asyncio.Task[None]) -> None:
        self._generators[name] = task
        task.add_done_callback(lambda _: self._generators.pop(name, None))

    @property
    def generators(self) -> list[str]:
        return sorted(self._generators)

    async def stop_generators(self, name: str | None = None) -> list[str]:
        """Cancel one generator, or all of them. Returns what was stopped."""
        targets = [name] if name else list(self._generators)
        stopped: list[str] = []
        for key in targets:
            task = self._generators.pop(key, None)
            if task is None:
                continue
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
            stopped.append(key)
        return stopped

    # ── Reporting ─────────────────────────────────────────────────────

    def status(self) -> dict[str, object]:
        return {
            "uptime_seconds": round((datetime.now(UTC) - self.started_at).total_seconds(), 1),
            "ticks_sent": self.ticks_sent,
            "bars_sent": self.bars_sent,
            "clients": [
                {
                    "id": subscriber.id,
                    "remote": subscriber.remote,
                    "symbols": sorted(subscriber.symbols) or ["*"],
                    "ticks": subscriber.sent,
                }
                for subscriber in self.subscribers
            ],
            "generators": self.generators,
            "cursors": self.cursors(),
            "last_prices": dict(sorted(self.last_prices.items())),
        }


__all__ = ["SimulatorHub", "Subscriber"]
