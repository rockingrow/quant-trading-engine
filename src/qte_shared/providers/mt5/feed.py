"""Closed bars from ``algo-trading-ingester``, consumed off NATS.

The ingester streams continuously and must never wait on QTE. So receiving and
processing are two tasks joined by a bounded queue:

* the **receiver** takes a message, puts its raw bytes on the queue and
  acknowledges it at once — a JetStream ``+ACK``, or ``{"code": 200}`` on the
  reply subject when a publisher asked for one over core NATS. It never parses,
  never touches Redis, never awaits the engine;
* the **worker** drains the queue: decode, filter to the planned series, hand
  the :class:`~qte_shared.models.Candle` to ingestion.

Neither mode lets a slow QTE block the ingester in the first place — a
JetStream publish is acknowledged by the *stream*, and core NATS does not wait
on subscribers at all. What acknowledging on receipt adds is that QTE never
holds a message long enough for JetStream to redeliver it, and a request-style
publisher gets its answer before any work is done.

The price is that an acknowledged bar lives only in memory until the worker
reaches it, so a crash in that window loses it. The window is one queue of
bars, not ticks, and ingestion retains a bar as soon as it is handed over
(:meth:`~qte_ingestion.service.IngestionService._emit_candles`).

Backpressure is the pull itself: the receiver asks the stream for no more
messages than the queue has room for. It never has to refuse a message it has
already taken, so "acked" always means "held".
"""

from __future__ import annotations

import asyncio
import json
import random
from collections.abc import Callable

import nats.errors
from nats.aio.msg import Msg
from nats.js import api as js_api
from nats.js.errors import NotFoundError

from qte_shared.bus.nats_bus import NatsBus
from qte_shared.interfaces.market_data import CandleHandler, LiveFeed, ProviderError
from qte_shared.logging_setup import get_logger
from qte_shared.providers.mt5.protocol import IngesterPayloadError, decode_bar_closed
from qte_shared.providers.mt5.settings import Mt5Settings

log = get_logger(__name__)

#: The core-NATS answer to a publisher that asked for one: received, not yet
#: processed.
ACCEPTED_REPLY = json.dumps({"code": 200, "status": "accepted"}).encode()
#: The answer when the queue is full; core NATS cannot redeliver, so say so.
BUSY_REPLY = json.dumps({"code": 503, "status": "queue full"}).encode()

#: How long the receiver waits for queue room before pulling again.
_ROOM_POLL_SECONDS = 0.05


class IngesterBarFeed(LiveFeed):
    """Streams the ingester's closed bars into a candle handler until stopped."""

    def __init__(
        self,
        series: dict[str, tuple[str, ...]],
        on_bar: CandleHandler,
        config: Mt5Settings,
        bus_factory: Callable[[], NatsBus] | None = None,
    ) -> None:
        """*series* maps each planned symbol to the timeframes it is fed."""
        self.name = f"{config.gateway}-ingester"
        self._series = {
            symbol.upper(): frozenset(timeframes) for symbol, timeframes in series.items()
        }
        self._on_bar = on_bar
        self._config = config
        self._bus_factory = bus_factory or self._default_bus
        self._bus: NatsBus | None = None
        self._inbox: asyncio.Queue[bytes] = asyncio.Queue(maxsize=config.max_pending_bars)
        self._running = False
        self._attempt = 0
        self._receiver: asyncio.Task[None] | None = None
        self._worker: asyncio.Task[None] | None = None
        #: Acknowledged messages, and the bars that reached the handler — a gap
        #: between the two is unplanned series or payloads that did not decode.
        self.messages_received = 0
        self.bars_delivered = 0

    # ── Lifecycle ─────────────────────────────────────────────────────

    @property
    def symbols(self) -> tuple[str, ...]:
        return tuple(sorted(self._series))

    def start(self) -> asyncio.Task[None] | None:
        if not self._series:
            log.info("MT5 ingester feed has no symbols — not subscribing")
            return None
        self._running = True
        self._worker = asyncio.create_task(self._process(), name=f"{self.name}-worker")
        self._receiver = asyncio.create_task(self._receive(), name=self.name)
        return self._receiver

    async def stop(self) -> None:
        self._running = False
        for task in (self._receiver, self._worker):
            if task is None:
                continue
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._receiver = None
        self._worker = None
        await self._close_bus()

    def _default_bus(self) -> NatsBus:
        return NatsBus(
            url=self._config.server_url,
            token=self._config.server_token,
            name=f"qte-{self.name}",
        )

    async def _close_bus(self) -> None:
        bus, self._bus = self._bus, None
        if bus is None:
            return
        try:
            await bus.close()
        except Exception:
            log.warning("Closing the %s NATS connection failed", self.name, exc_info=True)

    # ── Receiver: take, hold, acknowledge ─────────────────────────────

    async def _receive(self) -> None:
        """Connect and consume, reconnecting with capped backoff until stopped."""
        while self._running:
            try:
                self._bus = self._bus_factory()
                await self._bus.connect()
                if self._config.jetstream:
                    await self._consume_stream(self._bus)
                else:
                    await self._consume_core(self._bus)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if not self._running:
                    return
                self._attempt += 1
                delay = self._backoff(self._attempt)
                log.warning(
                    "MT5 ingester feed dropped (attempt %d): %s — retrying in %.1fs",
                    self._attempt,
                    exc,
                    delay,
                )
                await self._close_bus()
                await asyncio.sleep(delay)

    async def _consume_stream(self, bus: NatsBus) -> None:
        """Pull from the ingester's stream through a durable consumer."""
        stream = self._config.stream
        try:
            await bus.js.stream_info(stream)
        except NotFoundError as exc:
            # The ingester creates its stream on its first JetStream connect.
            # QTE must not: the duplicate window and retention are its promises.
            raise ProviderError(
                f"JetStream stream {stream!r} does not exist on {self._config.server_url} — "
                "start algo-trading-ingester with NATS_JETSTREAM_ENABLED=true, or check "
                "QTE_MT5__STREAM against its NATS_STREAM_NAME"
            ) from exc

        subscription = await bus.js.pull_subscribe(
            self._config.subject_filter,
            durable=self._config.consumer_name,
            stream=stream,
            config=js_api.ConsumerConfig(
                ack_policy=js_api.AckPolicy.EXPLICIT,
                deliver_policy=js_api.DeliverPolicy(self._config.deliver_policy),
            ),
        )
        self._attempt = 0
        log.info(
            "MT5 ingester feed open stream=%s consumer=%s subject=%s symbols=%s",
            stream,
            self._config.consumer_name,
            self._config.subject_filter,
            ",".join(self.symbols),
        )
        while self._running:
            room = self._inbox.maxsize - self._inbox.qsize()
            if room <= 0:
                await asyncio.sleep(_ROOM_POLL_SECONDS)
                continue
            try:
                messages = await subscription.fetch(
                    min(self._config.fetch_batch, room), timeout=self._config.fetch_timeout
                )
            except nats.errors.TimeoutError:
                continue
            for message in messages:
                # Held first, acknowledged second: an ack that fails leaves a
                # redelivery, which the Redis stage drops as a duplicate.
                self._inbox.put_nowait(message.data)
                self.messages_received += 1
                await message.ack()

    async def _consume_core(self, bus: NatsBus) -> None:
        """Subscribe on core NATS; nothing is replayed after an outage."""
        await bus.subscribe(self._config.subject_filter, self._on_core_message)
        self._attempt = 0
        log.info(
            "MT5 ingester feed open (core NATS, no replay) subject=%s symbols=%s",
            self._config.subject_filter,
            ",".join(self.symbols),
        )
        while self._running and not bus.nc.is_closed:
            await asyncio.sleep(1.0)
        if self._running:
            raise ConnectionError("the NATS connection closed")

    async def _on_core_message(self, message: Msg) -> None:
        if self._inbox.full():
            log.warning(
                "MT5 ingester queue is full (%d bars) — dropping %s",
                self._inbox.maxsize,
                message.subject,
            )
            if message.reply:
                await message.respond(BUSY_REPLY)
            return
        self._inbox.put_nowait(message.data)
        self.messages_received += 1
        if message.reply:
            await message.respond(ACCEPTED_REPLY)

    def _backoff(self, attempt: int) -> float:
        base = min(self._config.max_backoff_seconds, 2.0 ** min(attempt, 6))
        return base * (0.5 + random.random() / 2)

    # ── Worker: decode, filter, hand over ─────────────────────────────

    async def _process(self) -> None:
        while True:
            raw = await self._inbox.get()
            try:
                await self._handle_raw(raw)
            finally:
                self._inbox.task_done()

    async def _handle_raw(self, raw: bytes) -> None:
        try:
            bar = decode_bar_closed(raw)
        except IngesterPayloadError as exc:
            log.warning("MT5 ingester sent an unusable message: %s — %.160r", exc, raw)
            return
        candle = bar.candle
        if candle.timeframe not in self._series.get(candle.symbol, ()):
            log.debug("Ignoring unplanned series %s %s", candle.symbol, candle.timeframe)
            return
        # The handler's failures are the handler's: letting one escape would end
        # the worker, and every bar after it would sit acknowledged in a queue
        # nothing reads.
        try:
            await self._on_bar(candle)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception(
                "Bar handler failed event_id=%s symbol=%s tf=%s open_time=%s",
                bar.event_id,
                candle.symbol,
                candle.timeframe,
                candle.open_time.isoformat(),
            )
            return
        self.bars_delivered += 1
