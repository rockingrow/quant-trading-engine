"""A thin, reconnect-aware NATS wrapper shared by every service.

Core NATS carries market data: at 20 ticks a second a dropped message is
replaced by a fresher one a moment later, so paying JetStream's persistence
cost for it would buy nothing. Signals are the opposite — losing one loses a
trade — and those go out over JetStream, which is why :meth:`publish_jetstream`
exists alongside :meth:`publish`.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Any

import nats
from nats.aio.client import Client as NATSClient
from nats.aio.msg import Msg
from nats.js import JetStreamContext
from nats.js import api as js_api

from qte_shared.config import settings
from qte_shared.logging_setup import get_logger

log = get_logger(__name__)

MessageHandler = Callable[[Msg], Awaitable[None]]


class NatsBus:
    """Owns one NATS connection and its JetStream context."""

    def __init__(
        self,
        url: str | None = None,
        token: str | None = None,
        name: str = "qte",
    ) -> None:
        self._url = url or settings.nats.url
        self._token = token if token is not None else settings.nats.token
        self._name = name
        self._nc: NATSClient | None = None
        self._js: JetStreamContext | None = None
        self._subscriptions: list[Any] = []

    # ── Lifecycle ─────────────────────────────────────────────────────

    @property
    def is_connected(self) -> bool:
        return self._nc is not None and self._nc.is_connected

    @property
    def nc(self) -> NATSClient:
        if self._nc is None:
            raise RuntimeError("NATS is not connected — call connect() first")
        return self._nc

    @property
    def js(self) -> JetStreamContext:
        """JetStream context, created lazily on first use."""
        if self._js is None:
            self._js = self.nc.jetstream()
        return self._js

    async def connect(self) -> None:
        if self.is_connected:
            return
        options: dict[str, Any] = {
            "servers": [self._url],
            "name": self._name,
            "connect_timeout": settings.nats.connect_timeout,
            "max_reconnect_attempts": settings.nats.max_reconnect_attempts,
            "reconnect_time_wait": settings.nats.reconnect_time_wait,
            "disconnected_cb": self._on_disconnected,
            "reconnected_cb": self._on_reconnected,
            "error_cb": self._on_error,
        }
        if self._token:
            options["token"] = self._token

        # nats-py applies ``max_reconnect_attempts`` to the *initial* connect as
        # well, so -1 (retry forever once we are up, which is what we want for a
        # live feed) would make a first connect against a dead server block
        # indefinitely. Bounding it here keeps the two behaviours separate: a
        # startup that cannot reach NATS fails fast and the caller decides
        # whether that is fatal, while a connection that was once established
        # still reconnects without limit.
        try:
            self._nc = await asyncio.wait_for(
                nats.connect(**options), timeout=settings.nats.connect_timeout
            )
        except TimeoutError as exc:
            raise ConnectionError(
                f"NATS at {self._url} did not answer within {settings.nats.connect_timeout:.1f}s"
            ) from exc
        log.info("NATS connected url=%s name=%s", self._url, self._name)

    async def close(self) -> None:
        if self._nc is None:
            return
        try:
            # Drain rather than close: in-flight publishes and queued messages
            # for our subscriptions get flushed instead of dropped on shutdown.
            await self._nc.drain()
        except Exception as exc:  # pragma: no cover - shutdown best effort
            log.warning("NATS drain failed, closing hard: %s", exc)
            await self._nc.close()
        finally:
            self._nc = None
            self._js = None
            self._subscriptions.clear()
            log.info("NATS closed name=%s", self._name)

    async def _on_disconnected(self) -> None:
        log.warning("NATS disconnected name=%s", self._name)

    async def _on_reconnected(self) -> None:
        log.info("NATS reconnected name=%s url=%s", self._name, self._url)

    async def _on_error(self, exc: Exception) -> None:
        log.error("NATS error name=%s: %s", self._name, exc)

    # ── Publish ───────────────────────────────────────────────────────

    async def publish(self, subject: str, payload: dict[str, Any] | bytes) -> None:
        """Fire-and-forget core publish — used for ticks and candle closes."""
        data = payload if isinstance(payload, bytes) else _encode(payload)
        await self.nc.publish(subject, data)

    async def publish_jetstream(
        self,
        subject: str,
        payload: dict[str, Any] | bytes,
        *,
        msg_id: str | None = None,
        timeout: float | None = None,
    ) -> js_api.PubAck:
        """Persisted publish, acknowledged by the stream.

        *msg_id* rides as ``Nats-Msg-Id``: inside the stream's duplicate window
        JetStream drops a second copy carrying an id it already stored, so a
        retry of a publish whose ack was merely slow cannot deliver the same
        signal to a worker twice.

        Raises ``ConnectionError`` when there is no live connection, because
        nats-py would otherwise buffer the write and let the caller sit out the
        full timeout waiting for an ack that can never arrive.
        """
        if not self.is_connected:
            raise ConnectionError(f"NATS ({self._url}) is not connected")
        data = payload if isinstance(payload, bytes) else _encode(payload)
        headers = {js_api.Header.MSG_ID.value: msg_id} if msg_id else None
        return await self.js.publish(subject, data, timeout=timeout, headers=headers)

    async def request(
        self, subject: str, payload: dict[str, Any] | bytes, timeout: float = 5.0
    ) -> dict[str, Any]:
        data = payload if isinstance(payload, bytes) else _encode(payload)
        reply = await self.nc.request(subject, data, timeout=timeout)
        return json.loads(reply.data)

    # ── Subscribe ─────────────────────────────────────────────────────

    async def subscribe(self, subject: str, handler: MessageHandler, queue: str = "") -> Any:
        """Subscribe with a handler wrapped so one bad message cannot kill the loop."""

        async def guarded(msg: Msg) -> None:
            try:
                await handler(msg)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Handler failed for subject=%s", msg.subject)

        subscription = await self.nc.subscribe(subject, cb=guarded, queue=queue)
        self._subscriptions.append(subscription)
        log.info("Subscribed subject=%s queue=%s", subject, queue or "-")
        return subscription

    async def ensure_stream(
        self, name: str, subjects: list[str], duplicate_window: float = 120.0
    ) -> None:
        """Create the stream if absent; leave an existing one alone.

        QTE never reconfigures a stream it did not create — the broker owns
        ``SIGNALS`` and updating its retention from here would be QTE quietly
        editing another service's durability guarantees.
        """
        try:
            await self.js.stream_info(name)
            log.debug("JetStream stream %s already exists", name)
            return
        except Exception:
            pass
        try:
            await self.js.add_stream(
                js_api.StreamConfig(
                    name=name,
                    subjects=subjects,
                    retention=js_api.RetentionPolicy.LIMITS,
                    storage=js_api.StorageType.FILE,
                    duplicate_window=duplicate_window,
                )
            )
            log.info("JetStream stream created name=%s subjects=%s", name, subjects)
        except Exception as exc:
            # Losing a create race with the broker (or another QTE replica) is
            # the expected outcome, not a failure.
            log.info("Could not create stream %s (may already exist): %s", name, exc)


def _encode(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, default=str, separators=(",", ":")).encode()
