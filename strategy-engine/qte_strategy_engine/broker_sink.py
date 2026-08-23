"""Delivering a signal to ``algo-trading-broker``.

Two transports, one payload. Both hand the broker exactly the bytes its
``WebhookPayload`` validator expects:

* **nats** (default) — publish ``{"payload": {...}}`` to JetStream subject
  ``SIGNALS.<strategy>``. That is the same durable buffer the broker's own HTTP
  endpoint writes to and its ``SignalWorker`` consumes, so we get the broker's
  persistence, retry and de-duplication without an HTTP hop in the trade path.
* **http** — ``POST {broker}/secret/webhook``. Slower, but it is the path that
  verifies the ``token`` field, so it is the right choice when QTE and the
  broker do not share a trusted NATS cluster.

Publishing straight to the broker's JetStream subject bypasses that token
check: on the NATS transport, access to the cluster *is* the authentication.
Keep the broker's NATS on a private network (or set a NATS token) if you run
that way, and use the HTTP transport across any boundary you do not control.

Shadow mode short-circuits both: the signal is built, validated and audited,
and then simply not sent. It is the phase-6 paper-trading switch.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass

import httpx
from qte_shared.bus import NatsBus, Subjects
from qte_shared.config import settings
from qte_shared.logging_setup import get_logger
from qte_shared.models import BrokerSignal

log = get_logger(__name__)


@dataclass(slots=True)
class DeliveryResult:
    """What happened to one signal on its way out."""

    status: str  # "sent" | "shadow" | "failed"
    transport: str
    detail: str = ""

    @property
    def delivered(self) -> bool:
        return self.status == "sent"


class BrokerSink:
    """Sends signals to the broker over the configured transport."""

    def __init__(
        self,
        transport: str | None = None,
        *,
        bus: NatsBus | None = None,
        shadow_mode: bool | None = None,
    ) -> None:
        self.transport = transport or settings.broker.transport
        self.shadow_mode = settings.broker.shadow_mode if shadow_mode is None else shadow_mode
        self._bus = bus
        self._owns_bus = bus is None
        self._http: httpx.AsyncClient | None = None

    # ── Lifecycle ─────────────────────────────────────────────────────

    async def start(self) -> None:
        if self.transport == "nats":
            if self._bus is None:
                self._bus = NatsBus(
                    url=settings.broker_nats_url,
                    token=settings.broker_nats_token,
                    name="qte-broker-sink",
                )
            await self._bus.connect()
        else:
            self._http = httpx.AsyncClient(
                base_url=settings.broker.http_url.rstrip("/"),
                timeout=settings.broker.publish_timeout,
            )
        log.info(
            "Broker sink ready transport=%s shadow_mode=%s target=%s",
            self.transport,
            self.shadow_mode,
            settings.broker_nats_url if self.transport == "nats" else settings.broker.http_url,
        )

    async def stop(self) -> None:
        if self._http is not None:
            await self._http.aclose()
            self._http = None
        if self._bus is not None and self._owns_bus:
            await self._bus.close()
            self._bus = None

    def set_shadow_mode(self, enabled: bool) -> None:
        log.warning(
            "Shadow mode %s",
            "ENABLED — signals will NOT reach the broker"
            if enabled
            else "DISABLED — signals are going live",
        )
        self.shadow_mode = enabled

    # ── Sending ───────────────────────────────────────────────────────

    async def send(self, signal: BrokerSignal) -> DeliveryResult:
        """Deliver *signal*, or record why it did not go.

        Never raises. A delivery failure is returned so the caller can audit it
        and carry on: an exception here would take down the runner and stop
        every other strategy from trading over one bad publish.
        """
        signal.validate_shape()

        if self.shadow_mode:
            log.info(
                "SHADOW %s %s %s price=%s qty=%s sl=%s tp1=%s tp2=%s uxid=%s",
                signal.strategy,
                signal.position.action.value,
                signal.symbol,
                signal.position.price,
                signal.position.quantity,
                signal.position.sl,
                signal.position.tp1,
                signal.position.tp2,
                signal.signal_uxid,
            )
            return DeliveryResult(status="shadow", transport=self.transport)

        try:
            if self.transport == "nats":
                detail = await self._send_nats(signal)
            else:
                detail = await self._send_http(signal)
        except Exception as exc:
            log.error(
                "Signal delivery FAILED transport=%s strategy=%s uxid=%s: %s",
                self.transport,
                signal.strategy,
                signal.signal_uxid,
                exc,
            )
            return DeliveryResult(status="failed", transport=self.transport, detail=str(exc))

        log.info(
            "Signal sent transport=%s %s %s %s uxid=%s (%s)",
            self.transport,
            signal.strategy,
            signal.position.action.value,
            signal.symbol,
            signal.signal_uxid,
            detail,
        )
        return DeliveryResult(status="sent", transport=self.transport, detail=detail)

    async def _send_nats(self, signal: BrokerSignal) -> str:
        if self._bus is None:
            raise RuntimeError("Broker sink was not started")
        subject = Subjects.broker_signal(signal.strategy)
        # A fresh Nats-Msg-Id per signal, inside the stream's duplicate window:
        # if an ack is slow and something retries the publish, JetStream stores
        # one copy and the worker opens one position.
        ack = await self._bus.publish_jetstream(
            subject,
            signal.to_envelope(),
            msg_id=uuid.uuid4().hex,
            timeout=settings.broker.publish_timeout,
        )
        return (
            f"subject={subject} seq={getattr(ack, 'seq', None)} "
            f"duplicate={getattr(ack, 'duplicate', False)}"
        )

    async def _send_http(self, signal: BrokerSignal) -> str:
        if self._http is None:
            raise RuntimeError("Broker sink was not started")
        response = await self._http.post("/secret/webhook", json=signal.model_dump(mode="json"))
        response.raise_for_status()
        return f"http {response.status_code} {response.json()}"
