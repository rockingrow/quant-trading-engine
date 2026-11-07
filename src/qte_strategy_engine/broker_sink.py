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
and then simply not sent. It is the paper-trading switch.

**An ambiguous failure is ``unknown``, never ``failed``.** ``failed`` tells the
runner nothing reached the broker, so it may enter again or keep managing a
cycle as still open; ``unknown`` sends the row through outbox reconciliation.
So only errors raised before a request could have left this host — a refused
connection, a payload that fails validation, a 4xx the broker answered with —
are ``failed``. A timeout, a connection dropped after the request was written,
and any 5xx (a proxy's 502 or 504 says nothing about whether the broker acted)
are ``unknown``.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

import httpx

from qte_shared.bus import NatsBus, Subjects
from qte_shared.config import BrokerSettings, settings
from qte_shared.logging_setup import get_logger
from qte_shared.models import BrokerSignal
from qte_shared.strategies.signal_serialization import authenticated_signal, signal_record
from qte_strategy_engine.delivery_status import DeliveryStatus

log = get_logger(__name__)


@dataclass(slots=True)
class DeliveryResult:
    """What happened to one signal on its way out."""

    #: One of ``sent``, ``shadow``, ``failed`` or ``unknown``.
    status: DeliveryStatus
    transport: str
    detail: str = ""

    def __post_init__(self) -> None:
        self.status = DeliveryStatus(self.status)

    @property
    def delivered(self) -> bool:
        return self.status is DeliveryStatus.SENT


#: Raised before a request can have left this host, so nothing reached the broker.
_NOT_SENT_ERRORS: tuple[type[Exception], ...] = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.PoolTimeout,
    httpx.UnsupportedProtocol,
    httpx.InvalidURL,
)


class BrokerSink:
    """Sends signals to the broker over the configured transport."""

    def __init__(
        self,
        transport: str | None = None,
        *,
        bus: NatsBus | None = None,
        shadow_mode: bool | None = None,
        broker_settings: BrokerSettings | None = None,
    ) -> None:
        #: An override for this sink alone; without one every read goes to the
        #: process settings at that moment (see :attr:`_broker`).
        self._broker_override = broker_settings
        self.transport = transport or self._broker.transport
        self._scope = settings.state_scope
        self.shadow_mode = (
            self._scope.is_paper
            or self._broker.force_shadow_mode
            or (self._broker.shadow_mode if shadow_mode is None else shadow_mode)
        )
        self._bus = bus
        self._owns_bus = bus is None
        self._http: httpx.AsyncClient | None = None

    @property
    def _broker(self) -> BrokerSettings:
        """The broker settings, read on every use rather than held.

        So a replaced ``settings.broker`` — a rotated token, a test — reaches
        the next signal, as it did before the sink took an override.
        """
        return self._broker_override or settings.broker

    @property
    def force_shadow_mode(self) -> bool:
        """Whether ``ALGO_BROKER__FORCE_SHADOW_MODE`` holds this sink in shadow.

        The runner reads it from here rather than from the settings, so the two
        agree on the switch even when this sink was given its own settings.
        """
        return self._broker.force_shadow_mode

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
                base_url=self._broker.http_url.rstrip("/"),
                timeout=self._broker.publish_timeout,
            )
        log.info(
            "Broker sink ready transport=%s shadow_mode=%s target=%s",
            self.transport,
            self.shadow_mode,
            settings.broker_nats_url if self.transport == "nats" else self._broker.http_url,
        )

    async def stop(self) -> None:
        if self._http is not None:
            await self._http.aclose()
            self._http = None
        if self._bus is not None and self._owns_bus:
            await self._bus.close()
            self._bus = None

    def set_shadow_mode(self, enabled: bool) -> None:
        enabled = enabled or self._scope.is_paper or self._broker.force_shadow_mode
        log.warning(
            "Shadow mode %s",
            "ENABLED — signals will NOT reach the broker"
            if enabled
            else "DISABLED — signals are going live",
        )
        self.shadow_mode = enabled

    # ── Sending ───────────────────────────────────────────────────────

    async def send(self, signal: BrokerSignal, *, delivery_id: str | None = None) -> DeliveryResult:
        """Deliver *signal*, or record why it did not go.

        Never raises. A delivery failure is returned so the caller can audit it
        and carry on: an exception here would take down the runner and stop
        every other strategy from trading over one bad publish. A payload that
        fails validation is ``failed``: nothing was sent, and calling it
        ``unknown`` would block the pair for a reconciliation that has nothing to
        find.
        """
        try:
            signal.validate_shape()
        except ValueError as invalid:
            log.error(
                "Signal NOT SENT transport=%s strategy=%s uxid=%s: invalid payload: %s",
                self.transport,
                signal.strategy,
                signal.signal_uxid,
                invalid,
            )
            return DeliveryResult(
                status=DeliveryStatus.FAILED, transport=self.transport, detail=str(invalid)
            )

        if not self._scope.is_paper and (self.shadow_mode or self._broker.force_shadow_mode):
            return DeliveryResult(
                status=DeliveryStatus.FAILED,
                transport=self.transport,
                detail="Live delivery is paused",
            )
        if self._scope.is_paper:
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
            return DeliveryResult(status=DeliveryStatus.SHADOW, transport=self.transport)

        try:
            if self.transport == "nats":
                detail = await self._send_nats(signal, delivery_id=delivery_id)
            else:
                detail = await self._send_http(signal, delivery_id=delivery_id)
        except _NOT_SENT_ERRORS as failure:
            return self._report_failed(signal, failure)
        except httpx.HTTPStatusError as failure:
            # The broker (or something in front of it) answered. A 4xx is the
            # broker refusing this request; a 5xx is a server that may or may
            # not have acted before it failed.
            if failure.response.status_code < 500:
                return self._report_failed(signal, failure)
            return self._report_unknown(signal, failure)
        except (TimeoutError, httpx.TimeoutException, httpx.TransportError) as failure:
            # Not a negative acknowledgement. The remote side may have stored
            # the request and only lost the response, so calling it "failed"
            # would invite a fresh entry against an already-open position.
            # (Connect-phase errors were caught above: those sent nothing.)
            return self._report_unknown(signal, failure)
        except Exception as failure:  # noqa: BLE001 - the contract is "never raises"
            return self._report_failed(signal, failure)

        log.info(
            "Signal sent transport=%s %s %s %s uxid=%s (%s)",
            self.transport,
            signal.strategy,
            signal.position.action.value,
            signal.symbol,
            signal.signal_uxid,
            detail,
        )
        return DeliveryResult(status=DeliveryStatus.SENT, transport=self.transport, detail=detail)

    def _report_unknown(self, signal: BrokerSignal, failure: BaseException) -> DeliveryResult:
        log.error(
            "Signal delivery UNKNOWN transport=%s strategy=%s uxid=%s: %s",
            self.transport,
            signal.strategy,
            signal.signal_uxid,
            failure,
        )
        return DeliveryResult(
            status=DeliveryStatus.UNKNOWN, transport=self.transport, detail=str(failure)
        )

    def _report_failed(self, signal: BrokerSignal, failure: BaseException) -> DeliveryResult:
        log.error(
            "Signal delivery FAILED transport=%s strategy=%s uxid=%s: %s",
            self.transport,
            signal.strategy,
            signal.signal_uxid,
            failure,
        )
        return DeliveryResult(
            status=DeliveryStatus.FAILED, transport=self.transport, detail=str(failure)
        )

    async def _send_nats(self, signal: BrokerSignal, *, delivery_id: str | None = None) -> str:
        if self._bus is None:
            raise RuntimeError("Broker sink was not started")
        subject = Subjects.broker_signal(signal.strategy)
        # Stable across retries of this outbox row.  If an ack is lost after
        # JetStream stored the message, replaying the row produces the same
        # Nats-Msg-Id and the stream returns a duplicate ack rather than a
        # second trade command.
        ack = await self._bus.publish_jetstream(
            subject,
            {"payload": authenticated_signal(signal, self._broker.token)},
            msg_id=delivery_id or signal_delivery_id(signal),
            timeout=self._broker.publish_timeout,
        )
        return (
            f"subject={subject} seq={getattr(ack, 'seq', None)} "
            f"duplicate={getattr(ack, 'duplicate', False)}"
        )

    async def _send_http(self, signal: BrokerSignal, *, delivery_id: str | None = None) -> str:
        if self._http is None:
            raise RuntimeError("Broker sink was not started")
        response = await self._http.post(
            "/secret/webhook",
            json=authenticated_signal(signal, self._broker.token),
            headers={"Idempotency-Key": delivery_id or signal_delivery_id(signal)},
        )
        response.raise_for_status()
        return f"http {response.status_code}"


def signal_delivery_id(signal: BrokerSignal) -> str:
    """Deterministic fallback id for callers that do not own an outbox row."""
    serialized = json.dumps(signal_record(signal), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode()).hexdigest()
