"""Wire models shared by every QTE service.

Two families live here and they must not be confused:

* **Market data** — :class:`Tick` and :class:`Candle`, QTE's own internal
  vocabulary, published on the ``QTE.*`` NATS subjects.
* **The broker contract** — :class:`BrokerSignal` and its nested blocks, which
  mirror ``broker/schemas/webhook_schema.py`` in ``rockingrow/algo-trading-broker``
  field for field. That repo is the source of truth: it validates what we send
  and rejects anything off-shape, so every change here starts by re-reading its
  ``WebhookPayload``. ``tests/test_broker_contract.py`` pins the shape.
"""

from __future__ import annotations

import re
import uuid
from datetime import UTC, datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

# ── Correlation ids ───────────────────────────────────────────────────────

#: The broker accepts exactly 16 uppercase alphanumeric characters and 422s on
#: anything else, so QTE mints ids in that shape rather than discovering the
#: constraint at delivery time.
UXID_LENGTH = 16
UXID_PATTERN = re.compile(rf"^[0-9A-Z]{{{UXID_LENGTH}}}$")


def new_uxid() -> str:
    """Fresh trade-cycle id, e.g. ``"9F2C4B7E18A3D605"``.

    One value is shared by an entry and every TP/SL/FLAT that follows it — it
    is how the broker groups a whole trade into a single broadcast message.
    """
    return uuid.uuid4().hex[:UXID_LENGTH].upper()


def is_valid_uxid(value: str) -> bool:
    return bool(UXID_PATTERN.fullmatch(value))


def utcnow() -> datetime:
    return datetime.now(UTC)


# ── Market data ───────────────────────────────────────────────────────────


class Tick(BaseModel):
    """A single quote update off the ingestion feed."""

    model_config = ConfigDict(frozen=True)

    symbol: str
    ts: datetime
    bid: float | None = None
    ask: float | None = None
    last: float | None = None
    volume: float = 0.0

    @property
    def price(self) -> float:
        """Mid price when both sides are quoted, else whatever side we have.

        FX ticks from Tiingo carry bid/ask and no trade price; crypto ticks
        carry a last price. Strategies should not have to care which.
        """
        if self.last is not None:
            return self.last
        if self.bid is not None and self.ask is not None:
            return (self.bid + self.ask) / 2
        side = self.bid if self.bid is not None else self.ask
        if side is None:
            raise ValueError(f"Tick for {self.symbol} at {self.ts} carries no price")
        return side


class Candle(BaseModel):
    """One completed OHLCV bar, keyed by its **open** time."""

    model_config = ConfigDict(frozen=True)

    symbol: str
    timeframe: str
    open_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    tick_count: int = 0
    is_closed: bool = True


class CandleClosedEvent(BaseModel):
    """Payload of a ``QTE.candle.closed.<symbol>.<timeframe>`` message."""

    symbol: str
    timeframe: str
    candle: Candle
    published_at: datetime = Field(default_factory=utcnow)


class TickEvent(BaseModel):
    """Payload of a ``QTE.tick.<symbol>`` message."""

    symbol: str
    tick: Tick
    published_at: datetime = Field(default_factory=utcnow)


# ── Broker contract (mirrors algo-trading-broker WebhookPayload) ──────────


class SignalAction(str, Enum):
    """Trade actions the broker's ``SignalActionEnum`` accepts."""

    LONG = "LONG"
    SHORT = "SHORT"
    TP1 = "TP1"
    TP2 = "TP2"
    R_SL = "R_SL"
    SL = "SL"
    FLAT = "FLAT"

    @property
    def is_entry(self) -> bool:
        return self in (SignalAction.LONG, SignalAction.SHORT)

    @property
    def is_close(self) -> bool:
        return not self.is_entry


class Scaling(BaseModel):
    """Levels and size used when adding to an existing position."""

    tp: float | None = None
    sl: float | None = None
    quantity: float | None = None


class PositionBlock(BaseModel):
    """The ``position`` block of a broker webhook payload.

    ``price``/``quantity`` are optional on the wire because a ``FLAT`` carries
    neither — it means "close everything on this strategy". Entries do need
    both, which :meth:`BrokerSignal.validate_shape` enforces before we publish.
    """

    action: SignalAction
    price: float | None = None
    quantity: float | None = None
    sl: float | None = None
    tp1: float | None = None
    tp2: float | None = None
    risk_percent: float | None = None
    tp1_percent: float | None = None
    move_sl_to_be: bool | None = None
    is_running: bool | None = None
    is_scale_position: bool | None = None
    scale_strategy: str | None = None
    scaling: Scaling | None = None


class BrokerSignal(BaseModel):
    """A signal in exactly the shape ``algo-trading-broker`` ingests.

    It travels either as the JSON body of ``POST {broker}/secret/webhook`` or,
    on the NATS transport, wrapped in ``{"payload": ...}`` and published to the
    broker's JetStream subject ``SIGNALS.<strategy>``. Both routes hand the
    broker the same bytes; see :mod:`qte_strategy_engine.broker_sink`.
    """

    model_config = ConfigDict(use_enum_values=False)

    strategy: str
    symbol: str
    timeframe: str
    timestamp: datetime = Field(default_factory=utcnow)
    signal_uxid: str = Field(default_factory=new_uxid)
    position: PositionBlock
    indicators: dict[str, Any] = Field(default_factory=dict)
    inputs: dict[str, Any] = Field(default_factory=dict)
    token: str = ""

    @field_validator("signal_uxid", mode="before")
    @classmethod
    def _fill_or_validate_uxid(cls, value: Any) -> str:
        """Mint one when absent, normalise case, reject anything malformed.

        Same rule as the broker's own validator: a bad id could collide with a
        live cycle and silently merge two unrelated trades, so it fails here —
        in our own process, with our own stack trace — rather than as a 422
        from the broker after the trade decision has already been made.
        """
        if value is None:
            return new_uxid()
        text = str(value).strip()
        if not text:
            return new_uxid()
        normalised = text.upper()
        if not is_valid_uxid(normalised):
            raise ValueError(
                f"signal_uxid must be exactly {UXID_LENGTH} uppercase alphanumeric "
                f"characters, got {value!r}"
            )
        return normalised

    def validate_shape(self) -> None:
        """Raise when the payload is one the broker would take but a worker cannot fill.

        The broker's schema marks ``price``/``quantity`` optional so a FLAT can
        omit them, which means an entry missing its size passes validation and
        only fails at the worker, one hop too late.
        """
        if self.position.action.is_entry:
            if self.position.price is None or self.position.quantity is None:
                raise ValueError(
                    f"{self.position.action.value} signal for {self.symbol} needs both "
                    "price and quantity"
                )
            if self.position.quantity <= 0:
                raise ValueError(f"quantity must be positive, got {self.position.quantity}")

    def to_envelope(self) -> dict[str, Any]:
        """Wrap into the JetStream envelope the broker's ``SignalWorker`` consumes."""
        return {"payload": self.model_dump(mode="json")}
