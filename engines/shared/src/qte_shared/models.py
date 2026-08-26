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

    model_config = ConfigDict(frozen=True, allow_inf_nan=False)

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

    model_config = ConfigDict(frozen=True, allow_inf_nan=False)

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

    model_config = ConfigDict(allow_inf_nan=False)

    tp: float | None = None
    sl: float | None = None
    quantity: float | None = None


class PositionBlock(BaseModel):
    """The ``position`` block of a broker webhook payload.

    ``price``/``quantity`` are optional on the wire because a ``FLAT`` carries
    neither — it means "close everything on this strategy". Entries do need
    both, which :meth:`BrokerSignal.validate_shape` enforces before we publish.

    ``quantity`` is the engine's number, not the strategy's: it is risk-sized
    against the configured account by :class:`~qte_shared.sizing.PositionSizer`
    on the way through :class:`~qte_shared.signal_factory.SignalFactory`.
    """

    model_config = ConfigDict(allow_inf_nan=False)

    action: SignalAction
    price: float | None = None
    quantity: float | None = None
    sl: float | None = None
    tp1: float | None = None
    tp2: float | None = None
    risk_percent: float | None = None
    tp1_percent: float | None = None
    move_sl_to_be: bool | None = None
    #: Mirrors ``inputs.use_equity_sizing`` so the broker sees the pair's
    #: configured sizing mode. It is reported, not obeyed: QTE sizes off the
    #: fixed ``QTE_ACCOUNT__CAPITAL`` either way — see :mod:`qte_shared.sizing`.
    use_equity_sizing: bool | None = None
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
            self._validate_bracket()

    def _validate_bracket(self) -> None:
        """Refuse an entry whose stop or targets sit on the wrong side of it.

        A long stopped above its own entry is stopped out on the fill, and
        because the default first target is one R away, an inverted stop puts
        TP1 *exactly on* the stop — an order no worker can resolve. Nothing
        upstream catches it: ``BracketPolicy`` derives targets from
        ``abs(price - sl)``, so it mirrors a wrong-side stop instead of
        rejecting it, and the broker's schema is happy with any three numbers.

        Cheap to hit — a sign error in an ATR stop, or a short's levels applied
        to a long — and expensive to discover at the fill.
        """
        position = self.position
        price = position.price
        if price is None:
            return

        # A long loses below its entry and profits above it; a short is the
        # mirror, so one sign flips every comparison. Equality is left alone —
        # a level exactly at entry is degenerate but not wrong, and a stop
        # moved to breakeven is a real thing.
        long = position.action is SignalAction.LONG
        side = "LONG" if long else "SHORT"
        loss_side, profit_side = ("below", "above") if long else ("above", "below")
        direction = 1.0 if long else -1.0

        if position.sl is not None and (position.sl - price) * direction > 0:
            raise ValueError(
                f"{side} {self.symbol} entered at {price} has its stop at {position.sl}, "
                f"which is not {loss_side} the entry — it would be stopped out on the fill"
            )
        for name, level in (("tp1", position.tp1), ("tp2", position.tp2)):
            if level is not None and (level - price) * direction < 0:
                raise ValueError(
                    f"{side} {self.symbol} entered at {price} has {name} at {level}, "
                    f"which is not {profit_side} the entry — that target is a loss"
                )

    def to_envelope(self) -> dict[str, Any]:
        """Wrap into the JetStream envelope the broker's ``SignalWorker`` consumes."""
        return {"payload": self.model_dump(mode="json")}


# ── Position state (QTE's own, never sent to the broker) ──────────────────


#: Actions that end a trade cycle outright, whatever size they name. ``TP1`` is
#: absent on purpose — it is a *partial* by contract, and only ends the cycle
#: when it happens to take the whole entry quantity, which is a question about
#: size rather than about the action. :meth:`OpenPosition.apply_close` answers
#: it in the one place that knows the remaining size.
TERMINAL_ACTIONS: frozenset[SignalAction] = frozenset(
    {SignalAction.TP2, SignalAction.SL, SignalAction.R_SL, SignalAction.FLAT}
)

#: Below this, a remaining size is rounding dust rather than a live position.
#: Quantities reach the wire rounded to ``QTE_ACCOUNT__QUANTITY_PRECISION``
#: decimals, so anything smaller than this cannot be a real residual.
QUANTITY_EPSILON = 1e-9


class OpenPosition(BaseModel):
    """The one trade cycle currently live on a (strategy, symbol) pair.

    QTE's own state, never published: the broker learns about a position from
    the signals themselves. This is what the runner has to *remember*, and the
    reason it is a model rather than a bare ``uxid`` string is the exit rule.

    A cycle ends on ``TP2``/``SL``/``R_SL``/``FLAT``, and also on a ``TP1``
    that happens to close the entry's whole quantity — a strategy taking "50%"
    of a position it sized at one lot has closed the trade, and treating it as
    a partial would leave the runner holding a cycle the broker has finished
    with. Deciding that needs the entry size and what is left of it, so both
    live here and both are persisted.

    ``scale`` is the ratio between the size QTE sent and the size the strategy
    asked for. Strategies emit closes in their own units (``quantity=remaining``
    off their internal mirror), so a close is multiplied by this to land back
    on the position QTE actually opened.

    Persisted to Redis on every transition and mirrored into Postgres; see
    :class:`~qte_shared.cache.RedisState` and the ``open_positions`` table. A
    restart reloads it, because the alternative — minting a fresh cycle for a
    position the broker still holds — leaves a ghost nobody will ever close.
    """

    model_config = ConfigDict(allow_inf_nan=False)

    signal_uxid: str
    strategy: str = ""
    symbol: str = ""
    action: SignalAction = SignalAction.LONG
    opened_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)

    price: float | None = None
    #: Size the entry was sent with — the denominator of every partial.
    quantity: float | None = None
    #: Size still open. ``None`` when the cycle was restored from a record that
    #: predates size tracking, which reads as "unknown", not as "flat".
    remaining: float | None = None

    sl: float | None = None
    tp1: float | None = None
    tp2: float | None = None
    risk_percent: float | None = None
    tp1_percent: float | None = None
    move_sl_to_be: bool | None = None
    use_equity_sizing: bool | None = None
    is_scale_position: bool | None = None
    scale_strategy: str | None = None
    scaling: Scaling | None = None
    #: QTE's entry size ÷ the strategy's proposed entry size.
    scale: float = 1.0
    tp1_filled: bool = False

    @property
    def is_flat(self) -> bool:
        """Whether nothing is left of the position."""
        return self.remaining is not None and self.remaining <= QUANTITY_EPSILON

    def share(self, percent: float | None) -> float | None:
        """*percent* of the **entry** size, clamped to what is still open."""
        if self.quantity is None or percent is None:
            return None
        return min(self.quantity * percent / 100.0, self.remaining_or(self.quantity))

    def remaining_or(self, fallback: float | None) -> float:
        return self.remaining if self.remaining is not None else (fallback or 0.0)

    def apply_close(self, action: SignalAction, quantity: float | None) -> bool:
        """Book a close against the position; return whether the cycle is over.

        A terminal action ends it regardless of the size it names — a stop is a
        stop even if the payload rounds its quantity. A ``TP1`` ends it only by
        taking everything, which is the rule this class exists to hold.
        """
        if quantity is not None and self.remaining is not None:
            self.remaining = max(0.0, self.remaining - abs(quantity))
        if action is SignalAction.TP1:
            self.tp1_filled = True
        self.updated_at = utcnow()
        return action in TERMINAL_ACTIONS or self.is_flat
