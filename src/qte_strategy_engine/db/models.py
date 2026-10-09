"""The runner's own tables — it is the only writer of all three.

``signals`` is the audit/outbox trail: ``payload`` holds the broker envelope's
trading fields, without authentication. Delivery status records preparation,
broker acceptance and completion of local position persistence separately.

``open_positions`` is the opposite kind of table — one mutable row per
(strategy, symbol), holding the trade cycle currently live on that pair. Redis
is where the runner reads it on the hot path; this is the copy that survives a
flushed cache, because the failure it guards against is expensive and silent:
a runner that forgets an open cycle mints a fresh one on the next entry and
leaves the broker holding a position nobody will ever close.

``telegram_cycle_messages`` is the notification side of that same cycle: the
message a chat holds for it, and the events the message is re-rendered from.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import Boolean, DateTime, Float, Index, String, Text, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from qte_shared.db.base import Base, new_uuid


class SignalAudit(Base):
    """One row per signal the runner produced — delivered, shadowed, or failed."""

    __tablename__ = "signals"

    namespace: Mapped[str] = mapped_column(
        String(160), nullable=False, server_default="legacy", index=True
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=new_uuid)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    signal_uxid: Mapped[str] = mapped_column(String(32), nullable=False)
    strategy: Mapped[str] = mapped_column(String(128), nullable=False)
    symbol: Mapped[str] = mapped_column(String(64), nullable=False)
    timeframe: Mapped[str] = mapped_column(String(16), nullable=False)
    action: Mapped[str] = mapped_column(String(16), nullable=False)
    signal_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    price: Mapped[float | None] = mapped_column(Float)
    quantity: Mapped[float | None] = mapped_column(Float)
    sl: Mapped[float | None] = mapped_column(Float)
    tp1: Mapped[float | None] = mapped_column(Float)
    tp2: Mapped[float | None] = mapped_column(Float)

    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    indicators: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    inputs: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)

    transport: Mapped[str] = mapped_column(String(16), default="nats")
    #: ``prepared`` (unsent), legacy ``pending``/``unknown`` (ambiguous),
    #: ``sent_pending``/``shadow_pending`` (local reconciliation), then
    #: terminal ``sent``/``shadow``/``failed``. No schema change is needed.
    delivery_status: Mapped[str] = mapped_column(String(16), default="shadow")
    delivery_error: Mapped[str | None] = mapped_column(Text)
    shadow: Mapped[bool] = mapped_column(Boolean, default=True)

    __table_args__ = (
        Index("ix_signals_strategy_created", "strategy", "created_at"),
        Index("ix_signals_uxid", "signal_uxid"),
        Index("ix_signals_symbol_created", "symbol", "created_at"),
    )


class OpenPositionRow(Base):
    """One trade cycle live on a (strategy, symbol) pair.

    A pair holds one row, or up to ``max_open_cycles`` when its mapping sets
    ``allow_multiple_cycles``. That limit is the signal factory's to enforce —
    it depends on configuration the database cannot see — so the table only
    guarantees the rule that holds everywhere: one row per cycle id.

    Mirrors :class:`qte_shared.models.OpenPosition`. ``state`` carries the whole
    record so a field added there does not need a migration to be persisted;
    the columns beside it exist because operating a book means querying it
    ("what is open right now, and how big"), and digging that out of JSONB in
    an incident is the wrong time to discover you cannot.
    """

    __tablename__ = "open_positions"

    namespace: Mapped[str] = mapped_column(
        String(160), nullable=False, server_default="legacy", index=True
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=new_uuid)
    strategy: Mapped[str] = mapped_column(String(128), nullable=False)
    symbol: Mapped[str] = mapped_column(String(64), nullable=False)
    signal_uxid: Mapped[str] = mapped_column(String(32), nullable=False)
    action: Mapped[str] = mapped_column(String(16), nullable=False)

    opened_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    price: Mapped[float | None] = mapped_column(Float)
    #: Size the entry was sent with — the denominator of every partial.
    quantity: Mapped[float | None] = mapped_column(Float)
    #: Size still open. Reaching zero closes the cycle and deletes the row.
    remaining: Mapped[float | None] = mapped_column(Float)
    sl: Mapped[float | None] = mapped_column(Float)
    tp1: Mapped[float | None] = mapped_column(Float)
    tp2: Mapped[float | None] = mapped_column(Float)

    state: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)

    __table_args__ = (
        # Every cycle a pair holds — one, or several with allow_multiple_cycles.
        Index("ix_open_positions_pair", "namespace", "strategy", "symbol"),
        # One row per cycle id. The broker groups a whole trade by
        # `signal_uxid`, so two rows sharing one would let a close on either
        # close the other's position — a loss nothing in the audit trail would
        # explain. Scoped by namespace: separate books (paper and live, one per
        # provider) reusing an id is not a collision. Also the conflict target
        # of every upsert, since a pair no longer has a single row to replace.
        UniqueConstraint("namespace", "signal_uxid", name="uq_open_positions_uxid"),
    )


class TelegramCycleMessage(Base):
    """The Telegram message that represents one trade cycle, and its history.

    A cycle owns a single message per chat, edited in place as the position
    progresses, so the chat needs two things to survive a restart: which
    message to rewrite (``messages``, one id per audience/chat) and the events
    to re-render it from (``events``, appended to in order). Redis would lose
    both on a flush and leave every live cycle posting a second message.

    Keyed by (namespace, strategy, symbol, ``signal_uxid``) — the same triple
    the broker groups a broadcast by, plus the state namespace, so a paper book
    and a live one never share a message.
    """

    __tablename__ = "telegram_cycle_messages"

    namespace: Mapped[str] = mapped_column(
        String(160), nullable=False, server_default="legacy", index=True
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=new_uuid)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    strategy: Mapped[str] = mapped_column(String(128), nullable=False)
    symbol: Mapped[str] = mapped_column(String(64), nullable=False)
    signal_uxid: Mapped[str] = mapped_column(String(32), nullable=False)
    timeframe: Mapped[str] = mapped_column(String(16), nullable=False, default="")
    #: ``RUNNING`` until a terminal action arrives, then ``CLOSED``.
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="RUNNING")

    #: Every action of the cycle, oldest first. The body is re-rendered from
    #: this list on each update, which is what makes one message readable as
    #: the whole trade rather than as its latest event.
    events: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False, default=list)
    #: ``"<audience>:<chat id>"`` → Telegram ``message_id``. One entry per chat
    #: because the two audiences carry different bodies and a chat may be added
    #: to the configuration while a cycle is already running.
    messages: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)

    __table_args__ = (
        UniqueConstraint(
            "namespace",
            "strategy",
            "symbol",
            "signal_uxid",
            name="uq_telegram_cycle_messages_cycle",
        ),
    )
