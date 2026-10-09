"""One Telegram message per position, edited in place for its whole lifecycle.

The runner emits a trade as a series of signals — an entry, then its TP1/TP2,
``R_SL``, ``SL`` or ``FLAT``. Posted one message each, a chat shows four
unrelated notifications and a reader has to stitch the position back together
by eye. So a cycle owns **one** message per chat: the entry sends it, every
later action *edits* it, appending to the Actions block until the position is
closed. That is the behaviour ``algo-trading-broker`` already has for its own
broadcast, and the body is deliberately shaped like it
(``broker/helpers/message_formatter.py``) so the same trade reads the same way
whichever service posted it.

What identifies a cycle is ``strategy`` + ``symbol`` + ``signal_uxid``, the
triple the broker groups a broadcast by. It is kept in Postgres with the
message ids, so a restart keeps editing the message already on screen. No reply
is ever posted under it: an edit is silent, and the notice-on-update the broker
sends is deliberately not reproduced here.

Two audiences, independently switchable because each is just a chat-id list:

* **broadcast** (``QTE_TELEGRAM__BROADCAST_CHAT_IDS``) — the position: side,
  symbol, price, levels and the action timeline.
* **private** (``QTE_TELEGRAM__PRIVATE_CHAT_IDS``) — the above plus the
  strategy name, the cycle id, how delivery went and (opt-in) the indicator
  dump. An empty list sends nothing to that audience, which is how a
  deployment runs broadcast-only, private-only, or neither.

Nothing here is on the trade path. :meth:`TelegramLifecycleNotifier.enqueue` is
synchronous and non-blocking — ``api.telegram.org`` is throttled or silently
dropped on plenty of networks, and a send that sits there for the whole HTTP
timeout must not hold up the next signal. One background worker drains the
queue, which also keeps a cycle's edits in the order its actions happened.
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from qte_shared.config import settings
from qte_shared.logging_setup import get_logger
from qte_shared.models import TERMINAL_ACTIONS, BrokerSignal, SignalAction
from qte_shared.notifications.telegram_sink import (
    ChatTarget,
    EditOutcome,
    TelegramSink,
    clipped,
    escaped,
    parse_chat_targets,
)
from qte_strategy_engine.db.repository import TelegramCycleRecord, TelegramCycleRepository

log = get_logger(__name__)

#: The two audiences, and the setting each reads its chats from.
BROADCAST_AUDIENCE = "broadcast"
PRIVATE_AUDIENCE = "private"

#: Cycle status, spelled as the broker spells it in its own broadcast header.
STATUS_RUNNING = "RUNNING"
STATUS_CLOSED = "CLOSED"

#: Same glyphs as ``broker/helpers/emoji_constants.py``, written out rather
#: than resolved through the ``emoji`` package: one shared message shape is not
#: worth a production dependency in the runner's image.
ACTION_ICONS: dict[SignalAction, str] = {
    SignalAction.LONG: "📈",
    SignalAction.SHORT: "📉",
    SignalAction.TP1: "🎯",
    SignalAction.TP2: "🚀",
    SignalAction.R_SL: "🛡️",
    SignalAction.SL: "⭕",
    SignalAction.FLAT: "🏳️",
}
DEFAULT_ICON = "📡"
STATUS_ICONS = {STATUS_RUNNING: "⏳", STATUS_CLOSED: "🏁"}
SHADOW_ICON = "🧪"
UNDELIVERED_ICON = "⚠️"

DIVIDER = "-----------"

#: The Bot API caps a message at 4096 characters and rejects a longer one
#: whole, so the two unbounded parts of the body are bounded here: how many
#: action blocks are rendered, and how much of the entry's raw dump.
MAX_ACTION_BLOCKS = 12
MAX_RAW_CHARS = 800
#: A delivery detail is exception text, which nothing bounds either.
MAX_DETAIL_CHARS = 200

#: Delivery outcomes that mean the broker has the signal (or was never meant to
#: get it). Anything else is reported in the message as not delivered.
ACCEPTED_DELIVERY = frozenset({"sent", "shadow"})


# ── Events ────────────────────────────────────────────────────────────────


def lifecycle_event(
    signal: BrokerSignal,
    *,
    delivery_id: str,
    delivery_status: str,
    transport: str,
    detail: str | None = None,
) -> dict[str, Any]:
    """The JSON record of one action, as it is stored and re-rendered from.

    A plain dict rather than a model: it goes into JSONB untouched and is read
    back by a formatter that must tolerate a record written by an older build,
    which is exactly what ``.get()`` on a dict does and a strict model does
    not.

    ``delivery_id`` is carried so a retried delivery updates its own event
    instead of appending a second one — the runner's outbox retries the *same*
    row, and a cycle must not grow an extra TP1 because its acknowledgement was
    lost once.
    """
    position = signal.position
    return {
        "delivery_id": delivery_id,
        "action": position.action.value,
        "timestamp": signal.timestamp.astimezone(UTC).isoformat(),
        "price": position.price,
        "quantity": position.quantity,
        "sl": position.sl,
        "tp1": position.tp1,
        "tp2": position.tp2,
        "risk_percent": position.risk_percent,
        "tp1_percent": position.tp1_percent,
        "move_sl_to_be": position.move_sl_to_be,
        "is_running": position.is_running,
        "is_scale_position": position.is_scale_position,
        "scale_strategy": position.scale_strategy,
        "delivery_status": delivery_status,
        "transport": transport,
        "detail": detail,
        "indicators": signal.indicators,
        "inputs": signal.inputs,
    }


def _event_action(event: dict) -> SignalAction | None:
    try:
        return SignalAction(str(event.get("action")))
    except ValueError:
        return None


def is_terminal_event(event: dict) -> bool:
    """Whether this action ends the cycle, so the header reads CLOSED.

    ``TP1`` is absent from :data:`qte_shared.models.TERMINAL_ACTIONS` on
    purpose — it is a partial by contract. A TP1 that happens to take the whole
    size leaves the message RUNNING until the position's real close arrives,
    which is the honest reading of what the broker was told.
    """
    action = _event_action(event)
    return action is not None and action in TERMINAL_ACTIONS


# ── Formatting ────────────────────────────────────────────────────────────


def format_number(value: Any) -> str:
    """Render a number the way a human writes it.

    ``str()`` on a float leaks the representation: ``2340.0`` should read
    ``2340`` in a chat, and a risk of ``1.0`` should read ``1``. Normalising
    through ``Decimal`` drops the trailing zeros, and the exponent guard turns
    the scientific forms it falls back to for zero and for large integers back
    into plain digits.
    """
    if value is None:
        return "—"
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, (int, float)):
        try:
            value = Decimal(str(value))
        except InvalidOperation:
            return str(value)
    if not isinstance(value, Decimal):
        return str(value)
    normalised = value.normalize()
    if normalised.as_tuple().exponent > 0:
        normalised = normalised.quantize(Decimal(1))
    return f"{normalised:f}"


def _format_moment(value: Any) -> str:
    """One event's stored ISO timestamp, in the configured display zone."""
    if not value:
        return ""
    try:
        moment = datetime.fromisoformat(str(value))
    except ValueError:
        return str(value)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return moment.astimezone(settings.telegram.display_zone).strftime("%Y-%m-%d %H:%M:%S %Z")


def _joined_fields(event: dict, fields: tuple[tuple[str, str], ...]) -> str:
    """``"Price: 2340 | Quantity: 0.05"`` for the fields the event carries."""
    parts = [
        f"{label}: {format_number(event.get(key))}"
        for label, key in fields
        if event.get(key) is not None
    ]
    return " | ".join(parts)


def _delivery_warning(event: dict) -> str:
    """The line that says the broker never took this action, or ``""``."""
    status = str(event.get("delivery_status") or "")
    if status in ACCEPTED_DELIVERY:
        return ""
    detail = str(event.get("detail") or "").strip()
    suffix = f" — {clipped(escaped(detail), MAX_DETAIL_CHARS)}" if detail else ""
    return f"{UNDELIVERED_ICON} NOT DELIVERED ({escaped(status or 'unknown')}){suffix}"


def _entry_block(event: dict) -> str:
    lines = []
    if event.get("price") is not None:
        lines.append(f"Price: {format_number(event.get('price'))}")
    sizing = _joined_fields(event, (("Quantity", "quantity"), ("Risk", "risk_percent")))
    if sizing:
        if event.get("risk_percent") is not None:
            sizing += "%"
        lines.append(sizing)
    levels = _joined_fields(event, (("SL", "sl"), ("TP1", "tp1"), ("TP2", "tp2")))
    if levels:
        lines.append(levels)
    moment = _format_moment(event.get("timestamp"))
    if moment:
        lines.append(moment)
    warning = _delivery_warning(event)
    if warning:
        lines.append(warning)
    return "\n".join(lines)


def _action_block(event: dict) -> str:
    action = _event_action(event)
    icon = ACTION_ICONS.get(action, DEFAULT_ICON) if action is not None else DEFAULT_ICON
    lines = [f"{icon} {escaped(event.get('action'))}"]
    numbers = _joined_fields(event, (("Price", "price"), ("Quantity", "quantity")))
    if numbers:
        lines.append(numbers)
    moment = _format_moment(event.get("timestamp"))
    if moment:
        lines.append(moment)
    warning = _delivery_warning(event)
    if warning:
        lines.append(warning)
    return "\n".join(lines)


def _flags_line(event: dict) -> str:
    """Compact rendering of the entry's optional position flags.

    Only the flags the strategy actually set are shown, so a minimal signal
    stays minimal in the chat.
    """

    def _flag(value: Any) -> str:
        return "🟢" if value else "🔴"

    parts: list[str] = []
    if event.get("tp1_percent") is not None:
        parts.append(f"TP1%: {format_number(event.get('tp1_percent'))}%")
    if event.get("move_sl_to_be") is not None:
        parts.append(f"SL→BE: {_flag(event.get('move_sl_to_be'))}")
    if event.get("is_running") is not None:
        parts.append(f"Running: {_flag(event.get('is_running'))}")
    if event.get("is_scale_position") is not None:
        scale = f"Scale: {_flag(event.get('is_scale_position'))}"
        if event.get("scale_strategy"):
            scale += f" {escaped(event.get('scale_strategy'))}"
        parts.append(scale)
    return " | ".join(parts)


def _raw_block(event: dict) -> str:
    """The entry's indicator/input dump, for the private audience only.

    Truncated, because the content is a private repo's own dict and nothing
    bounds it: a strategy dumping a wide ``inputs`` could push the message past
    the Bot API's 4096-character limit, and Telegram answers that by rejecting
    the message outright — the chat would lose the whole cycle over a debug aid.
    """
    parts: list[str] = []
    for title, data in (("Indicators", event.get("indicators")), ("Inputs", event.get("inputs"))):
        if not isinstance(data, dict):
            continue
        values = {name: value for name, value in data.items() if value is not None}
        if values:
            rendered = "\n".join(
                f"  {escaped(name)}: {escaped(value)}" for name, value in values.items()
            )
            parts.append(f"{title}:\n{rendered}")
    return clipped("\n".join(parts), MAX_RAW_CHARS, marker="  … truncated")


def format_cycle(record: TelegramCycleRecord, *, audience: str, include_raw: bool = False) -> str:
    """Render the whole cycle, from its first event to its latest.

    Re-rendered in full on every update rather than appended to, because the
    header has to change with the cycle (RUNNING → CLOSED) and because the body
    is the only copy a chat holds: a message Telegram rejected once must come
    back complete on the next action, not missing the action in between.

    The nested ``</pre><pre>`` seams split the body into separate boxes in the
    chat — Telegram's HTML parser does not allow a ``<pre>`` inside a ``<pre>``,
    so the blocks are closed and reopened instead. The outer pair comes from
    :func:`qte_strategy_engine.telegram_sink.boxed`.
    """
    events = [event for event in record.events if isinstance(event, dict)]
    entry_event = events[0] if events else {}
    entry_action = _event_action(entry_event)
    entry_icon = ACTION_ICONS.get(entry_action, DEFAULT_ICON) if entry_action else DEFAULT_ICON
    timeframe = f" ({escaped(record.timeframe)})" if record.timeframe else ""

    header = f"[{STATUS_ICONS.get(record.status, STATUS_ICONS[STATUS_RUNNING])}{record.status}]"
    if entry_event.get("delivery_status") == "shadow":
        header += f" [{SHADOW_ICON}SHADOW]"

    position_lines = [header]
    label = f"{entry_action.value} " if entry_action else ""
    position_lines.append(f"{entry_icon} {label}{escaped(record.symbol)}{timeframe}")
    position_lines.append(DIVIDER)
    entry_block = _entry_block(entry_event)
    if entry_block:
        position_lines.append(entry_block)
        position_lines.append(DIVIDER)
    body = "\n".join(position_lines)

    actions = events[1:]
    if actions:
        # Newest actions win if a cycle somehow collects more than the message
        # can hold: an edit that exceeds 4096 characters is rejected outright,
        # and a chat left on a stale body is worse than one missing the oldest
        # partial. A real position never comes near the cap.
        shown = actions[-MAX_ACTION_BLOCKS:]
        action_lines = ["Actions:", DIVIDER]
        if len(shown) < len(actions):
            action_lines.append(f"… {len(actions) - len(shown)} earlier action(s) not shown")
        action_lines.append("\n\n".join(_action_block(event) for event in shown))
        action_lines.append(DIVIDER)
        body += "\n</pre>\n<pre>" + "\n".join(action_lines)

    if audience == PRIVATE_AUDIENCE:
        meta_lines = ["Signal info:", DIVIDER]
        meta_lines.append(f"Strategy: {escaped(record.strategy)}")
        meta_lines.append(f"Signal: {escaped(record.signal_uxid)}")
        latest = events[-1] if events else {}
        transport = latest.get("transport")
        status = latest.get("delivery_status")
        if transport or status:
            meta_lines.append(f"Delivery: {escaped(status or '—')} via {escaped(transport or '—')}")
        meta_lines.append(DIVIDER)
        flags = _flags_line(entry_event)
        if flags:
            meta_lines.extend(["📊Settings:", DIVIDER, flags, DIVIDER])
        if include_raw:
            raw_block = _raw_block(entry_event)
            if raw_block:
                meta_lines.extend([raw_block, DIVIDER])
        body += "\n</pre>\n<pre>" + "\n".join(meta_lines)

    return body


# ── The notifier ──────────────────────────────────────────────────────────


class TelegramLifecycleNotifier:
    """Keeps each cycle's Telegram message up to date, off the trade path.

    The runner only ever calls :meth:`enqueue`, which cannot block and cannot
    raise. One worker task drains the queue and performs the Postgres read, the
    render and the Bot API calls, in the order the actions were emitted — the
    ordering matters, because two actions on one cycle edit the same message
    and the later body must win.

    Inactive by configuration is the normal case for a deployment that does not
    use Telegram: with no token, or with both chat lists empty, every method is
    a no-op and nothing is queued.
    """

    def __init__(
        self,
        sink: TelegramSink | None = None,
        cycles: TelegramCycleRepository | None = None,
    ) -> None:
        self._sink = sink or TelegramSink()
        self._cycles = cycles or TelegramCycleRepository()
        self._audiences: dict[str, list[ChatTarget]] = {
            BROADCAST_AUDIENCE: parse_chat_targets(settings.telegram.broadcast_chat_ids),
            PRIVATE_AUDIENCE: parse_chat_targets(settings.telegram.private_chat_ids),
        }
        self._queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(
            maxsize=settings.telegram.queue_capacity
        )
        self._worker: asyncio.Task[None] | None = None

    @property
    def active(self) -> bool:
        """Whether anything would be sent: a usable token and at least one chat."""
        return self._sink.configured and any(self._audiences.values())

    @property
    def audiences(self) -> dict[str, list[ChatTarget]]:
        """The chats each audience resolved to — read for logging and tests."""
        return dict(self._audiences)

    # ── Lifecycle ─────────────────────────────────────────────────────

    async def start(self) -> None:
        if not self.active:
            log.info(
                "Telegram lifecycle notifications are off "
                "(enabled=%s token=%s broadcast=%d private=%d)",
                settings.telegram.enabled,
                "set" if settings.telegram.bot_token else "unset",
                len(self._audiences[BROADCAST_AUDIENCE]),
                len(self._audiences[PRIVATE_AUDIENCE]),
            )
            return
        await self._sink.start()
        self._worker = asyncio.create_task(self._drain_queue(), name="telegram-lifecycle")
        log.info(
            "Telegram lifecycle notifications ready broadcast=%s private=%s",
            [target.label for target in self._audiences[BROADCAST_AUDIENCE]],
            [target.label for target in self._audiences[PRIVATE_AUDIENCE]],
        )

    async def stop(self, *, drain_timeout: float = 5.0) -> None:
        """Let the queue finish, then shut the worker and the HTTP client down.

        Bounded on purpose: the runner's shutdown releases its ownership claim
        at the end, and a Telegram outage must not hold that up past Docker's
        stop grace period.
        """
        worker = self._worker
        self._worker = None
        if worker is not None:
            try:
                await asyncio.wait_for(self._queue.join(), timeout=drain_timeout)
            except TimeoutError:
                log.warning(
                    "Dropping %d queued Telegram update(s) on shutdown", self._queue.qsize()
                )
            worker.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker
        await self._sink.stop()

    # ── Producing ─────────────────────────────────────────────────────

    def enqueue(self, event: dict[str, Any]) -> None:
        """Hand one action off for delivery. Never blocks, never raises."""
        if self._worker is None:
            return
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            # The signal outbox is the record of what was sent; this queue is
            # only the chat's view of it, so the newest update is what gets
            # dropped rather than the loop that produced it.
            log.warning(
                "Telegram queue is full (%d); dropped the %s update for %s",
                self._queue.maxsize,
                event.get("action"),
                event.get("signal_uxid"),
            )

    def note_signal(
        self,
        signal: BrokerSignal,
        *,
        delivery_id: str,
        delivery_status: str,
        transport: str,
        detail: str | None = None,
    ) -> None:
        """Queue the message update for one emitted signal.

        The runner's single call site. Deliberately synchronous: it is called
        from the emit path, where awaiting anything that talks to Telegram
        would put a third-party HTTP timeout in front of the next bar.
        """
        if self._worker is None:
            return
        event = lifecycle_event(
            signal,
            delivery_id=delivery_id,
            delivery_status=delivery_status,
            transport=transport,
            detail=detail,
        )
        event["strategy"] = signal.strategy
        event["symbol"] = signal.symbol
        event["timeframe"] = signal.timeframe
        event["signal_uxid"] = signal.signal_uxid
        self.enqueue(event)

    # ── Consuming ─────────────────────────────────────────────────────

    async def _drain_queue(self) -> None:
        while True:
            event = await self._queue.get()
            try:
                await self.deliver(event)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception(
                    "Could not update the Telegram message for %s", event.get("signal_uxid")
                )
            finally:
                self._queue.task_done()

    async def deliver(self, event: dict[str, Any]) -> TelegramCycleRecord:
        """Fold one action into its cycle and rewrite every chat's message.

        Returns the record as it was stored, which is what the tests assert on:
        the events the message now shows and the id it holds per chat.
        """
        strategy = str(event.get("strategy") or "")
        symbol = str(event.get("symbol") or "")
        signal_uxid = str(event.get("signal_uxid") or "")

        record = await self._cycles.load(strategy, symbol, signal_uxid) or TelegramCycleRecord(
            strategy=strategy, symbol=symbol, signal_uxid=signal_uxid
        )
        record.timeframe = record.timeframe or str(event.get("timeframe") or "")
        _merge_event(record, event)
        if is_terminal_event(event):
            record.status = STATUS_CLOSED

        for audience, targets in self._audiences.items():
            if not targets:
                continue
            message_text = format_cycle(
                record,
                audience=audience,
                include_raw=settings.telegram.include_signal_raw,
            )
            for target in targets:
                await self._deliver_to(record, audience, target, message_text)

        await self._cycles.save(record)
        return record

    async def _deliver_to(
        self,
        record: TelegramCycleRecord,
        audience: str,
        target: ChatTarget,
        message_text: str,
    ) -> None:
        """Edit this chat's message, or send it the first (or a fresh) one."""
        stored_key = f"{audience}:{target.stored_key}"
        message_id = record.messages.get(stored_key)
        if message_id:
            outcome = await self._sink.edit_message(target, message_id, message_text)
            if outcome is EditOutcome.OK:
                return
            if outcome is EditOutcome.FAILED:
                # Keep the id: the message is still in the chat, and the next
                # action re-renders the whole cycle, so nothing is lost.
                return
            record.messages.pop(stored_key, None)

        sent_id = await self._sink.send_message(target, message_text)
        if sent_id is not None:
            record.messages[stored_key] = sent_id


def _merge_event(record: TelegramCycleRecord, event: dict[str, Any]) -> None:
    """Append the action, or replace the one this delivery already wrote.

    The runner retries an ambiguous delivery on its *own* outbox row, so the
    same ``delivery_id`` can reach this twice — once as ``unknown``, then as
    ``sent``. Replacing updates the chat from "not delivered" to delivered
    instead of showing the action twice.
    """
    delivery_id = event.get("delivery_id")
    if delivery_id:
        for index, stored in enumerate(record.events):
            if isinstance(stored, dict) and stored.get("delivery_id") == delivery_id:
                record.events[index] = event
                return
    record.events.append(event)
