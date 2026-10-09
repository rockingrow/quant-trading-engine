"""Telegram Bot API calls for the position-lifecycle message.

Ported from ``algo-trading-broker``'s ``broker/services/notification_service.py``
so a cycle reads the same way in a chat whichever service posted it: the same
comma-separated chat-id lists, the same forum-topic suffix, the same ``<pre>``
box, and the same tri-state answer to an edit.

Everything here is best-effort and never raises. A notification is downstream
of a trade that has already happened: an unreachable Bot API must cost the chat
its update, never the runner its loop. What it does *not* do is retry behind
the caller's back — ``EditOutcome`` hands that decision to
:mod:`qte_strategy_engine.telegram_notify`, which is the half that knows what
the cycle currently looks like.
"""

from __future__ import annotations

import asyncio
import html
import re
from enum import Enum
from typing import Any, NamedTuple

try:
    import httpx
except ImportError:  # pragma: no cover - exercised by an image without the extra
    # Shipped by the ``broker`` and ``tiingo`` extras, which every image that
    # could want Telegram already installs. An image built without either keeps
    # importing this module and simply never sends: `configured` stays False,
    # the same answer as "no token".
    httpx = None  # type: ignore[assignment]

from qte_shared.config import settings
from qte_shared.logging_setup import get_logger

log = get_logger(__name__)

#: A chat id carrying a forum topic, e.g. ``-1002173777783_924584``. Only a
#: *numeric* chat id may be suffixed this way: a channel username can itself
#: contain underscores (``@my_group_2``), so splitting those would invent a
#: topic out of part of the name.
CHAT_TOPIC_PATTERN = re.compile(r"(?P<chat>-?\d+)_(?P<topic>\d+)")


def boxed(message_text: str) -> str:
    """Wrap a body in the preformatted box every QTE/broker notification uses."""
    return f"<pre>{message_text.strip()}</pre>"


def escaped(value: Any) -> str:
    """Text safe to drop into a body, which Telegram parses as HTML.

    ``parse_mode=HTML`` means an unescaped ``<`` or ``&`` anywhere in the body
    is a parse error, and the Bot API then rejects the *whole* message with
    "can't parse entities" — the chat loses the update entirely. Numbers cannot
    carry one, but free text can: a delivery error is exception text
    (``<ConnectError ...>``), a traceback frame reads ``in <module>``, and a
    strategy's own indicator names are whatever its private repo called them.

    Applied per field rather than to a finished body, because the body carries
    markup of our own — the ``</pre><pre>`` seams that split it into boxes.
    """
    return html.escape(str(value), quote=False)


def clipped(message_text: str, limit: int, *, marker: str = "… truncated") -> str:
    """*message_text* cut to *limit* characters, with *marker* where it was cut.

    Always applied **after** :func:`escaped`, never before: escaping expands
    text (one ``&`` becomes five characters), so a limit enforced first bounds
    nothing. A cut that lands inside an entity would leave a bare ``&lt``, so
    the tail is trimmed back to the last complete one.
    """
    if len(message_text) <= limit:
        return message_text
    kept = message_text[:limit]
    # Prefer a line boundary, so a clipped block still reads as whole lines —
    # but not when that throws away most of what fits. A traceback's last frame
    # or a single huge line would otherwise be cut back to the header.
    head, separator, _ = kept.rpartition("\n")
    if separator and len(head) >= limit // 2:
        kept = head
    ampersand = kept.rfind("&")
    if ampersand != -1 and ";" not in kept[ampersand:]:
        kept = kept[:ampersand]
    return f"{kept.rstrip()}\n{marker}"


class ChatTarget(NamedTuple):
    """One Telegram destination: a chat, optionally a topic inside it.

    ``message_thread_id`` is the Bot API's name for a forum topic. Sending
    without it lands the message in the group's *General* topic, which is why a
    group with topics enabled needs the id carried all the way down to the
    payload.
    """

    chat_id: str
    message_thread_id: int | None = None

    @property
    def label(self) -> str:
        """Identify this target in a log line."""
        if self.message_thread_id is None:
            return self.chat_id
        return f"{self.chat_id} (topic {self.message_thread_id})"

    @property
    def stored_key(self) -> str:
        """How this target is spelled in a cycle's ``messages`` map."""
        if self.message_thread_id is None:
            return self.chat_id
        return f"{self.chat_id}_{self.message_thread_id}"


def parse_chat_targets(raw_setting: str | None) -> list[ChatTarget]:
    """Parse a chat-id setting into the chats (and topics) to deliver to.

    The value is a comma-separated list, so one audience can fan out to several
    groups: ``"-1001111111111,-1002173777783_924584,@public_channel"``.

    An entry of the form ``<chat id>_<topic id>`` addresses a *topic* inside a
    supergroup that has the Topics feature switched on — the shape Telegram
    itself shows in a topic link (``t.me/c/2173777783/924584``). It is split
    back into the chat and its ``message_thread_id``; everything else is passed
    through untouched, so plain ids, user ids and ``@username`` handles keep
    working.

    An empty setting yields no targets, which is how an audience is switched
    off. Blank entries are skipped and duplicates collapse, so a stray comma or
    a chat listed twice costs nothing and never double-posts.
    """
    if not raw_setting:
        return []

    targets: list[ChatTarget] = []
    for entry in raw_setting.split(","):
        entry = entry.strip()
        # "-" is how a disabled audience is spelled in the broker's own
        # .env.example; treat it as blank rather than as a chat named "-".
        if not entry or entry == "-":
            continue
        topic_match = CHAT_TOPIC_PATTERN.fullmatch(entry)
        target = (
            ChatTarget(topic_match["chat"], int(topic_match["topic"]))
            if topic_match
            else ChatTarget(entry)
        )
        if target not in targets:
            targets.append(target)
    return targets


class EditOutcome(Enum):
    """Result of trying to rewrite an existing cycle message.

    A tri-state, not a bool, because the caller reacts differently to each: an
    OK edit keeps the stored id, a MISSING one falls back to a fresh send (the
    message is gone from the chat and cannot be edited), and a FAILED one keeps
    the id and lets the next action try again — re-sending on a rate limit
    would duplicate the whole cycle in the chat.
    """

    OK = "OK"
    #: The message is gone or can no longer be edited — re-send to recover.
    MISSING = "MISSING"
    #: Transient failure — keep the message id and retry on the next action.
    FAILED = "FAILED"


#: Bot API error fragments (lower-cased) that mean the message is unrecoverable.
MESSAGE_GONE_MARKERS = (
    "message to edit not found",
    "message can't be edited",
    "message_id_invalid",
    # A chat the bot was removed from, or a user who never started it. Treated
    # as gone so the id is forgotten; the re-send that follows fails the same
    # way and is logged once, rather than every action retrying an edit.
    "chat not found",
    "bot was blocked by the user",
)


class TelegramSink:
    """Sends and edits Telegram messages. One class for all three uses.

    Stateless on purpose: the body arrives already rendered and escaped, and a
    ``message_id`` is handed in by whoever owns it, because what a message is
    *about* — a trade cycle in Postgres, an error record, a service coming up —
    is the caller's problem. All this holds is the token, the HTTP client and
    the two Bot API calls.
    """

    def __init__(self, bot_token: str | None = None, *, http_timeout: float | None = None) -> None:
        self.enabled = settings.telegram.enabled
        self.bot_token = (
            bot_token if bot_token is not None else settings.telegram.bot_token
        ).strip()
        self._timeout = http_timeout if http_timeout is not None else settings.telegram.http_timeout
        self._client: httpx.AsyncClient | None = None

    # ── Lifecycle ─────────────────────────────────────────────────────

    @property
    def configured(self) -> bool:
        """Whether a call would do anything at all."""
        if httpx is None:
            if self.enabled and self.bot_token:
                log.warning(
                    "Telegram is configured but httpx is not installed in this image; "
                    "nothing will be sent. Build with the `broker` or `tiingo` extra."
                )
            return False
        return bool(self.enabled and self.bot_token)

    async def start(self) -> None:
        if self._client is None and self.configured:
            self._client = httpx.AsyncClient(timeout=self._timeout)

    async def stop(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    # ── Bot API ───────────────────────────────────────────────────────

    async def send_message(self, target: ChatTarget, message_text: str) -> str | None:
        """Post *message_text* to *target* and return Telegram's ``message_id``.

        ``None`` means nothing was sent, or that the send succeeded but its
        response could not be read: without an id the cycle cannot be edited
        later, so the caller must treat both as "no message to update".
        """
        if not self._ready(target):
            return None
        response = await self._call("sendMessage", self._payload(target, message_text))
        if response is None:
            return None
        if response.status_code != 200:
            log.error("Telegram sendMessage failed chat_id=%s: %s", target.label, response.text)
            return None
        message_id = _result_field(response, "message_id")
        if message_id is None:
            log.warning("Telegram sendMessage chat_id=%s returned no message_id", target.label)
            return None
        return str(message_id)

    async def edit_message(
        self, target: ChatTarget, message_id: str, message_text: str
    ) -> EditOutcome:
        """Rewrite an already-sent cycle message.

        An edit Telegram rejects because the body is byte-identical
        (``message is not modified``) counts as OK — it is a no-op, and calling
        it a failure would make a re-delivered signal re-post the whole cycle.
        """
        if not self._ready(target):
            return EditOutcome.FAILED
        payload = self._payload(target, message_text)
        payload["message_id"] = message_id
        response = await self._call("editMessageText", payload)
        if response is None:
            return EditOutcome.FAILED
        if response.status_code == 200:
            return EditOutcome.OK
        body = (response.text or "").lower()
        if "message is not modified" in body:
            return EditOutcome.OK
        log.error(
            "Telegram editMessageText failed chat_id=%s message_id=%s: %s",
            target.label,
            message_id,
            response.text,
        )
        if any(marker in body for marker in MESSAGE_GONE_MARKERS):
            return EditOutcome.MISSING
        return EditOutcome.FAILED

    # ── Internals ─────────────────────────────────────────────────────

    def _ready(self, target: ChatTarget) -> bool:
        if httpx is None or not self.enabled:
            log.debug("Telegram notifications are disabled")
            return False
        if not self.bot_token:
            log.warning("QTE_TELEGRAM__BOT_TOKEN must be set to notify a chat")
            return False
        return bool(target.chat_id)

    def _payload(self, target: ChatTarget, message_text: str) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "chat_id": target.chat_id,
            "text": boxed(message_text),
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        # Only for topic targets: the Bot API answers "400 Bad Request: message
        # thread not found" when a group has no such thread.
        if target.message_thread_id is not None:
            payload["message_thread_id"] = target.message_thread_id
        return payload

    async def _call(self, method: str, payload: dict[str, Any]) -> httpx.Response | None:
        """One Bot API call. ``None`` is a transport failure, already logged."""
        client = self._client
        owns_client = client is None
        if client is None:
            client = httpx.AsyncClient(timeout=self._timeout)
        try:
            return await client.post(
                f"https://api.telegram.org/bot{self.bot_token}/{method}", json=payload
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.error("Telegram %s raised for chat_id=%s: %s", method, payload["chat_id"], exc)
            return None
        finally:
            if owns_client:
                await client.aclose()


def _result_field(response: httpx.Response, field_name: str) -> Any:
    """Best-effort read of one field out of a Bot API ``result`` object."""
    try:
        body = response.json()
    except Exception:
        return None
    result = body.get("result") if isinstance(body, dict) else None
    return result.get(field_name) if isinstance(result, dict) else None
