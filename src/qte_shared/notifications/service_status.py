"""Telling the chat that a service came up, and that it went down again.

``make start`` brings the stack up and ``make stop`` takes it down, and until
now the only evidence of either was a line in a terminal somebody had to be
watching. Each long-running service announces itself here instead: one message
when it has finished starting, one when it has finished stopping.

Only the two Python services can speak for themselves — Redis, Postgres, NATS
and the Tailscale node are images this repository does not control, so what the
chat shows is ``data-ingestion`` and ``strategy-runner``. Between them they
cover what an operator actually needs to know, because neither one finishes
starting without the infrastructure it depends on: the runner's announcement is
itself proof that Redis, Postgres and NATS answered.

Unlike everything else on the Telegram path, these two sends are **awaited**
rather than queued. There are exactly two per process lifetime, and the second
one has to leave before the event loop does — a queued "DOWN" would still be
sitting in a worker nobody is going to run again. They are bounded by
``QTE_TELEGRAM__HTTP_TIMEOUT`` and, like every other notification here, they
never raise: a service must not fail to stop because a chat could not be told.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from qte_shared.config import settings
from qte_shared.logging_setup import get_logger
from qte_shared.notifications.telegram_sink import (
    ChatTarget,
    TelegramSink,
    clipped,
    escaped,
    parse_chat_targets,
)

log = get_logger(__name__)

UP_ICON = "🟢"
DOWN_ICON = "🛑"
DIVIDER = "-----------"

#: Enough for a service's details without risking the Bot API's 4096 cap.
MAX_STATUS_CHARS = 1500


class ServiceStatusNotifier:
    """Announces one service's start and stop.

    Goes to the operational chats — ``QTE_TELEGRAM__LOG_CHAT_IDS``, falling back
    to the private audience — and never to the broadcast one, for the same
    reason the error hook does not: a signal channel is for signals, and
    "ingestion restarted" is not one.
    """

    def __init__(self, service: str, sink: TelegramSink | None = None) -> None:
        self.service = service
        self._sink = sink or TelegramSink(
            bot_token=settings.telegram.log_bot_token or settings.telegram.bot_token
        )
        self._targets: list[ChatTarget] = parse_chat_targets(
            settings.telegram.log_chat_ids or settings.telegram.private_chat_ids
        )
        #: Set by the start announcement, so the stop one can report uptime.
        self._started_at: datetime | None = None

    @property
    def active(self) -> bool:
        """Whether an announcement would reach anyone."""
        return bool(
            settings.telegram.service_status_enabled and self._sink.configured and self._targets
        )

    @property
    def targets(self) -> list[ChatTarget]:
        """The chats announcements go to — read for logging and tests."""
        return list(self._targets)

    async def announce_started(self, details: Mapping[str, Any] | None = None) -> None:
        """Say the service is up, with whatever it wants to show about itself."""
        self._started_at = datetime.now(UTC)
        await self._announce(f"{UP_ICON} {self.service} UP", details)

    async def announce_stopped(
        self, details: Mapping[str, Any] | None = None, *, reason: str | None = None
    ) -> None:
        """Say the service is down, and for how long it had been up.

        A stop that follows no start is reported as one: a service that died on
        the way up is exactly the case where the chat is the only place anybody
        is looking.
        """
        lines: dict[str, Any] = dict(details or {})
        if self._started_at is None:
            lines = {"State": "did not finish starting", **lines}
        else:
            lines = {"Uptime": _elapsed(datetime.now(UTC) - self._started_at), **lines}
        if reason:
            lines["Reason"] = reason
        await self._announce(f"{DOWN_ICON} {self.service} DOWN", lines)
        self._started_at = None

    async def aclose(self) -> None:
        """Release the HTTP client. Nothing is announced by this."""
        await self._sink.stop()

    # ── Internals ─────────────────────────────────────────────────────

    async def _announce(self, headline: str, details: Mapping[str, Any] | None) -> None:
        if not self.active:
            return
        body = _format_status(headline, details)
        for target in self._targets:
            await self._sink.send_message(target, body)

    def describe(self) -> str:
        """One line for the service's own log, so a silent chat is explainable."""
        if self.active:
            return f"announcing to {[target.label for target in self._targets]}"
        return "off (no token, no chat, or QTE_TELEGRAM__SERVICE_STATUS_ENABLED=false)"


def _format_status(headline: str, details: Mapping[str, Any] | None) -> str:
    """The message body: a headline, the service's own lines, a timestamp."""
    lines = [escaped(headline), DIVIDER]
    for label, value in (details or {}).items():
        lines.append(f"{escaped(label)}: {escaped(_rendered(value))}")
    if len(lines) > 2:
        lines.append(DIVIDER)
    lines.append(escaped(_stamped(datetime.now(UTC))))
    return clipped("\n".join(lines), MAX_STATUS_CHARS)


def _rendered(value: Any) -> str:
    """A detail's value, with sequences joined rather than shown as a repr."""
    if isinstance(value, bool) or not isinstance(value, (list, tuple, set)):
        return str(value)
    return ", ".join(str(item) for item in value) or "—"


def _stamped(moment: datetime) -> str:
    return moment.astimezone(settings.telegram.display_zone).strftime("%Y-%m-%d %H:%M:%S %Z")


def _elapsed(duration: Any) -> str:
    """``4h 12m`` / ``12m 03s`` / ``41s`` — readable at a glance, not precise."""
    seconds = int(duration.total_seconds())
    if seconds < 60:
        return f"{seconds}s"
    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m {seconds:02d}s"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h {minutes:02d}m"
    days, hours = divmod(hours, 24)
    return f"{days}d {hours:02d}h {minutes:02d}m"
