"""Dispatching control-plane requests: one parse, one table, one error reply.

The runner and ingestion both answer operator commands — from ``qte-control``
and the Telegram bot — as NATS request/reply on their control subjects. Each
used to parse the JSON, branch on ``action`` with an ``if``/``elif`` chain and
keep its own ``_reply``. Two things went wrong the same way in both: a payload
that was valid JSON but not an object raised ``AttributeError``, and a handler
that raised sent no reply at all, so the caller waited out its timeout and
reported "no runner answered" about a runner that was up.

:class:`ControlRouter` is the Command pattern for that: actions register a
handler, :meth:`ControlRouter.dispatch` does the parsing, the lookup and the
reply, and every failure answers when the message asked for an answer. A
handler returning ``None`` sends nothing — the broadcasts (``set_shadow_mode``)
are notifications, not requests.

A failure reply carries :data:`CONTROL_ERROR_KEY`, never a bare ``error``. Some
handlers answer with an ``error`` of their own that *is* their answer (``flat``
with an empty scope), and a client must be able to tell "the service refused
this request" from "the service could not serve any request": the second is
:func:`is_control_failure`, and a client treats it as a service that is down.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from typing import Any, Protocol

from qte_shared.logging_setup import get_logger

log = get_logger(__name__)

#: The key a failure reply carries, and no ordinary answer does.
CONTROL_ERROR_KEY = "control_error"

#: A control action: the parsed command in, the reply (or ``None``) out.
ControlHandler = Callable[[dict[str, Any]], Awaitable[dict[str, Any] | None]]


class _ReplyClient(Protocol):
    async def publish(self, subject: str, payload: bytes) -> None: ...


class _ReplyBus(Protocol):
    @property
    def nc(self) -> _ReplyClient: ...


class _ControlMessage(Protocol):
    data: bytes
    reply: str


class ControlRouter:
    """Routes one service's control commands to their handlers."""

    def __init__(self, service: str) -> None:
        self._service = service
        self._handlers: dict[str, ControlHandler] = {}

    def register(self, action: str, handler: ControlHandler) -> None:
        if action in self._handlers:
            raise ValueError(f"{self._service} already handles control action {action!r}")
        self._handlers[action] = handler

    @property
    def actions(self) -> tuple[str, ...]:
        return tuple(self._handlers)

    async def dispatch(self, message: _ControlMessage, *, reply_bus: _ReplyBus) -> None:
        """Answer *message*, over *reply_bus*, with its handler's reply or an error.

        *reply_bus* is passed per call rather than held, so a service that swaps
        its connection (a reconnect, a test) answers over the one it has now.
        """
        try:
            command = json.loads(message.data)
        except (json.JSONDecodeError, UnicodeDecodeError):
            log.warning("Unparseable %s control message: %.120r", self._service, message.data)
            await self._fail(message, "the command is not valid JSON", reply_bus=reply_bus)
            return
        if not isinstance(command, dict):
            log.warning("%s control message is not a JSON object: %.120r", self._service, command)
            await self._fail(message, "the command must be a JSON object", reply_bus=reply_bus)
            return

        action = command.get("action")
        handler = self._handlers.get(action) if isinstance(action, str) else None
        if handler is None:
            log.warning("Unknown %s control action: %r", self._service, action)
            await self._fail(
                message,
                f"unknown action {action!r}",
                reply_bus=reply_bus,
                actions=list(self._handlers),
            )
            return

        try:
            answer = await handler(command)
        except Exception as failure:  # noqa: BLE001 - the caller gets an answer either way
            log.exception("%s control action %r failed", self._service, action)
            await self._fail(
                message,
                f"{action} failed: {type(failure).__name__}: {failure}",
                reply_bus=reply_bus,
            )
            return
        if answer is not None:
            await self._reply(message, answer, reply_bus=reply_bus)

    async def _fail(
        self,
        message: _ControlMessage,
        reason: str,
        *,
        reply_bus: _ReplyBus,
        **context: Any,
    ) -> None:
        await self._reply(message, {CONTROL_ERROR_KEY: reason, **context}, reply_bus=reply_bus)

    @staticmethod
    async def _reply(
        message: _ControlMessage, payload: dict[str, Any], *, reply_bus: _ReplyBus
    ) -> None:
        """Answer a request, if it was one. A broadcast has no reply subject."""
        if message.reply:
            await reply_bus.nc.publish(message.reply, json.dumps(payload, default=str).encode())


def is_control_failure(answer: Any) -> bool:
    """Whether *answer* says the service could not serve the request at all."""
    return isinstance(answer, dict) and CONTROL_ERROR_KEY in answer


__all__ = ["CONTROL_ERROR_KEY", "ControlHandler", "ControlRouter", "is_control_failure"]
