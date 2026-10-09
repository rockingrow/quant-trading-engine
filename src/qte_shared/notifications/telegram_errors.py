"""Forwarding the runner's ERROR logs to Telegram, de-duplicated.

The counterpart to :mod:`qte_strategy_engine.telegram_notify`: that module
reports what the book *did*, this one reports what broke. Ported from
``algo-trading-broker``'s error-log hook in
``broker/services/notification_service.py``, down to the setting name
(``QTE_TELEGRAM__LOG_ERRORS_ENABLED``) and the one-minute dedup window.

A :class:`logging.Handler` cannot ``await`` anything — ``emit`` is synchronous
and may run from any context, including a worker thread — while the Bot API
call must be awaited. The two are bridged by a queue and a background worker,
exactly as the broker does it:

* ``emit`` formats the record, drops it if it is a repeat, and hands it to the
  event loop with ``loop.call_soon_threadsafe``. It never blocks, never awaits
  and never raises.
* One worker task drains the queue and performs the sends.

Three things keep this from making a bad situation worse:

* **No recursion.** A failing send logs an error of its own. Records from the
  Telegram modules (and from the HTTP client underneath them) are filtered out,
  so a dead Bot API cannot feed itself.
* **No spam.** Identical errors are suppressed for
  ``QTE_TELEGRAM__LOG_DEDUP_WINDOW`` seconds — a reconnect loop logging the
  same line every second is one message a minute, not sixty.
* **No public leak.** The chat defaults to the *private* audience and never to
  the broadcast one. An error carries file paths, symbols and internal state,
  which is not something to publish to a signal channel by accident.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time

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

#: Records from these loggers are never forwarded. The first three are the
#: Telegram path itself; the HTTP client is excluded with them because it is
#: the layer that fails *during* a send, and an error logged there would be
#: forwarded through the same dead socket that produced it.
EXCLUDED_LOGGER_PREFIXES = (
    "qte_shared.notifications",
    "qte_strategy_engine.telegram_notify",
    "httpx",
    "httpcore",
)

#: Errors held while the worker is sending. Under a storm the queue is the
#: wrong place to keep a backlog: the console and the daily log file have every
#: line, so a full queue drops rather than grows.
QUEUE_CAPACITY = 100

#: A traceback is unbounded and the Bot API caps a message at 4096 characters,
#: rejecting a longer one whole. The tail is what gets cut, which keeps the
#: header and the exception type — the part that says what broke.
MAX_ERROR_CHARS = 3000

ERROR_ICON = "🚨"


class _RecursionFilter(logging.Filter):
    """Drop records emitted by the Telegram send path itself."""

    def filter(self, record: logging.LogRecord) -> bool:
        return not record.name.startswith(EXCLUDED_LOGGER_PREFIXES)


class TelegramErrorHandler(logging.Handler):
    """Hands every ERROR+ record to a notifier, without ever blocking.

    Attached to the *root* logger, because that is where this repository's
    console and daily-file handlers already live: one handler then covers every
    module, including the ones a strategy repo brings with it.
    """

    def __init__(self, notifier: TelegramErrorNotifier, *, service: str) -> None:
        super().__init__(level=logging.ERROR)
        self.addFilter(_RecursionFilter())
        self.setFormatter(
            logging.Formatter(fmt=f"[QTE {service}]\n%(levelname)s | %(name)s\n%(message)s")
        )
        self._notifier = notifier

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._notifier.offer(record, self.format(record))
        except Exception:  # pragma: no cover — a handler must never raise
            self.handleError(record)


class TelegramErrorNotifier:
    """Sends forwarded ERROR logs to the operator's chat.

    Owns the queue, the worker and the dedup window; the handler above is only
    the adapter that gets records into it. Inactive unless
    ``QTE_TELEGRAM__LOG_ERRORS_ENABLED`` is on *and* a token and a chat are
    configured, in which case nothing is attached to the root logger at all.
    """

    def __init__(self, sink: TelegramSink | None = None) -> None:
        self._sink = sink or TelegramSink(
            bot_token=settings.telegram.log_bot_token or settings.telegram.bot_token
        )
        # The private audience, never the broadcast one — see the module
        # docstring. A dedicated QTE_TELEGRAM__LOG_CHAT_IDS overrides both.
        self._targets: list[ChatTarget] = parse_chat_targets(
            settings.telegram.log_chat_ids or settings.telegram.private_chat_ids
        )
        self._queue: asyncio.Queue[str] = asyncio.Queue(maxsize=QUEUE_CAPACITY)
        self._loop: asyncio.AbstractEventLoop | None = None
        self._worker: asyncio.Task[None] | None = None
        self._handler: TelegramErrorHandler | None = None
        #: dedup key → monotonic time it was last forwarded.
        self._recent: dict[str, float] = {}

    @property
    def active(self) -> bool:
        """Whether an error would be forwarded anywhere."""
        return bool(
            settings.telegram.log_errors_enabled and self._sink.configured and self._targets
        )

    @property
    def targets(self) -> list[ChatTarget]:
        """The chats errors go to — read for logging and tests."""
        return list(self._targets)

    # ── Lifecycle ─────────────────────────────────────────────────────

    async def start(self, service: str) -> None:
        """Attach to the root logger and launch the worker. Idempotent."""
        if not self.active or self._handler is not None:
            if not self.active:
                log.debug("Telegram error forwarding is off")
            return
        await self._sink.start()
        self._loop = asyncio.get_running_loop()
        self._worker = asyncio.create_task(self._drain_queue(), name="telegram-errors")
        self._handler = TelegramErrorHandler(self, service=service)
        logging.getLogger().addHandler(self._handler)
        log.info(
            "Telegram error forwarding ready chats=%s dedup=%ss",
            [target.label for target in self._targets],
            settings.telegram.log_dedup_window,
        )

    async def stop(self, *, drain_timeout: float = 5.0) -> None:
        """Detach first, then let the backlog go out and close the client.

        Detaching before draining is what keeps shutdown finite: a send that
        fails while the worker is stopping logs an error of its own, and with
        the handler still attached that error would queue another send.
        """
        if self._handler is not None:
            logging.getLogger().removeHandler(self._handler)
            self._handler = None
        worker = self._worker
        self._worker = None
        if worker is not None:
            try:
                await asyncio.wait_for(self._queue.join(), timeout=drain_timeout)
            except TimeoutError:
                log.warning("Dropping %d queued Telegram error(s) on shutdown", self._queue.qsize())
            worker.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker
        await self._sink.stop()

    # ── Producing ─────────────────────────────────────────────────────

    def offer(self, record: logging.LogRecord, message_text: str) -> None:
        """Take one formatted record, unless it repeats a recent one.

        Called from :meth:`TelegramErrorHandler.emit`, which may run on any
        thread, so the queue is only ever touched from the event loop —
        ``asyncio.Queue`` is not thread-safe.
        """
        loop = self._loop
        if self._worker is None or loop is None or loop.is_closed():
            return
        if self._is_repeat(record):
            return
        # Escaped before it is clipped, never after: escaping only ever makes
        # the text longer, so a limit applied first would bound nothing.
        loop.call_soon_threadsafe(self._enqueue, clipped(escaped(message_text), MAX_ERROR_CHARS))

    def _is_repeat(self, record: logging.LogRecord) -> bool:
        """Whether this error was already forwarded inside the dedup window.

        Keyed on the logger, the level and the *interpolated* message rather
        than on the whole formatted text: two occurrences of one error carry
        the same line but not always a byte-identical traceback.
        """
        window = settings.telegram.log_dedup_window
        if window <= 0:
            return False
        try:
            key = f"{record.name}|{record.levelno}|{record.getMessage()}"
        except Exception:
            key = f"{record.name}|{record.levelno}|{record.msg!r}"
        now = time.monotonic()
        # Pruned on every call, so the map cannot grow without bound.
        self._recent = {seen: stamp for seen, stamp in self._recent.items() if now - stamp < window}
        if key in self._recent:
            return True
        self._recent[key] = now
        return False

    def _enqueue(self, message_text: str) -> None:
        """Queue from inside the loop thread; drop when the backlog is full."""
        try:
            self._queue.put_nowait(message_text)
        except asyncio.QueueFull:
            # Deliberately silent: logging here is what the recursion filter
            # exists to prevent, and the console already holds every line.
            pass

    # ── Consuming ─────────────────────────────────────────────────────

    async def _drain_queue(self) -> None:
        while True:
            message_text = await self._queue.get()
            try:
                await self.deliver(message_text)
            except asyncio.CancelledError:
                raise
            except Exception:
                # Never log from here: the sink reports its own failures, and
                # anything this task logged would be filtered out anyway.
                pass
            finally:
                self._queue.task_done()

    async def deliver(self, message_text: str) -> None:
        """Send one already-escaped, already-clipped error to every chat."""
        for target in self._targets:
            await self._sink.send_message(target, f"{ERROR_ICON} {message_text}")
