"""Telegram notifications both long-running services use.

What is here is what two services need: the Bot API calls themselves
(:mod:`telegram_sink`), the ERROR-log forwarder (:mod:`telegram_errors`) and
the start/stop announcement (:mod:`service_status`). The position-lifecycle
message is not — one trade cycle, one message edited in place, is the strategy
runner's alone and lives in :mod:`qte_strategy_engine.telegram_notify`.

``httpx`` is imported by the sink at module level, the same way
:mod:`qte_shared.providers.tiingo.rest` does it: it ships in the ``broker`` and
``tiingo`` extras, and an image without it simply never imports this package.
"""

from qte_shared.notifications.service_status import ServiceStatusNotifier
from qte_shared.notifications.telegram_errors import TelegramErrorNotifier
from qte_shared.notifications.telegram_sink import (
    ChatTarget,
    EditOutcome,
    TelegramSink,
    boxed,
    clipped,
    escaped,
    parse_chat_targets,
)

__all__ = [
    "ChatTarget",
    "EditOutcome",
    "ServiceStatusNotifier",
    "TelegramErrorNotifier",
    "TelegramSink",
    "boxed",
    "clipped",
    "escaped",
    "parse_chat_targets",
]
