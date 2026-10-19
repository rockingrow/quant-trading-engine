"""Executable spec for forwarding ERROR logs to Telegram.

Four rules are pinned here, and each one is a way the feature could make an
incident worse rather than better: it must not recurse when the send itself
fails, it must not repeat the same error inside the dedup window, it must not
publish an error to the broadcast audience, and it must not leave a traceback
unescaped (which would make Telegram reject the message whole).
"""

from __future__ import annotations

import asyncio
import logging

import pytest

from qte_shared.config import settings
from qte_shared.notifications.telegram_errors import (
    EXCLUDED_LOGGER_PREFIXES,
    MAX_ERROR_CHARS,
    TelegramErrorNotifier,
)
from qte_shared.notifications.telegram_sink import ChatTarget


class FakeSink:
    """Records every send; fails on demand, the way a dead Bot API would."""

    def __init__(self, *, failing: bool = False) -> None:
        self.configured = True
        self.started = False
        self.stopped = False
        self.failing = failing
        self.sent: list[tuple[ChatTarget, str]] = []

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def send_message(self, target: ChatTarget, message_text: str) -> str | None:
        self.sent.append((target, message_text))
        if self.failing:
            raise RuntimeError("Bot API is unreachable")
        return "100"


@pytest.fixture
def forwarding_on(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings.telegram, "enabled", True)
    monkeypatch.setattr(settings.telegram, "bot_token", "123:ABC")
    monkeypatch.setattr(settings.telegram, "broadcast_chat_ids", "-1009999999999")
    monkeypatch.setattr(settings.telegram, "private_chat_ids", "-1001111111111")
    monkeypatch.setattr(settings.telegram, "log_chat_ids", "")
    monkeypatch.setattr(settings.telegram, "log_errors_enabled", True)
    monkeypatch.setattr(settings.telegram, "log_dedup_window", 60.0)


async def _settled() -> None:
    """Let ``call_soon_threadsafe`` land and the worker drain what it queued."""
    for _ in range(5):
        await asyncio.sleep(0)


async def test_an_error_log_reaches_the_private_chat(forwarding_on):
    sink = FakeSink()
    notifier = TelegramErrorNotifier(sink=sink)
    await notifier.start("strategy-runner")
    try:
        logging.getLogger("qte_strategy_engine.runner").error("Broker refused %s", "XAUUSD")
        await _settled()
    finally:
        await notifier.stop()

    [(target, body)] = sink.sent
    assert target == ChatTarget("-1001111111111")
    assert "Broker refused XAUUSD" in body
    assert "[QTE strategy-runner]" in body
    assert "ERROR | qte_strategy_engine.runner" in body
    assert sink.started and sink.stopped


async def test_errors_never_go_to_the_broadcast_audience(forwarding_on):
    """An error carries internal state; a signal channel is not the place."""
    notifier = TelegramErrorNotifier(sink=FakeSink())
    assert notifier.targets == [ChatTarget("-1001111111111")]


async def test_a_dedicated_log_chat_overrides_the_private_one(forwarding_on, monkeypatch):
    monkeypatch.setattr(settings.telegram, "log_chat_ids", "-1002173777783_924584")
    notifier = TelegramErrorNotifier(sink=FakeSink())
    assert notifier.targets == [ChatTarget("-1002173777783", 924584)]


async def test_the_same_error_is_sent_once_inside_the_dedup_window(forwarding_on):
    sink = FakeSink()
    notifier = TelegramErrorNotifier(sink=sink)
    await notifier.start("strategy-runner")
    try:
        logger = logging.getLogger("qte_strategy_engine.runner")
        for _ in range(20):
            logger.error("NATS connection lost")
        logger.error("Something else broke")
        await _settled()
    finally:
        await notifier.stop()

    bodies = [body for _, body in sink.sent]
    assert len(bodies) == 2, "the repeat was suppressed, the different error was not"
    assert any("NATS connection lost" in body for body in bodies)
    assert any("Something else broke" in body for body in bodies)


async def test_the_same_message_from_a_different_logger_is_not_a_repeat(forwarding_on):
    sink = FakeSink()
    notifier = TelegramErrorNotifier(sink=sink)
    await notifier.start("strategy-runner")
    try:
        logging.getLogger("qte_ingestion.service").error("Feed stalled")
        logging.getLogger("qte_strategy_engine.runner").error("Feed stalled")
        await _settled()
    finally:
        await notifier.stop()

    assert len(sink.sent) == 2


async def test_a_zero_window_forwards_every_record(forwarding_on, monkeypatch):
    monkeypatch.setattr(settings.telegram, "log_dedup_window", 0)
    sink = FakeSink()
    notifier = TelegramErrorNotifier(sink=sink)
    await notifier.start("strategy-runner")
    try:
        for _ in range(3):
            logging.getLogger("qte_strategy_engine.runner").error("Same line")
        await _settled()
    finally:
        await notifier.stop()

    assert len(sink.sent) == 3


async def test_a_failing_send_does_not_feed_itself(forwarding_on):
    """The send path logs its own failures; forwarding those would never end."""
    sink = FakeSink(failing=True)
    notifier = TelegramErrorNotifier(sink=sink)
    await notifier.start("strategy-runner")
    try:
        logging.getLogger("qte_strategy_engine.runner").error("First failure")
        await _settled()
        # What the sink would log on failure, logged for real.
        for prefix in EXCLUDED_LOGGER_PREFIXES:
            logging.getLogger(prefix).error("Telegram sendMessage failed")
        await _settled()
    finally:
        await notifier.stop()

    assert len(sink.sent) == 1, "only the original error was ever offered"


async def test_only_errors_are_forwarded(forwarding_on):
    sink = FakeSink()
    notifier = TelegramErrorNotifier(sink=sink)
    await notifier.start("strategy-runner")
    try:
        logger = logging.getLogger("qte_strategy_engine.runner")
        logger.info("A bar closed")
        logger.warning("Shadow mode is on")
        logger.critical("The book is unreadable")
        await _settled()
    finally:
        await notifier.stop()

    assert len(sink.sent) == 1
    assert "The book is unreadable" in sink.sent[0][1]


async def test_a_traceback_is_escaped_and_clipped(forwarding_on):
    """``in <module>`` would break HTML parsing; a long one breaks the 4096 cap."""
    sink = FakeSink()
    notifier = TelegramErrorNotifier(sink=sink)
    await notifier.start("strategy-runner")
    try:
        try:
            raise ValueError("levels <inverted> & unusable\n" + "frame\n" * 4000)
        except ValueError:
            logging.getLogger("qte_strategy_engine.runner").exception("Signal was refused")
        await _settled()
    finally:
        await notifier.stop()

    [(_, body)] = sink.sent
    assert "&lt;inverted&gt; &amp; unusable" in body
    assert "<inverted>" not in body
    assert len(body) < 4096
    assert "truncated" in body


async def test_nothing_is_attached_while_the_feature_is_off(monkeypatch):
    monkeypatch.setattr(settings.telegram, "enabled", True)
    monkeypatch.setattr(settings.telegram, "bot_token", "123:ABC")
    monkeypatch.setattr(settings.telegram, "private_chat_ids", "-1001111111111")
    monkeypatch.setattr(settings.telegram, "log_errors_enabled", False)
    sink = FakeSink()
    notifier = TelegramErrorNotifier(sink=sink)

    before = list(logging.getLogger().handlers)
    await notifier.start("strategy-runner")
    try:
        logging.getLogger("qte_strategy_engine.runner").error("Broker refused the signal")
        await _settled()
        assert logging.getLogger().handlers == before, "the root logger was left alone"
    finally:
        await notifier.stop()

    assert not notifier.active
    assert sink.sent == [] and not sink.started


async def test_the_handler_is_detached_even_after_a_failed_drain(forwarding_on):
    """A stop that times out must still leave the root logger clean."""
    sink = FakeSink()
    notifier = TelegramErrorNotifier(sink=sink)
    before = list(logging.getLogger().handlers)
    await notifier.start("strategy-runner")
    assert logging.getLogger().handlers != before
    await notifier.stop(drain_timeout=0.01)

    assert logging.getLogger().handlers == before
    # Records after the stop go nowhere, so a late failure cannot recurse.
    logging.getLogger("qte_strategy_engine.runner").error("Too late")
    await _settled()
    assert sink.sent == []


def test_the_clip_limit_leaves_room_for_the_bot_api_cap():
    assert MAX_ERROR_CHARS < 4096


async def test_an_error_made_of_markup_still_fits_the_cap(forwarding_on):
    """Escaping expands text, so the clip has to come after it, not before."""
    sink = FakeSink()
    notifier = TelegramErrorNotifier(sink=sink)
    await notifier.start("strategy-runner")
    try:
        logging.getLogger("qte_strategy_engine.runner").error("&" * 4000)
        await _settled()
    finally:
        await notifier.stop()

    [(_, body)] = sink.sent
    assert len(body) < 4096
    # Nothing was cut mid-entity: every & that survived belongs to one.
    assert "&amp;" in body
    assert not body.rstrip().endswith("&")
