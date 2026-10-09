"""Executable spec for the service UP/DOWN announcements.

What `make start` and `make stop` put in the chat, and the rules that keep it
honest: the message says what the service reports about itself, a stop reports
how long it had been up, a service that died on the way up says so, and none of
it ever reaches the broadcast audience or raises into a shutdown.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from qte_shared.config import settings
from qte_shared.notifications.service_status import (
    ServiceStatusNotifier,
    _elapsed,
    _format_status,
)
from qte_shared.notifications.telegram_sink import ChatTarget


class FakeSink:
    def __init__(self, *, failing: bool = False) -> None:
        self.configured = True
        self.stopped = False
        self.failing = failing
        self.sent: list[tuple[ChatTarget, str]] = []

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        self.stopped = True

    async def send_message(self, target: ChatTarget, message_text: str) -> str | None:
        self.sent.append((target, message_text))
        if self.failing:
            raise RuntimeError("Bot API is unreachable")
        return "100"


@pytest.fixture
def announcing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings.telegram, "enabled", True)
    monkeypatch.setattr(settings.telegram, "bot_token", "123:ABC")
    monkeypatch.setattr(settings.telegram, "broadcast_chat_ids", "-1009999999999")
    monkeypatch.setattr(settings.telegram, "private_chat_ids", "-1001111111111")
    monkeypatch.setattr(settings.telegram, "log_chat_ids", "")
    monkeypatch.setattr(settings.telegram, "service_status_enabled", True)
    monkeypatch.setattr(settings.telegram, "timezone", "UTC")


async def test_a_service_announces_what_it_is_running(announcing):
    sink = FakeSink()
    notifier = ServiceStatusNotifier("strategy-runner", sink=sink)

    await notifier.announce_started(
        {
            "Book": "dev.shadow.mt5",
            "Strategies": ["XAUUSD M15 QTE_EXAMPLE_EMA_ATR"],
            "Shadow mode": "ON",
        }
    )

    [(target, body)] = sink.sent
    assert target == ChatTarget("-1001111111111")
    assert "🟢 strategy-runner UP" in body
    assert "Book: dev.shadow.mt5" in body
    assert "Strategies: XAUUSD M15 QTE_EXAMPLE_EMA_ATR" in body
    assert "Shadow mode: ON" in body


async def test_the_stop_message_reports_how_long_it_had_been_up(announcing):
    sink = FakeSink()
    notifier = ServiceStatusNotifier("data-ingestion", sink=sink)

    await notifier.announce_started({"Providers": ["mt5"]})
    notifier._started_at = datetime.now(UTC) - timedelta(hours=4, minutes=12)  # noqa: SLF001
    await notifier.announce_stopped()

    body = sink.sent[-1][1]
    assert "🛑 data-ingestion DOWN" in body
    assert "Uptime: 4h 12m" in body


async def test_a_service_that_never_came_up_says_so(announcing):
    """A crash on the way up is the case nobody is watching a terminal for."""
    sink = FakeSink()
    notifier = ServiceStatusNotifier("strategy-runner", sink=sink)

    await notifier.announce_stopped(reason="Redis refused the connection")

    [(_, body)] = sink.sent
    assert "did not finish starting" in body
    assert "Reason: Redis refused the connection" in body
    assert "Uptime" not in body


async def test_announcements_never_go_to_the_broadcast_audience(announcing):
    notifier = ServiceStatusNotifier("strategy-runner", sink=FakeSink())
    assert notifier.targets == [ChatTarget("-1001111111111")]


async def test_a_dedicated_log_chat_takes_the_announcements(announcing, monkeypatch):
    monkeypatch.setattr(settings.telegram, "log_chat_ids", "-1002173777783_924584")
    notifier = ServiceStatusNotifier("strategy-runner", sink=FakeSink())
    assert notifier.targets == [ChatTarget("-1002173777783", 924584)]


async def test_the_switch_turns_both_messages_off(announcing, monkeypatch):
    monkeypatch.setattr(settings.telegram, "service_status_enabled", False)
    sink = FakeSink()
    notifier = ServiceStatusNotifier("strategy-runner", sink=sink)

    await notifier.announce_started({"Book": "dev.shadow.mt5"})
    await notifier.announce_stopped()

    assert not notifier.active
    assert sink.sent == []


async def test_nothing_is_sent_without_a_chat(monkeypatch):
    monkeypatch.setattr(settings.telegram, "enabled", True)
    monkeypatch.setattr(settings.telegram, "bot_token", "123:ABC")
    monkeypatch.setattr(settings.telegram, "private_chat_ids", "")
    monkeypatch.setattr(settings.telegram, "log_chat_ids", "")
    sink = FakeSink()
    notifier = ServiceStatusNotifier("strategy-runner", sink=sink)

    await notifier.announce_started()

    assert not notifier.active
    assert sink.sent == []


async def test_a_dead_bot_api_does_not_stop_a_service_from_stopping(announcing):
    """The caller suppresses, but the send must not be the thing that raises."""
    sink = FakeSink(failing=True)
    notifier = ServiceStatusNotifier("strategy-runner", sink=sink)

    with pytest.raises(RuntimeError):
        # The sink itself is what raises here; the real one never does, which
        # is why the runner's suppress is a second line of defence, not the first.
        await notifier.announce_started()

    await notifier.aclose()
    assert sink.stopped


def test_details_render_as_lines_a_person_reads():
    body = _format_status("🟢 data-ingestion UP", {"Symbols": ["XAUUSD", "EURUSD"], "Feeds": 2})
    assert "Symbols: XAUUSD, EURUSD" in body
    assert "Feeds: 2" in body


def test_a_detail_carrying_markup_is_escaped():
    body = _format_status("🟢 strategy-runner UP", {"Note": "<b>live</b> & loaded"})
    assert "&lt;b&gt;live&lt;/b&gt; &amp; loaded" in body
    assert "<b>" not in body


@pytest.mark.parametrize(
    ("duration", "expected"),
    [
        (timedelta(seconds=41), "41s"),
        (timedelta(minutes=12, seconds=3), "12m 03s"),
        (timedelta(hours=4, minutes=12), "4h 12m"),
        (timedelta(days=2, hours=3, minutes=4), "2d 03h 04m"),
    ],
)
def test_uptime_reads_at_a_glance(duration, expected):
    assert _elapsed(duration) == expected
