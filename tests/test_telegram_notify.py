"""Executable spec for the Telegram position-lifecycle message.

One message per cycle, edited in place: the tests below pin the three things
that make that true — the chat-id parsing (and therefore how an audience is
switched off), the body rendered from a cycle's stored events, and the
send-then-edit bookkeeping including what happens when the message is gone.

Nothing here touches the network: :class:`FakeSink` stands in for the Bot API,
and :class:`FakeCycles` for Postgres.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from qte_shared.config import settings
from qte_shared.models import BrokerSignal, PositionBlock, SignalAction
from qte_shared.notifications.telegram_sink import (
    ChatTarget,
    EditOutcome,
    boxed,
    clipped,
    parse_chat_targets,
)
from qte_strategy_engine.db.repository import TelegramCycleRecord
from qte_strategy_engine.telegram_notify import (
    BROADCAST_AUDIENCE,
    PRIVATE_AUDIENCE,
    STATUS_CLOSED,
    STATUS_RUNNING,
    TelegramLifecycleNotifier,
    format_cycle,
    format_number,
    lifecycle_event,
)

CYCLE_UXID = "9F2C4B7E18A3D605"


# ── Doubles ───────────────────────────────────────────────────────────────


class FakeSink:
    """Records every Bot API call and replays scripted answers."""

    def __init__(self, *, edit_outcomes: list[EditOutcome] | None = None) -> None:
        self.configured = True
        self.started = False
        self.stopped = False
        self.sent: list[tuple[ChatTarget, str]] = []
        self.edited: list[tuple[ChatTarget, str, str]] = []
        self._edit_outcomes = edit_outcomes or []
        self._next_message_id = 100

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def send_message(self, target: ChatTarget, message_text: str) -> str | None:
        self.sent.append((target, message_text))
        self._next_message_id += 1
        return str(self._next_message_id)

    async def edit_message(
        self, target: ChatTarget, message_id: str, message_text: str
    ) -> EditOutcome:
        self.edited.append((target, message_id, message_text))
        if self._edit_outcomes:
            return self._edit_outcomes.pop(0)
        return EditOutcome.OK


class FakeCycles:
    """In-memory stand-in for ``telegram_cycle_messages``."""

    def __init__(self) -> None:
        self.stored: dict[tuple[str, str, str], TelegramCycleRecord] = {}
        self.saves = 0

    async def load(self, strategy: str, symbol: str, signal_uxid: str):
        return self.stored.get((strategy, symbol, signal_uxid))

    async def save(self, record: TelegramCycleRecord) -> bool:
        self.saves += 1
        self.stored[(record.strategy, record.symbol, record.signal_uxid)] = record
        return True


def build_signal(
    action: SignalAction,
    *,
    price: float | None = 2340.0,
    quantity: float | None = 0.05,
    sl: float | None = None,
    tp1: float | None = None,
    tp2: float | None = None,
    moment: datetime | None = None,
) -> BrokerSignal:
    return BrokerSignal(
        strategy="QTE_EXAMPLE_EMA_ATR",
        symbol="XAUUSD",
        timeframe="M15",
        timestamp=moment or datetime(2026, 10, 8, 12, 0, tzinfo=UTC),
        signal_uxid=CYCLE_UXID,
        position=PositionBlock(
            action=action,
            price=price,
            quantity=quantity,
            sl=sl,
            tp1=tp1,
            tp2=tp2,
            risk_percent=1.0,
        ),
    )


def queued_event(
    signal: BrokerSignal,
    *,
    delivery_id: str = "delivery-1",
    delivery_status: str = "sent",
    transport: str = "nats",
    detail: str | None = None,
) -> dict:
    """What the runner hands the notifier for one emitted signal."""
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
    return event


@pytest.fixture
def configured_chats(monkeypatch: pytest.MonkeyPatch) -> None:
    """Both audiences on, each with one chat."""
    monkeypatch.setattr(settings.telegram, "enabled", True)
    monkeypatch.setattr(settings.telegram, "bot_token", "123:ABC")
    monkeypatch.setattr(settings.telegram, "broadcast_chat_ids", "-1001111111111")
    monkeypatch.setattr(settings.telegram, "private_chat_ids", "-1002173777783_924584")


# ── Chat targets ──────────────────────────────────────────────────────────


def test_an_empty_chat_setting_names_no_target():
    """This is how an audience is switched off — see the module docstring."""
    assert parse_chat_targets("") == []
    assert parse_chat_targets(None) == []
    assert parse_chat_targets("  ,  ") == []
    assert parse_chat_targets("-") == []


def test_a_chat_list_fans_out_and_collapses_duplicates():
    targets = parse_chat_targets(" -1001111111111 , @public_channel ,-1001111111111")
    assert targets == [ChatTarget("-1001111111111"), ChatTarget("@public_channel")]


def test_a_topic_suffix_becomes_a_message_thread():
    [target] = parse_chat_targets("-1002173777783_924584")
    assert target == ChatTarget("-1002173777783", 924584)
    assert target.stored_key == "-1002173777783_924584"


def test_an_underscore_in_a_username_is_not_a_topic():
    """``@my_group_2`` is a name, not a chat with a topic."""
    assert parse_chat_targets("@my_group_2") == [ChatTarget("@my_group_2")]


# ── Formatting ────────────────────────────────────────────────────────────


def test_numbers_read_the_way_a_human_writes_them():
    assert format_number(2340.0) == "2340"
    assert format_number(0.10400000) == "0.104"
    assert format_number(0.0) == "0"
    assert format_number(None) == "—"


def test_the_entry_message_carries_the_position_and_its_levels(monkeypatch):
    monkeypatch.setattr(settings.telegram, "timezone", "UTC")
    signal = build_signal(SignalAction.LONG, sl=2330.0, tp1=2350.0, tp2=2360.0)
    record = TelegramCycleRecord(
        strategy=signal.strategy,
        symbol=signal.symbol,
        signal_uxid=signal.signal_uxid,
        timeframe="M15",
        events=[queued_event(signal)],
    )

    broadcast = format_cycle(record, audience=BROADCAST_AUDIENCE)
    assert f"[⏳{STATUS_RUNNING}]" in broadcast
    assert "📈 LONG XAUUSD (M15)" in broadcast
    assert "Price: 2340" in broadcast
    assert "Quantity: 0.05 | Risk: 1%" in broadcast
    assert "SL: 2330 | TP1: 2350 | TP2: 2360" in broadcast
    assert "Actions:" not in broadcast
    # The broadcast audience learns nothing about which strategy traded.
    assert signal.strategy not in broadcast
    assert CYCLE_UXID not in broadcast

    private = format_cycle(record, audience=PRIVATE_AUDIENCE)
    assert f"Strategy: {signal.strategy}" in private
    assert f"Signal: {CYCLE_UXID}" in private
    assert "Delivery: sent via nats" in private


def test_every_later_action_is_appended_to_the_same_body():
    entry = queued_event(build_signal(SignalAction.LONG, sl=2330.0, tp1=2350.0))
    take_profit = queued_event(
        build_signal(
            SignalAction.TP1,
            price=2350.0,
            quantity=0.025,
            moment=datetime(2026, 10, 8, 12, 30, tzinfo=UTC),
        ),
        delivery_id="delivery-2",
    )
    record = TelegramCycleRecord(
        strategy="QTE_EXAMPLE_EMA_ATR",
        symbol="XAUUSD",
        signal_uxid=CYCLE_UXID,
        timeframe="M15",
        events=[entry, take_profit],
    )

    body = format_cycle(record, audience=BROADCAST_AUDIENCE)
    # The entry block is still there: the message is the whole cycle, not its
    # latest event.
    assert "📈 LONG XAUUSD (M15)" in body
    assert "Actions:" in body
    assert "🎯 TP1" in body
    assert "Price: 2350 | Quantity: 0.025" in body
    # A TP1 is a partial by contract, so the cycle has not closed.
    assert f"[⏳{STATUS_RUNNING}]" in body


def test_a_shadow_entry_is_labelled_as_one():
    record = TelegramCycleRecord(
        strategy="QTE_EXAMPLE_EMA_ATR",
        symbol="XAUUSD",
        signal_uxid=CYCLE_UXID,
        events=[queued_event(build_signal(SignalAction.LONG), delivery_status="shadow")],
    )
    assert "[🧪SHADOW]" in format_cycle(record, audience=BROADCAST_AUDIENCE)


def test_an_action_the_broker_never_took_says_so():
    record = TelegramCycleRecord(
        strategy="QTE_EXAMPLE_EMA_ATR",
        symbol="XAUUSD",
        signal_uxid=CYCLE_UXID,
        events=[
            queued_event(build_signal(SignalAction.LONG)),
            queued_event(
                build_signal(SignalAction.SL, price=2330.0),
                delivery_id="delivery-2",
                delivery_status="failed",
                detail="connection refused",
            ),
        ],
    )
    body = format_cycle(record, audience=BROADCAST_AUDIENCE)
    assert "⚠️ NOT DELIVERED (failed) — connection refused" in body


def test_the_body_is_boxed_without_nesting_a_pre_inside_a_pre():
    """Telegram's HTML parser rejects a nested ``<pre>``; the seams reopen it."""
    record = TelegramCycleRecord(
        strategy="QTE_EXAMPLE_EMA_ATR",
        symbol="XAUUSD",
        signal_uxid=CYCLE_UXID,
        events=[
            queued_event(build_signal(SignalAction.LONG)),
            queued_event(build_signal(SignalAction.TP1), delivery_id="delivery-2"),
        ],
    )
    rendered = boxed(format_cycle(record, audience=PRIVATE_AUDIENCE))
    assert rendered.startswith("<pre>") and rendered.endswith("</pre>")
    assert rendered.count("<pre>") == rendered.count("</pre>")
    assert "<pre><pre>" not in rendered


# ── Delivery bookkeeping ──────────────────────────────────────────────────


async def test_the_entry_sends_one_message_per_chat_and_remembers_its_id(configured_chats):
    sink, cycles = FakeSink(), FakeCycles()
    notifier = TelegramLifecycleNotifier(sink=sink, cycles=cycles)

    record = await notifier.deliver(queued_event(build_signal(SignalAction.LONG)))

    assert len(sink.sent) == 2, "one message for the broadcast chat, one for the private chat"
    assert sink.edited == []
    assert set(record.messages) == {
        f"{BROADCAST_AUDIENCE}:-1001111111111",
        f"{PRIVATE_AUDIENCE}:-1002173777783_924584",
    }
    assert cycles.saves == 1


async def test_a_later_action_edits_the_message_instead_of_sending_another(configured_chats):
    sink, cycles = FakeSink(), FakeCycles()
    notifier = TelegramLifecycleNotifier(sink=sink, cycles=cycles)

    await notifier.deliver(queued_event(build_signal(SignalAction.LONG, sl=2330.0, tp1=2350.0)))
    record = await notifier.deliver(
        queued_event(build_signal(SignalAction.TP1, price=2350.0), delivery_id="delivery-2")
    )

    assert len(sink.sent) == 2, "still only the two messages the entry posted"
    assert len(sink.edited) == 2, "both chats were rewritten in place"
    assert [event["action"] for event in record.events] == ["LONG", "TP1"]
    assert "🎯 TP1" in sink.edited[-1][2]


async def test_a_terminal_action_closes_the_cycle(configured_chats):
    sink, cycles = FakeSink(), FakeCycles()
    notifier = TelegramLifecycleNotifier(sink=sink, cycles=cycles)

    await notifier.deliver(queued_event(build_signal(SignalAction.LONG)))
    record = await notifier.deliver(
        queued_event(build_signal(SignalAction.SL, price=2330.0), delivery_id="delivery-2")
    )

    assert record.status == STATUS_CLOSED
    assert f"[🏁{STATUS_CLOSED}]" in sink.edited[-1][2]


async def test_a_message_that_is_gone_is_sent_again(configured_chats):
    sink = FakeSink(edit_outcomes=[EditOutcome.MISSING, EditOutcome.MISSING])
    cycles = FakeCycles()
    notifier = TelegramLifecycleNotifier(sink=sink, cycles=cycles)

    await notifier.deliver(queued_event(build_signal(SignalAction.LONG)))
    first_ids = dict(cycles.stored[("QTE_EXAMPLE_EMA_ATR", "XAUUSD", CYCLE_UXID)].messages)
    record = await notifier.deliver(
        queued_event(build_signal(SignalAction.TP1), delivery_id="delivery-2")
    )

    assert len(sink.sent) == 4, "the two replacements joined the two original sends"
    assert record.messages != first_ids, "the new ids replaced the ones Telegram lost"


async def test_a_transient_edit_failure_keeps_the_message_id(configured_chats):
    """Re-sending on a rate limit would duplicate the cycle in the chat."""
    sink = FakeSink(edit_outcomes=[EditOutcome.FAILED, EditOutcome.FAILED])
    cycles = FakeCycles()
    notifier = TelegramLifecycleNotifier(sink=sink, cycles=cycles)

    await notifier.deliver(queued_event(build_signal(SignalAction.LONG)))
    before = dict(cycles.stored[("QTE_EXAMPLE_EMA_ATR", "XAUUSD", CYCLE_UXID)].messages)
    record = await notifier.deliver(
        queued_event(build_signal(SignalAction.TP1), delivery_id="delivery-2")
    )

    assert len(sink.sent) == 2, "nothing was re-sent"
    assert record.messages == before


async def test_a_retried_delivery_updates_its_own_action(configured_chats):
    """The outbox retries the same row; the chat must not grow a second TP1."""
    sink, cycles = FakeSink(), FakeCycles()
    notifier = TelegramLifecycleNotifier(sink=sink, cycles=cycles)

    await notifier.deliver(queued_event(build_signal(SignalAction.LONG)))
    await notifier.deliver(
        queued_event(
            build_signal(SignalAction.TP1),
            delivery_id="delivery-2",
            delivery_status="unknown",
        )
    )
    record = await notifier.deliver(
        queued_event(
            build_signal(SignalAction.TP1), delivery_id="delivery-2", delivery_status="sent"
        )
    )

    assert [event["action"] for event in record.events] == ["LONG", "TP1"]
    assert record.events[-1]["delivery_status"] == "sent"
    assert "NOT DELIVERED" not in sink.edited[-1][2]


async def test_one_audience_can_be_switched_off_on_its_own(monkeypatch):
    monkeypatch.setattr(settings.telegram, "enabled", True)
    monkeypatch.setattr(settings.telegram, "bot_token", "123:ABC")
    monkeypatch.setattr(settings.telegram, "broadcast_chat_ids", "")
    monkeypatch.setattr(settings.telegram, "private_chat_ids", "-1001111111111")
    sink, cycles = FakeSink(), FakeCycles()
    notifier = TelegramLifecycleNotifier(sink=sink, cycles=cycles)

    record = await notifier.deliver(queued_event(build_signal(SignalAction.LONG)))

    assert notifier.audiences[BROADCAST_AUDIENCE] == []
    assert len(sink.sent) == 1
    assert set(record.messages) == {f"{PRIVATE_AUDIENCE}:-1001111111111"}


async def test_without_chat_ids_the_notifier_never_starts_a_worker(monkeypatch):
    monkeypatch.setattr(settings.telegram, "enabled", True)
    monkeypatch.setattr(settings.telegram, "bot_token", "123:ABC")
    monkeypatch.setattr(settings.telegram, "broadcast_chat_ids", "")
    monkeypatch.setattr(settings.telegram, "private_chat_ids", "")
    sink, cycles = FakeSink(), FakeCycles()
    notifier = TelegramLifecycleNotifier(sink=sink, cycles=cycles)

    assert not notifier.active
    await notifier.start()
    notifier.note_signal(
        build_signal(SignalAction.LONG),
        delivery_id="delivery-1",
        delivery_status="sent",
        transport="nats",
    )
    await notifier.stop()

    assert not sink.started, "no HTTP client was opened"
    assert sink.sent == [] and cycles.saves == 0


async def test_a_queued_signal_reaches_the_chat_through_the_worker(configured_chats):
    sink, cycles = FakeSink(), FakeCycles()
    notifier = TelegramLifecycleNotifier(sink=sink, cycles=cycles)
    await notifier.start()
    try:
        notifier.note_signal(
            build_signal(SignalAction.LONG, sl=2330.0, tp1=2350.0),
            delivery_id="delivery-1",
            delivery_status="sent",
            transport="nats",
        )
    finally:
        await notifier.stop()

    assert len(sink.sent) == 2
    assert cycles.saves == 1
    assert sink.stopped


async def test_a_full_queue_drops_the_update_rather_than_the_runner(configured_chats, monkeypatch):
    monkeypatch.setattr(settings.telegram, "queue_capacity", 1)
    sink, cycles = FakeSink(), FakeCycles()
    notifier = TelegramLifecycleNotifier(sink=sink, cycles=cycles)
    # A worker that never runs, so the queue genuinely fills up.
    notifier._worker = object()  # noqa: SLF001

    for index in range(5):
        notifier.enqueue(queued_event(build_signal(SignalAction.LONG), delivery_id=str(index)))

    assert notifier._queue.qsize() == 1  # noqa: SLF001


def test_free_text_is_escaped_so_telegram_can_still_parse_the_body():
    """``parse_mode=HTML`` rejects the whole message over one stray ``<``.

    Exception text is the realistic source: a delivery detail carries whatever
    the transport raised, and the chat must still get the update.
    """
    record = TelegramCycleRecord(
        strategy="A&B_PROBE",
        symbol="XAUUSD",
        signal_uxid=CYCLE_UXID,
        events=[
            queued_event(build_signal(SignalAction.LONG)),
            queued_event(
                build_signal(SignalAction.SL, price=2330.0),
                delivery_id="delivery-2",
                delivery_status="failed",
                detail="<ConnectError> & no ack",
            ),
        ],
    )
    body = format_cycle(record, audience=PRIVATE_AUDIENCE)

    assert "&lt;ConnectError&gt; &amp; no ack" in body
    assert "<ConnectError>" not in body
    assert "Strategy: A&amp;B_PROBE" in body
    # The box seams are markup of ours and must survive escaping.
    assert "</pre>\n<pre>" in body


def test_a_long_cycle_stays_inside_the_bot_api_message_limit(monkeypatch):
    """A message over 4096 characters is rejected whole, not truncated."""
    monkeypatch.setattr(settings.telegram, "include_signal_raw", True)
    entry = queued_event(build_signal(SignalAction.LONG, sl=2330.0, tp1=2350.0))
    entry["indicators"] = {f"indicator_{index}": index * 1.5 for index in range(200)}
    record = TelegramCycleRecord(
        strategy="QTE_EXAMPLE_EMA_ATR",
        symbol="XAUUSD",
        signal_uxid=CYCLE_UXID,
        timeframe="M15",
        events=[entry]
        + [
            queued_event(build_signal(SignalAction.TP1, price=2350.0), delivery_id=f"d{index}")
            for index in range(40)
        ],
    )

    body = boxed(format_cycle(record, audience=PRIVATE_AUDIENCE, include_raw=True))

    assert len(body) < 4096
    assert "… truncated" in body, "the raw dump was clipped"
    assert "earlier action(s) not shown" in body, "the oldest actions were dropped"
    # What a reader needs most is still there: the entry and the newest action.
    assert "📈 LONG XAUUSD (M15)" in body
    assert "🎯 TP1" in body


def test_a_huge_delivery_detail_cannot_blow_up_the_message():
    """``DeliveryResult.detail`` is exception text, and nothing bounds it."""
    record = TelegramCycleRecord(
        strategy="QTE_EXAMPLE_EMA_ATR",
        symbol="XAUUSD",
        signal_uxid=CYCLE_UXID,
        events=[
            queued_event(build_signal(SignalAction.LONG)),
            queued_event(
                build_signal(SignalAction.SL, price=2330.0),
                delivery_id="delivery-2",
                delivery_status="failed",
                detail="<broken> " * 2000,
            ),
        ],
    )

    body = boxed(format_cycle(record, audience=PRIVATE_AUDIENCE))

    assert len(body) < 4096
    assert "NOT DELIVERED (failed)" in body
    assert "&lt;broken&gt;" in body


def test_clipping_a_single_long_line_keeps_the_line_not_just_the_header():
    """A line boundary is a preference, not a rule — see ``clipped``."""
    one_line = "x" * 500
    assert clipped(f"header\n{one_line}", 200).startswith("header\nxxxx")
    # With room for whole lines, the boundary is respected.
    assert (
        clipped("first line\nsecond line\nthird line", 25) == "first line\nsecond line\n… truncated"
    )
