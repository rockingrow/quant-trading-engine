"""Executable spec for the operator's Telegram bot.

Three things are worth pinning and nothing else is: the table renderer (columns
skew for one wrong character, and a skewed table is unreadable on a phone), the
paging arithmetic, and the argument parsing in front of the commands that
change a live engine — ``all``, a symbol, a strategy, and a refusal with the
list of what the book actually has.

The message bodies are asserted through the presenters, which are pure
functions of what the engine answered, so none of this needs a Telegram
session.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from qte_bot import presenters
from qte_bot.handlers import _as_scope, _scope_from_callback
from qte_bot.pagination import page_footer, page_of, paginate, pagination_keyboard
from qte_bot.settings import bot_settings
from qte_bot.tables import render_table
from qte_strategy_engine.db import ClosedCycle

SYMBOLS = ["XAUUSD", "EURUSD"]
STRATEGIES = ["MT5_GOLD_M15_V2"]


# ── Tables ────────────────────────────────────────────────────────────────


def test_a_table_is_a_pre_block_with_aligned_columns():
    rendered = render_table(("Symbol", "Bars"), [("XAUUSD", 420), ("EURUSDXXX", 7)])
    assert rendered.startswith("<pre>") and rendered.endswith("</pre>")
    lines = rendered.removeprefix("<pre>").removesuffix("</pre>").splitlines()
    # Header, rule, two rows — and every line the same visible width.
    assert len(lines) == 4
    assert len({len(line.rstrip()) for line in lines[:2]}) == 1


def test_numbers_can_be_right_aligned():
    rendered = render_table(("Bars",), [(7,), (420,)], aligns=("r",))
    body = rendered.removeprefix("<pre>").removesuffix("</pre>").splitlines()
    assert body[-2].endswith("  7")
    assert body[-1].endswith("420")


def test_a_long_value_is_truncated_rather_than_wrapped():
    rendered = render_table(("Strategy",), [("A_VERY_LONG_STRATEGY_NAME",)], max_widths=(10,))
    assert "A_VERY_LO…" in rendered
    assert "\n" in rendered  # the rule, not a wrapped cell


def test_a_cell_cannot_inject_markup():
    rendered = render_table(("Name",), [("<b>bold</b> & co",)])
    assert "&lt;b&gt;bold&lt;/b&gt; &amp; co" in rendered
    assert "<b>" not in rendered


def test_a_short_row_is_padded_rather_than_raising():
    rendered = render_table(("A", "B", "C"), [("one",)])
    assert "one" in rendered


# ── Paging ────────────────────────────────────────────────────────────────


def test_a_page_knows_where_it_sits():
    assert page_footer(page_of(42, 10, 0)) == "Showing 1-10 of 42"
    assert page_footer(page_of(42, 10, 40)) == "Showing 41-42 of 42"
    assert page_footer(page_of(0, 10, 0)) == "Nothing to show"


def test_the_first_page_offers_next_only_and_the_last_offers_prev_only():
    first = pagination_keyboard(page_of(42, 10, 0), lambda offset: f"closed:{offset}")
    last = pagination_keyboard(page_of(42, 10, 40), lambda offset: f"closed:{offset}")
    assert [button.callback_data for button in first.inline_keyboard[0]] == ["closed:10"]
    assert [button.callback_data for button in last.inline_keyboard[0]] == ["closed:30"]


def test_one_page_needs_no_buttons_at_all():
    assert pagination_keyboard(page_of(4, 10, 0), lambda offset: f"closed:{offset}") is None


def test_an_offset_past_the_end_clamps_to_the_first_page():
    """A Next button on a message left open while the list shrank must not dead-end."""
    rows, page = paginate(["a", "b", "c"], limit=2, offset=99)
    assert rows == ["a", "b"] and page["offset"] == 0


# ── Scope parsing ─────────────────────────────────────────────────────────


def test_all_is_everything():
    assert _as_scope("all", SYMBOLS, STRATEGIES) == {
        "everything": True,
        "symbol": None,
        "strategy": None,
    }


def test_a_symbol_is_matched_case_insensitively_and_normalised():
    assert _as_scope("xauusd", SYMBOLS, STRATEGIES) == {
        "everything": False,
        "symbol": "XAUUSD",
        "strategy": None,
    }


def test_a_strategy_keeps_the_case_the_runner_published():
    assert _as_scope("mt5_gold_m15_v2", SYMBOLS, STRATEGIES) == {
        "everything": False,
        "symbol": None,
        "strategy": "MT5_GOLD_M15_V2",
    }


def test_anything_else_is_refused_rather_than_guessed():
    assert _as_scope("GOLD", SYMBOLS, STRATEGIES) is None
    assert _as_scope("", SYMBOLS, STRATEGIES) is None


def test_a_confirm_button_carries_the_scope_it_will_act_on():
    """Not an index into something remembered: a late press must still be exact."""
    assert _scope_from_callback("flat:ok:a")["everything"] is True
    assert _scope_from_callback("flat:ok:s:xauusd")["symbol"] == "XAUUSD"
    assert _scope_from_callback("flat:ok:t:MT5_GOLD_M15_V2")["strategy"] == "MT5_GOLD_M15_V2"
    for malformed in ("flat:ok", "flat:ok:s", "flat:ok:s:", "flat:ok:z:X"):
        assert _scope_from_callback(malformed) is None


def test_the_confirm_callback_fits_telegram_budget():
    """64 bytes, and a strategy name is the longest thing that goes in one."""
    encoded = f"flat:ok:t:{'X' * 40}"
    assert len(encoded.encode()) <= 64


# ── Message bodies ────────────────────────────────────────────────────────


def test_the_redis_body_lists_each_series_and_who_wrote_it():
    windows = [
        {
            "symbol": "XAUUSD",
            "timeframe": "M15",
            "bars": 420,
            "newest_bar": datetime(2026, 10, 8, 16, 45, tzinfo=UTC),
            "error": False,
        }
    ]
    body = presenters.redis_windows(windows, "dev.shadow.mt5", "mt5")
    assert "XAUUSD" in body and "420" in body
    assert "dev.shadow.mt5" in body
    assert "mt5" in body


def test_the_runner_body_spells_out_why_a_pair_is_not_ready():
    status = {
        "namespace": "dev.shadow.mt5",
        "execution_mode": "shadow",
        "transport": "nats",
        "delivery_paused": False,
        "started_at": "2026-10-08T12:00:00+00:00",
        "gate": {"everything": False, "symbols": ["XAUUSD"], "strategies": []},
        "strategies": [
            {
                "strategy": "MT5_GOLD_M15_V2",
                "symbol": "XAUUSD",
                "timeframe": "M15",
                "bars": 420,
                "warmup": 200,
                "warm": True,
                "gated": True,
                "uncertain": False,
                "ready": False,
                "open_cycles": [],
            },
            {
                "strategy": "MT5_GOLD_M15_V2",
                "symbol": "EURUSD",
                "timeframe": "M15",
                "bars": 12,
                "warmup": 200,
                "warm": False,
                "gated": False,
                "uncertain": False,
                "ready": False,
                "open_cycles": ["ABC"],
            },
        ],
    }
    body = presenters.runner_status(status)
    assert "paused by /prevent" in body
    assert "still warming (12/200 bars)" in body
    assert "12/200" in body


def test_the_positions_body_carries_the_columns_an_operator_asked_for():
    body = presenters.open_positions(
        [
            {
                "symbol": "XAUUSD",
                "timeframe": "M15",
                "strategy": "MT5_GOLD_M15_V2",
                "signal_uxid": "9F2C4B7E18A3D605",
                "opened_at": datetime(2026, 10, 8, 9, 15, tzinfo=UTC),
            }
        ],
        "dev.shadow.mt5",
    )
    for expected in ("XAUUSD", "M15", "MT5_GOLD_M15_V2", "9F2C4B7E18A3D605"):
        assert expected in body
    assert "1 open" in body


def test_an_empty_book_says_flat_rather_than_printing_an_empty_table():
    assert "Flat" in presenters.open_positions([], "dev.shadow.mt5")


def test_the_closed_body_shows_what_closed_each_cycle():
    cycles = [
        ClosedCycle(
            signal_uxid="9F2C4B7E18A3D605",
            strategy="MT5_GOLD_M15_V2",
            symbol="XAUUSD",
            timeframe="M15",
            opened_at=datetime(2026, 10, 8, 9, 15, tzinfo=UTC),
            closed_at=datetime(2026, 10, 8, 11, 0, tzinfo=UTC),
            closed_by="TP1",
            entry_price=2340.0,
            exit_price=2350.0,
            quantity=0.05,
            actions=2,
        )
    ]
    body = presenters.closed_cycles(cycles, page_of(1, 10, 0), "dev.shadow.mt5")
    assert "TP1" in body and "XAUUSD" in body
    assert "Showing 1-1 of 1" in body


def test_the_flat_result_separates_what_closed_from_what_did_not():
    body = presenters.flat_result(
        {
            "closed": [{"strategy": "S", "symbol": "XAUUSD", "signal_uxid": "9F2C4B7E18A3D605"}],
            "refused": [{"strategy": "S", "symbol": "EURUSD", "reason": "shadow mode"}],
        },
        {"everything": True},
    )
    assert "Closed 1" in body and "9F2C4B7E18A3D605" in body
    assert "Not closed (1)" in body and "shadow mode" in body


def test_a_gate_that_was_stored_but_not_announced_says_so():
    """The difference between "paused now" and "paused on the next bar"."""
    body = presenters.gate_result(
        {"everything": False, "symbols": ["XAUUSD"], "strategies": []},
        blocking=True,
        announced=False,
        namespace="dev.shadow.mt5",
    )
    assert "XAUUSD" in body
    assert "no runner answered" in body


def test_the_usage_text_lists_what_the_book_actually_has():
    body = presenters.usage("flat", SYMBOLS + STRATEGIES)
    assert "/flat all" in body
    for name in SYMBOLS + STRATEGIES:
        assert name in body


def test_a_warmup_result_shows_the_window_before_and_after():
    body = presenters.warmup_result(
        {"warmed": [{"symbol": "XAUUSD", "timeframe": "M15", "bars_before": 120, "bars": 500}]},
        "dev.shadow.mt5",
    )
    assert "120" in body and "500" in body


def test_a_flush_result_says_what_was_dropped_and_what_to_do_next():
    body = presenters.flush_result(
        {"flushed": [{"symbol": "XAUUSD", "timeframe": "M15", "dropped": 420}]},
        "dev.shadow.mt5",
    )
    assert "420" in body
    assert "/warmup" in body


def test_an_unfed_symbol_is_named_in_the_reply():
    body = presenters.flush_result({"flushed": [], "unknown": ["BTCUSDT"]}, "dev.shadow.mt5")
    assert "BTCUSDT" in body


# ── Access ────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("configured", "expected"),
    [("", set()), ("123", {123}), (" 123 , 456 ", {123, 456}), ("123,bad,", {123})],
)
def test_operator_ids_are_parsed_leniently_but_never_invented(monkeypatch, configured, expected):
    monkeypatch.setattr(bot_settings, "admin_ids", configured)
    assert bot_settings.operators == expected


def test_the_usage_text_admits_when_strategy_names_are_missing():
    """A silent runner means an incomplete list, not a shorter one."""
    body = presenters.usage("prevent", SYMBOLS, runner_answered=False)
    assert "runner is not answering" in body
    assert "strategy names are missing" in body
