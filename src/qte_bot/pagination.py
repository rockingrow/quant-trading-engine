"""Shared paging: page metadata plus Prev/Next buttons.

Ported from ``algo-trading-broker``'s ``bot/app/utils/pagination.py``, including
the page shape ``{"total", "limit", "offset"}`` — a listing then differs only
in the callback-data prefix it pages through.

Both sources of rows here produce that shape: the closed-cycle query returns a
total alongside its page, and listings that come back whole are sliced by
:func:`paginate`.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup


def page_of(total: int, limit: int, offset: int) -> dict[str, int]:
    """The page metadata for a listing the source already paginated."""
    return {"total": total, "limit": max(1, limit), "offset": max(0, offset)}


def paginate[Row](rows: Sequence[Row], limit: int, offset: int) -> tuple[list[Row], dict[str, int]]:
    """Slice *rows* into one page, plus the metadata describing it.

    An out-of-range offset clamps to the first page rather than rendering an
    empty table: a Next button on a message left open while the underlying list
    shrank should not dead-end.
    """
    limit = max(1, limit)
    total = len(rows)
    if offset < 0 or offset >= total:
        offset = 0
    return list(rows[offset : offset + limit]), {
        "total": total,
        "limit": limit,
        "offset": offset,
    }


def pagination_row(
    page: dict[str, Any], callback_for: Callable[[int], str]
) -> list[InlineKeyboardButton]:
    """Prev/Next buttons for *page*, or an empty row when one page covers it.

    ``callback_for`` maps a target offset to its callback-data string. Returned
    as a bare row so a keyboard that already has rows can append it below them.
    """
    total = int(page.get("total", 0))
    limit = int(page.get("limit", 0)) or 1
    offset = int(page.get("offset", 0))

    buttons: list[InlineKeyboardButton] = []
    if offset > 0:
        buttons.append(
            InlineKeyboardButton(text="◀ Prev", callback_data=callback_for(max(0, offset - limit)))
        )
    if offset + limit < total:
        buttons.append(
            InlineKeyboardButton(text="Next ▶", callback_data=callback_for(offset + limit))
        )
    return buttons


def pagination_keyboard(
    page: dict[str, Any], callback_for: Callable[[int], str]
) -> InlineKeyboardMarkup | None:
    """A keyboard of nothing but Prev/Next, or ``None`` when it would be empty."""
    buttons = pagination_row(page, callback_for)
    return InlineKeyboardMarkup(inline_keyboard=[buttons]) if buttons else None


def page_footer(page: dict[str, Any]) -> str:
    """``Showing 1-10 of 42`` — what the buttons alone do not say."""
    total = int(page.get("total", 0))
    limit = int(page.get("limit", 0)) or 1
    offset = int(page.get("offset", 0))
    if total == 0:
        return "Nothing to show"
    first = offset + 1
    last = min(offset + limit, total)
    return f"Showing {first}-{last} of {total}"
