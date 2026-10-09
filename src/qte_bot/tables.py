"""Fixed-width text tables for Telegram messages.

Ported from ``algo-trading-broker``'s ``bot/app/utils/table.py`` so a table
reads the same in both bots. Telegram has no table markup, so a table is a
monospace ``<pre>`` block whose columns are space-padded to a common width, and
two constraints follow from that:

- **Every character must be single-width**, or the columns skew. Emoji are
  double-width and vary by platform, so markers inside a table use
  text-presentation glyphs and the emoji stay in the surrounding message.
- **Cells are escaped, never markup.** Callers pass raw values; padding is
  computed on the visible text and the escaping happens last.

Long values are truncated with an ellipsis rather than wrapped, so every row
stays one line — Telegram scrolls a ``<pre>`` block horizontally, which reads
better than a ragged wrap.
"""

from __future__ import annotations

import html
from collections.abc import Iterable, Sequence
from typing import Any

ELLIPSIS = "…"
COLUMN_SEPARATOR = "  "
RULE_CHARACTER = "─"

#: Text-presentation marks, single-width in a monospace font unlike the emoji
#: of the same name. ``YES``/``NO`` answer the readiness columns.
MARK_YES = "✓"
MARK_NO = "✗"


def truncated(text: str, width: int | None) -> str:
    if width is None or width <= 0 or len(text) <= width:
        return text
    if width == 1:
        return ELLIPSIS
    return text[: width - 1] + ELLIPSIS


def render_table(
    headers: Sequence[str],
    rows: Iterable[Sequence[Any]],
    aligns: Sequence[str] | None = None,
    max_widths: Sequence[int | None] | None = None,
) -> str:
    """Render *rows* as a monospace table wrapped in ``<pre>``.

    ``aligns`` is one of ``"l"``/``"r"`` per column (default all left); use
    ``"r"`` for numbers. ``max_widths`` caps a column, truncating longer values
    (``None`` for a column leaves it uncapped).

    Each column is sized to its widest cell, header included, so a table of
    short values stays compact instead of padding out to the caps.
    """
    column_count = len(headers)
    caps = list(max_widths) if max_widths else [None] * column_count

    def cells_of(row: Sequence[Any]) -> list[str]:
        # Pad short rows, so a caller may omit trailing cells.
        values = list(row[:column_count]) + [None] * (column_count - len(row[:column_count]))
        return [
            truncated("" if value is None else str(value), caps[index])
            for index, value in enumerate(values)
        ]

    head = cells_of(headers)
    body = [cells_of(row) for row in rows]
    widths = [max(len(row[index]) for row in [head, *body]) for index in range(column_count)]
    right = [
        (list(aligns) + ["l"] * column_count)[index] == "r" if aligns else False
        for index in range(column_count)
    ]

    def line_of(values: Sequence[str]) -> str:
        padded = [
            values[index].rjust(widths[index])
            if right[index]
            else values[index].ljust(widths[index])
            for index in range(column_count)
        ]
        # rstrip, so a short trailing cell leaves no dead whitespace behind.
        return COLUMN_SEPARATOR.join(padded).rstrip()

    rule = RULE_CHARACTER * (sum(widths) + len(COLUMN_SEPARATOR) * (column_count - 1))
    rendered = "\n".join([line_of(head), rule, *(line_of(row) for row in body)])
    return f"<pre>{html.escape(rendered, quote=False)}</pre>"


def marked(value: bool) -> str:
    """A boolean as a single-width mark, safe inside a table."""
    return MARK_YES if value else MARK_NO
