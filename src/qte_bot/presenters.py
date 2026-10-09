"""Message bodies for every command.

Separated from the handlers for the same reason the broker's bot separates
them: a body is a pure function of what the engine answered, so it can be
asserted on in a test without a Telegram session, and a handler stays a few
lines of "ask, render, send".

Tables go through :mod:`qte_bot.tables`, which escapes and pads them; anything
outside a table is escaped here. Timestamps render in the display zone the
notifications use, so one chat does not show two clocks.
"""

from __future__ import annotations

import html
from datetime import UTC, datetime
from typing import Any

from qte_bot.pagination import page_footer
from qte_bot.tables import marked, render_table
from qte_shared.config import settings
from qte_strategy_engine.db import ClosedCycle

NO_RUNNER = (
    "⚠️ No runner answered. It may be down, or wired to another book — "
    "this bot is on <code>{namespace}</code>."
)
NO_INGESTION = (
    "⚠️ Ingestion did not answer. It may be down, or wired to another book — "
    "this bot is on <code>{namespace}</code>."
)


def escaped(value: Any) -> str:
    return html.escape(str(value), quote=False)


def moment(value: Any) -> str:
    """A timestamp as the chat shows it, or an em dash when there is none."""
    if value is None:
        return "—"
    if not isinstance(value, datetime):
        try:
            value = datetime.fromisoformat(str(value))
        except ValueError:
            return escaped(value)
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(settings.telegram.display_zone).strftime("%m-%d %H:%M")


def header(title: str, namespace: str) -> str:
    return f"<b>{escaped(title)}</b> · <code>{escaped(namespace)}</code>"


# ── Look ──────────────────────────────────────────────────────────────────


def redis_windows(windows: list[dict[str, Any]], namespace: str, provider: str | None) -> str:
    """``/redis``: how many bars each series holds, and how fresh they are."""
    if not windows:
        return (
            f"{header('Redis warm-up', namespace)}\n\n"
            "No series are planned. Check the data-provider file "
            "(<code>config/data_providers/</code>)."
        )
    rows = [
        (
            window["symbol"],
            window["timeframe"],
            "?" if window.get("error") else window.get("bars", 0),
            "?" if window.get("error") else moment(window.get("newest_bar")),
        )
        for window in windows
    ]
    table = render_table(
        ("Symbol", "TF", "Bars", "Newest"),
        rows,
        aligns=("l", "l", "r", "l"),
        max_widths=(12, 5, 6, 12),
    )
    written_by = f"\nWritten by: <code>{escaped(provider)}</code>" if provider else ""
    return f"{header('Redis warm-up', namespace)}\n{table}{written_by}"


def runner_status(status: dict[str, Any]) -> str:
    """``/runner``: every strategy, and whether it can decide on the next bar."""
    strategies = status.get("strategies") or []
    lines = [header("Runner", str(status.get("namespace", "?")))]

    gate = status.get("gate") or {}
    blocking = (
        "everything"
        if gate.get("everything")
        else ", ".join([*gate.get("symbols", []), *gate.get("strategies", [])]) or "nothing"
    )
    lines.append(
        "Mode: <code>{mode}</code> · Delivery: {delivery} · Transport: <code>{transport}</code>"
        "\nPaused: <code>{paused}</code> · Up since: {since}".format(
            mode=escaped(status.get("execution_mode", "?")),
            delivery="PAUSED" if status.get("delivery_paused") else "live",
            transport=escaped(status.get("transport", "?")),
            paused=escaped(blocking),
            since=moment(status.get("started_at")),
        )
    )

    if not strategies:
        lines.append("\nNo strategies are loaded.")
        return "\n".join(lines)

    rows = [
        (
            row.get("symbol"),
            row.get("timeframe"),
            row.get("strategy"),
            f"{row.get('bars', 0)}/{row.get('warmup', 0)}",
            marked(bool(row.get("ready"))),
            len(row.get("open_cycles") or []),
        )
        for row in strategies
    ]
    lines.append(
        render_table(
            ("Symbol", "TF", "Strategy", "Bars", "Go", "Open"),
            rows,
            aligns=("l", "l", "l", "r", "l", "r"),
            max_widths=(10, 5, 22, 9, 2, 4),
        )
    )
    # "Go" is one character wide on purpose, so the reason it is not set has to
    # be spelled out underneath rather than guessed from the table.
    held = [row for row in strategies if not row.get("ready")]
    if held:
        lines.append("<b>Not ready</b>")
        for row in held:
            lines.append(
                f"· {escaped(row.get('symbol'))} {escaped(row.get('strategy'))}: "
                f"{escaped(_why_not_ready(row, status))}"
            )
    return "\n".join(lines)


def _why_not_ready(row: dict[str, Any], status: dict[str, Any]) -> str:
    if not row.get("warm"):
        return f"still warming ({row.get('bars', 0)}/{row.get('warmup', 0)} bars)"
    if row.get("gated"):
        return "paused by /prevent"
    if row.get("uncertain"):
        return "an unreconciled delivery is blocking the pair"
    if status.get("delivery_paused"):
        return "live delivery is paused (shadow mode)"
    return "unknown"


def open_positions(positions: list[dict[str, Any]], namespace: str) -> str:
    """``/positions``: the whole book, one row per cycle."""
    title = header("Open positions", namespace)
    if not positions:
        return f"{title}\n\nFlat — nothing is open."
    rows = [
        (
            position.get("symbol"),
            position.get("timeframe") or "—",
            position.get("strategy"),
            position.get("signal_uxid"),
            moment(position.get("opened_at")),
        )
        for position in positions
    ]
    table = render_table(
        ("Symbol", "TF", "Strategy", "Signal", "Opened"),
        rows,
        max_widths=(10, 5, 18, 16, 12),
    )
    return f"{title}\n{table}\n{len(positions)} open"


def closed_cycles(cycles: list[ClosedCycle], page: dict[str, Any], namespace: str) -> str:
    """``/closed``: one page of finished cycles, newest close first."""
    title = header("Closed positions", namespace)
    if not cycles:
        return f"{title}\n\nNothing has closed in this book yet."
    rows = [
        (
            cycle.symbol,
            cycle.timeframe,
            cycle.strategy,
            cycle.closed_by,
            moment(cycle.opened_at),
            moment(cycle.closed_at),
        )
        for cycle in cycles
    ]
    table = render_table(
        ("Symbol", "TF", "Strategy", "By", "Opened", "Closed"),
        rows,
        max_widths=(10, 5, 16, 5, 12, 12),
    )
    return f"{title}\n{table}\n{page_footer(page)}"


# ── Act ───────────────────────────────────────────────────────────────────


def scope_text(scope: dict[str, Any]) -> str:
    """How a scope reads in a message: the words the operator typed."""
    if scope.get("everything"):
        return "<b>everything</b>"
    if scope.get("symbol"):
        return f"symbol <code>{escaped(scope['symbol'])}</code>"
    return f"strategy <code>{escaped(scope.get('strategy'))}</code>"


def flat_confirmation(scope: dict[str, Any], namespace: str, open_count: int) -> str:
    return (
        f"⚠️ Close {open_count} position(s) on {scope_text(scope)}?\n"
        f"Book: <code>{escaped(namespace)}</code>\n\n"
        "This sends a FLAT through the broker, the same as any other exit."
    )


def flat_result(reply: dict[str, Any], scope: dict[str, Any]) -> str:
    """What the runner managed, and what it refused and why."""
    if reply.get("error"):
        return f"⚠️ {escaped(reply['error'])}"
    closed = reply.get("closed") or []
    refused = reply.get("refused") or []
    if not closed and not refused:
        return f"Nothing was open on {scope_text(scope)}."
    lines = []
    if closed:
        lines.append(f"✅ Closed {len(closed)}:")
        lines.extend(
            f"· {escaped(row.get('symbol'))} {escaped(row.get('strategy'))} "
            f"<code>{escaped(row.get('signal_uxid'))}</code>"
            for row in closed
        )
    if refused:
        lines.append(f"⚠️ Not closed ({len(refused)}):")
        lines.extend(
            f"· {escaped(row.get('symbol'))} {escaped(row.get('strategy'))}: "
            f"{escaped(row.get('reason'))}"
            for row in refused
        )
    return "\n".join(lines)


def gate_result(
    gate_payload: dict[str, Any], *, blocking: bool, announced: bool, namespace: str
) -> str:
    """What the gate now blocks, and whether the running runner knows yet."""
    blocked = (
        "everything"
        if gate_payload.get("everything")
        else ", ".join([*gate_payload.get("symbols", []), *gate_payload.get("strategies", [])])
        or "nothing"
    )
    headline = "⏸ Not deciding on" if blocking else "▶️ Deciding again"
    reach = (
        "Applied now."
        if announced
        else "Stored, but no runner answered — it will pick this up on its next bar or restart."
    )
    return (
        f"{headline} — now paused: <code>{escaped(blocked)}</code>\n"
        f"Book: <code>{escaped(namespace)}</code>\n{reach}"
    )


def warmup_result(reply: dict[str, Any], namespace: str) -> str:
    """``/warmup``: what each series holds now, and what failed."""
    warmed = reply.get("warmed") or []
    unknown = reply.get("unknown") or []
    if not warmed:
        return f"Nothing to warm up on <code>{escaped(namespace)}</code>.{_unknown_line(unknown)}"
    rows = [
        (
            row.get("symbol"),
            row.get("timeframe"),
            row.get("bars_before", "—") if "error" not in row else "—",
            row.get("bars", "—") if "error" not in row else "err",
        )
        for row in warmed
    ]
    table = render_table(
        ("Symbol", "TF", "Before", "Now"),
        rows,
        aligns=("l", "l", "r", "r"),
        max_widths=(10, 5, 7, 7),
    )
    failures = [row for row in warmed if row.get("error")]
    body = f"{header('Warm-up requested', namespace)}\n{table}"
    for row in failures:
        body += (
            f"\n⚠️ {escaped(row.get('symbol'))} {escaped(row.get('timeframe'))}: "
            f"{escaped(row.get('error'))}"
        )
    return body + _unknown_line(unknown)


def flush_result(reply: dict[str, Any], namespace: str) -> str:
    """``/flush``: what was dropped."""
    flushed = reply.get("flushed") or []
    unknown = reply.get("unknown") or []
    if not flushed:
        return f"Nothing was flushed on <code>{escaped(namespace)}</code>.{_unknown_line(unknown)}"
    rows = [(row.get("symbol"), row.get("timeframe"), row.get("dropped", 0)) for row in flushed]
    table = render_table(
        ("Symbol", "TF", "Dropped"), rows, aligns=("l", "l", "r"), max_widths=(10, 5, 8)
    )
    return (
        f"{header('Warm-up flushed', namespace)}\n{table}\n"
        "Each window is owed again; /warmup asks for it now."
        f"{_unknown_line(unknown)}"
    )


def _unknown_line(unknown: list[str]) -> str:
    if not unknown:
        return ""
    return "\n⚠️ Not fed here: <code>" + escaped(", ".join(unknown)) + "</code>"


def usage(
    command: str,
    choices: list[str],
    *,
    include_all: bool = True,
    runner_answered: bool = True,
) -> str:
    """What to type, with the values this book actually has.

    When the runner did not answer, the strategy names are missing from the
    list rather than wrong — so the caveat is printed instead of letting the
    operator read "these are your options" off an incomplete list.
    """
    options = (["all"] if include_all else []) + choices
    listed = "\n".join(f"· <code>/{command} {escaped(option)}</code>" for option in options)
    caveat = (
        ""
        if runner_answered
        else "\n\n⚠️ The runner is not answering, so strategy names are missing from this list."
    )
    return f"Name what to act on:\n{listed}{caveat}"
