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

from qte_bot.mounts import MountedRepository
from qte_bot.pagination import page_footer
from qte_bot.tables import marked, render_table
from qte_shared.config import settings
from qte_shared.market_data_plan import MarketDataPlan
from qte_shared.notifications.telegram_sink import boxed, clipped
from qte_shared.strategies.mapping import SymbolMapping
from qte_strategy_engine.db import ClosedCycle
from qte_strategy_engine.demo_signal import DemoDelivery

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


def moment(value: Any, *, full: bool = False) -> str:
    """A timestamp as the chat shows it, or an em dash when there is none.

    ``full`` adds the year, the seconds and the zone, the way a notification
    stamps an event — for a moment that has to be told apart from one a few
    minutes away, such as the second a build was made.
    """
    if value is None:
        return "—"
    if not isinstance(value, datetime):
        try:
            value = datetime.fromisoformat(str(value))
        except ValueError:
            return escaped(value)
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    pattern = "%Y-%m-%d %H:%M:%S %Z" if full else "%m-%d %H:%M"
    return value.astimezone(settings.telegram.display_zone).strftime(pattern)


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


#: The rule between a box's sections, the same one a position notification uses.
BOX_DIVIDER = "-----------"

#: Where one message ends and the next begins. The Bot API rejects a body past
#: 4096 characters whole, so a long mount is sent as several messages rather
#: than risking the one; this leaves room for the header and the footer.
MAX_MESSAGE_CHARS = 3500

#: Strategies listed inside one box before the rest is counted instead. A
#: repository's alias table is whatever its owner wrote, and nothing bounds it.
MAX_STRATEGIES_PER_BOX = 40

#: How much of a parse error a box repeats; it is library text, not ours.
MAX_PROBLEM_CHARS = 120


def _bundle_lines(repository: MountedRepository) -> list[str]:
    """The build a repository's ``BUNDLE_VERSION.json`` describes, one fact a line.

    A folder with no stamp keeps one honest line rather than a column of
    blanks: a checkout that was never built has no version, and that is a fact
    about it, not a gap in the message.
    """
    stamp = repository.bundle
    if stamp is None:
        if repository.bundle_problem:
            reason = clipped(escaped(repository.bundle_problem), MAX_PROBLEM_CHARS)
            return [f"⚠️ BUNDLE_VERSION.json unreadable: {reason}"]
        return ["Version: — (no BUNDLE_VERSION.json)"]

    def text(key: str) -> str:
        value = stamp.get(key)
        return escaped(value) if value not in (None, "") else "—"

    tree = stamp.get("tree")
    dirty = stamp.get("dirty")
    if dirty is None:
        dirty_text = "—"
    else:
        dirty_text = "⚠️ yes" if dirty else "no"
    return [
        f"Version: {text('version')}",
        f"Built: {moment(stamp.get('built_at') or None, full=True)}",
        f"Branch: {text('branch')} | Commit: {text('commit')}",
        # The full tree hash is forty characters and the version token already
        # carries its first eight; a phone shows the same eight here.
        f"Tree: {escaped(str(tree)[:8]) if tree else '—'} | Content: {text('content')}",
        f"Mode: {text('mode')} | Dirty: {dirty_text}",
    ]


def _strategy_lines(repository: MountedRepository) -> list[str]:
    """What the repository publishes, from the mount record."""
    if not repository.recorded:
        return ["Strategies: not in strategies.toml", "Run: make strategy-mount"]
    if not repository.strategies:
        return ["Strategies: none published"]
    names = sorted(repository.strategies)
    lines = [f"Strategies ({len(names)}):"]
    for name in names[:MAX_STRATEGIES_PER_BOX]:
        if repository.strategies[name]:
            lines.append(f"🟢 {escaped(name)}")
        else:
            lines.append(f"🔴 {escaped(name)} (failed audit)")
    if len(names) > MAX_STRATEGIES_PER_BOX:
        lines.append(f"… {len(names) - MAX_STRATEGIES_PER_BOX} more")
    return lines


def strategy_box(repository: MountedRepository) -> str:
    """One mounted repository as one box: its build, then what it publishes."""
    lines = [
        f"[📦 {escaped(repository.name)}]",
        BOX_DIVIDER,
        *_bundle_lines(repository),
        BOX_DIVIDER,
        *_strategy_lines(repository),
        BOX_DIVIDER,
    ]
    return "\n".join(lines)


def strategy_bundles(repositories: list[MountedRepository], namespace: str) -> list[str]:
    """``/strategies``: one box per mounted repository, in as many messages as fit.

    The boxes are separate ``<pre>`` blocks inside one message, joined the way
    a position notification joins its sections: Telegram's parser does not
    allow a ``<pre>`` inside a ``<pre>``, so each seam closes one and opens the
    next. What is read is the disk — ``/runner`` is the process's own account
    of what it loaded — and the footer says so.
    """
    title = header("Strategies", namespace)
    if not repositories:
        return [
            f"{title}\n\nNo strategy repository is mounted in <code>__strategies__/</code>. "
            "Clone one in and run <code>make strategy-mount</code>."
        ]

    messages: list[str] = []
    boxes: list[str] = []
    used = len(title)
    for repository in repositories:
        box = strategy_box(repository)
        if boxes and used + len(box) > MAX_MESSAGE_CHARS:
            messages.append(_boxes_message(title if not messages else "", boxes))
            boxes, used = [], 0
        boxes.append(box)
        used += len(box)
    messages.append(_boxes_message(title if not messages else "", boxes))

    count = len(repositories)
    footer = (
        f"{count} {'repository' if count == 1 else 'repositories'} · read from disk — "
        "<code>/runner</code> shows what is loaded"
    )
    messages[-1] += f"\n{footer}"
    return messages


def _boxes_message(title: str, boxes: list[str]) -> str:
    body = boxed("\n</pre>\n<pre>".join(boxes))
    return f"{title}\n{body}" if title else body


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


# ── Configuration, read back from the files an operator edits ─────────────

#: Lines of a configuration listing in one message. A listing is a page of a
#: file rather than a window on a database, so there are no paging buttons and
#: a long file is simply sent as several messages.
CONFIG_ROWS_PER_MESSAGE = 30

#: Parameters spelled out beside a pair before the rest is counted instead.
MAX_PARAMS_PER_ROW = 6


def _as_messages(lines: list[str]) -> list[str]:
    """Pack *lines* into as few messages as the Bot API's length limit allows.

    A body past 4096 characters is refused whole, and the refusal lands after
    whatever the message was reporting has already happened — a fired drill
    whose report never arrives reads to the operator like a drill that never
    ran. Splitting on a line boundary keeps each message complete instead.
    """
    messages: list[str] = []
    current: list[str] = []
    used = 0
    for line in lines:
        if current and used + len(line) > MAX_MESSAGE_CHARS:
            messages.append("\n".join(current))
            current, used = [], 0
        current.append(line)
        used += len(line) + 1
    messages.append("\n".join(current))
    return messages


def _params_text(params: dict[str, Any]) -> str:
    """One pair's overrides as ``key=value``, in the file's own order."""
    if not params:
        return "—"
    items = list(params.items())
    rendered = " ".join(f"{key}={value}" for key, value in items[:MAX_PARAMS_PER_ROW])
    if len(items) > MAX_PARAMS_PER_ROW:
        rendered += f" +{len(items) - MAX_PARAMS_PER_ROW}"
    return rendered


def strategy_mapping(mapping: SymbolMapping, namespace: str) -> list[str]:
    """``/mapping``: the symbol → strategy table, as the file states it.

    The absent file and the empty one are told apart, because they are two
    different instructions: no table at all means every strategy keeps the
    symbols it declares on itself, while a table that maps nothing means trade
    nothing. They differ by a deploy, so a message that collapsed them would be
    the one an operator reads on the quiet weekend that was not meant to be
    quiet.
    """
    title = header("Strategies mapping", namespace)
    if not mapping:
        return [
            f"{title}\n\nNo mapping table is deployed "
            f"(<code>{escaped(settings.engine.mapping_file.name)}</code>), so every strategy "
            "keeps the symbols it declares on itself."
        ]
    source = f"File: <code>{escaped(mapping.source.name if mapping.source else '—')}</code>"
    if not mapping.pairings:
        return [
            f"{title}\n{source}\n\n⚠️ The table maps no pairs, so nothing is traded. "
            "A symbol switched off keeps its entry with <code>enabled = false</code>."
        ]

    # Grouped under each symbol rather than tabulated, for the reason the
    # drill's report is not a table either: these names run past thirty
    # characters and a pair's long and short variant differ by a suffix, so a
    # column wide enough to tell them apart is wider than a phone.
    rows: list[str] = []
    for symbol in mapping.symbols:
        rows.append(f"<b>{escaped(symbol)}</b>")
        rows.extend(
            f"· <code>{escaped(pairing.strategy)}</code> — {escaped(_params_text(pairing.params))}"
            for pairing in mapping.pairings
            if pairing.symbol == symbol
        )

    messages = []
    for start in range(0, len(rows), CONFIG_ROWS_PER_MESSAGE):
        body = "\n".join(rows[start : start + CONFIG_ROWS_PER_MESSAGE])
        messages.append(f"{title}\n{source}\n{body}" if start == 0 else body)

    tail = [f"{len(mapping.pairings)} pair(s) · {len(mapping.symbols)} symbol(s)"]
    if mapping.strategy_defaults:
        tail.append("<b>Per-strategy defaults</b> (a pair's own params win over these)")
        tail.extend(
            f"· <code>{escaped(name)}</code>: {escaped(_params_text(values))}"
            for name, values in sorted(mapping.strategy_defaults.items())
        )
    tail.append("Read from the file — <code>/runner</code> shows what is loaded.")
    messages[-1] += "\n" + "\n".join(tail)
    return messages


def data_providers(plans: dict[str, MarketDataPlan], namespace: str) -> list[str]:
    """``/provider``: each provider's plan — its symbols, and its own knobs.

    One message per provider, because each reads its own
    ``config/data_providers/<provider>.toml`` and a merged view would lose the
    ``[provider]`` table that half the question is about. No credential can
    appear here: the parser drops every credential-shaped key as it reads the
    file, and the vendor key lives in ``.env`` instead.
    """
    title = header("Data providers", namespace)
    if not plans:
        return [f"{title}\n\nNo provider is configured (<code>QTE_MARKET_DATA__PROVIDER</code>)."]

    messages: list[str] = []
    for index, (provider, plan) in enumerate(plans.items()):
        lines = [title] if index == 0 else []
        lines.append(f"<b>{escaped(provider)}</b>")
        if not plan:
            lines.append(
                "No plan file is deployed, so the <code>QTE_ENGINE__*</code> defaults decide "
                "what is subscribed."
            )
            messages.append("\n".join(lines))
            continue
        lines.append(f"File: <code>{escaped(plan.source.name if plan.source else '—')}</code>")
        if not plan.feeds:
            lines.append("⚠️ The plan enables no symbol, so nothing is subscribed.")
        for start in range(0, len(plan.feeds), CONFIG_ROWS_PER_MESSAGE):
            # One table per thirty symbols: a plan feeding a hundred of them
            # would otherwise render a body past the 4096 characters the Bot
            # API accepts, and the whole message would be refused.
            page = plan.feeds[start : start + CONFIG_ROWS_PER_MESSAGE]
            if start:
                messages.append("\n".join(lines))
                lines = [f"<b>{escaped(provider)}</b> (continued)"]
            lines.append(
                render_table(
                    ("Symbol", "Market", "Timeframes"),
                    [(feed.symbol, feed.market, ", ".join(feed.timeframes)) for feed in page],
                    max_widths=(12, 8, 24),
                )
            )
        if plan.options:
            lines.append("<b>Vendor options</b>")
            lines.extend(
                f"· <code>{escaped(key)}</code> = <code>{escaped(value)}</code>"
                for key, value in sorted(plan.options.items())
            )
        messages.append("\n".join(lines))

    messages[-1] += (
        f"\nDeciding on: <code>{escaped(settings.engine.signal_timeframe)}</code> · "
        "read from the files — <code>/redis</code> shows what is held."
    )
    return messages


# ── The drill ─────────────────────────────────────────────────────────────

DEMO_RUNNING = (
    "⏳ A demo drill is already running on <code>{namespace}</code>. It fires one open cycle "
    "per strategy and takes a few seconds each; wait for its report rather than starting a "
    "second one."
)


def demo_started(target: str, namespace: str) -> str:
    """The holding message the drill's own report replaces when it is done."""
    return (
        f"⏳ Firing a demo signal for {target} on <code>{escaped(namespace)}</code>…\n"
        "Driving each strategy over ideal candles, which takes a few seconds per pair."
    )


def demo_result(
    deliveries: list[DemoDelivery], failures: list[str], namespace: str, target: str
) -> list[str]:
    """``/demo``: what each strategy decided, and whether its envelope left.

    Three outcomes are kept apart because they mean different things. An
    envelope that did not leave is a transport fault. A strategy that *raised*
    while being driven is the drill's own finding — it would raise on a live
    bar too. A strategy that merely declined every regime is neither: the
    payload went out as a synthetic fallback, and what is proven is the path
    rather than the edge.
    """
    title = header("Demo signal", namespace)
    if not deliveries and not failures:
        return [
            f"{title}\n\nNothing to fire for {target}: no mounted strategy is mapped to a "
            "symbol. <code>/mapping</code> shows the table, <code>/strategies</code> the mount."
        ]

    # One entry per delivery rather than a table: a strategy name runs well past
    # thirty characters here, and the two that differ only by a suffix — a
    # pair's long variant and its short one — would truncate into the same row
    # and the report would say nothing about which of them fired.
    lines = [title]
    for delivery in deliveries:
        signal = delivery.demo.signal
        mark = "✅" if delivery.delivered else "❌"
        lines.append(
            f"{mark} <code>{escaped(signal.strategy)}</code>\n"
            f"   {escaped(signal.symbol)} {escaped(signal.position.action.value)} · "
            f"{escaped(delivery.demo.source)} data"
        )
    for delivery in deliveries:
        if not delivery.delivered:
            lines.append(
                f"⚠️ {escaped(delivery.demo.signal.strategy)} not delivered: "
                f"{escaped(delivery.error)}"
            )
    for delivery in deliveries:
        if delivery.demo.strategy_error:
            lines.append(
                f"🔴 {escaped(delivery.demo.signal.strategy)} raised while being driven: "
                f"<code>{escaped(delivery.demo.strategy_error)}</code> — it would raise on a "
                "live bar too."
            )
    for failure in failures:
        lines.append(f"🔴 No payload could be built for {escaped(failure)}")

    delivered = sum(1 for delivery in deliveries if delivery.delivered)
    lines.append(
        f"{delivered} of {len(deliveries) + len(failures)} fired on "
        f"<code>{escaped(settings.broker.demo_subject_prefix)}.*</code>. The broker answers "
        "each with a Telegram card and forwards nothing to a worker."
    )
    synthetic = [delivery for delivery in deliveries if not delivery.demo.from_strategy]
    if synthetic:
        lines.append(
            f"<i>{len(synthetic)} payload(s) are a synthetic fallback: the strategy entered on "
            "none of the four regimes, which is ordinary — the transport is what was tested.</i>"
        )
    # A mount of forty strategies reports past the length the Bot API accepts,
    # and the refusal would land after every envelope had already left.
    return _as_messages(lines)


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
