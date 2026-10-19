"""Every command handler, behind one operator gate.

Each one is the same three lines of work — ask the engine, render, send — with
the rendering in :mod:`qte_bot.presenters` and the asking in
:mod:`qte_bot.engine_client`. What is left here is the part that is genuinely
about Telegram: parsing an argument, paging a table, and the one confirmation
step that stands in front of ``/flat``.

Callback data, and its 64-byte budget:

* ``closed:{offset}`` — page the closed-cycle table
* ``flat:ok:a`` / ``flat:ok:s:{SYMBOL}`` / ``flat:ok:t:{STRATEGY}`` — confirmed
  FLAT, carrying the scope rather than an index into something remembered
  server-side: a button pressed ten minutes later must still act on exactly
  what the message it is attached to says it will.
* ``flat:no`` — cancel

The scope words are deliberately the same three everywhere: ``all``, a symbol,
or a strategy. A name that is neither is refused with the list of what this
book actually has, which is also what a bare ``/flat`` prints.
"""

from __future__ import annotations

from typing import Any

from aiogram import F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandObject
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from qte_bot import presenters
from qte_bot.access import IsOperator
from qte_bot.commands import HELP
from qte_bot.engine_client import EngineClient
from qte_bot.pagination import page_of, pagination_keyboard
from qte_shared.logging_setup import get_logger

log = get_logger(__name__)

#: Rows per page of ``/closed``. A code constant and not a setting: it is sized
#: to the width of that table, not to a deployment's taste.
CLOSED_PER_PAGE = 10

router = Router(name="qte-bot")
router.message.filter(IsOperator())
router.callback_query.filter(IsOperator())


async def _safe_edit(message: Message, text: str, reply_markup=None) -> None:
    """Edit a message, swallowing the "not modified" Telegram raises on a double tap."""
    try:
        await message.edit_text(text, reply_markup=reply_markup)
    except TelegramBadRequest as refusal:
        if "message is not modified" not in str(refusal).lower():
            raise


# ── Look ──────────────────────────────────────────────────────────────────


@router.message(Command("start"))
@router.message(Command("help"))
async def show_help(message: Message, engine: EngineClient) -> None:
    await message.answer(HELP.format(namespace=engine.namespace))


@router.message(Command("redis"))
async def show_redis(message: Message, engine: EngineClient) -> None:
    windows = await engine.candle_windows()
    provider = await engine.history_provider()
    await message.answer(presenters.redis_windows(windows, engine.namespace, provider))


@router.message(Command("runner"))
async def show_runner(message: Message, engine: EngineClient) -> None:
    status = await engine.runner_status()
    if status is None:
        await message.answer(presenters.NO_RUNNER.format(namespace=engine.namespace))
        return
    await message.answer(presenters.runner_status(status))


@router.message(Command("positions"))
async def show_positions(message: Message, engine: EngineClient) -> None:
    positions = await engine.open_positions()
    await message.answer(presenters.open_positions(positions, engine.namespace))


@router.message(Command("closed"))
async def show_closed(message: Message, engine: EngineClient) -> None:
    await message.answer(**await _closed_page(engine, offset=0))


@router.callback_query(F.data.startswith("closed:"))
async def page_closed(call: CallbackQuery, engine: EngineClient) -> None:
    try:
        offset = int(call.data.split(":", 1)[1])
    except (IndexError, ValueError):
        await call.answer()
        return
    rendered = await _closed_page(engine, offset=max(0, offset))
    if isinstance(call.message, Message):
        await _safe_edit(call.message, **rendered)
    await call.answer()


async def _closed_page(engine: EngineClient, *, offset: int) -> dict[str, Any]:
    cycles, total = await engine.closed_cycles(limit=CLOSED_PER_PAGE, offset=offset)
    page = page_of(total, CLOSED_PER_PAGE, offset)
    return {
        "text": presenters.closed_cycles(cycles, page, engine.namespace),
        "reply_markup": pagination_keyboard(page, lambda target: f"closed:{target}"),
    }


# ── Scope parsing ─────────────────────────────────────────────────────────


async def _known_names(engine: EngineClient) -> tuple[list[str], list[str], bool]:
    """The symbols and strategies this book has, and whether the runner answered.

    Symbols come from the data-provider plan, which is the file an operator
    edits; strategies can only come from the running runner, since what is
    loaded is decided by a private repository this one cannot see. So when the
    runner is silent the strategy half of the list is empty — and a refusal has
    to say that, rather than claiming a perfectly good name does not exist.
    """
    symbols = engine.planned_symbols()
    status = await engine.runner_status()
    strategies = sorted(
        {
            str(row.get("strategy"))
            for row in (status or {}).get("strategies", [])
            if row.get("strategy")
        }
    )
    return symbols, strategies, status is not None


def _as_scope(argument: str, symbols: list[str], strategies: list[str]) -> dict[str, Any] | None:
    """``all``, a symbol or a strategy — or ``None`` when it is none of them."""
    wanted = argument.strip()
    if wanted.lower() == "all":
        return {"everything": True, "symbol": None, "strategy": None}
    if wanted.upper() in {name.upper() for name in symbols}:
        return {"everything": False, "symbol": wanted.upper(), "strategy": None}
    for name in strategies:
        if name.lower() == wanted.lower():
            return {"everything": False, "symbol": None, "strategy": name}
    return None


async def _resolve_scope(
    message: Message, command: CommandObject, engine: EngineClient, *, verb: str
) -> dict[str, Any] | None:
    """Parse the argument, or answer with what the operator could have typed."""
    symbols, strategies, runner_answered = await _known_names(engine)
    if not (command.args or "").strip():
        await message.answer(
            presenters.usage(verb, symbols + strategies, runner_answered=runner_answered)
        )
        return None
    scope = _as_scope(command.args, symbols, strategies)
    if scope is None:
        await message.answer(
            f"<code>{presenters.escaped(command.args.strip())}</code> is not a symbol or a "
            "strategy in this book.\n\n"
            + presenters.usage(verb, symbols + strategies, runner_answered=runner_answered)
        )
        return None
    return scope


# ── Act: FLAT, behind a confirmation ──────────────────────────────────────


def _flat_keyboard(scope: dict[str, Any]) -> InlineKeyboardMarkup:
    if scope.get("everything"):
        encoded = "flat:ok:a"
    elif scope.get("symbol"):
        encoded = f"flat:ok:s:{scope['symbol']}"
    else:
        encoded = f"flat:ok:t:{scope['strategy']}"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Close them", callback_data=encoded),
                InlineKeyboardButton(text="✖ Cancel", callback_data="flat:no"),
            ]
        ]
    )


def _scope_from_callback(encoded: str) -> dict[str, Any] | None:
    """Read back a scope a confirm button is carrying."""
    parts = encoded.split(":", 3)
    if len(parts) < 3:
        return None
    if parts[2] == "a":
        return {"everything": True, "symbol": None, "strategy": None}
    if len(parts) < 4 or not parts[3]:
        return None
    if parts[2] == "s":
        return {"everything": False, "symbol": parts[3].upper(), "strategy": None}
    if parts[2] == "t":
        return {"everything": False, "symbol": None, "strategy": parts[3]}
    return None


@router.message(Command("flat"))
async def ask_flat(message: Message, command: CommandObject, engine: EngineClient) -> None:
    scope = await _resolve_scope(message, command, engine, verb="flat")
    if scope is None:
        return
    positions = await engine.open_positions()
    matching = [row for row in positions if _matches(row, scope)]
    if not matching:
        await message.answer(f"Nothing is open on {presenters.scope_text(scope)}.")
        return
    await message.answer(
        presenters.flat_confirmation(scope, engine.namespace, len(matching)),
        reply_markup=_flat_keyboard(scope),
    )


def _matches(position: dict[str, Any], scope: dict[str, Any]) -> bool:
    if scope.get("everything"):
        return True
    if scope.get("symbol"):
        return str(position.get("symbol", "")).upper() == scope["symbol"]
    return position.get("strategy") == scope.get("strategy")


@router.callback_query(F.data == "flat:no")
async def cancel_flat(call: CallbackQuery) -> None:
    if isinstance(call.message, Message):
        await _safe_edit(call.message, "Cancelled — nothing was closed.")
    await call.answer()


@router.callback_query(F.data.startswith("flat:ok:"))
async def confirm_flat(call: CallbackQuery, engine: EngineClient) -> None:
    scope = _scope_from_callback(call.data or "")
    if scope is None:
        await call.answer("Unreadable scope", show_alert=True)
        return
    log.warning("Operator %s asked for FLAT on %s", call.from_user.id, scope)
    reply = await engine.flat(
        everything=bool(scope.get("everything")),
        symbol=scope.get("symbol"),
        strategy=scope.get("strategy"),
    )
    if isinstance(call.message, Message):
        body = (
            presenters.NO_RUNNER.format(namespace=engine.namespace)
            if reply is None
            else presenters.flat_result(reply, scope)
        )
        await _safe_edit(call.message, body)
    await call.answer()


# ── Act: the bar gate ─────────────────────────────────────────────────────


@router.message(Command("prevent"))
async def prevent_bars(message: Message, command: CommandObject, engine: EngineClient) -> None:
    await _change_gate(message, command, engine, blocking=True, verb="prevent")


@router.message(Command("allow"))
async def allow_bars(message: Message, command: CommandObject, engine: EngineClient) -> None:
    await _change_gate(message, command, engine, blocking=False, verb="allow")


async def _change_gate(
    message: Message,
    command: CommandObject,
    engine: EngineClient,
    *,
    blocking: bool,
    verb: str,
) -> None:
    scope = await _resolve_scope(message, command, engine, verb=verb)
    if scope is None:
        return
    log.warning("Operator %s asked to %s deciding on %s", message.from_user.id, verb, scope)
    gate, announced = await engine.change_gate(
        blocking=blocking,
        everything=bool(scope.get("everything")),
        symbol=scope.get("symbol"),
        strategy=scope.get("strategy"),
    )
    if gate is None:
        await message.answer(
            "⚠️ Could not store the change in Redis, so nothing was changed — a pause that "
            "is not stored would be lost by the next restart."
        )
        return
    await message.answer(
        presenters.gate_result(
            gate.to_payload(),
            blocking=blocking,
            announced=announced,
            namespace=engine.namespace,
        )
    )


# ── Act: the warm-up windows ──────────────────────────────────────────────


@router.message(Command("warmup"))
async def request_warmup(message: Message, command: CommandObject, engine: EngineClient) -> None:
    symbols = await _resolve_symbols(message, command, engine, verb="warmup")
    if symbols is _REFUSED:
        return
    reply = await engine.warmup(symbols)
    if reply is None:
        await message.answer(presenters.NO_INGESTION.format(namespace=engine.namespace))
        return
    await message.answer(presenters.warmup_result(reply, engine.namespace))


@router.message(Command("flush"))
async def request_flush(message: Message, command: CommandObject, engine: EngineClient) -> None:
    symbols = await _resolve_symbols(message, command, engine, verb="flush")
    if symbols is _REFUSED:
        return
    reply = await engine.flush(symbols)
    if reply is None:
        await message.answer(presenters.NO_INGESTION.format(namespace=engine.namespace))
        return
    await message.answer(presenters.flush_result(reply, engine.namespace))


#: Sentinel for "the argument was refused and the operator has been told", kept
#: distinct from ``None``, which these two commands use for "every symbol".
_REFUSED: Any = object()


async def _resolve_symbols(
    message: Message, command: CommandObject, engine: EngineClient, *, verb: str
) -> list[str] | None | Any:
    """``all`` → ``None`` (every planned symbol), a name → ``[name]``.

    Only symbols here, never strategies: both commands act on a vendor's
    series, and a strategy is not one — two strategies can share a symbol, so
    "flush this strategy" has no meaning in Redis.
    """
    symbols = engine.planned_symbols()
    argument = (command.args or "").strip()
    if not argument:
        await message.answer(presenters.usage(verb, symbols))
        return _REFUSED
    if argument.lower() == "all":
        return None
    if argument.upper() in {name.upper() for name in symbols}:
        return [argument.upper()]
    await message.answer(
        f"<code>{presenters.escaped(argument)}</code> is not a symbol this book feeds.\n\n"
        f"{presenters.usage(verb, symbols)}"
    )
    return _REFUSED
