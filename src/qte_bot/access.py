"""Who may use the bot, and the menu they see.

Two small pieces that both answer the same question from different ends: the
filter stops a non-operator's update reaching a handler, and the menu makes
sure only an operator is offered the commands in the first place.
"""

from __future__ import annotations

from aiogram import Bot
from aiogram.exceptions import TelegramAPIError
from aiogram.filters import BaseFilter
from aiogram.types import BotCommandScopeChat, BotCommandScopeDefault, TelegramObject

from qte_bot.commands import COMMANDS
from qte_bot.settings import bot_settings
from qte_shared.logging_setup import get_logger

log = get_logger(__name__)


class IsOperator(BaseFilter):
    """Router-level gate: an id outside ``QTE_BOT__ADMIN_IDS`` reaches nothing.

    Attached to the router's message *and* callback observers, because a button
    press is an update like any other — a callback_data string copied out of a
    forwarded message must not act on the engine.
    """

    async def __call__(self, event: TelegramObject) -> bool:
        sender = getattr(event, "from_user", None)
        if sender is None:
            return False
        allowed = sender.id in bot_settings.operators
        if not allowed:
            log.warning("Ignored a Telegram update from %s, who is not an operator", sender.id)
        return allowed


class CommandMenu:
    """Owns every ``setMyCommands`` call.

    Telegram stores a command menu per scope and keeps it until something
    overwrites it, so the menu is state to maintain rather than something to
    compute per message. The *default* scope is left empty on purpose: a chat
    the bot has never spoken to belongs to nobody in particular, and
    advertising engine commands there would invite presses that only ever get
    refused. Each operator's own chat gets the real menu.
    """

    def __init__(self) -> None:
        self._applied: set[int] = set()

    async def setup(self, bot: Bot) -> None:
        operators = bot_settings.operators
        try:
            await bot.set_my_commands([], scope=BotCommandScopeDefault())
        except TelegramAPIError as failure:
            log.warning("Could not clear the default command menu: %s", failure)
        self._applied.clear()
        for operator_id in sorted(operators):
            await self.sync(bot, operator_id)
        log.info("Command menu applied for %d operator(s)", len(self._applied))

    async def sync(self, bot: Bot, user_id: int) -> None:
        """Apply the operator menu to one chat, once.

        Telegram answers "chat not found" until the user has messaged the bot
        at least once, which is the normal state of a configured operator who
        has not opened it yet. Nothing is cached then, so the next update
        retries; the menu is cosmetic and must never take an update down.
        """
        if user_id in self._applied or user_id not in bot_settings.operators:
            return
        try:
            await bot.set_my_commands(COMMANDS, scope=BotCommandScopeChat(chat_id=user_id))
        except TelegramAPIError as failure:
            log.warning("Skip the command menu for %s: %s", user_id, failure)
            return
        self._applied.add(user_id)
