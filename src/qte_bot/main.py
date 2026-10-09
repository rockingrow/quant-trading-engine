"""Entry point for the ``qte-bot`` container.

Long-polling, like ``algo-trading-broker``'s bot: there is no public URL to
hang a webhook on and no reason to want one. aiogram handles SIGINT/SIGTERM and
drains in-flight handlers, so the shutdown hook only has to close what this
module opened — the NATS connection, Redis, and the bot's own session.

The engine client is injected through a middleware rather than reached for as a
global, so a handler declares ``engine: EngineClient`` as a parameter and a
test can hand it a double.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from aiogram import BaseMiddleware, Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import TelegramObject

from qte_bot import handlers
from qte_bot.access import CommandMenu
from qte_bot.engine_client import EngineClient
from qte_bot.settings import bot_settings
from qte_shared.logging_setup import configure_logging, get_logger

log = get_logger(__name__)

SERVICE_NAME = "qte-bot"


class Dependencies(BaseMiddleware):
    """Hands every handler the engine client, and keeps the menu in step."""

    def __init__(self, engine: EngineClient, menu: CommandMenu) -> None:
        self._engine = engine
        self._menu = menu

    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: dict[str, Any],
    ) -> Any:
        data["engine"] = self._engine
        bot = data.get("bot")
        sender = getattr(event, "from_user", None)
        if bot is not None and sender is not None:
            # An operator who has just opened the chat for the first time gets
            # their menu on this update rather than on the next restart.
            await self._menu.sync(bot, sender.id)
        return await handler(event, data)


async def main() -> None:
    configure_logging(service=SERVICE_NAME)
    if not bot_settings.telegram_token:
        raise SystemExit(
            "QTE_BOT__TELEGRAM_TOKEN is required. Create a bot with @BotFather and put its "
            "token there; it must be its own bot, not the one QTE_TELEGRAM__BOT_TOKEN sends "
            "notifications with, because only one process may poll a token at a time."
        )
    operators = bot_settings.operators
    if not operators:
        log.warning(
            "QTE_BOT__ADMIN_IDS is empty, so the bot will answer nobody. Add your Telegram "
            "user id to it."
        )

    engine = EngineClient(
        request_timeout=bot_settings.request_timeout,
        flat_timeout=bot_settings.flat_timeout,
    )
    await engine.start()

    bot = Bot(
        token=bot_settings.telegram_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    menu = CommandMenu()
    dispatcher = Dispatcher()
    dispatcher.update.outer_middleware(Dependencies(engine, menu))
    dispatcher.include_router(handlers.router)

    async def on_startup() -> None:
        await menu.setup(bot)
        log.info("Bot polling for %d operator(s) on book %s", len(operators), engine.namespace)

    async def on_shutdown() -> None:
        log.info("Bot shutting down")
        await engine.aclose()
        await bot.session.close()

    dispatcher.startup.register(on_startup)
    dispatcher.shutdown.register(on_shutdown)
    try:
        await dispatcher.start_polling(bot)
    finally:
        # Safety net: polling can exit before the shutdown hook runs.
        await engine.aclose()


def run() -> None:
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Interrupted")


if __name__ == "__main__":
    run()
