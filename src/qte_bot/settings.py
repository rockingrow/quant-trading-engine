"""Telegram bot configuration (prefix ``QTE_BOT__``).

Its own BotFather bot, not the one :mod:`qte_shared.notifications` sends with.
Two reasons, the same ones ``algo-trading-broker`` separates them for: only one
process may poll ``getUpdates`` for a token at a time, so a shared token ties
this service's lifecycle to anything else that might ever poll; and an
interactive bot in a group is a different audience from a send-only channel
bot, which is a decision an operator should be able to make per chat.

``admin_ids`` is the whole authorisation model. Every command here reads or
changes a live trading engine — there is no read-only tier worth offering — so
an id that is not on the list gets nothing, and an empty list means the bot
answers nobody.
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class BotSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="QTE_BOT__", extra="ignore")

    #: BotFather token for *this* bot. Empty refuses to start: a bot with no
    #: token has nothing to poll, and starting quietly would look like a bot
    #: that is up and ignoring you.
    telegram_token: str = ""
    #: Comma-separated Telegram user ids allowed to use the bot.
    admin_ids: str = ""
    #: Seconds to wait for the runner or ingestion to answer a control request.
    #: A FLAT may have to emit several signals, so it gets its own, longer one.
    request_timeout: float = Field(default=5.0, gt=0)
    flat_timeout: float = Field(default=30.0, gt=0)

    @property
    def operators(self) -> set[int]:
        """``admin_ids`` as ids, ignoring blanks and anything not a number."""
        parsed: set[int] = set()
        for entry in self.admin_ids.split(","):
            entry = entry.strip()
            if entry.isdigit():
                parsed.add(int(entry))
        return parsed


bot_settings = BotSettings()
