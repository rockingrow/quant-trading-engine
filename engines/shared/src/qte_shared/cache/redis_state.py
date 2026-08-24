"""Hot state in Redis: last tick, recent candles, per-strategy cycle ids.

This is the state that must survive a container restart but is far too hot for
Postgres — the strategy runner rebuilds its warm-up window from here on boot
instead of replaying history over the network. Redis runs with AOF on
(``docker-compose.yml``) so a restart loses at most the last write, not the
whole book.

Postgres stays the audit trail; nothing here is a system of record.
"""

from __future__ import annotations

import json
from typing import Any

import redis.asyncio as redis

from qte_shared.config import settings
from qte_shared.logging_setup import get_logger
from qte_shared.models import Candle, Tick

log = get_logger(__name__)


class RedisState:
    """Namespaced async Redis accessor. One instance per service."""

    def __init__(self, url: str | None = None, prefix: str | None = None) -> None:
        self._url = url or settings.redis.url
        self._prefix = prefix or settings.redis.key_prefix
        self._client: redis.Redis | None = None

    @property
    def client(self) -> redis.Redis:
        if self._client is None:
            raise RuntimeError("Redis is not connected — call connect() first")
        return self._client

    async def connect(self) -> None:
        if self._client is not None:
            return
        self._client = redis.from_url(self._url, decode_responses=True)
        await self._client.ping()
        log.info("Redis connected url=%s prefix=%s", self._url, self._prefix)

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def key(self, *parts: str) -> str:
        return ":".join((self._prefix, *parts))

    # ── Ticks ─────────────────────────────────────────────────────────

    async def set_last_tick(self, tick: Tick) -> None:
        await self.client.set(self.key("tick", tick.symbol), tick.model_dump_json())

    async def get_last_tick(self, symbol: str) -> Tick | None:
        raw = await self.client.get(self.key("tick", symbol))
        return Tick.model_validate_json(raw) if raw else None

    # ── Candles ───────────────────────────────────────────────────────

    async def push_candle(self, candle: Candle, max_len: int | None = None) -> None:
        """Append a closed candle to its capped history list.

        Newest is pushed on the right and the list trimmed from the left, so
        :meth:`get_candles` can return oldest-first without reversing.
        """
        limit = max_len or settings.redis.candle_history
        key = self.key("candles", candle.symbol, candle.timeframe)
        pipe = self.client.pipeline()
        pipe.rpush(key, candle.model_dump_json())
        pipe.ltrim(key, -limit, -1)
        if settings.redis.ttl_seconds:
            pipe.expire(key, settings.redis.ttl_seconds)
        await pipe.execute()

    async def get_candles(self, symbol: str, timeframe: str, count: int = 0) -> list[Candle]:
        """Oldest-first history; ``count=0`` returns everything stored."""
        key = self.key("candles", symbol, timeframe)
        start = -count if count else 0
        raw = await self.client.lrange(key, start, -1)
        return [Candle.model_validate_json(item) for item in raw]

    async def set_open_candle(self, candle: Candle) -> None:
        """Persist the bar currently being built so a restart mid-bar resumes it."""
        await self.client.set(
            self.key("open_candle", candle.symbol, candle.timeframe),
            candle.model_dump_json(),
        )

    async def get_open_candle(self, symbol: str, timeframe: str) -> Candle | None:
        raw = await self.client.get(self.key("open_candle", symbol, timeframe))
        return Candle.model_validate_json(raw) if raw else None

    # ── Strategy cycle state ──────────────────────────────────────────

    async def set_open_cycle(self, strategy: str, symbol: str, uxid: str) -> None:
        """Remember the cycle id of the position *strategy* holds on *symbol*.

        Every close the strategy emits later must carry this id, or the broker
        renders the exit as an unrelated trade instead of closing the entry's
        broadcast. Losing it on restart is exactly what Redis-with-AOF prevents.
        """
        await self.client.hset(self.key("cycle", strategy), symbol, uxid)

    async def get_open_cycle(self, strategy: str, symbol: str) -> str | None:
        return await self.client.hget(self.key("cycle", strategy), symbol)

    async def clear_open_cycle(self, strategy: str, symbol: str) -> None:
        await self.client.hdel(self.key("cycle", strategy), symbol)

    # ── Generic flags (shadow mode, kill switch, …) ───────────────────

    async def set_flag(self, name: str, value: Any) -> None:
        await self.client.set(self.key("flag", name), json.dumps(value))

    async def get_flag(self, name: str, default: Any = None) -> Any:
        raw = await self.client.get(self.key("flag", name))
        return json.loads(raw) if raw is not None else default

    async def ping(self) -> bool:
        try:
            return bool(await self.client.ping())
        except Exception:
            return False
