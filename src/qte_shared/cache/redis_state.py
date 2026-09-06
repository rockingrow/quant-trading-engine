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
from qte_shared.models import Candle, OpenPosition, Tick

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

    async def stage_closed_candle(self, candle: Candle, max_len: int | None = None) -> None:
        """Persist a closed candle and enqueue its event in one Redis transaction.

        The resampler has already retired the bucket by the time this is
        called.  If NATS is unavailable, the queue is therefore the only place
        from which the event can be replayed.  Keeping the history write and
        queue append in one transaction also prevents a restart from seeing a
        candle in one representation but not the other.
        """
        limit = max_len or settings.redis.candle_history
        history = self.key("candles", candle.symbol, candle.timeframe)
        outbox = self.key("outbox", "candles")
        pipe = self.client.pipeline(transaction=True)
        pipe.rpush(history, candle.model_dump_json())
        pipe.ltrim(history, -limit, -1)
        if settings.redis.ttl_seconds:
            pipe.expire(history, settings.redis.ttl_seconds)
        pipe.rpush(outbox, candle.model_dump_json())
        # A quiet-market flush may close the only builder without a following
        # tick to overwrite this key.  Leaving it behind would resurrect an
        # already-closed bar after a restart.
        pipe.delete(self.key("open_candle", candle.symbol, candle.timeframe))
        await pipe.execute()

    async def peek_pending_candle(self) -> Candle | None:
        """Oldest closed candle whose NATS event has not been acknowledged."""
        raw = await self.client.lindex(self.key("outbox", "candles"), 0)
        return Candle.model_validate_json(raw) if raw else None

    async def ack_pending_candle(self) -> None:
        """Remove the oldest candle after its Core NATS publish succeeds."""
        await self.client.lpop(self.key("outbox", "candles"))

    async def get_candles(self, symbol: str, timeframe: str, count: int = 0) -> list[Candle]:
        """Oldest-first history; ``count=0`` returns everything stored."""
        key = self.key("candles", symbol, timeframe)
        start = -count if count else 0
        raw = await self.client.lrange(key, start, -1)
        return [Candle.model_validate_json(item) for item in raw]

    async def count_candles(self, symbol: str, timeframe: str) -> int:
        """How many bars the history list holds, without decoding any of them."""
        return int(await self.client.llen(self.key("candles", symbol, timeframe)))

    async def replace_candles(
        self, symbol: str, timeframe: str, candles: list[Candle], max_len: int | None = None
    ) -> int:
        """Rewrite the whole history list, oldest first, in one transaction.

        Warm-up backfill cannot use :meth:`push_candle`: appending historical
        bars to a list that already ends at *now* would leave the newest bar in
        the middle, and the runner reads the tail as its most recent window. So
        the merged series is written whole, and a reader either sees the old
        list or the new one -- never a half-filled key.
        """
        if not candles:
            return 0
        limit = max_len or settings.redis.candle_history
        retained = candles[-limit:]
        key = self.key("candles", symbol, timeframe)
        pipe = self.client.pipeline()
        pipe.delete(key)
        pipe.rpush(key, *[candle.model_dump_json() for candle in retained])
        if settings.redis.ttl_seconds:
            pipe.expire(key, settings.redis.ttl_seconds)
        await pipe.execute()
        return len(retained)

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
    #
    # One hash per strategy, one field per symbol, holding the whole
    # :class:`~qte_shared.models.OpenPosition` as JSON. The size matters as
    # much as the id: a TP1 that closed the entry's full quantity ends the
    # cycle, and a runner that reloaded only the uxid could not tell that from
    # a partial. Redis runs with AOF on, so a restart loses at most the last
    # write — and the strategy runner mirrors the same record into Postgres,
    # which is what covers a flushed cache.

    async def set_open_position(self, position: OpenPosition) -> None:
        """Remember the whole cycle *position* describes.

        Every close the strategy emits later must carry its ``signal_uxid``, or
        the broker renders the exit as an unrelated trade instead of closing
        the entry's broadcast.
        """
        await self.client.hset(
            self.key("cycle", position.strategy), position.symbol, position.model_dump_json()
        )

    async def get_open_position(self, strategy: str, symbol: str) -> OpenPosition | None:
        raw = await self.client.hget(self.key("cycle", strategy), symbol)
        return _decode_position(raw, strategy=strategy, symbol=symbol)

    async def get_open_positions(self, strategy: str) -> dict[str, OpenPosition]:
        """Every cycle *strategy* holds, keyed by symbol."""
        stored = await self.client.hgetall(self.key("cycle", strategy))
        positions = {}
        for symbol, raw in (stored or {}).items():
            position = _decode_position(raw, strategy=strategy, symbol=symbol)
            if position is not None:
                positions[symbol] = position
        return positions

    async def set_open_cycle(self, strategy: str, symbol: str, uxid: str) -> None:
        """Remember a cycle by id alone, with no size attached.

        The lossy form of :meth:`set_open_position`, kept for a caller that has
        nothing but the id.
        """
        await self.set_open_position(
            OpenPosition(signal_uxid=uxid, strategy=strategy, symbol=symbol)
        )

    async def get_open_cycle(self, strategy: str, symbol: str) -> str | None:
        position = await self.get_open_position(strategy, symbol)
        return position.signal_uxid if position else None

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


def _decode_position(raw: str | None, *, strategy: str, symbol: str) -> OpenPosition | None:
    """Parse a stored cycle, tolerating the bare-uxid values that predate this.

    A value written before the position record existed is just the id. Reading
    it as one — rather than discarding it as unparseable — is what lets a
    runner upgraded mid-trade still close the position it is holding.
    """
    text = (raw or "").strip()
    if not text:
        return None
    if text.startswith("{"):
        try:
            return OpenPosition.model_validate_json(text)
        except ValueError:
            log.error("Unreadable cycle record for %s %s: %.120r", strategy, symbol, raw)
            return None
    return OpenPosition(signal_uxid=text, strategy=strategy, symbol=symbol)
