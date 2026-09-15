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
from datetime import UTC, datetime
from typing import Any

import redis.asyncio as redis

from qte_shared.config import settings
from qte_shared.logging_setup import get_logger
from qte_shared.models import Candle, OpenPosition, Tick
from qte_shared.state_scope import stamp_market_data
from qte_shared.timeframes import CLOCK_TOLERANCE

log = get_logger(__name__)

STAGE_CLOSED_CANDLE = """
local previous = redis.call('GET', KEYS[4])
if previous and tonumber(previous) >= tonumber(ARGV[2]) then return 0 end
local history_type = redis.call('TYPE', KEYS[1]).ok
local outbox_type = redis.call('TYPE', KEYS[2]).ok
if (history_type ~= 'none' and history_type ~= 'list') or
   (outbox_type ~= 'none' and outbox_type ~= 'list') then
    return redis.error_reply('Candle history and outbox must be lists')
end
local opened = redis.call('GET', KEYS[3])
local remove_open = false
if opened then
    local decoded, stored = pcall(cjson.decode, opened)
    if not decoded or type(stored) ~= 'table' then
        return redis.error_reply('Open candle must contain valid JSON')
    end
    local candle = cjson.decode(ARGV[1])
    remove_open = stored.open_time == candle.open_time
end
redis.call('RPUSH', KEYS[1], ARGV[1])
redis.call('LTRIM', KEYS[1], -tonumber(ARGV[3]), -1)
if tonumber(ARGV[4]) > 0 then redis.call('EXPIRE', KEYS[1], ARGV[4]) end
redis.call('RPUSH', KEYS[2], ARGV[1])
redis.call('SET', KEYS[4], ARGV[2])
if remove_open then redis.call('DEL', KEYS[3]) end
return 1
"""


class RedisState:
    """Namespaced async Redis accessor. One instance per service."""

    def __init__(self, url: str | None = None, prefix: str | None = None) -> None:
        self._url = url or settings.redis.url
        self._scope = settings.state_scope
        self._prefix = f"{prefix or settings.redis.key_prefix}:{self._scope.namespace}"
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

    # ── Runner ownership ──────────────────────────────────────────────

    async def claim_runner(self, owner_id: str) -> bool:
        """Allow one runner per state namespace, without automatic lease expiry.

        A paused process must not outlive a lease and resume alongside a new
        writer. After an unclean exit an operator must first stop that process
        and explicitly remove the stale ownership key before restarting.
        """
        return bool(await self.client.set(self.key("runner", "owner"), owner_id, nx=True))

    async def owns_runner(self, owner_id: str) -> bool:
        return await self.client.get(self.key("runner", "owner")) == owner_id

    async def release_runner(self, owner_id: str) -> bool:
        """Release only our own claim, never a replacement runner's claim."""
        return bool(
            await self.client.eval(
                "if redis.call('GET', KEYS[1]) == ARGV[1] then "
                "return redis.call('DEL', KEYS[1]) else return 0 end",
                1,
                self.key("runner", "owner"),
                owner_id,
            )
        )

    # ── Ticks ─────────────────────────────────────────────────────────

    async def set_last_tick(self, tick: Tick) -> None:
        tick = stamp_market_data(tick, self._scope.origin())
        await self.client.set(self.key("tick", tick.symbol), tick.model_dump_json())

    async def get_last_tick(self, symbol: str) -> Tick | None:
        raw = await self.client.get(self.key("tick", symbol))
        if not raw:
            return None
        tick = Tick.model_validate_json(raw)
        if not self._scope.accepts(tick.origin):
            return None
        if tick.ts.tzinfo is None or (
            not tick.origin.synthetic and tick.ts > datetime.now(UTC) + CLOCK_TOLERANCE
        ):
            return None
        return tick

    async def discard_last_tick(self, symbol: str) -> None:
        await self.client.delete(self.key("tick", symbol))

    # ── Candles ───────────────────────────────────────────────────────

    async def push_candle(self, candle: Candle, max_len: int | None = None) -> None:
        """Append a closed candle to its capped history list.

        Newest is pushed on the right and the list trimmed from the left, so
        :meth:`get_candles` can return oldest-first without reversing.
        """
        candle = stamp_market_data(candle, self._scope.origin())
        limit = max_len or settings.redis.candle_history
        key = self.key("candles", candle.symbol, candle.timeframe)
        pipe = self.client.pipeline()
        pipe.rpush(key, candle.model_dump_json())
        pipe.ltrim(key, -limit, -1)
        if settings.redis.ttl_seconds:
            pipe.expire(key, settings.redis.ttl_seconds)
        await pipe.execute()

    async def stage_closed_candle(self, candle: Candle, max_len: int | None = None) -> None:
        """Persist and enqueue a close atomically, once per series/open time.

        The resampler has already retired the bucket by the time this is
        called.  If NATS is unavailable, the queue is therefore the only place
        from which the event can be replayed.  Keeping the history write and
        queue append in one transaction also prevents a restart from seeing a
        candle in one representation but not the other.
        """
        candle = stamp_market_data(candle, self._scope.origin())
        await self.client.eval(
            STAGE_CLOSED_CANDLE,
            4,
            self.key("candles", candle.symbol, candle.timeframe),
            self.key("outbox", "candles"),
            self.key("open_candle", candle.symbol, candle.timeframe),
            self.key("staged", candle.symbol, candle.timeframe),
            candle.model_dump_json(),
            candle.open_time.timestamp(),
            max_len or settings.redis.candle_history,
            settings.redis.ttl_seconds,
        )

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
        retained = [stamp_market_data(candle, self._scope.origin()) for candle in candles[-limit:]]
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
        candle = stamp_market_data(candle, self._scope.origin())
        await self.client.set(
            self.key("open_candle", candle.symbol, candle.timeframe),
            candle.model_dump_json(),
        )

    async def get_open_candle(self, symbol: str, timeframe: str) -> Candle | None:
        raw = await self.client.get(self.key("open_candle", symbol, timeframe))
        return Candle.model_validate_json(raw) if raw else None

    async def pending_candles(self) -> list[Candle]:
        """Every staged close still waiting in the outbox, oldest first."""
        staged = await self.client.lrange(self.key("outbox", "candles"), 0, -1)
        return [Candle.model_validate_json(staged_json) for staged_json in staged]

    # ── Candle state provenance ───────────────────────────────────────
    #
    # Redis outlives a change of market-data provider, and bars one provider
    # wrote are not market history for another. See
    # :mod:`qte_ingestion.state_guard`.

    async def get_history_provider(self) -> str | None:
        """The provider whose feed last wrote the candle state, if recorded."""
        return await self.client.get(self.key("history", "provider"))

    async def set_history_provider(self, provider_name: str) -> None:
        await self.client.set(self.key("history", "provider"), provider_name)

    async def discard_candle_state(self, symbol: str, timeframe: str) -> None:
        """Forget one pair's closed history and its open bar. Positions stay."""
        await self.client.delete(
            self.key("candles", symbol, timeframe),
            self.key("open_candle", symbol, timeframe),
            self.key("staged", symbol, timeframe),
        )

    async def discard_candle_outbox(self) -> None:
        """Drop every staged close that has not been published yet."""
        await self.client.delete(self.key("outbox", "candles"))

    # ── Decision watermark ────────────────────────────────────────────
    #
    # The open time of the newest bar each (strategy, symbol, timeframe) was fed.
    # Closes published while a runner was down or still starting are the bars
    # newer than this; see ``StrategyRunner._catch_up``.

    async def get_decided_open_time(
        self, strategy: str, symbol: str, timeframe: str
    ) -> datetime | None:
        stored = await self.client.get(self.key("decided", strategy, symbol, timeframe))
        return datetime.fromisoformat(stored) if stored else None

    async def set_decided_open_time(
        self, strategy: str, symbol: str, timeframe: str, open_time: datetime
    ) -> None:
        await self.client.set(
            self.key("decided", strategy, symbol, timeframe), open_time.isoformat()
        )

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
        if position.state_namespace not in (None, self._scope.namespace):
            raise ValueError("Cannot persist a position from another state namespace")
        position = position.model_copy(update={"state_namespace": self._scope.namespace})
        await self.client.hset(
            self.key("cycle", position.strategy), position.symbol, position.model_dump_json()
        )

    async def get_open_position(self, strategy: str, symbol: str) -> OpenPosition | None:
        raw = await self.client.hget(self.key("cycle", strategy), symbol)
        position = _decode_position(raw, strategy=strategy, symbol=symbol)
        self._validate_position_scope(position)
        return position

    def _validate_position_scope(self, position: OpenPosition | None) -> None:
        if position is not None and position.state_namespace != self._scope.namespace:
            raise ValueError("Stored position has missing or foreign state provenance")

    async def get_open_positions(self, strategy: str) -> dict[str, OpenPosition]:
        """Every cycle *strategy* holds, keyed by symbol."""
        stored = await self.client.hgetall(self.key("cycle", strategy))
        positions = {}
        for symbol, raw in (stored or {}).items():
            position = _decode_position(raw, strategy=strategy, symbol=symbol)
            self._validate_position_scope(position)
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
