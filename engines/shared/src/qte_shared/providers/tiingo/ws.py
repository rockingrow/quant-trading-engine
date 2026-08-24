"""Tiingo WebSocket feed -- one socket per market, reconnecting forever.

Tiingo runs FX and crypto on separate endpoints with the same envelope, so one
class serves both and the market only changes the URL, the threshold and how a
data row is read. Every parsed quote/trade becomes a
:class:`~qte_shared.models.Tick` handed to the callback; nothing here knows
about candles, Redis or NATS.

Reconnect policy is deliberately unbounded with capped backoff: a feed that
gives up at 3am leaves the engine silently blind, which is worse than a socket
that keeps rattling the door and logging that it cannot get in.
"""

from __future__ import annotations

import asyncio
import json
import random
from datetime import UTC, datetime
from typing import Any

import websockets

from qte_shared.interfaces.market_data import (
    LiveFeed,
    ProviderNotConfigured,
    TickHandler,
)
from qte_shared.logging_setup import get_logger
from qte_shared.models import Tick
from qte_shared.providers.tiingo.settings import TiingoSettings
from qte_shared.symbols import Market

log = get_logger(__name__)

_MAX_BACKOFF_SECONDS = 60.0


class TiingoLiveFeed(LiveFeed):
    """Subscribes to one market's tickers and emits ticks until stopped."""

    def __init__(
        self,
        market: Market,
        tickers: dict[str, str],
        on_tick: TickHandler,
        config: TiingoSettings,
    ) -> None:
        """*tickers* maps the Tiingo ticker back to the QTE symbol it stands for."""
        self.market = market
        self.name = f"tiingo-{market}"
        self._by_ticker = dict(tickers)
        self._on_tick = on_tick
        self._config = config
        self._running = False
        self._task: asyncio.Task[None] | None = None

    # -- Lifecycle ---------------------------------------------------------

    @property
    def symbols(self) -> tuple[str, ...]:
        return tuple(sorted(self._by_ticker.values()))

    @property
    def url(self) -> str:
        return self._config.fx_ws_url if self.market == "fx" else self._config.crypto_ws_url

    @property
    def threshold(self) -> int:
        return self._config.fx_threshold if self.market == "fx" else self._config.crypto_threshold

    def start(self) -> asyncio.Task[None] | None:
        """Spawn the connect loop. No-op when this market has no symbols."""
        if not self._by_ticker:
            log.info("No %s symbols configured -- skipping that socket", self.market)
            return None
        if not self._config.api_key:
            raise ProviderNotConfigured(
                "QTE_TIINGO__API_KEY is not set; the Tiingo WebSocket cannot authenticate"
            )
        self._running = True
        self._task = asyncio.create_task(self._run(), name=self.name)
        return self._task

    async def stop(self) -> None:
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    # -- Connect loop ------------------------------------------------------

    async def _run(self) -> None:
        attempt = 0
        while self._running:
            try:
                async with websockets.connect(
                    self.url, ping_interval=20, ping_timeout=20, max_queue=1024
                ) as socket:
                    await self._subscribe(socket)
                    attempt = 0
                    log.info(
                        "Tiingo %s socket open tickers=%s",
                        self.market,
                        ",".join(sorted(self._by_ticker)),
                    )
                    async for raw in socket:
                        await self._handle_raw(raw)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if not self._running:
                    return
                attempt += 1
                delay = _backoff(attempt)
                log.warning(
                    "Tiingo %s socket dropped (attempt %d): %s -- retrying in %.1fs",
                    self.market,
                    attempt,
                    exc,
                    delay,
                )
                await asyncio.sleep(delay)

    async def _subscribe(self, socket: Any) -> None:
        await socket.send(
            json.dumps(
                {
                    "eventName": "subscribe",
                    "authorization": self._config.api_key,
                    "eventData": {
                        "thresholdLevel": self.threshold,
                        "tickers": sorted(self._by_ticker),
                    },
                }
            )
        )

    # -- Message handling --------------------------------------------------

    async def _handle_raw(self, raw: str | bytes) -> None:
        try:
            message = json.loads(raw)
        except json.JSONDecodeError:
            log.warning("Tiingo %s sent non-JSON frame: %.120r", self.market, raw)
            return

        message_type = message.get("messageType")
        if message_type == "I":
            log.info("Tiingo %s subscription ack: %s", self.market, message.get("data"))
            return
        if message_type == "E":
            log.error("Tiingo %s error frame: %s", self.market, message)
            return
        if message_type != "A":
            return

        tick = self._parse(message.get("data") or [])
        if tick is None:
            return
        # A consumer that raises must not cost us the socket. Without this the
        # exception reaches the reconnect handler in `_run`, which drops a
        # healthy connection, loses every tick that arrives during the backoff,
        # and reports a Redis or NATS failure as "Tiingo socket dropped".
        try:
            await self._on_tick(tick)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception(
                "Tick handler failed market=%s symbol=%s ts=%s — the socket stays open",
                self.market,
                tick.symbol,
                tick.ts.isoformat(),
            )

    def _parse(self, data: list[Any]) -> Tick | None:
        """Turn one Tiingo data row into a tick, or ``None`` if it isn't one.

        FX quote row:    ``["Q", ticker, ts, bidSize, bidPrice, midPrice, askSize, askPrice]``
        Crypto quote:    ``["Q", ticker, ts, exchange, bidSize, bidPrice, midPrice,
                          askSize, askPrice]``
        Crypto trade:    ``["T", ticker, ts, exchange, size, price]``

        The crypto rows carry an extra ``exchange`` field, which is the whole
        reason the two markets cannot share one index map.
        """
        if len(data) < 3:
            return None
        row_type, ticker = data[0], str(data[1]).lower()
        symbol = self._by_ticker.get(ticker)
        if symbol is None:
            return None
        moment = _parse_timestamp(data[2])
        if moment is None:
            return None

        try:
            if row_type == "Q":
                offset = 4 if self.market == "crypto" else 3
                bid = _as_float(data[offset + 1])
                ask = _as_float(data[offset + 4])
                if bid is None and ask is None:
                    return None
                return Tick(symbol=symbol, ts=moment, bid=bid, ask=ask)
            if row_type == "T" and self.market == "crypto":
                return Tick(
                    symbol=symbol,
                    ts=moment,
                    last=_as_float(data[5]),
                    volume=_as_float(data[4]) or 0.0,
                )
        except IndexError:
            log.warning("Tiingo %s row too short for type %s: %s", self.market, row_type, data)
        return None


def _backoff(attempt: int) -> float:
    """Exponential backoff with jitter, capped -- never a tight reconnect loop."""
    base = min(_MAX_BACKOFF_SECONDS, 2.0 ** min(attempt, 6))
    return base * (0.5 + random.random() / 2)


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).replace("Z", "+00:00")
    try:
        moment = datetime.fromisoformat(text)
    except ValueError:
        log.warning("Unparseable Tiingo timestamp: %r", value)
        return None
    return moment if moment.tzinfo else moment.replace(tzinfo=UTC)


def _as_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
