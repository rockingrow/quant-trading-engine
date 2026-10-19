"""The newest bars of a series, asked of ``algo-trading-ingester`` over NATS.

The ingester publishes a bar when it closes and nothing else, so a QTE that
starts on a cold Redis has no indicator window until enough bars have printed
live — a day and a half for 150 bars of M15. This is how the window is filled
instead: QTE asks for it. One request per series::

    <rpc_prefix>.history.<gateway>.<SYMBOL>.<TIMEFRAME>
    {"schema_version": "1.0.0", "request_id": "…",
     "symbol": "XAUUSD", "timeframe": "M15", "count": 500}

and one reply carrying every bar. QTE names the series and the count; the
ingester decides neither, and holds no warm-up setting of its own.

**Why a request, and not bars replayed onto the bar subject.** A window sent as
a numbered series of events has to be reassembled, and each message of it can
be lost on its own — the stream de-duplicates on the bar's id, so a window sent
twice inside its duplicate window arrives with holes or not at all. A reply is
one message: whole, or absent and asked for again. It also puts the trigger
where the need is. QTE knows when its window is short; the ingester only knows
when it restarted.

**Core NATS, outside the stream.** The request subject is never under the
prefix the ingester publishes bars on. The stream captures that whole tree, and
a request sent into it would be stored as market data and answered first by the
stream's own acknowledgement.

**It serves** :attr:`~qte_shared.interfaces.market_data.Capability.RECENT_BARS`,
**not** ``HISTORY``. What comes back for a date range is the newest
``history_bars`` bars that fall inside it, which is right for a window and
wrong for an archive: a backtest download still refuses this provider, and
nothing here is written to the parquet history cache.

**QTE does not wait for the ingester, and does not poll for it.** It starts on
whatever Redis holds. With no ingester subscribed NATS answers a request *no
responders* at once, which is raised as
:class:`~qte_shared.interfaces.market_data.HistoryOffline` — "nobody to ask",
not "ask again in a moment". The ingester then says when it is there: it
publishes on ``<rpc_prefix>.online.<gateway>`` once it is answering requests,
and again after every reconnect. :meth:`IngesterHistorySource.watch_online`
listens for that, and ingestion reacts by checking each planned window and
asking for the short ones. Nothing is asked on a timer in between. The one
announcement that could go unheard is one made while this watch's own
connection was down, so the watch also fires once whenever that connection
comes back.

A connection per fetch rather than one kept open: history is asked for a
handful of times and rarely after. The watch is the one connection held.
"""

from __future__ import annotations

import math
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, time, timedelta
from typing import ClassVar

import nats.errors
import pandas as pd

from qte_shared.bus.nats_bus import NatsBus
from qte_shared.interfaces.market_data import (
    HistoryOffline,
    HistoryRequest,
    HistorySource,
    ProviderError,
    empty_ohlcv_frame,
    normalize_ohlcv,
)
from qte_shared.logging_setup import get_logger
from qte_shared.models import Candle
from qte_shared.providers.mt5.protocol import (
    HistoryAnswer,
    IngesterPayloadError,
    decode_history_reply,
    encode_history_request,
)
from qte_shared.providers.mt5.settings import Mt5Settings
from qte_shared.timeframes import timeframe_seconds

log = get_logger(__name__)


class IngesterHistorySource(HistorySource):
    """Asks the ingester for the newest closed bars of one series at a time."""

    cacheable: ClassVar[bool] = False

    def __init__(
        self,
        config: Mt5Settings | None = None,
        bus_factory: Callable[[], NatsBus] | None = None,
        utc_clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._config = config or Mt5Settings()
        self._bus_factory = bus_factory or self._default_bus
        self._utc_clock = utc_clock or (lambda: datetime.now(UTC))
        #: The connection the online watch listens on; none until it is asked for.
        self._watch_bus: NatsBus | None = None

    @property
    def max_bars(self) -> int:
        return self._config.history_bars

    async def watch_online(self, on_online: Callable[[], Awaitable[None]]) -> bool:
        """Call *on_online* each time the ingester announces its gateway.

        Raises :class:`~qte_shared.interfaces.market_data.ProviderError` when
        NATS cannot be reached to listen. The subscription survives a
        reconnect, so one call lasts the life of the process.
        """
        if self._watch_bus is not None:
            return True
        bus = self._bus_factory()
        try:
            await bus.connect()
        except Exception as exc:
            raise ProviderError(
                f"cannot reach NATS at {self._config.server_url} to watch for the ingester: {exc}"
            ) from exc

        async def announced(message) -> None:
            log.info(
                "Ingester announced itself on %s: %.200s",
                message.subject,
                message.data.decode(errors="replace"),
            )
            await on_online()

        async def reconnected() -> None:
            # An announcement made while this connection was down was never
            # delivered. Rather than probe on a timer for one that might have
            # been missed, check once, at the only moment one can have been.
            log.info(
                "The ingester watch reconnected — checking once in case its announcement was missed"
            )
            await on_online()

        await bus.subscribe(self._config.online_subject, announced)
        bus.on_reconnect(reconnected)
        self._watch_bus = bus
        return True

    async def close(self) -> None:
        bus, self._watch_bus = self._watch_bus, None
        if bus is None:
            return
        try:
            await bus.close()
        except Exception:
            log.warning("Closing the ingester watch connection failed", exc_info=True)

    def _default_bus(self) -> NatsBus:
        return NatsBus(
            url=self._config.server_url,
            token=self._config.server_token,
            name=f"qte-{self._config.gateway}-history",
        )

    async def fetch(self, request: HistoryRequest) -> pd.DataFrame:
        """The newest bars inside *request*'s dates, at most ``history_bars``.

        Raises :class:`~qte_shared.interfaces.market_data.ProviderError` when
        the ingester cannot be reached or refuses for now, and
        :class:`~qte_shared.interfaces.market_data.HistoryNotServed` when it
        refuses for good.
        """
        request = request.normalized()
        count = self._count_for(request)
        candles = await self.fetch_candles(request.symbol, request.timeframe, count)
        rows = [
            {
                "date": candle.open_time,
                "open": candle.open,
                "high": candle.high,
                "low": candle.low,
                "close": candle.close,
                "volume": candle.volume,
            }
            for candle in candles
            if request.start <= candle.open_time.date() <= request.end
        ]
        if not rows:
            return empty_ohlcv_frame()
        return normalize_ohlcv(rows, request.timeframe)

    def _count_for(self, request: HistoryRequest) -> int:
        """Bars to ask for: enough to cover the range, never over ``history_bars``.

        The ingester counts back from the newest closed bar, so a range is
        covered by asking for every bucket between its start and now. A range
        that began a week ago asks for the cap; a top-up from this morning asks
        for a few dozen.
        """
        range_start = datetime.combine(request.start, time.min, tzinfo=UTC)
        elapsed = max(self._utc_clock() - range_start, timedelta(0))
        buckets = math.ceil(elapsed.total_seconds() / timeframe_seconds(request.timeframe))
        return max(1, min(self._config.history_bars, buckets))

    async def fetch_candles(self, symbol: str, timeframe: str, count: int) -> list[Candle]:
        """The newest *count* closed bars of one series, oldest first."""
        subject = self._config.history_subject(symbol, timeframe)
        request_id = f"qte-{uuid.uuid4().hex[:12]}"
        body = encode_history_request(symbol, timeframe, count, request_id)
        answer = await self._ask(subject, body, symbol, timeframe, request_id)
        log.info(
            "Ingester history %s %s: %d bar(s) of %d asked for%s, %s (request %s, from %s)",
            symbol,
            timeframe,
            len(answer.candles),
            count,
            " — truncated by the ingester" if answer.truncated else "",
            _span(answer.candles),
            request_id,
            answer.ingester_id or "?",
        )
        return answer.candles

    async def _ask(
        self, subject: str, body: bytes, symbol: str, timeframe: str, request_id: str
    ) -> HistoryAnswer:
        bus = self._bus_factory()
        try:
            await bus.connect()
        except Exception as exc:
            raise ProviderError(
                f"cannot reach NATS at {self._config.server_url} to ask for history: {exc}"
            ) from exc
        try:
            attempts = self._config.history_attempts
            for attempt in range(1, attempts + 1):
                try:
                    reply = await bus.nc.request(
                        subject, body, timeout=self._config.history_timeout
                    )
                except nats.errors.NoRespondersError as exc:
                    # Not retried, here or on a timer: nobody is subscribed.
                    # The ingester announces itself when it is, and the caller
                    # asks then.
                    raise HistoryOffline(
                        f"no ingester is answering on {subject} — algo-trading-ingester is "
                        "not running, or its NATS_RPC_SUBJECT_PREFIX differs from this "
                        f"provider's rpc_prefix ({self._config.request_prefix})"
                    ) from exc
                except nats.errors.TimeoutError as exc:
                    if attempt == attempts:
                        raise ProviderError(
                            f"no reply on {subject} within {self._config.history_timeout:.0f}s "
                            f"after {attempts} attempt(s) (request {request_id})"
                        ) from exc
                    log.warning(
                        "History request %s on %s timed out (attempt %d of %d) — asking again",
                        request_id,
                        subject,
                        attempt,
                        attempts,
                    )
                    continue
                try:
                    return decode_history_reply(
                        reply.data, symbol, timeframe, self._config.schema_versions
                    )
                except IngesterPayloadError as exc:
                    raise ProviderError(f"unusable history reply on {subject}: {exc}") from exc
            raise AssertionError("unreachable: the loop returns or raises")  # pragma: no cover
        finally:
            try:
                await bus.close()
            except Exception:
                log.warning("Closing the history NATS connection failed", exc_info=True)


def _span(candles: list[Candle]) -> str:
    if not candles:
        return "no bars"
    return f"{candles[0].open_time.isoformat()}..{candles[-1].open_time.isoformat()}"
