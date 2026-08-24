"""Watching the far end of the pipeline, so "it worked" is a check and not a hope.

The simulator can say what it sent. What matters is what came out: whether
ingestion rebuilt the bar it was given, and whether the strategy runner turned
that bar into a signal. Both are events on QTE's own NATS subjects, so the
verifier is a plain subscriber — it neither reaches into a service nor asks one
to behave differently while being tested.

    QTE.candle.closed.<symbol>.<tf>   ← did ingestion rebuild the bar?
    QTE.signal.emitted                ← did the strategy act on it?

The subscription is opened **before** anything is sent and every message is
kept, because a fast replay produces its candles while the send is still in
flight. A verifier that only started listening once the send returned would
race its own data.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from nats.aio.msg import Msg
from qte_shared.bus import NatsBus, Subjects
from qte_shared.logging_setup import get_logger
from qte_shared.models import Candle, CandleClosedEvent

log = get_logger(__name__)

#: Prices and volumes travel as JSON floats and volume is re-accumulated tick
#: by tick, so the last bit can differ. Anything a trader would notice is many
#: orders of magnitude above this.
TOLERANCE = 1e-9


@dataclass(slots=True)
class CandleCheck:
    """One expected candle, what actually arrived, and where they differ."""

    expected: Candle
    actual: Candle | None = None
    mismatches: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.actual is not None and not self.mismatches

    @property
    def verdict(self) -> str:
        if self.actual is None:
            return "MISSING"
        return "OK" if not self.mismatches else "MISMATCH"


def compare(expected: Candle, actual: Candle | None) -> CandleCheck:
    """Field-by-field diff of the candle that was asked for against the one that came."""
    check = CandleCheck(expected=expected, actual=actual)
    if actual is None:
        return check
    for name in ("open", "high", "low", "close", "volume"):
        want, got = getattr(expected, name), getattr(actual, name)
        if not math.isclose(want, got, rel_tol=TOLERANCE, abs_tol=TOLERANCE):
            check.mismatches.append(f"{name}: expected {want}, got {got}")
    if expected.tick_count != actual.tick_count:
        check.mismatches.append(
            f"tick_count: expected {expected.tick_count}, got {actual.tick_count}"
        )
    if not actual.is_closed:
        check.mismatches.append("is_closed: the candle arrived still open")
    return check


class FlowWatcher:
    """Collects candle closes and emitted signals off NATS while a test runs."""

    def __init__(self, symbol: str, timeframe: str, bus: NatsBus | None = None) -> None:
        self.symbol = symbol.upper()
        self.timeframe = timeframe
        self.bus = bus or NatsBus(name="qte-simulator-verify")
        self.subjects = Subjects()
        self.candles: dict[datetime, Candle] = {}
        self.signals: list[dict[str, Any]] = []
        self._arrived: Any = None

    async def start(self) -> None:
        import asyncio

        self._arrived = asyncio.Event()
        await self.bus.connect()
        await self.bus.subscribe(
            self.subjects.candle_closed(self.symbol, self.timeframe), self._on_candle
        )
        await self.bus.subscribe(self.subjects.signal_emitted(), self._on_signal)

    async def stop(self) -> None:
        await self.bus.close()

    async def _on_candle(self, msg: Msg) -> None:
        event = CandleClosedEvent.model_validate_json(msg.data)
        # Last one wins: if a candle for this bucket is published twice, the
        # later publish is what the strategy runner acted on.
        self.candles[event.candle.open_time] = event.candle
        self._arrived.set()

    async def _on_signal(self, msg: Msg) -> None:
        import json

        payload = json.loads(msg.data)
        signal = payload.get("signal") or {}
        if str(signal.get("symbol", "")).upper() == self.symbol:
            self.signals.append(payload)
            self._arrived.set()

    # ── Waiting ───────────────────────────────────────────────────────

    async def wait_for_candles(
        self, expected: list[Candle], timeout: float = 20.0
    ) -> list[CandleCheck]:
        """Wait until every expected candle has arrived, or *timeout* expires.

        Returns a check per expected candle either way — a partial result is
        the diagnosis (which bar the pipeline stopped at), so a timeout is
        reported rather than raised.
        """
        await self._wait_until(
            lambda: all(candle.open_time in self.candles for candle in expected), timeout
        )
        return [compare(candle, self.candles.get(candle.open_time)) for candle in expected]

    async def wait_for_signal(self, timeout: float = 20.0) -> list[dict[str, Any]]:
        """Wait for at least one signal on this symbol; return everything seen."""
        await self._wait_until(lambda: bool(self.signals), timeout)
        return list(self.signals)

    async def _wait_until(self, predicate, timeout: float) -> None:
        import asyncio

        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while not predicate():
            remaining = deadline - loop.time()
            if remaining <= 0:
                return
            self._arrived.clear()
            try:
                await asyncio.wait_for(self._arrived.wait(), timeout=remaining)
            except TimeoutError:
                return


__all__ = ["TOLERANCE", "CandleCheck", "FlowWatcher", "compare"]
