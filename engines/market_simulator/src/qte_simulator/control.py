"""The control plane: one command in, one acknowledgement out.

Every command is answered, and the answer says what the simulator actually did
rather than that it accepted the request. ``bars`` returns the exact candles
ingestion is now expected to publish — resolved open times included — which is
what ``--verify`` compares against. Handing the client the expectation from the
server side matters: the client asked for "thirty bars, continuing from
wherever this symbol was", and only the server knows where that was.

Commands:

===========  ==================================================================
``status``   Attached feeds, tick counts, running generators, per-symbol cursors
``tick``     One tick, exactly as given
``bars``     A run of bars, expanded into ticks; the workhorse
``walk``     A background random walk in real time, until stopped
``stop``     Cancel one generator or all of them
``reset``    Forget cursors and counters (attached feeds stay attached)
===========  ==================================================================

Errors are returned, never raised: a malformed command from a CLI typo must not
take down a server that three other terminals are attached to.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from qte_shared.logging_setup import get_logger
from qte_shared.models import Tick
from qte_shared.providers.simulator.protocol import parse_timestamp
from qte_shared.timeframes import floor_to_bucket, normalize_timeframe

from qte_simulator.bars import (
    BarError,
    BarSpec,
    anchor_open_times,
    bar_ticks,
    expected_candle,
    reference_price,
    seal_tick,
)
from qte_simulator.hub import SimulatorHub

log = get_logger(__name__)

#: A replay longer than this is almost certainly a typo, and the acknowledgement
#: carrying one expected candle per bar would be megabytes of it.
MAX_BARS = 5000


class CommandError(Exception):
    """A command the simulator will not run, with the reason a human needs."""


async def dispatch(hub: SimulatorHub, command: Mapping[str, Any]) -> dict[str, Any]:
    """Run one control command against *hub* and return its result payload."""
    op = str(command.get("op") or "").lower()
    handler = _HANDLERS.get(op)
    if handler is None:
        raise CommandError(f"Unknown command {op!r}. Known: {', '.join(sorted(_HANDLERS))}")
    return await handler(hub, command)


# ── status / reset / stop ─────────────────────────────────────────────────


async def _status(hub: SimulatorHub, _command: Mapping[str, Any]) -> dict[str, Any]:
    return hub.status()


async def _reset(hub: SimulatorHub, _command: Mapping[str, Any]) -> dict[str, Any]:
    stopped = await hub.stop_generators()
    hub.reset()
    return {"reset": True, "stopped": stopped}


async def _stop(hub: SimulatorHub, command: Mapping[str, Any]) -> dict[str, Any]:
    name = command.get("name")
    return {"stopped": await hub.stop_generators(str(name) if name else None)}


# ── tick ──────────────────────────────────────────────────────────────────


async def _tick(hub: SimulatorHub, command: Mapping[str, Any]) -> dict[str, Any]:
    symbol = _symbol(command)
    bid, ask, last = (_optional_float(command, key) for key in ("bid", "ask", "last"))
    if bid is None and ask is None and last is None:
        raise CommandError("a tick needs at least one of bid, ask or last")

    tick = Tick(
        symbol=symbol,
        ts=parse_timestamp(command.get("ts")) or _next_moment(hub, symbol),
        bid=bid,
        ask=ask,
        last=last,
        volume=_optional_float(command, "volume") or 0.0,
    )
    delivered = await hub.publish(tick)
    return {
        "tick": tick.model_dump(mode="json"),
        "price": tick.price,
        "delivered": delivered,
    }


def _next_moment(hub: SimulatorHub, symbol: str) -> datetime:
    """When an unstamped tick happens: now, unless the series is already ahead.

    A forward-anchored replay leaves the series days in the future, and a tick
    stamped with the wall clock after one of those is behind the bar the
    resampler holds open — dropped as late. Taking the later of the two costs
    nothing when there is no history and is the only usable answer when there
    is.
    """
    now = datetime.now(UTC)
    last = hub.last_tick_ts.get(symbol)
    if last is None:
        return now
    return max(now, last + timedelta(milliseconds=1))


# ── bars ──────────────────────────────────────────────────────────────────


async def _bars(hub: SimulatorHub, command: Mapping[str, Any]) -> dict[str, Any]:
    """Expand a run of bars into ticks and play them in order.

    The run is always contiguous: bar *n+1* opens in the bucket after bar *n*.
    A feed cannot produce a gap — the print that closes one bucket and the
    print that opens the next are consecutive — so accepting a gapped run would
    be accepting data ingestion could never have received.
    """
    symbol = _symbol(command)
    timeframe = normalize_timeframe(str(command.get("timeframe") or "M15"))
    rows = command.get("bars")
    if not isinstance(rows, list) or not rows:
        raise CommandError("`bars` must be a non-empty list")
    if len(rows) > MAX_BARS:
        raise CommandError(f"{len(rows)} bars exceeds the {MAX_BARS}-bar limit for one command")

    open_times = _resolve_open_times(hub, symbol, timeframe, len(rows), command)
    spread = _optional_float(command, "spread") or 0.0
    rate = _optional_float(command, "rate") or 0.0
    seal = bool(command.get("seal", True))
    interval = 1.0 / rate if rate > 0 else 0.0

    try:
        bars = [
            BarSpec(
                symbol=symbol,
                timeframe=timeframe,
                open_time=open_time,
                open=_required_float(row, "open"),
                high=_required_float(row, "high"),
                low=_required_float(row, "low"),
                close=_required_float(row, "close"),
                volume=_optional_float(row, "volume") or 0.0,
            )
            for open_time, row in zip(open_times, rows, strict=True)
        ]
    except BarError as exc:
        raise CommandError(str(exc)) from exc

    sent = 0
    delivered = 0
    for index, bar in enumerate(bars):
        if interval and index:
            await asyncio.sleep(interval)
        ticks = bar_ticks(bar, spread=spread)
        delivered = await hub.publish_all(ticks)
        sent += len(ticks)
        hub.bars_sent += 1
        hub.advance_cursor(symbol, timeframe, bar.open_time)

    if seal:
        last = bars[-1]
        delivered = await hub.publish(seal_tick(last))
        sent += 1
        # The seal has opened a bar in the following bucket. Move the cursor
        # onto it so the next `anchor=next` command starts *after* it rather
        # than inheriting the seal's price as its open.
        hub.advance_cursor(symbol, timeframe, last.close_time)

    log.info(
        "Played %d %s %s bars (%d ticks) from %s to %s%s",
        len(bars),
        symbol,
        timeframe,
        sent,
        bars[0].open_time.isoformat(),
        bars[-1].open_time.isoformat(),
        " + seal" if seal else "",
    )
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "bars": len(bars),
        "ticks": sent,
        "sealed": seal,
        "delivered": delivered,
        "expected": [expected_candle(bar).model_dump(mode="json") for bar in bars],
    }


def _resolve_open_times(
    hub: SimulatorHub,
    symbol: str,
    timeframe: str,
    count: int,
    command: Mapping[str, Any],
) -> list[datetime]:
    """Turn the requested anchor into concrete bucket open times.

    ``anchor`` is ``"past"``, ``"next"``, or an ISO-8601 timestamp for the
    first bar. See :func:`~qte_simulator.bars.anchor_open_times` for what the
    two named modes buy and cost.
    """
    anchor = str(command.get("anchor") or "past")
    if anchor not in ("past", "next"):
        start = parse_timestamp(anchor)
        if start is None:
            raise CommandError(
                f"anchor must be 'past', 'next' or an ISO-8601 timestamp, got {anchor!r}"
            )
        return anchor_open_times(count, timeframe, mode="next", cursor=None, now=start)
    try:
        return anchor_open_times(
            count,
            timeframe,
            mode=anchor,
            cursor=_series_end(hub, symbol, timeframe) if anchor == "next" else None,
        )
    except BarError as exc:
        raise CommandError(str(exc)) from exc


def _series_end(hub: SimulatorHub, symbol: str, timeframe: str) -> datetime | None:
    """The last bucket anything has been sent into, bars and loose ticks alike.

    A bare `tick` does not move the bar cursor — it is not a bar — but it does
    open a bucket in the resampler, and a bar placed in that same bucket would
    inherit the tick's price as its open. So "next" is the first bucket nothing
    has touched, not merely the one after the last bar.
    """
    cursor = hub.cursor(symbol, timeframe)
    last_tick = hub.last_tick_ts.get(symbol)
    if last_tick is None:
        return cursor
    from_tick = floor_to_bucket(last_tick, timeframe)
    return from_tick if cursor is None or from_tick > cursor else cursor


# ── walk ──────────────────────────────────────────────────────────────────


async def _walk(hub: SimulatorHub, command: Mapping[str, Any]) -> dict[str, Any]:
    """Start a random walk in the background and return immediately.

    One clock rule: each tick is stamped with the later of the wall clock and
    where the series has got to. At ``speed=1`` on a fresh simulator that is
    the wall clock, so bars close on ingestion's wall-clock flush exactly as
    they would on a live feed. After a forward-anchored replay it is the
    series — a wall-clock tick behind the bar the resampler holds open would be
    discarded as late, loudly but discarded.

    ``speed`` is how many seconds of market time pass per second of real time,
    which is how an M15 bar closes without waiting fifteen minutes: ``--speed
    60`` closes one every quarter-minute.
    """
    import random

    symbol = _symbol(command)
    count = int(command.get("ticks") or 0)
    # `x or default` would rewrite an explicit zero into the default, which
    # turns "--rate 0" from a refusal into a surprise.
    rate = _defaulted(command, "rate", 1.0, positive=True)
    speed = _defaulted(command, "speed", 1.0, positive=True)
    # Continue from wherever the series is, exactly as a replay does — a walk
    # that restarted at a reference price would put a step in the bar it joins.
    resume = hub.last_prices.get(symbol) or reference_price(symbol)
    price = _defaulted(command, "price", resume, positive=True)
    volatility = _defaulted(command, "volatility", 0.0005)
    spread = _defaulted(command, "spread", 0.0)
    seed = command.get("seed")
    name = f"walk:{symbol}"

    await hub.stop_generators(name)
    rng = random.Random(None if seed is None else int(seed))

    step = timedelta(seconds=speed / rate)
    last = hub.last_tick_ts.get(symbol)
    start = max(datetime.now(UTC), last + step) if last else datetime.now(UTC)

    async def run() -> None:
        current = price
        moment = start
        emitted = 0
        half = spread / 2 if spread > 0 else None
        while count <= 0 or emitted < count:
            current = max(current + rng.gauss(0, volatility) * current, current * 0.5)
            await hub.publish(
                Tick(
                    symbol=symbol,
                    ts=moment,
                    last=round(current, 6),
                    bid=None if half is None else round(current - half, 6),
                    ask=None if half is None else round(current + half, 6),
                    volume=round(rng.uniform(0.1, 3.0), 3),
                )
            )
            emitted += 1
            # The later of the two, every tick: `asyncio.sleep` drifts a little
            # slower than the wall, and a walk left running for an hour at
            # speed 1 would otherwise fall behind it and start closing bars late.
            moment = max(datetime.now(UTC), moment + step)
            await asyncio.sleep(1.0 / rate)
        log.info("Walk finished symbol=%s ticks=%d", symbol, emitted)

    hub.register_generator(name, asyncio.create_task(run(), name=name))
    return {
        "generator": name,
        "symbol": symbol,
        "ticks": count or "unbounded",
        "rate": rate,
        "start_price": price,
        "speed": speed,
        "starts_at": start.isoformat(),
    }


# ── Argument helpers ──────────────────────────────────────────────────────


def _symbol(command: Mapping[str, Any]) -> str:
    symbol = str(command.get("symbol") or "").strip().upper()
    if not symbol:
        raise CommandError("`symbol` is required")
    return symbol


def _required_float(row: Mapping[str, Any], key: str) -> float:
    value = _optional_float(row, key)
    if value is None:
        raise CommandError(f"each bar needs `{key}`")
    return value


def _defaulted(
    command: Mapping[str, Any], key: str, default: float, *, positive: bool = False
) -> float:
    """Read a number, falling back only when it is genuinely absent."""
    value = _optional_float(command, key)
    value = default if value is None else value
    if positive and value <= 0:
        raise CommandError(f"`{key}` must be greater than zero, got {value}")
    return value


def _optional_float(row: Mapping[str, Any], key: str) -> float | None:
    value = row.get(key)
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise CommandError(f"`{key}` must be a number, got {value!r}") from exc


_HANDLERS = {
    "status": _status,
    "tick": _tick,
    "bars": _bars,
    "walk": _walk,
    "stop": _stop,
    "reset": _reset,
}


__all__ = ["MAX_BARS", "CommandError", "dispatch"]
