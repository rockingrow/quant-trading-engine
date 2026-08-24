"""The simulator's wire format, defined once and used by both ends.

Two processes speak it: the ``qte-simulator`` server, which publishes ticks,
and :class:`~qte_shared.providers.simulator.feed.SimulatorLiveFeed`, which is
what ``data-ingestion`` connects with. Defining the frames here rather than in
either of them is what keeps a change from being applied to one side only.

It is JSON with named fields, not the positional arrays a real vendor tends to
use, for one reason: you are meant to be able to open the socket by hand.

    websocat ws://127.0.0.1:8901/stream
    {"op":"subscribe","symbols":["XAUUSD"]}

Frames, all carrying ``type`` (server → client) or ``op`` (client → server):

    ← {"type":"welcome","protocol":1,"path":"/stream"}
    → {"op":"subscribe","symbols":["XAUUSD","BTCUSDT"]}
    ← {"type":"subscribed","symbols":["XAUUSD","BTCUSDT"]}
    ← {"type":"tick","symbol":"XAUUSD","ts":"2026-08-24T09:00:00+00:00",
       "last":2412.5,"bid":null,"ask":null,"volume":1.25,"seq":41}

An unrecognised ``type`` is ignored rather than refused, so the server can grow
a frame without every client needing to know about it first.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from qte_shared.models import Tick

#: Bumped only when a frame changes shape in a way an old client misreads.
PROTOCOL_VERSION = 1

#: The feed. What ingestion connects to; ticks flow one way.
STREAM_PATH = "/stream"
#: The control plane. What the CLI connects to; commands in, acks out.
CONTROL_PATH = "/control"

DEFAULT_PORT = 8901


def encode_tick(tick: Tick, *, seq: int = 0) -> dict[str, Any]:
    """One tick as a stream frame. ``seq`` lets a client spot a gap."""
    return {
        "type": "tick",
        "symbol": tick.symbol,
        "ts": tick.ts.isoformat(),
        "bid": tick.bid,
        "ask": tick.ask,
        "last": tick.last,
        "volume": tick.volume,
        "seq": seq,
    }


def decode_tick(frame: Mapping[str, Any]) -> Tick | None:
    """Read a stream frame back into a :class:`~qte_shared.models.Tick`.

    Returns ``None`` for anything that is not a usable tick — a frame of
    another type, a missing symbol, an unparseable timestamp, a quote with no
    price on either side. The feed logs and skips those rather than dying: a
    simulator is a thing under development, and a malformed frame from it
    should be visible, not fatal.
    """
    if frame.get("type") != "tick":
        return None
    symbol = frame.get("symbol")
    moment = parse_timestamp(frame.get("ts"))
    if not symbol or moment is None:
        return None
    bid, ask, last = (_as_float(frame.get(key)) for key in ("bid", "ask", "last"))
    if bid is None and ask is None and last is None:
        return None
    return Tick(
        symbol=str(symbol).upper(),
        ts=moment,
        bid=bid,
        ask=ask,
        last=last,
        volume=_as_float(frame.get("volume")) or 0.0,
    )


def subscribe_frame(symbols: list[str]) -> dict[str, Any]:
    """What a feed client sends to name the symbols it wants.

    An empty list means "everything", which is what makes ``websocat`` plus a
    single connect enough to watch the whole simulator.
    """
    return {"op": "subscribe", "symbols": [symbol.upper() for symbol in symbols]}


def welcome_frame(path: str) -> dict[str, Any]:
    return {"type": "welcome", "protocol": PROTOCOL_VERSION, "path": path}


def error_frame(message: str, *, op: str | None = None) -> dict[str, Any]:
    return {"type": "error", "op": op, "message": message}


def parse_timestamp(value: Any) -> datetime | None:
    """ISO-8601 in, aware UTC out. A naive timestamp is read as UTC."""
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    try:
        moment = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return moment if moment.tzinfo else moment.replace(tzinfo=UTC)


def dumps(payload: Mapping[str, Any]) -> str:
    return json.dumps(payload, default=str, separators=(",", ":"))


def loads(raw: str | bytes) -> dict[str, Any]:
    """Parse a frame. Raises ``ValueError`` on anything that is not a JSON object."""
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError(f"expected a JSON object, got {type(parsed).__name__}")
    return parsed


def _as_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


__all__ = [
    "CONTROL_PATH",
    "DEFAULT_PORT",
    "PROTOCOL_VERSION",
    "STREAM_PATH",
    "decode_tick",
    "dumps",
    "encode_tick",
    "error_frame",
    "loads",
    "parse_timestamp",
    "subscribe_frame",
    "welcome_frame",
]
