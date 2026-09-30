"""The ingester's wire format, read into QTE's own :class:`~qte_shared.models.Candle`.

``algo-trading-ingester`` publishes one ``BarClosedEvent`` per completed bar on
``<prefix>.bar.closed.<gateway>.<symbol>.<timeframe>``. Its schema lives in that
repository (``ingester/schemas/market_event_schema.py``, sample in
``examples/nats/bar.closed.mt5.json``); this module is the only place in QTE
that knows its shape::

    {
      "schema_version": "2.0",
      "event_id": "mt5:XAUUSD:M15:1790589600",
      "event_type": "bar.closed",
      "source": {"gateway": "mt5", "market": "forex", ...},
      "emitted_at": "2026-09-28T10:15:00.412000Z",
      "symbol": "XAUUSD",
      "timeframe": "M15",
      "bar": {"open_time": "2026-09-28T10:00:00Z", "close_time": "...",
              "open": 2651.12, "high": 2654.8, "low": 2649.95, "close": 2653.4,
              "volume": 1843.0, "tick_count": 1843, "quote_volume": null,
              "spread": 12.0}
    }

Only the major version is checked. The ingester bumps it on a breaking change,
and a payload this code would misread is refused rather than half-understood;
an added field under the same major is ignored.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from pydantic import ValidationError

from qte_shared.models import Candle
from qte_shared.timeframes import normalize_timeframe, timeframe_seconds

#: The ingester's ``SCHEMA_VERSION`` major this decoder understands.
SUPPORTED_SCHEMA_MAJOR = "2"

#: The ingester's ``EventTypeEnum.BAR_CLOSED``.
BAR_CLOSED_EVENT = "bar.closed"


class IngesterPayloadError(ValueError):
    """A message on the ingester's subject that is not a bar QTE can use."""


@dataclass(frozen=True, slots=True)
class IngestedBar:
    """One decoded bar, with the ingester's id kept for the logs."""

    event_id: str
    candle: Candle


def decode_bar_closed(raw: bytes | str) -> IngestedBar:
    """Read one ``bar.closed`` message; raise :class:`IngesterPayloadError` otherwise."""
    try:
        document = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise IngesterPayloadError(f"not JSON: {exc}") from exc
    if not isinstance(document, dict):
        raise IngesterPayloadError(f"expected a JSON object, got {type(document).__name__}")

    version = str(document.get("schema_version", ""))
    if version.split(".")[0] != SUPPORTED_SCHEMA_MAJOR:
        raise IngesterPayloadError(
            f"schema_version {version!r} is not {SUPPORTED_SCHEMA_MAJOR}.x — "
            "the ingester's contract changed; update qte_shared.providers.mt5.protocol"
        )
    if document.get("event_type") != BAR_CLOSED_EVENT:
        raise IngesterPayloadError(f"event_type {document.get('event_type')!r} is not a bar")

    symbol = document.get("symbol")
    bar = document.get("bar")
    if not isinstance(symbol, str) or not symbol.strip():
        raise IngesterPayloadError("missing symbol")
    if not isinstance(bar, dict):
        raise IngesterPayloadError("missing bar")
    try:
        timeframe = normalize_timeframe(str(document.get("timeframe", "")))
    except ValueError as exc:
        raise IngesterPayloadError(str(exc)) from exc

    open_time = _utc_timestamp(bar.get("open_time"), "open_time")
    close_time = _utc_timestamp(bar.get("close_time"), "close_time")
    if close_time - open_time != timedelta(seconds=timeframe_seconds(timeframe)):
        raise IngesterPayloadError(
            f"bar spans {open_time.isoformat()} → {close_time.isoformat()}, "
            f"which is not one {timeframe} bucket"
        )

    try:
        candle = Candle(
            symbol=symbol.strip().upper(),
            timeframe=timeframe,
            open_time=open_time,
            open=bar.get("open"),
            high=bar.get("high"),
            low=bar.get("low"),
            close=bar.get("close"),
            # MT5 forex reports tick volume here; "unknown" arrives as null.
            volume=bar.get("volume") or 0.0,
            tick_count=bar.get("tick_count") or 0,
            is_closed=True,
        )
    except ValidationError as exc:
        raise IngesterPayloadError(f"unusable bar: {exc.errors()[0]['msg']}") from exc
    return IngestedBar(event_id=str(document.get("event_id", "")), candle=candle)


def _utc_timestamp(value: Any, field_name: str) -> datetime:
    """ISO-8601 in, aware UTC out. A naive time is refused, never guessed."""
    if not isinstance(value, str) or not value:
        raise IngesterPayloadError(f"missing bar.{field_name}")
    try:
        moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise IngesterPayloadError(f"bar.{field_name} {value!r} is not ISO-8601") from exc
    if moment.tzinfo is None:
        raise IngesterPayloadError(f"bar.{field_name} {value!r} carries no timezone")
    return moment.astimezone(UTC)
