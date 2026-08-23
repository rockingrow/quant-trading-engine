"""Timeframe parsing and candle-bucket arithmetic.

Every service must agree on *which* bucket a timestamp belongs to, otherwise
the resampler and the backtest replay disagree about what "the M15 candle that
just closed" means. That agreement lives here and nowhere else.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta

#: Timeframe labels QTE speaks, mapped to their length in seconds.
TIMEFRAME_SECONDS: dict[str, int] = {
    "M1": 60,
    "M5": 300,
    "M15": 900,
    "M30": 1800,
    "H1": 3600,
    "H4": 14400,
    "D1": 86400,
}

_ALIAS = re.compile(r"^(?P<num>\d+)(?P<unit>[mhdMHD])?$")


def normalize_timeframe(value: str) -> str:
    """Accept ``M15``, ``15``, ``15m``, ``1h`` … and return a canonical label.

    TradingView-style alerts and the broker contract both speak the bare-minute
    form (``"15"``), while strategies read better with ``M15``; this bridges
    the two without letting either spelling leak past the boundary.
    """
    text = str(value).strip()
    upper = text.upper()
    if upper in TIMEFRAME_SECONDS:
        return upper
    match = _ALIAS.fullmatch(text)
    if match:
        num = int(match.group("num"))
        unit = (match.group("unit") or "m").lower()
        seconds = num * {"m": 60, "h": 3600, "d": 86400}[unit]
        for label, label_seconds in TIMEFRAME_SECONDS.items():
            if label_seconds == seconds:
                return label
    raise ValueError(f"Unsupported timeframe: {value!r}")


def timeframe_seconds(value: str) -> int:
    return TIMEFRAME_SECONDS[normalize_timeframe(value)]


def to_broker_timeframe(value: str) -> str:
    """Render a timeframe the way the broker's webhook contract expects it.

    The broker stores ``timeframe`` as the bare TradingView string — ``"15"``
    for M15, ``"60"`` for H1 — so anything above minutes is expressed in
    minutes too.
    """
    return str(timeframe_seconds(value) // 60)


def floor_to_bucket(moment: datetime, timeframe: str) -> datetime:
    """Return the open time of the bucket *moment* falls into (UTC)."""
    seconds = timeframe_seconds(timeframe)
    moment = _as_utc(moment)
    epoch = int(moment.timestamp())
    return datetime.fromtimestamp(epoch - (epoch % seconds), tz=UTC)


def next_bucket(moment: datetime, timeframe: str) -> datetime:
    """Open time of the bucket that follows the one *moment* falls into."""
    return floor_to_bucket(moment, timeframe) + timedelta(seconds=timeframe_seconds(timeframe))


def _as_utc(moment: datetime) -> datetime:
    """Treat a naive datetime as UTC; convert an aware one into UTC."""
    if moment.tzinfo is None:
        return moment.replace(tzinfo=UTC)
    return moment.astimezone(UTC)
