"""Timeframe parsing and candle-bucket arithmetic.

A strategy that folds a higher timeframe out of its own bar stream has to agree
with the engine about *which* bucket a timestamp falls into, or its H4 EMA
closes one bar away from the engine's and the port stops matching the chart.
The rule is simple enough to restate exactly: floor the epoch second to a
multiple of the timeframe length, in UTC.
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

    Pine writes timeframes as bare minute strings (``"240"`` for H4) and so
    does the broker contract, while strategy code reads better with ``H4``.
    Both spellings arrive here and only one leaves.
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


def floor_to_bucket(moment: datetime, timeframe: str) -> datetime:
    """Return the open time of the bucket *moment* falls into (UTC)."""
    seconds = timeframe_seconds(timeframe)
    moment = as_utc(moment)
    epoch = int(moment.timestamp())
    return datetime.fromtimestamp(epoch - (epoch % seconds), tz=UTC)


def next_bucket(moment: datetime, timeframe: str) -> datetime:
    """Open time of the bucket that follows the one *moment* falls into."""
    return floor_to_bucket(moment, timeframe) + timedelta(seconds=timeframe_seconds(timeframe))


def as_utc(moment: datetime) -> datetime:
    """Treat a naive datetime as UTC; convert an aware one into UTC."""
    if moment.tzinfo is None:
        return moment.replace(tzinfo=UTC)
    return moment.astimezone(UTC)
