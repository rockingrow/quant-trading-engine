"""The ingester's wire format, read into QTE's own :class:`~qte_shared.models.Candle`.

``algo-trading-ingester`` publishes one ``BarClosedEvent`` per completed bar on
``<prefix>.bar.closed.<gateway>.<symbol>.<timeframe>``. Its schema lives in that
repository (``ingester/schemas/market_event_schema.py``); a sample of what QTE
reads is kept here in ``examples/nats/bar.closed.mt5.json``, and this module is
the only place in QTE that knows its shape::

    {
      "schema_version": "1.0.0",
      "event_id": "mt5:XAUUSD:M15:1790589600",
      "event_type": "bar.closed",
      "source": {"gateway": "mt5", "market": "forex", ...},
      "emitted_at": "2026-09-28T10:15:00.412000Z",
      "warmup_bar": false,
      "symbol": "XAUUSD",
      "timeframe": "M15",
      "bar": {"open_time": "2026-09-28T10:00:00Z", "close_time": "...",
              "open": 2651.12, "high": 2654.8, "low": 2649.95, "close": 2653.4,
              "volume": 1843.0, "tick_count": 1843, "quote_volume": null,
              "spread": 12.0}
    }

**``warmup_bar`` splits the stream in two.** False or absent is a live close:
the bar the vendor has just finished, which ingestion stages and publishes. True
marks a bar the ingester is replaying to fill QTE's indicator window, which is
history and not an event — it carries no decision and reaches Redis by a merge
rather than by the forward-only staging path. A warmup bar must also say where
it sits in its batch (``warmup_index``, 1-based, and ``warmup_total``, both per
symbol and timeframe), because ingestion buffers the batch and writes it in one
transaction; a batch of 150 merged one bar at a time would rewrite the whole
stored window 150 times. A ``warmup_bar`` of true without that pair is refused
rather than read as a batch of one, which is exactly the cost it avoids.

Which ``schema_version`` values are read is **configuration, not a constant**:
each gateway lists them in the ``[provider]`` table of its own plan
(``schema_versions``, :class:`~qte_shared.providers.mt5.settings.Mt5Settings`),
so one QTE can read two ingesters while a fleet is upgraded, and a rollout is a
config edit rather than a release.

Every entry is a full ``major.minor.patch`` version and matches that version
and no other — which is why it is a list: reading ``1.0.0`` and ``1.1.0`` at
once means naming both. The version on the wire is the ingester's
``ContractSettings.VERSION`` (``ingester/settings.py``, ``SCHEMA_VERSION`` in
its environment, ``1.0.0`` by default), and it is read as semver: a publisher
that leaves the tail off sends ``"1.0"`` for the same version, which the entry
``"1.0.0"`` takes. A version nobody listed is refused rather than
half-understood, because a payload this code would misread is worse than a
missing bar — and the warning names both sides, so the fix is one config edit.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from pydantic import ValidationError

from qte_shared.interfaces.market_data import WarmupBatch
from qte_shared.models import Candle
from qte_shared.timeframes import normalize_timeframe, timeframe_seconds

#: What a gateway accepts when its plan says nothing: the ingester's own
#: default (``ContractSettings.VERSION``), written in full.
#:
#: The warm-up fields arrived *within* ``1.0.0`` rather than as a new version,
#: because they are additive: a decoder that does not read them sees the same
#: bars it always did. Nothing speculative is listed here — an accept-list is
#: the set of shapes this decoder has actually been checked against, so a future
#: version earns its entry by being read, not by being anticipated.
DEFAULT_SCHEMA_VERSIONS: tuple[str, ...] = ("1.0.0",)

#: A configured entry: ``major.minor.patch``, nothing shortened.
_VERSION_SPEC = re.compile(r"\d+\.\d+\.\d+")

#: A version on the wire, where a publisher may leave the tail off (``"1.0"``).
_WIRE_VERSION_SPEC = re.compile(r"\d+(?:\.\d+){0,2}")

#: The ingester's ``EventTypeEnum.BAR_CLOSED``.
BAR_CLOSED_EVENT = "bar.closed"


class IngesterPayloadError(ValueError):
    """A message on the ingester's subject that is not a bar QTE can use."""


@dataclass(frozen=True, slots=True)
class IngestedBar:
    """One decoded bar, with the ingester's id kept for the logs.

    *warmup* is ``None`` for a live close and carries the batch position for a
    replayed one, which is what routes the bar to the merge path instead of to
    staging.
    """

    event_id: str
    candle: Candle
    warmup: WarmupBatch | None = None


def normalize_schema_versions(values: Iterable[str]) -> tuple[str, ...]:
    """Clean one gateway's accept-list: stripped, deduplicated, shape-checked.

    Every entry must be a full ``major.minor.patch``. A shortened one is a
    configuration error rather than a guess, because ``"1.0"`` could mean the
    one version or the whole minor and the two differ by every later release.

    Raises :class:`ValueError`, so a gateway's settings class can hand its
    ``[provider].schema_versions`` straight here and a typo surfaces as a
    configuration error at start-up instead of as a feed that refuses every bar.
    """
    accepted: list[str] = []
    for value in values:
        wanted = str(value).strip()
        if not wanted:
            continue
        if not _VERSION_SPEC.fullmatch(wanted):
            raise ValueError(
                f"schema version {wanted!r} is not a major.minor.patch version — "
                "write it in full, as '1.0.0', and list every version to accept"
            )
        if wanted not in accepted:
            accepted.append(wanted)
    if not accepted:
        raise ValueError("schema_versions needs at least one accepted version")
    return tuple(accepted)


def accepts_schema_version(version: str, accepted: Sequence[str]) -> bool:
    """Whether *version* is one of *accepted*, read as semver.

    A publisher may leave the tail off — the ingester's own
    ``examples/nats/bar.closed.mt5.json`` still shows ``"1.0"`` — so a missing
    component reads as ``0`` and ``"1.0"`` matches the entry ``"1.0.0"``.
    Nothing else matches: an accept-list is the exact set of payload shapes this
    decoder has been checked against.
    """
    if not _WIRE_VERSION_SPEC.fullmatch(version):
        return False
    return _full_version(version) in accepted


def _full_version(version: str) -> str:
    """``"1"`` and ``"1.0"`` both as ``"1.0.0"`` — semver's own default."""
    parts = version.split(".")
    return ".".join(parts + ["0"] * (3 - len(parts)))


def _warmup_batch(document: dict[str, Any]) -> WarmupBatch | None:
    """The batch position of a replayed bar, or ``None`` for a live close.

    An ingester that never replays omits ``warmup_bar`` entirely, which is why
    a missing field reads as a live bar rather than as an error: that is the
    whole ``1.0.0`` fleet.
    """
    marker = document.get("warmup_bar", False)
    if marker is None or marker is False:
        return None
    if marker is not True:
        raise IngesterPayloadError(
            f"warmup_bar must be a JSON boolean, got {type(marker).__name__} {marker!r}"
        )
    position = _warmup_count(document, "warmup_index")
    total = _warmup_count(document, "warmup_total")
    if not 1 <= position <= total:
        raise IngesterPayloadError(
            f"warmup_index {position} is outside its batch of {total} — the index is "
            "1-based and never past the total"
        )
    return WarmupBatch(index=position, total=total)


def _warmup_count(document: dict[str, Any], field_name: str) -> int:
    """One of the batch counters, refused rather than guessed when unusable.

    A warmup bar with no batch position would have to be written on its own, and
    a batch arriving as 150 single-bar writes rewrites the stored window 150
    times — the cost the batch counters exist to avoid. So this is an error, not
    a default.
    """
    value = document.get(field_name)
    if value is None:
        raise IngesterPayloadError(
            f"warmup_bar is true but {field_name} is missing — a replayed bar has to say "
            "where it sits in its batch, so ingestion knows when the batch is whole"
        )
    # `bool` is an `int` in Python, and `True` as a count is a publisher bug.
    if isinstance(value, bool) or not isinstance(value, int):
        raise IngesterPayloadError(
            f"{field_name} must be a whole number, got {type(value).__name__} {value!r}"
        )
    if value < 1:
        raise IngesterPayloadError(f"{field_name} must be at least 1, got {value}")
    return value


def decode_bar_closed(
    raw: bytes | str, schema_versions: Sequence[str] = DEFAULT_SCHEMA_VERSIONS
) -> IngestedBar:
    """Read one ``bar.closed`` message; raise :class:`IngesterPayloadError` otherwise.

    *schema_versions* is the calling gateway's accept-list, normally straight
    from its ``[provider]`` table.
    """
    try:
        document = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise IngesterPayloadError(f"not JSON: {exc}") from exc
    if not isinstance(document, dict):
        raise IngesterPayloadError(f"expected a JSON object, got {type(document).__name__}")

    version = str(document.get("schema_version", ""))
    if not accepts_schema_version(version, schema_versions):
        raise IngesterPayloadError(
            f"schema_version {version!r} is not accepted "
            f"(configured: {', '.join(schema_versions) or 'none'}) — compare the ingester's "
            "SCHEMA_VERSION, then add it in full to [provider].schema_versions once "
            "qte_shared.providers.mt5.protocol can read that shape"
        )
    if document.get("event_type") != BAR_CLOSED_EVENT:
        raise IngesterPayloadError(f"event_type {document.get('event_type')!r} is not a bar")

    warmup = _warmup_batch(document)
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
    return IngestedBar(event_id=str(document.get("event_id", "")), candle=candle, warmup=warmup)


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
