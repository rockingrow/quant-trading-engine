"""The MT5 provider's configuration block (prefix ``QTE_MT5__``).

The MT5 terminal itself is not QTE's to reach: ``algo-trading-ingester`` runs
beside it on a Windows host, detects closed bars and publishes them to NATS.
So what this block describes is *where that ingester publishes* — the server,
the subject prefix, the JetStream stream — and how QTE consumes from it.

Three places can set a field here, highest first: the environment
(``QTE_MT5__STREAM=...``, for a one-run override), the ``[provider]`` table of
``config/data_providers/mt5.toml``, and the defaults below. The defaults mirror the ingester's
own ``.env`` (``NATS_SUBJECT_PREFIX=INGESTER``, ``NATS_STREAM_NAME=INGESTER``,
``NATS_JETSTREAM_ENABLED=true``), so the two sides agree out of the box.

The NATS **token is never read from the plan** — a credential-shaped key in
``[provider]`` is dropped with a warning (:mod:`qte_shared.market_data_plan`).
It comes from ``QTE_MT5__NATS_TOKEN`` in ``.env``, or, left blank, from QTE's
own ``QTE_NATS__TOKEN``: the ingester normally publishes onto QTE's bus, and one
server should not need its token written down twice.
"""

from __future__ import annotations

import re
from typing import Annotated, ClassVar, Literal

from pydantic import Field, field_validator
from pydantic_settings import NoDecode, SettingsConfigDict

from qte_shared.config import settings
from qte_shared.interfaces.market_data import ProviderSettings
from qte_shared.providers.mt5.protocol import (
    DEFAULT_SCHEMA_VERSIONS,
    normalize_schema_versions,
)

#: A JetStream consumer name may not contain ``.``, ``*``, ``>`` or whitespace.
_UNSAFE_CONSUMER_CHARS = re.compile(r"[^A-Za-z0-9_-]")


class Mt5Settings(ProviderSettings):
    """Where the ingester publishes closed bars, and how QTE consumes them."""

    model_config = SettingsConfigDict(env_prefix="QTE_MT5__", extra="ignore")
    provider_name: ClassVar[str] = "mt5"

    #: The NATS server the ingester publishes to. Blank = QTE's own bus
    #: (``QTE_NATS__URL``), which is where the ingester points by default.
    nats_url: str = ""
    #: Its token. Blank = ``QTE_NATS__TOKEN``. Environment only, never the plan.
    nats_token: str = Field(default="", repr=False)
    #: The ingester's ``NATS_SUBJECT_PREFIX``: subjects read
    #: ``<prefix>.bar.closed.<gateway>.<symbol>.<timeframe>``.
    subject_prefix: str = "INGESTER"
    #: The ingester gateway whose bars this provider takes.
    gateway: str = "mt5"
    #: Which of the ingester's ``schema_version`` values this gateway decodes.
    #: Each is a full ``major.minor.patch`` and matches that version alone, so
    #: reading two (``["1.0.0", "2.0.0"]``) is how one QTE follows a fleet
    #: through an upgrade. The ingester's shorter ``"1.0"`` on the wire reads as
    #: ``1.0.0``. Bars on any other version are refused with their version in
    #: the log, never decoded on a guess.
    #: ``NoDecode``: the environment hands this over as ``1,2``, not as JSON.
    schema_versions: Annotated[tuple[str, ...], NoDecode] = DEFAULT_SCHEMA_VERSIONS
    #: Mirror of the ingester's ``NATS_JETSTREAM_ENABLED``. JetStream lets a QTE
    #: that was down replay what it missed; core NATS drops it.
    jetstream: bool = True
    #: The ingester's ``NATS_STREAM_NAME``. The ingester owns and creates it —
    #: QTE never does, so a mismatch is reported rather than papered over.
    stream: str = "INGESTER"
    #: The durable consumer QTE reads the stream through. Blank = one per state
    #: namespace (``qte-ingestion-<env>-<mode>-mt5``), so a shadow and a live
    #: stack on one server each receive every bar instead of splitting them.
    durable_name: str = ""
    #: Where a *new* durable consumer starts. ``all`` replays what the stream
    #: still holds (the ingester keeps 7 days), which is this provider's only
    #: history: it warms Redis on the first start. Bars too old to decide on are
    #: kept as history by the runner (``QTE_RUNNER__CATCH_UP_MAX_AGE``). An
    #: existing consumer keeps its position whatever this says.
    deliver_policy: Literal["all", "new", "last_per_subject"] = "all"
    #: Messages asked for in one pull. Bars are small and rare; this only
    #: matters on the first replay.
    fetch_batch: int = Field(default=64, ge=1)
    #: Seconds one pull waits for a message before asking again.
    fetch_timeout: float = Field(default=5.0, gt=0)
    #: Bars held between receipt and processing. Every received bar is acked at
    #: once, so this bounds memory, not delivery: when it is full the stream is
    #: simply not pulled from until there is room.
    max_pending_bars: int = Field(default=10_000, ge=1)
    #: Cap on the reconnect backoff when the server or the stream is missing.
    max_backoff_seconds: float = Field(default=30.0, gt=0)

    @field_validator("schema_versions", mode="before")
    @classmethod
    def _read_schema_versions(cls, value: object) -> object:
        """A TOML list, or ``QTE_MT5__SCHEMA_VERSIONS=1.0.0,2.0.0`` for one run."""
        if isinstance(value, str):
            value = value.split(",")
        if isinstance(value, list | tuple):
            return normalize_schema_versions(value)
        return value

    @property
    def server_url(self) -> str:
        return self.nats_url or settings.nats.url

    @property
    def server_token(self) -> str:
        return self.nats_token or settings.nats.token

    @property
    def subject_filter(self) -> str:
        """Every bar the gateway publishes; the feed drops the unplanned ones."""
        return f"{self.subject_prefix}.bar.closed.{self.gateway}.>"

    @property
    def consumer_name(self) -> str:
        """The durable name, derived from the state namespace when left blank."""
        if self.durable_name:
            return _UNSAFE_CONSUMER_CHARS.sub("-", self.durable_name)
        namespace = settings.state_scope.namespace.replace(":", "-")
        return _UNSAFE_CONSUMER_CHARS.sub("-", f"qte-ingestion-{namespace}")
