"""``qte-control status`` — is every link the engine depends on actually up.

``make ping`` answers one question: is a runner listening. An engine that
trades needs six links, and each fails in its own quiet way — a runner with no
ingestion decides on a window that stopped moving, an ingestion with no ingester
warms nothing, and a runner whose broker is gone keeps logging failed
deliveries nobody reads. This module asks each of them once and reports the lot
on one screen:

* **Redis**, **Postgres** and **QTE's own NATS** — the stack's own services.
* **strategy-runner** and **data-ingestion** — a ``ping`` on each service's
  control subject, the same request/reply the Telegram bot uses.
* **The MT5 ingester** — a separate machine. It is asked nothing: a history
  request is work for it, and there is no ping in its contract. What is read
  instead is the NATS server's own connection list (``/connz`` on the
  monitoring port), looking for a client subscribed to the history request
  subject — which is exactly "would a request be answered right now".
* **The broker** — on the NATS transport, its ``SIGNALS`` stream and whether
  anything is pulling from it. A stream that exists with no pull request
  waiting is a broker that is down: signals would be stored and reach no
  worker. On the HTTP transport, its ``/v1/health``.

Every probe is read-only. Nothing here publishes a signal, requests history or
writes a flag, so it is safe against a live book.

**Run it inside the stack.** ``make status`` executes this in a running
service container rather than on the host, for two reasons: the container
carries the stack's real namespace (in production ``QTE_ENV`` comes from the
compose overlay, not from ``.env``), and with Tailscale on the broker's NATS is
reachable only through ``broker-nats-relay`` on the compose network.

Endpoints are printed as ``host:port`` only. A Redis URL or a Postgres DSN can
carry a password, and a status screen ends up pasted into chats.
"""

from __future__ import annotations

import asyncio
import json
import urllib.request
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Literal
from urllib.parse import urlsplit

from nats.errors import NoRespondersError
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.errors import NotFoundError

from qte_shared.bus import NatsBus, Subjects
from qte_shared.cache import RedisState
from qte_shared.config import settings
from qte_shared.db import Database

Verdict = Literal["ok", "warn", "fail"]

#: How long a service gets to answer a control ``ping``.
CONTROL_TIMEOUT_SECONDS = 3.0

#: How long the NATS monitoring endpoint and the broker's HTTP health get.
HTTP_TIMEOUT_SECONDS = 3.0

#: NATS' monitoring port as the server listens on it inside the stack.
#: ``QTE_NATS_MONITOR_PORT`` only moves the host side of the mapping.
NATS_MONITOR_PORT = 8222

#: A pull consumer has a request waiting almost all the time, but not while its
#: owner is busy with a message. Sampled a few times so one busy moment does
#: not read as a dead broker.
PULL_SAMPLE_COUNT = 4
PULL_SAMPLE_GAP_SECONDS = 0.4

_MARKERS: dict[Verdict, str] = {"ok": "[ OK ]", "warn": "[WARN]", "fail": "[FAIL]"}


@dataclass(frozen=True, slots=True)
class LinkReport:
    """One link, and what asking it showed."""

    target: str
    verdict: Verdict
    detail: str


def _endpoint(address: str) -> str:
    """``host:port`` of a URL — never its credentials, path or query."""
    parts = urlsplit(address)
    if not parts.hostname:
        return "<unparseable address>"
    return f"{parts.hostname}:{parts.port}" if parts.port else parts.hostname


def _describe(failure: BaseException) -> str:
    text = str(failure).strip()
    return f"{type(failure).__name__}: {text}" if text else type(failure).__name__


# ── The stack's own services ──────────────────────────────────────────────


async def probe_redis() -> LinkReport:
    endpoint = _endpoint(settings.redis.url)
    redis_state = RedisState()
    try:
        await redis_state.connect()
        holder = await redis_state.runner_owner()
    except Exception as failure:
        return LinkReport("Redis", "fail", f"{endpoint} unreachable ({_describe(failure)})")
    finally:
        await redis_state.close()
    claim = f"runner claim held by {holder}" if holder else "no runner claim held"
    return LinkReport("Redis", "ok", f"{endpoint}, {claim}")


async def probe_postgres() -> LinkReport:
    database = Database()
    try:
        location = database.engine.url
        endpoint = f"{location.host}:{location.port}/{location.database}"
        answered = await database.ping()
    except Exception as failure:
        return LinkReport("Postgres", "fail", _describe(failure))
    finally:
        await database.close()
    if not answered:
        return LinkReport("Postgres", "fail", f"{endpoint} did not answer SELECT 1")
    return LinkReport("Postgres", "ok", endpoint)


# ── QTE's bus, and the services answering on it ───────────────────────────


async def probe_engine_bus() -> list[LinkReport]:
    """QTE's NATS, the runner, ingestion, and the ingester behind ingestion.

    One connection for all of them. The ingester is checked last because its
    report leans on what ingestion said about the vendor.
    """
    endpoint = _endpoint(settings.nats.url)
    control_bus = NatsBus(name="qte-status")
    try:
        await control_bus.connect()
    except Exception as failure:
        unreachable = "not asked: QTE's NATS is unreachable"
        return [
            LinkReport("NATS (QTE)", "fail", f"{endpoint} unreachable ({_describe(failure)})"),
            LinkReport("strategy-runner", "fail", unreachable),
            LinkReport("data-ingestion", "fail", unreachable),
            *await _ingester_reports(None),
        ]
    try:
        subjects = Subjects()
        runner_reply, runner_silence = await _control_ping(control_bus, subjects.engine_control())
        ingestion_reply, ingestion_silence = await _control_ping(
            control_bus, subjects.ingestion_control()
        )
    finally:
        await control_bus.close()
    return [
        LinkReport("NATS (QTE)", "ok", endpoint),
        runner_report(runner_reply, runner_silence),
        ingestion_report(ingestion_reply, ingestion_silence),
        *await _ingester_reports(ingestion_reply),
    ]


async def _control_ping(control_bus: NatsBus, subject: str) -> tuple[dict[str, Any] | None, str]:
    """The service's ``ping`` reply, or ``None`` and why there was none.

    The two silences are different faults and are worded apart: no subscriber
    is a process that is not running, a timeout is one that is running and not
    answering.
    """
    try:
        reply = await control_bus.request(
            subject, {"action": "ping"}, timeout=CONTROL_TIMEOUT_SECONDS
        )
    except NoRespondersError:
        return None, f"nothing is subscribed to {subject} — the service is not running"
    except NatsTimeoutError:
        return None, (
            f"subscribed to {subject} but silent for {CONTROL_TIMEOUT_SECONDS:.0f}s "
            "— hung, or still starting"
        )
    except Exception as failure:
        return None, _describe(failure)
    return reply, ""


def runner_report(reply: dict[str, Any] | None, silence: str) -> LinkReport:
    if reply is None:
        return LinkReport("strategy-runner", "fail", silence)
    if reply.get("execution_mode") == "paper":
        delivery = "paper, nothing is sent to the broker"
    elif reply.get("delivery_paused"):
        delivery = "live delivery PAUSED"
    else:
        delivery = "delivering LIVE"
    summary = f"{reply.get('slots', 0)} slot(s), {delivery}, namespace {reply.get('namespace')}"
    if not reply.get("ready"):
        return LinkReport(
            "strategy-runner",
            "warn",
            f"{summary} — not ready: a window is still warming or a delivery is unreconciled",
        )
    return LinkReport("strategy-runner", "ok", summary)


def ingestion_report(reply: dict[str, Any] | None, silence: str) -> LinkReport:
    if reply is None:
        return LinkReport("data-ingestion", "fail", silence)
    series = reply.get("series") or []
    awaiting_history = [entry for entry in series if entry.get("owed")]
    newest_bars = [entry["newest_bar"] for entry in series if entry.get("newest_bar")]
    summary = (
        f"providers {', '.join(reply.get('providers') or []) or 'none'}, "
        f"{len(series)} series, newest bar {max(newest_bars) if newest_bars else 'none yet'}"
    )
    if awaiting_history:
        return LinkReport(
            "data-ingestion",
            "warn",
            f"{summary} — {len(awaiting_history)} series still awaiting their history window",
        )
    return LinkReport("data-ingestion", "ok", summary)


# ── The MT5 ingester ──────────────────────────────────────────────────────


async def _ingester_reports(ingestion_reply: dict[str, Any] | None) -> list[LinkReport]:
    """One report per ingester-fed provider; none when the book has none."""
    if "mt5" not in settings.market_data.providers:
        return []
    return [await probe_ingester(ingestion_reply)]


async def probe_ingester(ingestion_reply: dict[str, Any] | None) -> LinkReport:
    """Whether ``algo-trading-ingester`` would answer a history request now."""
    # Imported here: the provider's settings read its plan file, which a book
    # with no MT5 provider does not have.
    from qte_shared.providers.mt5 import Mt5Settings

    target = "Ingester (mt5)"
    try:
        gateway_setup = Mt5Settings()
    except Exception as failure:
        return LinkReport(target, "fail", f"MT5 provider settings unusable ({_describe(failure)})")

    request_tokens = [*gateway_setup.request_prefix.split("."), "history", gateway_setup.gateway]
    request_subject = ".".join(request_tokens)
    monitor_address = (
        f"http://{urlsplit(gateway_setup.server_url).hostname}:{NATS_MONITOR_PORT}/connz?subs=1"
    )
    try:
        connections = await asyncio.to_thread(_read_connections, monitor_address)
    except Exception as failure:
        return _ingester_unverified(target, monitor_address, failure, ingestion_reply)

    answering = [
        connection
        for connection in connections
        if any(
            covers_subject_prefix(pattern, request_tokens)
            for pattern in connection.get("subscriptions_list") or []
        )
    ]
    if not answering:
        return LinkReport(
            target,
            "fail",
            f"no NATS client is subscribed under {request_subject} — the ingester is not "
            "connected, or runs on another subject prefix",
        )
    clients = ", ".join(
        f"{connection.get('name') or 'unnamed client'} ({connection.get('ip', '?')})"
        for connection in answering
    )
    return LinkReport(target, "ok", f"answering history requests on {request_subject}: {clients}")


def _ingester_unverified(
    target: str,
    monitor_address: str,
    failure: BaseException,
    ingestion_reply: dict[str, Any] | None,
) -> LinkReport:
    """The monitor could not be read: fall back to what ingestion last saw."""
    cause = f"could not read {_endpoint(monitor_address)}/connz ({_describe(failure)})"
    if ingestion_reply is None:
        return LinkReport(target, "warn", f"unverified — {cause}, and ingestion did not answer")
    if ingestion_reply.get("vendor_offline"):
        return LinkReport(
            target, "fail", f"ingestion found nobody answering its history requests; {cause}"
        )
    return LinkReport(
        target, "warn", f"unverified — {cause}; ingestion has not found the vendor offline"
    )


def _read_connections(monitor_address: str) -> list[dict[str, Any]]:
    """The NATS server's client list, with each client's subscriptions."""
    with urllib.request.urlopen(monitor_address, timeout=HTTP_TIMEOUT_SECONDS) as response:
        return json.load(response).get("connections") or []


def covers_subject_prefix(pattern: str, prefix_tokens: Sequence[str]) -> bool:
    """Whether a subscription *pattern* takes subjects under *prefix_tokens*.

    ``INGESTER_RPC.history.mt5.XAUUSD.M15``, ``INGESTER_RPC.history.mt5.>`` and
    ``INGESTER_RPC.>`` all cover ``INGESTER_RPC.history.mt5``; the ``online``
    announcement subject and a reply inbox do not. A pattern shorter than the
    prefix covers nothing below it unless it ends in ``>``.
    """
    pattern_tokens = pattern.split(".")
    for position, wanted in enumerate(prefix_tokens):
        if position >= len(pattern_tokens):
            return False
        token = pattern_tokens[position]
        if token == ">":
            return True
        if token != "*" and token != wanted:
            return False
    return len(pattern_tokens) > len(prefix_tokens)


# ── The broker ────────────────────────────────────────────────────────────


async def probe_broker() -> LinkReport:
    if settings.broker.transport == "http":
        return await asyncio.to_thread(_probe_broker_http)
    return await _probe_broker_stream()


async def _probe_broker_stream() -> LinkReport:
    """The broker's ``SIGNALS`` stream, and whether the broker is consuming it."""
    endpoint = _endpoint(settings.broker_nats_url)
    stream_name = settings.broker.stream
    broker_bus = NatsBus(
        url=settings.broker_nats_url, token=settings.broker_nats_token, name="qte-status"
    )
    try:
        await broker_bus.connect()
    except Exception as failure:
        return LinkReport(
            "Broker", "fail", f"its NATS at {endpoint} is unreachable ({_describe(failure)})"
        )
    try:
        stream = await broker_bus.js.stream_info(stream_name)
        consumers = await broker_bus.js.consumers_info(stream_name)
        being_pulled = await _is_being_pulled(broker_bus, stream_name)
    except NotFoundError:
        return LinkReport(
            "Broker",
            "fail",
            f"NATS at {endpoint} answers but holds no {stream_name} stream — the broker has "
            "never started against this server",
        )
    except Exception as failure:
        return LinkReport("Broker", "fail", f"NATS at {endpoint}: {_describe(failure)}")
    finally:
        await broker_bus.close()

    buffered = f"{stream_name} stream holds {stream.state.messages} undelivered signal(s)"
    if not consumers:
        return LinkReport(
            "Broker",
            "fail",
            f"{endpoint}: {buffered} and has no consumer — the broker has never run here",
        )
    consumer_names = ", ".join(consumer.name for consumer in consumers)
    if not being_pulled:
        return LinkReport(
            "Broker",
            "fail",
            f"{endpoint}: {buffered}; nothing is pulling from {consumer_names} — the broker "
            "process looks down, signals would wait in the stream",
        )
    return LinkReport("Broker", "ok", f"{endpoint}: {buffered}; {consumer_names} is pulling")


async def _is_being_pulled(broker_bus: NatsBus, stream_name: str) -> bool:
    for sample in range(PULL_SAMPLE_COUNT):
        if sample:
            await asyncio.sleep(PULL_SAMPLE_GAP_SECONDS)
        consumers = await broker_bus.js.consumers_info(stream_name)
        if any(consumer.num_waiting for consumer in consumers):
            return True
    return False


def _probe_broker_http() -> LinkReport:
    health_address = f"{settings.broker.http_url.rstrip('/')}/v1/health"
    endpoint = _endpoint(health_address)
    try:
        with urllib.request.urlopen(health_address, timeout=HTTP_TIMEOUT_SECONDS) as response:
            status_code = response.status
    except Exception as failure:
        return LinkReport(
            "Broker", "fail", f"GET {endpoint}/v1/health failed ({_describe(failure)})"
        )
    if status_code != 200:
        return LinkReport("Broker", "fail", f"GET {endpoint}/v1/health answered {status_code}")
    return LinkReport("Broker", "ok", f"GET {endpoint}/v1/health answered 200")


# ── The whole picture ─────────────────────────────────────────────────────


async def collect_reports() -> list[LinkReport]:
    """Ask every link at once; the order returned is the order printed."""
    redis_report, postgres_report, bus_reports, broker_report = await asyncio.gather(
        probe_redis(), probe_postgres(), probe_engine_bus(), probe_broker()
    )
    return [redis_report, postgres_report, *bus_reports, broker_report]


def render_reports(reports: Sequence[LinkReport]) -> str:
    width = max(len(report.target) for report in reports)
    return "\n".join(
        f"  {_MARKERS[report.verdict]} {report.target.ljust(width)}  {report.detail}"
        for report in reports
    )


def exit_code(reports: Sequence[LinkReport]) -> int:
    """Non-zero when any link is down; a warning alone does not fail the run."""
    return 1 if any(report.verdict == "fail" for report in reports) else 0


__all__ = [
    "LinkReport",
    "collect_reports",
    "covers_subject_prefix",
    "exit_code",
    "ingestion_report",
    "render_reports",
    "runner_report",
]
