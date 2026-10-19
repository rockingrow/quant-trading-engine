"""``qte-control status``: one honest line per link, and an exit code to match.

A status screen is only worth having if its worst answer is the true one. So
these pin the cases that are easy to report wrong: a service that is not
running against one that is running and silent, a broker whose stream exists
while nothing consumes it, and an ingester that cannot be verified at all —
which must never be printed as up.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from nats.errors import NoRespondersError
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.errors import NotFoundError

from qte_shared.config import settings
from qte_strategy_engine import connectivity, control
from qte_strategy_engine.connectivity import LinkReport

RUNNER_REPLY = {
    "service": "strategy-runner",
    "slots": 3,
    "namespace": "dev:live:mt5",
    "execution_mode": "live",
    "delivery_paused": False,
    "ready": True,
}

INGESTION_REPLY = {
    "service": "data-ingestion",
    "providers": ["mt5"],
    "vendor_offline": False,
    "series": [
        {"symbol": "XAUUSD", "newest_bar": "2026-10-12T09:45:00+00:00", "owed": False},
        {"symbol": "EURUSD", "newest_bar": "2026-10-12T10:00:00+00:00", "owed": False},
    ],
}


class FakeControlBus:
    """A NATS double that answers, or fails, per control subject."""

    def __init__(self, *, runner=None, ingestion=None, unreachable=False):
        self.runner_answer = runner
        self.ingestion_answer = ingestion
        self.unreachable = unreachable
        self.closed = False

    async def connect(self):
        if self.unreachable:
            raise ConnectionError("NATS did not answer")

    async def close(self):
        self.closed = True

    async def request(self, subject, payload, timeout=5.0):
        asked_ingestion = subject.endswith(".ingestion.control")
        answer = self.ingestion_answer if asked_ingestion else self.runner_answer
        if isinstance(answer, Exception):
            raise answer
        return answer


def _consumer(name="broker_signal_handler", num_waiting=1):
    return SimpleNamespace(name=name, num_waiting=num_waiting)


class FakeBrokerBus:
    """The broker's NATS, reduced to the JetStream calls the probe makes."""

    def __init__(self, *, consumers=None, stream_missing=False, unreachable=False, buffered=0):
        self.consumers = [_consumer()] if consumers is None else consumers
        self.stream_missing = stream_missing
        self.unreachable = unreachable
        self.buffered = buffered
        self.closed = False
        self.js = self

    async def connect(self):
        if self.unreachable:
            raise ConnectionError("relay refused the connection")

    async def close(self):
        self.closed = True

    async def stream_info(self, stream_name):
        if self.stream_missing:
            raise NotFoundError
        return SimpleNamespace(state=SimpleNamespace(messages=self.buffered))

    async def consumers_info(self, stream_name):
        return self.consumers


@pytest.fixture
def no_ingester(monkeypatch):
    """A book with no ingester-fed provider, so the bus probe stops at ingestion."""

    async def nothing(ingestion_reply):
        return []

    monkeypatch.setattr(connectivity, "_ingester_reports", nothing)


@pytest.fixture
def fast_sampling(monkeypatch):
    monkeypatch.setattr(connectivity, "PULL_SAMPLE_GAP_SECONDS", 0)


@pytest.fixture
def mt5_gateway(monkeypatch):
    import qte_shared.providers.mt5 as mt5_provider

    gateway_setup = SimpleNamespace(
        request_prefix="INGESTER_RPC", gateway="mt5", server_url="nats://nats:4222"
    )
    monkeypatch.setattr(mt5_provider, "Mt5Settings", lambda: gateway_setup)


# ── Subject matching ───────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "pattern",
    [
        "INGESTER_RPC.history.mt5.XAUUSD.M15",
        "INGESTER_RPC.history.mt5.>",
        "INGESTER_RPC.history.mt5.*.*",
        "INGESTER_RPC.history.>",
        "INGESTER_RPC.>",
        "*.history.mt5.>",
    ],
)
def test_a_history_subscription_covers_the_request_subject(pattern):
    assert connectivity.covers_subject_prefix(pattern, ["INGESTER_RPC", "history", "mt5"])


@pytest.mark.parametrize(
    "pattern",
    [
        # Ingestion's own subscription: it listens for the announcement.
        "INGESTER_RPC.online.mt5",
        "INGESTER_RPC.history.binance.>",
        # Exactly the prefix is a subject no request is ever sent on.
        "INGESTER_RPC.history.mt5",
        "INGESTER_RPC.history",
        "_INBOX.4f9c1e.*",
        "OTHER_RPC.history.mt5.>",
    ],
)
def test_an_unrelated_subscription_does_not_count_as_an_ingester(pattern):
    assert not connectivity.covers_subject_prefix(pattern, ["INGESTER_RPC", "history", "mt5"])


# ── Runner and ingestion ───────────────────────────────────────────────────


def test_a_ready_live_runner_is_reported_up_with_its_delivery_mode():
    report = connectivity.runner_report(RUNNER_REPLY, "")
    assert report.verdict == "ok"
    assert "3 slot(s)" in report.detail and "delivering LIVE" in report.detail


def test_a_paused_runner_says_so_rather_than_reading_as_live():
    report = connectivity.runner_report({**RUNNER_REPLY, "delivery_paused": True}, "")
    assert report.verdict == "ok"
    assert "PAUSED" in report.detail and "LIVE" not in report.detail.replace("live", "")


def test_a_runner_that_is_not_ready_is_a_warning_not_a_pass():
    report = connectivity.runner_report({**RUNNER_REPLY, "ready": False}, "")
    assert report.verdict == "warn"
    assert "not ready" in report.detail


def test_ingestion_owing_history_is_a_warning():
    owing = {**INGESTION_REPLY, "series": [{"symbol": "XAUUSD", "newest_bar": None, "owed": True}]}
    report = connectivity.ingestion_report(owing, "")
    assert report.verdict == "warn"
    assert "1 series still awaiting" in report.detail and "none yet" in report.detail


def test_ingestion_reports_its_newest_bar_across_series():
    report = connectivity.ingestion_report(INGESTION_REPLY, "")
    assert report.verdict == "ok"
    assert "2026-10-12T10:00:00+00:00" in report.detail


async def test_not_running_and_not_answering_are_told_apart(monkeypatch, no_ingester):
    control_bus = FakeControlBus(runner=NoRespondersError(), ingestion=NatsTimeoutError())
    monkeypatch.setattr(connectivity, "NatsBus", lambda *a, **k: control_bus)

    bus_report, runner, ingestion = await connectivity.probe_engine_bus()

    assert bus_report.verdict == "ok"
    assert runner.verdict == "fail" and "not running" in runner.detail
    assert ingestion.verdict == "fail" and "silent" in ingestion.detail
    assert control_bus.closed


async def test_an_unreachable_bus_fails_everything_behind_it(monkeypatch, no_ingester):
    monkeypatch.setattr(connectivity, "NatsBus", lambda *a, **k: FakeControlBus(unreachable=True))

    reports = await connectivity.probe_engine_bus()

    assert [report.verdict for report in reports] == ["fail", "fail", "fail"]
    assert "not asked" in reports[1].detail


# ── The ingester ───────────────────────────────────────────────────────────


async def test_an_ingester_subscribed_to_history_requests_is_up(monkeypatch, mt5_gateway):
    connections = [
        {
            "name": "qte-ingestion",
            "ip": "172.18.0.5",
            "subscriptions_list": ["INGESTER_RPC.online.mt5"],
        },
        {
            "name": "ingester",
            "ip": "172.18.0.9",
            "subscriptions_list": ["INGESTER_RPC.history.mt5.>"],
        },
    ]
    monkeypatch.setattr(connectivity, "_read_connections", lambda address: connections)

    report = await connectivity.probe_ingester(INGESTION_REPLY)

    assert report.verdict == "ok"
    assert "ingester (172.18.0.9)" in report.detail and "qte-ingestion" not in report.detail


async def test_no_history_subscriber_means_the_ingester_is_down(monkeypatch, mt5_gateway):
    connections = [{"name": "qte-ingestion", "subscriptions_list": ["INGESTER_RPC.online.mt5"]}]
    monkeypatch.setattr(connectivity, "_read_connections", lambda address: connections)

    report = await connectivity.probe_ingester(INGESTION_REPLY)

    assert report.verdict == "fail"
    assert "INGESTER_RPC.history.mt5" in report.detail


@pytest.mark.parametrize(
    ("ingestion_reply", "verdict"),
    [
        (INGESTION_REPLY, "warn"),
        ({**INGESTION_REPLY, "vendor_offline": True}, "fail"),
        (None, "warn"),
    ],
)
async def test_an_unreadable_monitor_never_reports_the_ingester_up(
    monkeypatch, mt5_gateway, ingestion_reply, verdict
):
    def refused(address):
        raise ConnectionRefusedError("monitor port closed")

    monkeypatch.setattr(connectivity, "_read_connections", refused)

    report = await connectivity.probe_ingester(ingestion_reply)

    assert report.verdict == verdict
    assert "nats:8222/connz" in report.detail


# ── The broker ─────────────────────────────────────────────────────────────


@pytest.fixture
def nats_transport(monkeypatch):
    monkeypatch.setattr(settings.broker, "transport", "nats")


async def test_a_pulled_signals_stream_is_a_live_broker(monkeypatch, nats_transport):
    broker_bus = FakeBrokerBus(buffered=2)
    monkeypatch.setattr(connectivity, "NatsBus", lambda *a, **k: broker_bus)

    report = await connectivity.probe_broker()

    assert report.verdict == "ok"
    assert "2 undelivered" in report.detail and "broker_signal_handler is pulling" in report.detail
    assert broker_bus.closed


async def test_a_stream_nobody_pulls_from_is_a_dead_broker(
    monkeypatch, nats_transport, fast_sampling
):
    broker_bus = FakeBrokerBus(consumers=[_consumer(num_waiting=0)])
    monkeypatch.setattr(connectivity, "NatsBus", lambda *a, **k: broker_bus)

    report = await connectivity.probe_broker()

    assert report.verdict == "fail"
    assert "looks down" in report.detail


async def test_a_missing_stream_is_not_mistaken_for_an_unreachable_server(
    monkeypatch, nats_transport
):
    monkeypatch.setattr(connectivity, "NatsBus", lambda *a, **k: FakeBrokerBus(stream_missing=True))

    report = await connectivity.probe_broker()

    assert report.verdict == "fail"
    assert "holds no" in report.detail and "unreachable" not in report.detail


async def test_an_unreachable_broker_names_the_endpoint_without_the_token(
    monkeypatch, nats_transport
):
    monkeypatch.setattr(settings.broker, "nats_url", "nats://user:hunter2@broker-nats-relay:4222")
    monkeypatch.setattr(connectivity, "NatsBus", lambda *a, **k: FakeBrokerBus(unreachable=True))

    report = await connectivity.probe_broker()

    assert report.verdict == "fail"
    assert "broker-nats-relay:4222" in report.detail and "hunter2" not in report.detail


# ── The screen and the exit code ───────────────────────────────────────────


def test_only_a_failed_link_fails_the_command():
    warned = [LinkReport("Redis", "ok", "up"), LinkReport("Ingester (mt5)", "warn", "unverified")]
    assert connectivity.exit_code(warned) == 0
    assert connectivity.exit_code([*warned, LinkReport("Broker", "fail", "down")]) == 1


def test_the_screen_aligns_targets_and_marks_each_verdict():
    screen = connectivity.render_reports(
        [LinkReport("Redis", "ok", "up"), LinkReport("strategy-runner", "fail", "down")]
    )
    assert screen.splitlines() == [
        "  [ OK ] Redis            up",
        "  [FAIL] strategy-runner  down",
    ]


def test_the_status_command_exits_with_the_reports_verdict(monkeypatch, capsys):
    async def one_link_down():
        return [LinkReport("Broker", "fail", "down")]

    monkeypatch.setattr(connectivity, "collect_reports", one_link_down)
    monkeypatch.setattr(control, "configure_logging", lambda *a, **k: None)
    monkeypatch.setattr("sys.argv", ["qte-control", "status"])

    with pytest.raises(SystemExit) as stopped:
        control.main()

    assert stopped.value.code == 1
    assert "[FAIL] Broker" in capsys.readouterr().out
