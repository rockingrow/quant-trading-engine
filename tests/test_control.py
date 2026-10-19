"""The operator switch. Getting this wrong sends real orders to a real account."""

from __future__ import annotations

import pytest

from qte_strategy_engine import control


class FakeRedis:
    def __init__(self, stored=None):
        self.flags = {} if stored is None else dict(stored)
        self.closed = False

    async def connect(self):
        pass

    async def close(self):
        self.closed = True

    async def set_flag(self, name, value):
        self.flags[name] = value

    async def get_flag(self, name, default=None):
        return self.flags.get(name, default)


class FakeBus:
    def __init__(self, *, fail=False):
        self.published = []
        self.fail = fail

    async def connect(self):
        if self.fail:
            raise ConnectionError("NATS is not connected")

    async def close(self):
        pass

    async def publish(self, subject, payload):
        self.published.append((subject, payload))


class FakeEvents:
    def __init__(self):
        self.events = []

    async def record_event(self, **kwargs):
        self.events.append(kwargs)


@pytest.fixture
def wired(monkeypatch):
    """Swap Redis, NATS and the audit repo for recorders."""
    redis, bus, audit = FakeRedis(), FakeBus(), FakeEvents()
    monkeypatch.setattr(control, "RedisState", lambda *a, **k: redis)
    monkeypatch.setattr(control, "NatsBus", lambda *a, **k: bus)
    monkeypatch.setattr(control, "EventRepository", lambda *a, **k: audit)
    return redis, bus, audit


def test_the_parser_accepts_the_documented_commands():
    parser = control.build_parser()
    assert parser.parse_args(["shadow", "on"]).state == "on"
    assert parser.parse_args(["shadow", "off", "--yes"]).yes is True
    assert parser.parse_args(["ping"]).command == "ping"


def test_an_unknown_state_is_refused():
    with pytest.raises(SystemExit):
        control.build_parser().parse_args(["shadow", "maybe"])


async def test_turning_shadow_on_stores_the_flag_and_broadcasts_it(wired, capsys):
    redis, bus, audit = wired
    await control._set_shadow_mode(True)

    assert redis.flags["shadow_mode"] is True
    subject, payload = bus.published[0]
    assert subject.endswith(".control")
    assert payload == {"action": "set_shadow_mode", "enabled": True}
    assert "Live delivery PAUSED" in capsys.readouterr().out


async def test_going_live_is_announced_plainly(wired, capsys):
    redis, bus, _ = wired
    await control._set_shadow_mode(False)

    assert redis.flags["shadow_mode"] is False
    assert bus.published[0][1]["enabled"] is False
    assert "LIVE" in capsys.readouterr().out


async def test_redis_is_written_even_when_nats_is_down(monkeypatch, capsys):
    # The flag surviving matters: the next runner to start reads it from Redis.
    redis, bus, audit = FakeRedis(), FakeBus(fail=True), FakeEvents()
    monkeypatch.setattr(control, "RedisState", lambda *a, **k: redis)
    monkeypatch.setattr(control, "NatsBus", lambda *a, **k: bus)
    monkeypatch.setattr(control, "EventRepository", lambda *a, **k: audit)

    await control._set_shadow_mode(True)

    assert redis.flags["shadow_mode"] is True
    assert bus.published == []
    output = capsys.readouterr().out
    # Silence here would read as "applied everywhere", which is the one thing
    # it is not.
    assert "WARNING" in output and "refresh the stored flag" in output
    assert audit.events[0]["payload"]["broadcast"] is False


async def test_the_change_is_audited(wired):
    _, _, audit = wired
    await control._set_shadow_mode(False)
    event = audit.events[0]
    assert event["event"] == "shadow_mode_changed"
    assert event["level"] == "WARNING"
    assert event["payload"] == {"enabled": False, "broadcast": True}


async def test_status_reads_the_stored_flag(monkeypatch, capsys):
    monkeypatch.setattr(control, "RedisState", lambda *a, **k: FakeRedis({"shadow_mode": False}))
    await control._show_shadow_mode()
    assert "Live delivery is ENABLED" in capsys.readouterr().out


async def test_status_falls_back_to_the_configured_default(monkeypatch, capsys):
    monkeypatch.setattr(control, "RedisState", lambda *a, **k: FakeRedis())
    await control._show_shadow_mode()
    assert "No stored flag" in capsys.readouterr().out


def test_going_live_requires_typing_the_confirmation(wired, monkeypatch):
    # A typo must not be enough to put orders on a live account.
    monkeypatch.setattr("builtins.input", lambda _: "y")
    monkeypatch.setattr("sys.argv", ["qte-control", "shadow", "off"])
    with pytest.raises(SystemExit) as exit_info:
        control.main()
    assert exit_info.value.code == 1
    assert wired[1].published == []


def test_typing_live_confirms(wired, monkeypatch):
    monkeypatch.setattr("builtins.input", lambda _: "LIVE")
    monkeypatch.setattr("sys.argv", ["qte-control", "shadow", "off"])
    control.main()
    assert wired[1].published[0][1]["enabled"] is False


def test_yes_skips_the_prompt_for_scripted_use(wired, monkeypatch):
    def refuse(_):
        raise AssertionError("--yes must not prompt")

    monkeypatch.setattr("builtins.input", refuse)
    monkeypatch.setattr("sys.argv", ["qte-control", "shadow", "off", "--yes"])
    control.main()
    assert wired[1].published[0][1]["enabled"] is False


def test_turning_shadow_on_never_prompts(wired, monkeypatch):
    # Going *to* paper is always safe; only going live asks.
    def refuse(_):
        raise AssertionError("enabling shadow mode must not prompt")

    monkeypatch.setattr("builtins.input", refuse)
    monkeypatch.setattr("sys.argv", ["qte-control", "shadow", "on"])
    control.main()
    assert wired[1].published[0][1]["enabled"] is True


class DeadRedis(FakeRedis):
    async def connect(self):
        raise ConnectionError("Error 111 connecting to localhost:6379")


# ── owner: the claim an unclean exit leaves behind ──────────────────────


class OwnedRedis(FakeRedis):
    def __init__(self, holder=None, *, replaced_by=None):
        super().__init__()
        self.holder = holder
        self.replaced_by = replaced_by

    def key(self, *parts):
        return ":".join(("qte", "test", *parts))

    async def runner_owner(self):
        return self.holder

    async def release_runner(self, owner_id):
        if self.replaced_by is not None:
            self.holder = self.replaced_by
        if self.holder != owner_id:
            return False
        self.holder = None
        return True


class PingBus(FakeBus):
    def __init__(self, *, runner_reply=None, failure=None, fail=False):
        super().__init__(fail=fail)
        self.runner_reply = runner_reply
        self.failure = failure

    async def request(self, subject, payload, timeout=5.0):
        if self.runner_reply is not None:
            return self.runner_reply
        raise self.failure or control.NatsTimeoutError()


def wire_owner(monkeypatch, redis, bus):
    audit = FakeEvents()
    monkeypatch.setattr(control, "RedisState", lambda *a, **k: redis)
    monkeypatch.setattr(control, "NatsBus", lambda *a, **k: bus)
    monkeypatch.setattr(control, "EventRepository", lambda *a, **k: audit)
    return audit


def test_the_parser_accepts_the_owner_commands():
    parser = control.build_parser()
    assert parser.parse_args(["owner", "status"]).operation == "status"
    assert parser.parse_args(["owner", "clear", "--yes"]).yes is True
    with pytest.raises(SystemExit):
        parser.parse_args(["owner", "delete"])


async def test_owner_status_names_the_key_and_its_holder(monkeypatch, capsys):
    wire_owner(monkeypatch, OwnedRedis("stale-owner"), PingBus())
    await control._show_runner_owner()
    output = capsys.readouterr().out
    assert "qte:test:runner:owner" in output and "stale-owner" in output


async def test_clearing_without_a_claim_changes_nothing(monkeypatch, capsys):
    redis = OwnedRedis()
    audit = wire_owner(monkeypatch, redis, PingBus(fail=True))
    await control._clear_runner_owner(assume_yes=True)
    assert "nothing to clear" in capsys.readouterr().out
    assert audit.events == []


async def test_a_stale_claim_is_cleared_and_audited(monkeypatch, capsys):
    redis = OwnedRedis("stale-owner")
    audit = wire_owner(monkeypatch, redis, PingBus())
    await control._clear_runner_owner(assume_yes=True)
    assert redis.holder is None
    assert redis.closed
    assert audit.events[0]["event"] == "runner_owner_cleared"
    assert audit.events[0]["payload"] == {"holder": "stale-owner"}
    assert "Cleared" in capsys.readouterr().out


async def test_no_responders_counts_as_no_runner(monkeypatch):
    redis = OwnedRedis("stale-owner")
    wire_owner(monkeypatch, redis, PingBus(failure=control.NoRespondersError()))
    await control._clear_runner_owner(assume_yes=True)
    assert redis.holder is None


async def test_a_runner_that_answers_keeps_its_claim(monkeypatch, capsys):
    redis = OwnedRedis("live-owner")
    audit = wire_owner(monkeypatch, redis, PingBus(runner_reply={"owner_id": "live-owner"}))
    with pytest.raises(SystemExit, match="not stale"):
        await control._clear_runner_owner(assume_yes=True)
    assert redis.holder == "live-owner"
    assert audit.events == []
    assert "live-owner" in capsys.readouterr().err


@pytest.mark.parametrize(
    "bus", [PingBus(fail=True), PingBus(failure=ConnectionError("connection lost mid-request"))]
)
async def test_an_unverifiable_runner_keeps_its_claim(monkeypatch, capsys, bus):
    # A bus that is down cannot say that no runner is alive.
    redis = OwnedRedis("stale-owner")
    audit = wire_owner(monkeypatch, redis, bus)
    with pytest.raises(SystemExit) as exit_info:
        await control._clear_runner_owner(assume_yes=True)
    assert exit_info.value.code == 2
    assert redis.holder == "stale-owner"
    assert audit.events == []
    assert "nothing was changed" in capsys.readouterr().err


async def test_a_claim_that_changed_meanwhile_is_not_removed(monkeypatch):
    redis = OwnedRedis("stale-owner", replaced_by="new-runner")
    audit = wire_owner(monkeypatch, redis, PingBus())
    with pytest.raises(SystemExit, match="claim changed"):
        await control._clear_runner_owner(assume_yes=True)
    assert redis.holder == "new-runner"
    assert audit.events == []


def test_clearing_requires_typing_the_namespace(monkeypatch):
    redis = OwnedRedis("stale-owner")
    audit = wire_owner(monkeypatch, redis, PingBus())
    monkeypatch.setattr("builtins.input", lambda _: "yes")
    monkeypatch.setattr("sys.argv", ["qte-control", "owner", "clear"])
    with pytest.raises(SystemExit) as exit_info:
        control.main()
    assert exit_info.value.code == 1
    assert redis.holder == "stale-owner"
    assert audit.events == []


def test_typing_the_namespace_clears_the_claim(monkeypatch):
    redis = OwnedRedis("stale-owner")
    wire_owner(monkeypatch, redis, PingBus())
    monkeypatch.setattr("builtins.input", lambda _: control.settings.state_scope.namespace)
    monkeypatch.setattr("sys.argv", ["qte-control", "owner", "clear"])
    control.main()
    assert redis.holder is None


async def test_an_unreachable_redis_reports_one_line_for_the_owner_commands(monkeypatch, capsys):
    monkeypatch.setattr(control, "RedisState", lambda *a, **k: DeadRedis())
    with pytest.raises(SystemExit) as exit_info:
        await control._clear_runner_owner(assume_yes=True)
    assert exit_info.value.code == 2
    assert "Could not reach Redis" in capsys.readouterr().err


async def test_an_unreachable_redis_changes_nothing_rather_than_half_applying(monkeypatch, capsys):
    """The dangerous case: broadcasting a flag that was never stored.

    A runner restarting later would read the *old* flag from Redis, and for
    "off" that means quietly going live again on its own.
    """
    bus = FakeBus()
    monkeypatch.setattr(control, "RedisState", lambda *a, **k: DeadRedis())
    monkeypatch.setattr(control, "NatsBus", lambda *a, **k: bus)
    monkeypatch.setattr(control, "EventRepository", lambda *a, **k: FakeEvents())

    with pytest.raises(SystemExit) as exit_info:
        await control._set_shadow_mode(False)

    assert exit_info.value.code == 2
    assert bus.published == [], "nothing may be broadcast if the flag was not stored"
    assert "Could not reach Redis" in capsys.readouterr().err


async def test_a_dependency_failure_prints_one_line_not_a_traceback(monkeypatch, capsys):
    monkeypatch.setattr(control, "RedisState", lambda *a, **k: DeadRedis())
    with pytest.raises(SystemExit):
        await control._show_shadow_mode()

    error = capsys.readouterr().err
    assert "Could not reach Redis" in error
    assert "Traceback" not in error
    assert "ConnectionError" in error  # the cause is still named


@pytest.fixture(autouse=True)
def live_state_scope(monkeypatch):
    """Exercise broker paths with an explicitly selected live book and fake transports."""
    from qte_shared.config import settings

    monkeypatch.setattr(settings, "env", "prod")
    monkeypatch.setattr(settings.state_config, "execution_mode", "live")
