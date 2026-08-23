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
    assert "will NOT reach the broker" in capsys.readouterr().out


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
    assert "WARNING" in output and "keep their old mode" in output
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
    assert "OFF (live)" in capsys.readouterr().out


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
