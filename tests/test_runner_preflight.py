"""The runner's own reading of __strategies__/, before it trades.

The loader is forgiving and the audit is strict; this is the switch that says
which of the two the *runner* obeys, and the thing worth testing is that it
refuses when it was told to refuse. A gate that only ever logs is a gate the
operator believes in and does not have.

Nothing is connected here. `run_preflight_audit` is a synchronous function that
reads files, and the one asynchronous case below asserts precisely that it runs
before any connection is opened.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
from qte_shared.config import settings
from qte_strategy_engine.preflight import StrategyAuditFailed, run_preflight_audit

#: A self-contained strategy the loader can drive and the audit passes.
CLEAN = """
    from abc import ABC


    class Edge(ABC):
        name = "PREFLIGHT_EDGE"
        symbols = ("XAUUSD",)
        timeframe = "M15"
        warmup = 10
        max_history = 200

        def __init__(self, params=None):
            self.params = dict(params or {})

        def on_start(self, context): pass
        def on_stop(self): pass
        def on_tick(self, price, context): return None
        def history_window(self): return self.max_history
        def describe(self): return {"name": self.name}
        def on_candle_closed(self, df, context): return None

        def long(self, df, context): return None
        def short(self, df, context): return None
        def tp1(self, df, context): return None
        def tp2(self, df, context): return None
        def sl(self, df, context): return None
"""

#: The commonest real failure: a plugin importing something the venv lacks.
#: The loader logs it and carries on with nothing; the audit calls it an error.
BROKEN = """
    import a_dependency_nobody_installed  # noqa: F401


    class Edge:
        name = "BROKEN_EDGE"
"""


@pytest.fixture
def strategies(monkeypatch, tmp_path) -> Path:
    """Point the audit at a directory of our own, with no routing table.

    `audit()` reads the process settings, which otherwise resolve to the repo's
    real `__strategies__/` — whatever the developer running the suite happens
    to have cloned in there.
    """
    directory = tmp_path / "__strategies__"
    directory.mkdir()
    monkeypatch.setattr(settings.engine, "strategies_dir", directory)
    monkeypatch.setattr(settings.engine, "routing_file", tmp_path / "no-such-table.toml")
    return directory


def write(directory: Path, name: str, source: str) -> None:
    (directory / name).write_text(textwrap.dedent(source).lstrip(), encoding="utf-8")


# ── Off ──────────────────────────────────────────────────────────────────


def test_off_does_not_audit_at_all(strategies, monkeypatch):
    write(strategies, "broken.py", BROKEN)
    monkeypatch.setattr(
        "qte_strategy_engine.preflight.audit",
        lambda *a, **k: pytest.fail("the audit ran with the mode off"),
    )
    assert run_preflight_audit("off") is None


# ── Warn: the default, and it never stops anything ───────────────────────


def test_warn_reports_errors_and_starts_anyway(strategies):
    """The pre-existing behaviour, plus a report. Nothing new refuses to run."""
    write(strategies, "broken.py", BROKEN)
    report = run_preflight_audit("warn")

    assert [finding.code for finding in report.errors] == ["load-failed"]


def test_warn_is_the_default(strategies):
    from qte_strategy_engine.settings import runner_settings

    assert runner_settings.audit_on_start == "warn"
    write(strategies, "broken.py", BROKEN)
    assert run_preflight_audit() is not None


# ── Error: refuse on errors, tolerate warnings ───────────────────────────


def test_error_refuses_to_start_on_a_broken_strategy(strategies):
    write(strategies, "broken.py", BROKEN)
    with pytest.raises(StrategyAuditFailed) as raised:
        run_preflight_audit("error")

    # The message has to name the switch: whoever reads it in a restart loop
    # did not necessarily set it.
    assert "QTE_RUNNER__AUDIT_ON_START=error" in str(raised.value)


def test_error_starts_on_a_clean_directory(strategies):
    write(strategies, "edge.py", CLEAN)
    report = run_preflight_audit("error")

    assert report.ok and [entry.name for entry in report.strategies] == ["PREFLIGHT_EDGE"]


def test_error_tolerates_warnings(monkeypatch, tmp_path):
    """A missing directory is a warning — `error` is not `strict`."""
    monkeypatch.setattr(settings.engine, "strategies_dir", tmp_path / "never-cloned")
    monkeypatch.setattr(settings.engine, "routing_file", tmp_path / "no-such-table.toml")
    report = run_preflight_audit("error")

    assert report.warnings and not report.errors


# ── Strict: warnings stop it too ─────────────────────────────────────────


def test_strict_refuses_on_a_warning(monkeypatch, tmp_path):
    monkeypatch.setattr(settings.engine, "strategies_dir", tmp_path / "never-cloned")
    monkeypatch.setattr(settings.engine, "routing_file", tmp_path / "no-such-table.toml")
    with pytest.raises(StrategyAuditFailed):
        run_preflight_audit("strict")


def test_strict_starts_on_a_clean_directory(strategies):
    write(strategies, "edge.py", CLEAN)
    assert run_preflight_audit("strict").exit_code(strict=True) == 0


# ── Where it sits in start() ─────────────────────────────────────────────


async def test_the_audit_runs_before_anything_is_connected(strategies, monkeypatch):
    """A refusal must cost nothing to unwind — no NATS, no Redis, no broker."""
    from qte_strategy_engine.runner import StrategyRunner

    write(strategies, "broken.py", BROKEN)
    monkeypatch.setattr(
        "qte_strategy_engine.settings.runner_settings.audit_on_start", "error", raising=False
    )

    runner = StrategyRunner()
    connected: list[str] = []
    for target, name in ((runner.bus, "bus"), (runner.state, "state"), (runner.sink, "sink")):
        method = "start" if name == "sink" else "connect"

        async def record(*_args, _name=name, **_kwargs):
            connected.append(_name)

        monkeypatch.setattr(target, method, record)

    with pytest.raises(StrategyAuditFailed):
        await runner.start()
    assert connected == []
