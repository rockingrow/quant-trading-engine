"""The gate between "a repo was cloned in" and "it is fit to trade".

The loader is forgiving by design: what it cannot drive it logs and skips, so
one broken file does not stop the other four. That is right for a running
process and useless as a deploy check — "skipped" and "there were none" read
identically in a log. This is the strict pass over the same directory, and what
it is really being tested for is that it *fails* when it should.

The fixtures below are written the way a real plugin repo is: no ``qte_shared``
import anywhere inside them.
"""

from __future__ import annotations

import shutil
import textwrap
from pathlib import Path

import pytest
from qte_strategy_audit import Severity, StrategyAuditor

#: A plugin repo's own restatement of the QTE interface — five abstract signal
#: methods, two optional no-ops, and the driver hook implemented on top.
CONTRACT = """
    from abc import ABC, abstractmethod
    from dataclasses import dataclass, field


    @dataclass
    class Intent:
        action: str
        price: float | None = None
        sl: float | None = None
        indicators: dict = field(default_factory=dict)


    class Base(ABC):
        name = ""
        symbols = ()
        timeframe = "M15"
        warmup = 10
        max_history = None

        def __init__(self, params=None):
            self.params = dict(params or {})

        def on_start(self, context): pass
        def on_stop(self): pass
        def on_tick(self, price, context): return None
        def history_window(self): return self.max_history or 400
        def describe(self): return {"name": self.name}

        def on_candle_closed(self, df, context):
            if context.open_uxid is not None:
                return [i for i in (self.sl(df, context), self.tp1(df, context)) if i]
            return self.long(df, context) or self.short(df, context)

        @abstractmethod
        def long(self, df, context): ...
        @abstractmethod
        def short(self, df, context): ...
        @abstractmethod
        def tp1(self, df, context): ...
        @abstractmethod
        def tp2(self, df, context): ...
        @abstractmethod
        def sl(self, df, context): ...

        def r_sl(self, df, context): return None
        def flat(self, df, context): return None
"""

GOOD = """
    from contract import Base, Intent


    class GoldEdge(Base):
        name = "GOLD_EDGE_V1"
        timeframe = "M15"
        warmup = 50

        def long(self, df, context): return Intent(action="LONG", price=2000.0, sl=1990.0)
        def short(self, df, context): return None
        def tp1(self, df, context): return None
        def tp2(self, df, context): return None
        def sl(self, df, context): return None
"""

#: The other way in: one self-contained file, importing nothing of its own,
#: found by the directory scan rather than named by a manifest.
LOOSE_FILE = (
    CONTRACT
    + """

    class LooseEdge(Base):
        name = "LOOSE_EDGE_V1"
        timeframe = "M15"
        warmup = 50

        def long(self, df, context): return Intent(action="LONG", price=2000.0, sl=1990.0)
        def short(self, df, context): return None
        def tp1(self, df, context): return None
        def tp2(self, df, context): return None
        def sl(self, df, context): return None
"""
)

MANIFEST = """
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

    from gold import GoldEdge

    def load_all():
        return {"GOLD_EDGE_V1": GoldEdge}
"""


def write(root: Path, files: dict[str, str]) -> Path:
    for relative, source in files.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(source).lstrip(), encoding="utf-8")
    return root


@pytest.fixture(autouse=True)
def fresh_imports():
    """Undo what importing a plugin repo does to this process.

    A manifest puts its own ``src/`` on ``sys.path`` and imports ``gold`` and
    ``contract`` under those bare names. Left in ``sys.modules`` they are what
    the *next* test gets, so every case below that rewrites ``gold.py`` would
    silently audit the previous case's class. The runner imports each repo once
    per process and never has this problem; the test suite imports twenty.
    """
    import sys

    modules = dict(sys.modules)
    path = list(sys.path)
    yield
    sys.modules.clear()
    sys.modules.update(modules)
    sys.path[:] = path


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    """A clean plugin repo, one level below the strategies directory."""
    write(
        tmp_path / "my-strategies",
        {"src/contract.py": CONTRACT, "src/gold.py": GOOD, "manifest.py": MANIFEST},
    )
    return tmp_path


def audit_of(directory: Path, routing: Path | None = None):
    return StrategyAuditor(directory=directory, routing_file=routing).run()


def codes(report) -> set[str]:
    return {finding.code for finding in report.all_findings}


# ── The clean case ───────────────────────────────────────────────────────


def test_a_conforming_repo_passes(repo):
    report = audit_of(repo)

    assert report.ok and report.exit_code() == 0
    assert [entry.name for entry in report.strategies] == ["GOLD_EDGE_V1"]
    assert report.strategies[0].via == "manifest"


def test_it_reports_which_actions_each_strategy_can_emit(repo):
    """The report is also documentation: this column is why the interface exists."""
    entry = audit_of(repo).strategies[0]
    assert entry.signals == ("long", "short", "tp1", "tp2", "sl")


def test_the_repos_own_base_class_is_not_audited_as_a_strategy(repo):
    """It declares the interface; it does not implement it."""
    assert [entry.class_name for entry in audit_of(repo).strategies] == ["GoldEdge"]


# ── Missing signal methods ───────────────────────────────────────────────


def test_a_strategy_missing_required_methods_fails_once_per_method(repo):
    write(
        repo / "my-strategies",
        {"src/gold.py": GOOD.replace("        def tp2(self, df, context): return None\n", "")},
    )
    report = audit_of(repo)

    missing = [f for f in report.all_findings if f.code == "missing-signal-method"]
    assert [f.subject for f in missing] == ["GOLD_EDGE_V1.tp2"]
    assert not report.ok and report.exit_code() == 1


def test_the_finding_says_what_to_write(repo):
    write(
        repo / "my-strategies",
        {"src/gold.py": GOOD.replace("        def sl(self, df, context): return None\n", "")},
    )
    finding = next(f for f in audit_of(repo).all_findings if f.code == "missing-signal-method")
    assert "def sl(self, df, context)" in finding.fix


def test_the_optional_methods_are_genuinely_optional(repo):
    """``r_sl`` and ``flat`` are inherited no-ops here and nothing complains."""
    report = audit_of(repo)
    assert report.ok
    assert "r_sl" not in report.strategies[0].signals


# ── Shapes that would fail at the first bar instead ──────────────────────


def test_a_signal_method_that_cannot_be_called_with_df_and_context_fails(repo):
    write(
        repo / "my-strategies",
        {"src/gold.py": GOOD.replace("def tp1(self, df, context)", "def tp1(self, df)")},
    )
    report = audit_of(repo)

    finding = next(f for f in report.all_findings if f.code == "signal-method-arity")
    assert finding.subject == "GOLD_EDGE_V1.tp1"
    assert not report.ok


def test_var_positional_counts_as_accepting_both(repo):
    write(
        repo / "my-strategies",
        {"src/gold.py": GOOD.replace("def flat(self, df, context)", "def flat(self, *args)")},
    )
    assert audit_of(repo).ok


def test_a_class_the_engine_cannot_drive_at_all_is_reported_as_such(repo):
    """Published by the manifest, so it was certainly *meant* as a strategy."""
    write(repo / "my-strategies", {"src/gold.py": "class GoldEdge:\n    pass\n"})
    report = audit_of(repo)

    assert "undrivable" in codes(report)
    assert not report.ok


def test_a_constructor_the_runner_cannot_satisfy_is_caught_here(repo):
    """Otherwise it is a traceback at boot, with the market already open."""
    write(
        repo / "my-strategies",
        {
            "src/gold.py": GOOD.replace(
                '        name = "GOLD_EDGE_V1"',
                '        name = "GOLD_EDGE_V1"\n\n'
                "        def __init__(self, params, broker):\n"
                "            super().__init__(params)",
            )
        },
    )
    report = audit_of(repo)

    assert "not-instantiable" in codes(report)
    assert not report.ok


def test_a_timeframe_the_engine_does_not_resample_is_refused(repo):
    write(repo / "my-strategies", {"src/gold.py": GOOD.replace('"M15"', '"fortnightly"')})
    assert "bad-timeframe" in codes(audit_of(repo))


def test_a_nonsense_warmup_is_refused(repo):
    write(repo / "my-strategies", {"src/gold.py": GOOD.replace("warmup = 50", "warmup = 0")})
    assert "bad-warmup" in codes(audit_of(repo))


def test_a_nameless_strategy_is_a_warning_not_a_failure(repo):
    """It still runs — under its class name, on the subject workers subscribe to."""
    write(repo / "my-strategies", {"src/gold.py": GOOD.replace('name = "GOLD_EDGE_V1"', "")})
    report = audit_of(repo)

    assert "unnamed" in codes(report)
    assert report.ok, "it runs; it just publishes under a name nobody chose"
    assert report.exit_code(strict=True) == 1, "--strict is where this stops a deploy"


# ── Directory-wide ───────────────────────────────────────────────────────


def test_two_strategies_under_one_name_is_a_failure(repo):
    """Workers subscribe by name: two algorithms would close each other's trades."""
    write(
        repo / "my-strategies",
        {
            "manifest.py": MANIFEST.replace(
                'return {"GOLD_EDGE_V1": GoldEdge}',
                'return {"GOLD_EDGE_V1": GoldEdge, "GOLD_EDGE_V1 ".strip(): GoldEdge}',
            )
        },
    )
    # A dict cannot hold the key twice, so publish it from two repos instead.
    write(tmp := (repo / "other-strategies"), {})
    write(tmp, {"src/contract.py": CONTRACT, "src/gold.py": GOOD, "manifest.py": MANIFEST})

    report = audit_of(repo)
    assert "duplicate-name" in codes(report)
    assert not report.ok


def test_a_repo_without_a_manifest_is_flagged_but_still_passes(repo):
    # Drop the manifest *and* the src/ layout it put on sys.path: a repo the
    # scan can read is one whose files import nothing of their own.
    shutil.rmtree(repo / "my-strategies")
    write(repo / "my-strategies", {"edge.py": LOOSE_FILE})

    report = audit_of(repo)

    assert "no-manifest" in codes(report)
    assert report.ok, "the scan works; it is just less deliberate"
    assert [entry.via for entry in report.strategies] == ["scan"]


def test_a_manifest_that_raises_is_a_failure_not_a_silent_pass(repo):
    """The finding this whole service exists for.

    The loader logs the traceback and carries on, which is right for a process
    holding positions. Here it would mean a repo whose every strategy failed to
    import reporting "0 strategies, 0 errors" — a green deploy that trades
    nothing. The usual cause is a dependency the plugins need and the venv does
    not have.
    """
    write(
        repo / "my-strategies",
        {
            "manifest.py": (
                "import qte_dependency_that_must_not_exist_7f31c9\n\n\n"
                "def load_all():\n    return {}\n"
            )
        },
    )
    report = audit_of(repo)

    finding = next(f for f in report.all_findings if f.code == "load-failed")
    assert "qte_dependency_that_must_not_exist_7f31c9" in finding.message
    assert "strategy-deps" in finding.fix
    assert not report.ok, "0 strategies and a green exit code is the failure mode"


def test_a_manifest_missing_its_hook_is_reported(repo):
    write(repo / "my-strategies", {"manifest.py": "ALIASES = {}\n"})
    report = audit_of(repo)

    assert "load-failed" in codes(report)
    assert "load_all()" in next(f for f in report.all_findings if f.code == "load-failed").message


def test_a_loose_file_that_will_not_import_is_reported(repo):
    write(repo, {"broken.py": "this is not python(\n"})
    report = audit_of(repo)

    assert "load-failed" in codes(report)
    assert not report.ok


def test_a_repo_carrying_two_manifests_is_a_finding_not_a_crash(repo):
    """The loader raises on this; the audit's job is to report, not to die."""
    manifest = repo / "my-strategies" / "manifest.py"
    (manifest.parent / "strategies.py").write_text(manifest.read_text(), encoding="utf-8")

    report = audit_of(repo)
    assert "ambiguous-manifest" in codes(report)
    assert not report.ok


def test_a_missing_directory_is_a_warning(tmp_path):
    """A fresh clone has none — failing CI over that teaches people to skip the audit."""
    report = audit_of(tmp_path / "absent")

    assert "no-strategies-directory" in codes(report)
    assert report.ok and report.exit_code(strict=True) == 1


# ── Cross-checking the routing table ─────────────────────────────────────


def routing_file(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "strategies_mapping.toml"
    path.write_text(textwrap.dedent(body).lstrip(), encoding="utf-8")
    return path


def test_a_routing_entry_nobody_publishes_fails(repo, tmp_path):
    """The symptom without this is a symbol that quietly trades nothing."""
    table = routing_file(tmp_path, '[symbols.XAUUSD]\nstrategies = ["TYPOD_NAME"]\n')
    report = audit_of(repo, routing=table)

    finding = next(f for f in report.all_findings if f.code == "routed-to-nothing")
    assert finding.subject == "TYPOD_NAME" and "XAUUSD" in finding.message
    assert not report.ok


def test_a_strategy_nobody_routed_is_a_warning(repo, tmp_path):
    table = routing_file(tmp_path, "[symbols.XAUUSD]\nstrategies = []\n")
    report = audit_of(repo, routing=table)

    assert "unrouted-strategy" in codes(report)
    assert report.ok, "deployed and never called is harmless, if usually a forgotten rename"


def test_a_matching_table_passes(repo, tmp_path):
    table = routing_file(tmp_path, '[symbols.XAUUSD]\nstrategies = ["GOLD_EDGE_V1"]\n')
    report = audit_of(repo, routing=table)

    assert report.ok and not report.all_findings
    assert report.routing.symbols_for("GOLD_EDGE_V1") == ["XAUUSD"]


def test_a_table_that_will_not_parse_is_reported_rather_than_raised(repo, tmp_path):
    """It takes the runner down at boot; better to learn that in CI."""
    table = routing_file(tmp_path, '[symbols.XAUUSD]\nstrategies = "GOLD_EDGE_V1"\n')
    report = audit_of(repo, routing=table)

    assert "routing-unreadable" in codes(report)
    assert not report.ok


def test_no_routing_file_means_no_routing_findings(repo, tmp_path):
    report = audit_of(repo, routing=tmp_path / "absent.toml")
    assert not codes(report) & {"routed-to-nothing", "unrouted-strategy"}


# ── Rendering ────────────────────────────────────────────────────────────


def test_every_finding_carries_a_fix(repo):
    write(repo / "my-strategies", {"src/gold.py": "class GoldEdge:\n    pass\n"})
    findings = audit_of(repo).all_findings

    assert findings
    assert all(finding.fix for finding in findings)


def test_the_json_report_is_serialisable_and_says_whether_it_passed(repo):
    import json

    payload = json.loads(json.dumps(audit_of(repo).as_dict()))

    assert payload["ok"] is True
    assert payload["counts"] == {"strategies": 1, "errors": 0, "warnings": 0}
    assert payload["strategies"][0]["name"] == "GOLD_EDGE_V1"


def test_the_text_and_markdown_reports_name_the_failure(repo):
    write(
        repo / "my-strategies",
        {"src/gold.py": GOOD.replace("        def tp2(self, df, context): return None\n", "")},
    )
    report = audit_of(repo)

    assert "tp2" in report.to_text() and "FAIL" in report.to_text()
    assert "tp2" in report.to_markdown() and "FAIL" in report.to_markdown()


def test_the_terminal_report_stays_ascii(repo):
    """A Windows console is cp1252, and this is what an operator reads at deploy.

    An arrow or an em dash here is a ``UnicodeEncodeError`` instead of a report,
    at the moment someone is trying to find out why the deploy failed.
    """
    write(
        repo / "my-strategies",
        {"src/gold.py": GOOD.replace("        def tp2(self, df, context): return None\n", "")},
    )
    audit_of(repo).to_text().encode("ascii")


def test_severity_covers_only_what_the_exit_code_distinguishes():
    assert {member.value for member in Severity} == {"error", "warning"}
