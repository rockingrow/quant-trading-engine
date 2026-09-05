"""Walk ``__strategies__/``, judge everything in it, cross-check the routing.

The loader already knows how to find things — manifests at a repo root, loose
files below it, repo furniture skipped — so this reuses
:meth:`~qte_shared.plugin_loader.StrategyLoader.collect` rather than growing a
second, subtly different walker. What the auditor adds is that it keeps what
the loader discards, and that it looks at the directory as a whole: duplicate
names, repos that never declared a manifest, and a routing table pointing at
strategies nobody publishes.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from qte_shared.config import settings
from qte_shared.logging_setup import get_logger
from qte_shared.plugin_loader import (
    MANIFEST_FILENAMES,
    Candidate,
    LoadFailure,
    StrategyLoader,
)
from qte_shared.routing import SymbolRouting

from qte_strategy_audit.contract import Finding, Severity, StrategyAudit, check_strategy
from qte_strategy_audit.report import AuditReport

log = get_logger(__name__)


@dataclass(slots=True)
class StrategyAuditor:
    """Audits one strategies directory against one routing table."""

    directory: Path
    routing_file: Path | None = None

    def run(self) -> AuditReport:
        directory = Path(self.directory)
        findings: list[Finding] = []

        if not directory.is_dir():
            # Not an error. A fresh clone has no __strategies__/ — it is left
            # untracked so `git clone <repo> __strategies__` has an empty
            # destination — and failing CI over that would teach everyone to
            # skip the audit.
            findings.append(
                Finding(
                    code="no-strategies-directory",
                    severity=Severity.WARNING,
                    subject=str(directory),
                    message="the strategies directory does not exist, so nothing was audited",
                    fix="clone your strategy repo into it, or point QTE_ENGINE__STRATEGIES_DIR "
                    "somewhere else",
                )
            )
            return AuditReport(directory=directory, findings=findings)

        loader = StrategyLoader(directory)
        try:
            candidates = loader.collect()
        except RuntimeError as error:
            # The loader refuses a repo carrying two manifests. That is a hard
            # stop for it and a finding for us: the audit's job is to report
            # everything it can, not to die on the first thing it meets.
            findings.append(
                Finding(
                    code="ambiguous-manifest",
                    severity=Severity.ERROR,
                    subject=str(directory),
                    message=str(error),
                    fix=f"keep exactly one of {' / '.join(MANIFEST_FILENAMES)} per repo",
                )
            )
            return AuditReport(directory=directory, findings=findings)

        audits = [
            check_strategy(candidate.name, candidate.obj, candidate.source, candidate.via)
            for candidate in candidates
        ]
        findings.extend(_load_failures(loader.failures))
        findings.extend(_duplicate_names(audits))
        findings.extend(_repos_without_a_manifest(directory, candidates))

        routing = self._load_routing(findings)
        findings.extend(_routing_findings(routing, audits))

        return AuditReport(
            directory=directory, strategies=audits, findings=findings, routing=routing
        )

    def _load_routing(self, findings: list[Finding]) -> SymbolRouting:
        """Parse the routing table, turning a malformed one into a finding.

        A table that will not parse takes the runner down at boot. Reporting it
        here — with the same message the runner would have raised — is the
        difference between finding out in CI and finding out at the open.
        """
        if self.routing_file is None:
            return SymbolRouting()
        try:
            return SymbolRouting.load(self.routing_file)
        except (ValueError, OSError) as error:
            findings.append(
                Finding(
                    code="routing-unreadable",
                    severity=Severity.ERROR,
                    subject=str(self.routing_file),
                    message=f"the routing table could not be read: {error}",
                    fix="compare it against config/strategies_mapping.example.toml",
                    source=Path(self.routing_file),
                )
            )
            return SymbolRouting()


def audit(directory: Path | str | None = None, routing: Path | str | None = None) -> AuditReport:
    """Audit the configured directory and routing table, or the ones given."""
    return StrategyAuditor(
        directory=Path(directory or settings.engine.strategies_dir),
        routing_file=Path(routing) if routing is not None else settings.engine.routing_file,
    ).run()


# ── Directory-wide checks ────────────────────────────────────────────────


def _load_failures(failures: list[LoadFailure]) -> list[Finding]:
    """A manifest that raised is a repo publishing nothing, and it must say so.

    This is the finding the audit exists for. The loader logs the traceback and
    carries on, which in a running process is right; here it would mean a repo
    whose every strategy failed to import passing as "0 strategies, 0 errors" —
    a green deploy that trades nothing. The commonest cause is a dependency the
    plugins need and the venv does not have, which is `make strategy-mount`.
    """
    return [
        Finding(
            code="load-failed",
            severity=Severity.ERROR,
            subject=failure.path.name,
            message=failure.detail,
            fix=(
                "if it is a missing import, install the plugin repo's dependencies with "
                "`make strategy-mount STRATEGY=<name>` - they are imported into the "
                "runner's own process"
            ),
            source=failure.path,
        )
        for failure in failures
    ]


def _duplicate_names(audits: list[StrategyAudit]) -> list[Finding]:
    """Two strategies under one name publish to one broker subject.

    Workers subscribe by strategy name, so this is two algorithms' signals
    arriving on the same subject and closing one another's positions. The
    runner warns; the audit fails, because there is no deploy where this is
    what someone meant.
    """
    by_name: dict[str, list[StrategyAudit]] = defaultdict(list)
    for entry in audits:
        by_name[entry.name].append(entry)

    return [
        Finding(
            code="duplicate-name",
            severity=Severity.ERROR,
            subject=name,
            message="published by "
            + ", ".join(f"{entry.class_name} in {entry.source}" for entry in entries),
            fix="rename one - the name is the NATS subject the broker's workers subscribe to",
            source=entries[0].source,
        )
        for name, entries in by_name.items()
        if len(entries) > 1
    ]


def _repos_without_a_manifest(directory: Path, candidates: list[Candidate]) -> list[Finding]:
    """A cloned repo found only by the scan is a repo that lost its alias table.

    The scan works, which is the problem: the repo runs, but every module in it
    was imported and every strategy-shaped class in the tree was registered —
    including the experiment someone left in a branch. Worth saying out loud,
    not worth failing on, since a directory of loose example files is the
    documented other way in.
    """
    scanned_repos = {
        candidate.source.parent
        for candidate in candidates
        if candidate.via == "scan" and candidate.source.parent != directory
    }
    findings: list[Finding] = []
    for repo in sorted(scanned_repos):
        # Walk up to the child of the strategies directory — the clone root.
        root = repo
        while root.parent != directory and root.parent != root:
            root = root.parent
        if any((root / name).is_file() for name in MANIFEST_FILENAMES):
            continue
        findings.append(
            Finding(
                code="no-manifest",
                severity=Severity.WARNING,
                subject=str(root.name),
                message=(
                    "this repo declares no manifest, so every module in it was imported "
                    "and every strategy-shaped class registered"
                ),
                fix=f"add {MANIFEST_FILENAMES[0]} at its root exposing "
                "load_all() -> {alias: class}",
                source=root,
            )
        )
    return findings


def _routing_findings(routing: SymbolRouting, audits: list[StrategyAudit]) -> list[Finding]:
    """Does the table name things that exist, and does everything found get used?

    Both directions matter and they fail differently. A name in the table that
    nobody publishes means a symbol trades nothing, and reads in a log exactly
    like a strategy that found no setups. A strategy nobody routed means code
    was deployed and never called — harmless, but almost always the half of a
    rename that was forgotten.
    """
    if not routing:
        return []

    published = {entry.name for entry in audits}
    findings = [
        Finding(
            code="routed-to-nothing",
            severity=Severity.ERROR,
            subject=name,
            message=(
                f"the routing table sends {', '.join(routing.symbols_for(name))} to {name!r}, "
                "which no repo publishes"
            ),
            fix="check the spelling against `make strategies`, or the manifest alias it renamed",
            source=routing.source,
        )
        for name in routing.strategies
        if name not in published
    ]
    findings += [
        Finding(
            code="unrouted-strategy",
            severity=Severity.WARNING,
            subject=entry.name,
            message="loaded but routed to no symbol, so it will never see a candle",
            fix=f"add it to a [symbols.<SYMBOL>].strategies list in {routing.source}",
            source=entry.source,
        )
        for entry in audits
        if not routing.symbols_for(entry.name)
    ]
    return findings
