"""The audit's result, and the three ways of reading it.

Text for a terminal, Markdown for a pull request, JSON for anything that has to
act on it. The same object renders all three so a CI job and a human never see
different answers to the same question.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from qte_shared.strategies.mapping import SymbolMapping
from qte_strategy_audit.contract import Finding, Severity, StrategyAudit


@dataclass(slots=True)
class AuditReport:
    """Everything one run of the audit found."""

    directory: Path
    strategies: list[StrategyAudit] = field(default_factory=list)
    #: Findings about the directory or the mapping table rather than one class.
    findings: list[Finding] = field(default_factory=list)
    mapping: SymbolMapping = field(default_factory=SymbolMapping)
    #: alias → manifest, for what a repository keeps out of service through its
    #: ``load_disabled`` hook. Listed, never audited — nothing of it is imported —
    #: and not a finding: switching a strategy off is not a defect. A mapping
    #: table that still names one is, and is among :attr:`findings`.
    disabled: dict[str, Path] = field(default_factory=dict)

    # ── Rollup ────────────────────────────────────────────────────────

    @property
    def all_findings(self) -> list[Finding]:
        return [finding for entry in self.strategies for finding in entry.findings] + self.findings

    @property
    def errors(self) -> list[Finding]:
        return [f for f in self.all_findings if f.severity is Severity.ERROR]

    @property
    def warnings(self) -> list[Finding]:
        return [f for f in self.all_findings if f.severity is Severity.WARNING]

    @property
    def ok(self) -> bool:
        return not self.errors

    def exit_code(self, *, strict: bool = False) -> int:
        """0 when it passed. ``strict`` promotes warnings to failures."""
        if self.errors or (strict and self.warnings):
            return 1
        return 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "directory": str(self.directory),
            "mapping": str(self.mapping.source) if self.mapping.source else None,
            "ok": self.ok,
            "counts": {
                "strategies": len(self.strategies),
                "errors": len(self.errors),
                "warnings": len(self.warnings),
                "disabled": len(self.disabled),
            },
            "strategies": [entry.as_dict() for entry in self.strategies],
            "disabled": [
                {"name": name, "source": str(source)}
                for name, source in sorted(self.disabled.items())
            ],
            "findings": [finding.as_dict() for finding in self.findings],
        }

    def _summary(self) -> str:
        summary = (
            f"{len(self.strategies)} strategies, "
            f"{len(self.errors)} errors, {len(self.warnings)} warnings"
        )
        return summary + (f", {len(self.disabled)} disabled" if self.disabled else "")

    # ── Rendering ─────────────────────────────────────────────────────

    def to_text(self) -> str:
        """A terminal report: one block per strategy, findings indented under it.

        Deliberately ASCII. A Windows console defaults to cp1252, and an arrow
        or an em dash in the one output an operator reads at deploy time is a
        ``UnicodeEncodeError`` instead of a report. The Markdown and JSON
        renderings below are written for files and keep their typography.
        """
        lines = [f"Strategy audit - {self.directory}"]
        if self.mapping.source:
            lines.append(f"Mapping table  - {self.mapping.source}")
        lines.append("")

        if not self.strategies and not self.disabled:
            lines.append("  No strategies found.")
        for entry in self.strategies:
            status = "ok" if entry.ok else "FAIL"
            signals = ", ".join(entry.signals) or "none"
            lines.append(f"  [{status:>4}] {entry.name}  ({entry.class_name}, via {entry.via})")
            lines.append(f"         {entry.source}")
            lines.append(f"         signals: {signals}")
            lines.extend(_finding_lines(entry.findings, indent=9))
            lines.append("")
        for name, source in sorted(self.disabled.items()):
            lines.append(f"  [ off] {name}  (disabled by its repository, not loaded)")
            lines.append(f"         {source}")
            lines.append("")

        if self.findings:
            lines.append("  Directory and mapping")
            lines.extend(_finding_lines(self.findings, indent=9))
            lines.append("")

        lines.append(self._summary())
        return "\n".join(lines)

    def to_markdown(self) -> str:
        """A report that survives being pasted into a pull request."""
        headline = "PASS" if self.ok else "FAIL"
        lines = [
            f"# Strategy audit — {headline}",
            "",
            f"- Directory: `{self.directory}`",
        ]
        if self.mapping.source:
            lines.append(f"- Mapping: `{self.mapping.source}`")
        lines += [
            f"- {self._summary()}",
            "",
            "| Strategy | Class | Via | Signals | Status |",
            "| --- | --- | --- | --- | --- |",
        ]
        for entry in self.strategies:
            lines.append(
                f"| `{entry.name}` | `{entry.class_name}` | {entry.via} | "
                f"{', '.join(entry.signals) or '—'} | {'ok' if entry.ok else '**FAIL**'} |"
            )
        for name in sorted(self.disabled):
            lines.append(f"| `{name}` | — | manifest | — | disabled by its repository |")

        findings = self.all_findings
        if findings:
            lines += ["", "## Findings", ""]
            for finding in findings:
                lines.append(
                    f"- **{finding.severity.marker}** `{finding.subject}` "
                    f"({finding.code}) — {finding.message}"
                )
                lines.append(f"  - Fix: {finding.fix}")
        return "\n".join(lines)


def _finding_lines(findings: list[Finding], *, indent: int) -> list[str]:
    pad = " " * indent
    lines: list[str] = []
    for finding in findings:
        lines.append(f"{pad}{finding.severity.marker} {finding.subject}: {finding.message}")
        lines.append(f"{pad}     -> {finding.fix}")
    return lines
