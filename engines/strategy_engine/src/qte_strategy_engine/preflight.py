"""Audit ``__strategies__/`` in the runner's own process, before it trades.

The loader is forgiving on purpose: a strategy it cannot drive is logged and
skipped so one broken file does not stop the other four. That is the right
behaviour for a process that is already trading, and it is how a deploy goes
out with half the book quietly missing — "skipped" and "there were none" read
identically in a log.

This is the opt-in gate. It runs :mod:`qte_strategy_audit` against the same
directory the runner is about to load, in the runner's own process, and three
things follow from *where* it runs rather than from what it checks:

* It reads the files that will actually be imported — the same bind mount, at
  the same moment — not a copy some other container saw.
* It re-runs on **every** start. A ``depends_on`` on the one-shot audit
  container gates on a container that has already exited successfully, so it
  answers the question once, when the stack first came up, and never again
  however many times the runner restarts or the mount changes underneath it.
* A refusal stops exactly one service. The strategies are the runner's problem
  and nobody else's: data ingestion has never imported one, and taking the feed
  down over a strategy defect would also cost the candles needed to debug it.

``QTE_RUNNER__AUDIT_ON_START`` chooses how much authority it has:

``off``
    Do not audit.
``warn``
    Audit and log the report; start regardless. The default, because it changes
    which strategies run not at all — it only makes the loader's silence
    legible.
``error``
    Refuse to start when the audit found errors.
``strict``
    Refuse on warnings too, matching ``qte-strategy-audit --strict``.

The audit imports the plugin modules and ``_build_slots`` then imports them
again — the loader execs by file path rather than consulting ``sys.modules``,
so module-level code in a strategy file runs twice per start. Deliberate, and
the reason this is not on by default in ``strict``: the alternative is handing
the runner the auditor's class objects, which would make the thing that
validates and the thing that trades share a cache and stop being independent
readings of the directory.
"""

from __future__ import annotations

from typing import Literal

from qte_shared.logging_setup import get_logger
from qte_strategy_audit import AuditReport, audit

from qte_strategy_engine.settings import runner_settings

log = get_logger(__name__)

#: Values of ``QTE_RUNNER__AUDIT_ON_START``. Ordered by how much they stop.
AuditMode = Literal["off", "warn", "error", "strict"]


class StrategyAuditFailed(RuntimeError):
    """The pre-flight audit found what the configured mode refuses to trade."""


def run_preflight_audit(mode: AuditMode | None = None) -> AuditReport | None:
    """Audit the strategies directory; raise when the mode says not to trade it.

    Returns the report, or ``None`` when auditing is off. Raising rather than
    returning a verdict is the point: the caller is a ``start()`` that has not
    connected to anything yet, and "refuse to start" is the only honest
    response to a book the operator said they would not trade.
    """
    chosen: AuditMode = mode or runner_settings.audit_on_start
    if chosen == "off":
        log.debug("Pre-flight strategy audit disabled (QTE_RUNNER__AUDIT_ON_START=off)")
        return None

    report = audit()
    # `warn` never blocks whatever it finds; the other two differ only in
    # whether warnings count, which is exactly what exit_code(strict=) decides.
    blocking = chosen != "warn" and report.exit_code(strict=chosen == "strict") != 0

    write = log.error if blocking or report.errors else log.info
    write("Pre-flight strategy audit (mode=%s)\n%s", chosen, report.to_text())

    if blocking:
        raise StrategyAuditFailed(
            f"{len(report.errors)} error(s) and {len(report.warnings)} warning(s) in "
            f"{report.directory}, and QTE_RUNNER__AUDIT_ON_START={chosen}. Refusing to "
            "trade this book — fix what the report lists, or set the mode to 'warn' to "
            "start on whatever the loader can drive."
        )
    if report.errors or report.warnings:
        log.warning(
            "Starting anyway: %d error(s), %d warning(s) in %s. Those strategies will be "
            "skipped by the loader; set QTE_RUNNER__AUDIT_ON_START=error to refuse instead.",
            len(report.errors),
            len(report.warnings),
            report.directory,
        )
    return report


__all__ = ["AuditMode", "StrategyAuditFailed", "run_preflight_audit"]
