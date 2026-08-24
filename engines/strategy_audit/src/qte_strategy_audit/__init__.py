"""Validate what ``__strategies__/`` publishes, before it trades.

The loader is deliberately forgiving: a strategy it cannot drive is logged and
skipped so one broken file does not stop the other four. That is right for a
running process and wrong for a deploy — "skipped" and "there were none" look
identical in a log until the P&L does not arrive.

This service is the strict reading of the same directory. It collects every
class each mounted repo offers, checks it against the QTE signal contract in
:mod:`qte_shared.strategy_base`, cross-checks the routing table against what was
actually found, and exits non-zero when something is wrong. Run it in CI and
before ``make up``.
"""

from qte_strategy_audit.auditor import StrategyAuditor, audit
from qte_strategy_audit.contract import Finding, Severity, StrategyAudit
from qte_strategy_audit.report import AuditReport

__all__ = [
    "AuditReport",
    "Finding",
    "Severity",
    "StrategyAudit",
    "StrategyAuditor",
    "audit",
]
