"""What "a valid QTE strategy" means, expressed as checks that name their fix.

:mod:`qte_shared.strategy_base` holds the contract; this holds the *diagnosis*.
The split is deliberate: the loader only ever needs a yes or no, and paying for
signature inspection and instantiation on every process start to produce a
richer answer nobody reads would be the wrong trade. The audit is where the
richer answer is the whole point.

Every finding carries a ``fix``. A report that says "missing tp2" and stops
leaves the reader to work out whether that means "write the method" or "rename
the one you have"; saying both is a line of text and saves the round trip.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

from qte_shared.strategy_base import (
    OPTIONAL_SIGNAL_METHODS,
    REQUIRED_SIGNAL_METHODS,
    SIGNAL_METHOD_ARITY,
    SIGNAL_METHODS,
    defines_signal_method,
    implemented_signal_methods,
    implements_strategy_contract,
    missing_signal_methods,
)
from qte_shared.timeframes import normalize_timeframe


class Severity(str, Enum):
    """How much a finding should stop a deploy.

    ``ERROR`` means the strategy will not do what its author intended — it
    cannot be driven, or it silently cannot emit an action it looks like it
    emits. ``WARNING`` means it will run but something is worth a second look.
    """

    ERROR = "error"
    WARNING = "warning"

    @property
    def marker(self) -> str:
        return "FAIL" if self is Severity.ERROR else "WARN"


@dataclass(frozen=True, slots=True)
class Finding:
    """One thing wrong, where it is, and what to do about it."""

    code: str
    severity: Severity
    subject: str
    message: str
    fix: str
    source: Path | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "severity": self.severity.value,
            "subject": self.subject,
            "message": self.message,
            "fix": self.fix,
            "source": str(self.source) if self.source else None,
        }


@dataclass(slots=True)
class StrategyAudit:
    """One strategy class, everything known about it, and what is wrong."""

    name: str
    class_name: str
    source: Path
    via: str
    signals: tuple[str, ...] = ()
    findings: list[Finding] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not any(finding.severity is Severity.ERROR for finding in self.findings)

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "class": self.class_name,
            "source": str(self.source),
            "via": self.via,
            "signals": list(self.signals),
            "ok": self.ok,
            "findings": [finding.as_dict() for finding in self.findings],
        }


def check_strategy(name: str, candidate: Any, source: Path, via: str) -> StrategyAudit:
    """Run every per-class check and return the result.

    Checks are structural throughout — ``getattr`` and ``inspect``, never
    ``issubclass`` — because a plugin repository restates the contract on its
    own side rather than importing ours. See the module docstring of
    :mod:`qte_shared.strategy_base`.
    """
    audit = StrategyAudit(
        name=name,
        class_name=getattr(candidate, "__name__", str(candidate)),
        source=source,
        via=via,
        signals=implemented_signal_methods(candidate),
    )
    audit.findings.extend(_check_drivable(name, candidate, source))
    audit.findings.extend(_check_signal_surface(name, candidate, source))
    audit.findings.extend(_check_signatures(name, candidate, source))
    audit.findings.extend(_check_metadata(name, candidate, source))
    audit.findings.extend(_check_instantiable(name, candidate, source))
    return audit


# ── The driving contract ─────────────────────────────────────────────────


def _check_drivable(name: str, candidate: Any, source: Path) -> list[Finding]:
    """Can the engine call this at all?

    Reported first because everything below is moot when it fails: a class the
    runner will not instantiate cannot emit a wrong action.
    """
    if implements_strategy_contract(candidate):
        return []
    return [
        Finding(
            code="undrivable",
            severity=Severity.ERROR,
            subject=name,
            message=(
                "the engine cannot drive this class - it needs a concrete "
                "on_candle_closed, on_start, on_stop and history_window, plus "
                "name, timeframe and warmup attributes"
            ),
            fix=(
                "subclass qte_shared.strategy_base.SignalStrategy, or restate that "
                "interface on the plugin's side - see 'How strategies are found' in "
                "the README"
            ),
            source=source,
        )
    ]


# ── The signal surface ───────────────────────────────────────────────────


def _check_signal_surface(name: str, candidate: Any, source: Path) -> list[Finding]:
    """One finding per required action the class cannot emit.

    Per method rather than one summary line, because the fix is per method and
    a reader fixing four of five wants the fifth to still be listed on the next
    run rather than a count that went from 5 to 1.
    """
    return [
        Finding(
            code="missing-signal-method",
            severity=Severity.ERROR,
            subject=f"{name}.{missing}",
            message=f"required signal method {missing}() is not implemented",
            fix=(
                f"def {missing}(self, df, context) -> IntentResult: return None - an "
                "explicit 'never' is an answer; an absence is not"
            ),
            source=source,
        )
        for missing in missing_signal_methods(candidate)
    ]


def _check_signatures(name: str, candidate: Any, source: Path) -> list[Finding]:
    """Every implemented signal method must take ``(df, context)``.

    The dispatcher calls them positionally, so a method written as
    ``def long(self, df)`` raises a ``TypeError`` on the first bar that reaches
    it — which, for an exit hook on a strategy that has not traded yet, can be
    weeks after the deploy that introduced it.
    """
    findings: list[Finding] = []
    for method_name in SIGNAL_METHODS:
        if not defines_signal_method(candidate, method_name):
            continue
        method = getattr(candidate, method_name, None)
        try:
            signature = inspect.signature(method)
        except (TypeError, ValueError):
            # A C-implemented or otherwise uninspectable callable. Rare enough
            # in a strategy that saying so beats guessing either way.
            findings.append(
                Finding(
                    code="signal-method-uninspectable",
                    severity=Severity.WARNING,
                    subject=f"{name}.{method_name}",
                    message="signature could not be read, so its arity was not checked",
                    fix=f"define it as a plain method: def {method_name}(self, df, context)",
                    source=source,
                )
            )
            continue

        if not _accepts_two_positionals(signature):
            findings.append(
                Finding(
                    code="signal-method-arity",
                    severity=Severity.ERROR,
                    subject=f"{name}.{method_name}",
                    message=(
                        f"{method_name}{signature} cannot be called as {method_name}(df, context)"
                    ),
                    fix=f"def {method_name}(self, df, context) -> IntentResult",
                    source=source,
                )
            )
    return findings


def _accepts_two_positionals(signature: inspect.Signature) -> bool:
    """Whether ``(df, context)`` can be passed positionally, ``*args`` included."""
    positional = 0
    for parameter in signature.parameters.values():
        if parameter.name == "self":
            continue
        if parameter.kind is inspect.Parameter.VAR_POSITIONAL:
            return True
        if parameter.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        ):
            positional += 1
        elif parameter.default is inspect.Parameter.empty and parameter.kind in (
            inspect.Parameter.KEYWORD_ONLY,
        ):
            # A required keyword-only argument the dispatcher never passes.
            return False
    return positional >= SIGNAL_METHOD_ARITY


# ── Metadata the runner reads before the first bar ───────────────────────


def _check_metadata(name: str, candidate: Any, source: Path) -> list[Finding]:
    findings: list[Finding] = []

    declared = getattr(candidate, "name", "")
    if not declared:
        findings.append(
            Finding(
                code="unnamed",
                severity=Severity.WARNING,
                subject=name,
                message=(
                    "no name attribute, so the class name becomes the NATS subject "
                    "the broker's workers subscribe to"
                ),
                fix='set name = "..." to the strategy the workers are configured for',
                source=source,
            )
        )

    timeframe = getattr(candidate, "timeframe", None)
    try:
        normalize_timeframe(str(timeframe))
    except (ValueError, KeyError):
        findings.append(
            Finding(
                code="bad-timeframe",
                severity=Severity.ERROR,
                subject=name,
                message=f"timeframe {timeframe!r} is not one QTE resamples",
                fix="use one of qte_shared.timeframes.TIMEFRAME_SECONDS, e.g. M1, M15, H1",
                source=source,
            )
        )

    warmup = getattr(candidate, "warmup", None)
    if not isinstance(warmup, int) or isinstance(warmup, bool) or warmup < 1:
        findings.append(
            Finding(
                code="bad-warmup",
                severity=Severity.ERROR,
                subject=name,
                message=f"warmup {warmup!r} is not a positive number of candles",
                fix="set warmup to the bars your slowest indicator needs before it is valid",
                source=source,
            )
        )

    optional = [
        method for method in OPTIONAL_SIGNAL_METHODS if defines_signal_method(candidate, method)
    ]
    required = [
        method for method in REQUIRED_SIGNAL_METHODS if defines_signal_method(candidate, method)
    ]
    if optional and not required:
        findings.append(
            Finding(
                code="optional-only",
                severity=Severity.WARNING,
                subject=name,
                message=(
                    f"implements only the optional {', '.join(optional)} - this looks like a "
                    "mixin or a helper rather than a strategy"
                ),
                fix="if it is a helper, move it to a _-prefixed file so the scan skips it",
                source=source,
            )
        )
    return findings


def _check_instantiable(name: str, candidate: Any, source: Path) -> list[Finding]:
    """The runner constructs these with a params dict; find out here if it cannot.

    Cheap, and it catches the class whose ``__init__`` demands a constructor
    argument the engine has no way to supply — a failure that otherwise
    surfaces as a traceback at boot with the market already open.
    """
    try:
        candidate({})
    except Exception as error:  # noqa: BLE001 — the point is to survive anything
        return [
            Finding(
                code="not-instantiable",
                severity=Severity.ERROR,
                subject=name,
                message=(
                    f"{type(error).__name__} constructing it with an empty params dict: {error}"
                ),
                fix="accept params as the only argument: def __init__(self, params=None)",
                source=source,
            )
        ]
    return []
