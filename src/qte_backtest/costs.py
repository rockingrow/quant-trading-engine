"""Broker cost profiles: what a replay charges a fill, per symbol.

A backtest prices its own fills — live, the broker does it — and the three
numbers it needs are not properties of a run but of the account the run is
pretending to trade. ``QTE_ACCOUNT__SPREAD`` holds one number for the whole
engine, which stops being right the moment a second symbol is replayed: the
default 0.04 is oil's spread, while the measured XAUUSDm median is 0.20
(2023-24), 0.16 (2025) and 0.26 (2026) — see
``data/audits/2026-09-26-mt5-gold-stoch-rsi-m15-xauusd.md``. Passing
``--spread`` per run moves that problem rather than solving it: forgetting the
flag prices gold six times too cheaply, and the report says only "0.04" with
no hint that nobody meant it.

So the numbers move into a file the operator owns, keyed by symbol and grouped
into *profiles*:

.. code-block:: toml

    default_profile = "exness_standard"

    [profiles.exness_standard.defaults]
    spread = 0.0
    slippage = 0.0
    commission_per_unit = 0.0

    [profiles.exness_standard.symbols.XAUUSD]
    spread = 0.25
    slippage = 0.05
    commission_per_unit = 0.0

A profile is a whole cost model rather than a spread table, because an account
type is a whole cost model: a Standard account hides its fee inside the spread
and charges no commission, a Zero/Raw account quotes a tighter spread and bills
the commission separately. Which of the two a strategy survives is a real
question, and ``--cost-profile`` answers it without editing a file.

**Nothing here reaches the live path.** The runner's fills come from the broker,
so this module is read by the backtest alone — which is why it lives in
``qte_backtest`` and not in ``qte_shared``.

Resolution, highest precedence first: an explicit ``--spread`` / ``--slippage``
/ ``--commission`` flag, the chosen profile's entry for the symbol, that
profile's ``defaults``, then ``QTE_ACCOUNT__*``. Every key resolves on its own,
so a profile stating only a spread leaves the other two to the fallback, and
:class:`CostOrigin` records which layer won for each one. A report whose prices
cannot be traced back is a report nobody can reproduce.

**A malformed file is an error, loudly** — an unknown key, a negative number, a
profile name that does not exist. The failure this file exists to prevent is a
silently wrong cost, and "misspelled, therefore ignored" is exactly that.
"""

from __future__ import annotations

import tomllib
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from qte_shared.config import settings
from qte_shared.logging_setup import get_logger

log = get_logger(__name__)

#: The costs a profile may state. The names are :class:`CostModel`'s fields and
#: the ``QTE_ACCOUNT__`` variables they fall back to, deliberately: one spelling
#: from the TOML table through to the report.
COST_KEYS: tuple[str, ...] = ("spread", "slippage", "commission_per_unit")

#: Top-level table holding one sub-table per profile.
PROFILES_TABLE = "profiles"

#: Inside a profile: the costs every symbol it does not name falls back to.
DEFAULTS_TABLE = "defaults"

#: Inside a profile: one cost table per symbol.
SYMBOLS_TABLE = "symbols"

#: Top-level key naming the profile a run uses when it names none itself.
DEFAULT_PROFILE_KEY = "default_profile"

#: Values of :attr:`CostOrigin.sources` — which layer a number came from.
FROM_FLAG = "flag"
FROM_SYMBOL = "profile.symbols"
FROM_DEFAULTS = "profile.defaults"
FROM_ACCOUNT = "account"


@dataclass(frozen=True, slots=True)
class CostOrigin:
    """Where a run's fill costs came from. Report metadata, never pricing.

    Carried beside the numbers rather than inside them: the fill simulator has
    no use for this, and the report cannot do without it — two runs of one
    strategy that differ only in their account type are otherwise two files of
    numbers with nothing on either saying which is which.
    """

    #: The profile that priced the run, or empty when no profile file applied.
    profile: str = ""
    #: The file that profile was read from, as written in the settings.
    profile_file: str = ""
    #: Cost key → the layer that supplied it: ``flag``, ``profile.symbols``,
    #: ``profile.defaults`` or ``account``.
    sources: Mapping[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """The shape the JSON report's ``costs`` block carries.

        Keys are present even when nothing applied — ``null`` for a run priced
        entirely from flags and the environment — so a consumer reads one shape
        and not two.
        """
        return {
            "profile": self.profile or None,
            "profile_file": self.profile_file or None,
            "sources": dict(self.sources),
        }

    def summary(self) -> str:
        """One line for the Markdown report and the run log."""
        if self.profile:
            overridden = sorted(key for key, name in self.sources.items() if name == FROM_FLAG)
            flagged = (
                f", {', '.join(overridden)} overridden on the command line" if overridden else ""
            )
            return f"profile {self.profile} ({self.profile_file}){flagged}"
        if any(name == FROM_FLAG for name in self.sources.values()):
            return "command-line flags, no cost profile"
        return "QTE_ACCOUNT__* defaults, no cost profile"


@dataclass(frozen=True, slots=True)
class ResolvedCosts:
    """The three numbers one run prices its fills at, and where they came from."""

    spread: float
    slippage: float
    commission_per_unit: float
    origin: CostOrigin


@dataclass(frozen=True, slots=True)
class CostProfile:
    """One account type's costs: a fallback table, and a table per symbol."""

    name: str
    defaults: dict[str, float] = field(default_factory=dict)
    symbols: dict[str, dict[str, float]] = field(default_factory=dict)

    def states(self, symbol: str) -> bool:
        """Whether this profile names *symbol* outright."""
        return symbol.upper() in self.symbols

    def quote(self, symbol: str) -> tuple[dict[str, float], dict[str, str]]:
        """What this profile charges *symbol*, and which table said so.

        The symbol's own table merged over :attr:`defaults`, key by key — a
        symbol that restates only the spread keeps the profile's slippage
        rather than silently losing it.
        """
        values = dict(self.defaults)
        origins = dict.fromkeys(values, FROM_DEFAULTS)
        for key, value in self.symbols.get(symbol.upper(), {}).items():
            values[key] = value
            origins[key] = FROM_SYMBOL
        return values, origins


@dataclass(slots=True)
class CostProfiles:
    """The parsed file. Falsy means "no file" — not "no profiles in it"."""

    profiles: dict[str, CostProfile] = field(default_factory=dict)
    #: ``default_profile`` from the file; empty when it names none.
    default_profile: str = ""
    #: The file that was read, or ``None`` when there was none to read.
    source: Path | None = None
    #: Where one was looked for, read or not, so a message can name the path.
    searched: Path | None = None

    def __bool__(self) -> bool:
        return self.source is not None

    @property
    def names(self) -> list[str]:
        """Every profile in the file, in file order."""
        return list(self.profiles)

    def select(self, name: str | None = None) -> CostProfile | None:
        """The profile a run trades under.

        *name* is ``--cost-profile``; empty falls back to the file's
        ``default_profile``, and then — only when the file holds exactly one
        profile — to that one. A file with several and no stated default is
        refused rather than guessed at: picking the first table in a file is
        not a decision anyone would want made on their behalf.
        """
        if not self.profiles:
            return None
        wanted = (name or self.default_profile or "").strip()
        if not wanted:
            if len(self.profiles) == 1:
                return next(iter(self.profiles.values()))
            raise ValueError(
                f"{self.source}: {len(self.profiles)} profiles ({', '.join(self.names)}) "
                f"and no {DEFAULT_PROFILE_KEY} — name one with --cost-profile, or set "
                f"{DEFAULT_PROFILE_KEY} in the file"
            )
        profile = self.profiles.get(wanted)
        if profile is None:
            raise ValueError(
                f"{self.source}: no profile named {wanted!r}. It holds: {', '.join(self.names)}"
            )
        return profile

    @classmethod
    def load(cls, path: Path | str) -> CostProfiles:
        """Parse *path*, or return an empty table when there is no file.

        A missing file is normal — a fresh clone has none, and the template
        beside it is what gets committed — and means every symbol is priced
        from ``QTE_ACCOUNT__*`` and the CLI flags, which is what the engine did
        before this file existed.
        """
        path = Path(path)
        if not path.is_file():
            log.info(
                "No cost profiles at %s — fills are priced from QTE_ACCOUNT__* and the flags",
                path,
            )
            return cls(searched=path)

        with path.open("rb") as handle:
            document = tomllib.load(handle)
        return _parse(document, path)


def resolve_costs(
    symbol: str,
    *,
    profiles: CostProfiles,
    profile: str | None = None,
    flags: Mapping[str, float | None] | None = None,
    fallback: Mapping[str, float] | None = None,
) -> ResolvedCosts:
    """What *symbol* costs to trade on this run, and where each number came from.

    *flags* are the CLI overrides, ``None`` for "not passed" — which is why the
    flags default to ``None`` and not to the account's numbers: a flag that
    defaulted to ``QTE_ACCOUNT__SPREAD`` could not be told apart from one the
    operator typed, and would silently outrank the profile.

    *fallback* is the last layer, ``QTE_ACCOUNT__*`` unless a caller states
    otherwise.
    """
    wanted = (profile or "").strip()
    if wanted and not profiles:
        raise ValueError(
            f"--cost-profile {wanted!r} was asked for, but there is no cost-profile file "
            f"at {profiles.searched} — copy config/costs.example.toml to it, or drop the flag"
        )
    chosen = profiles.select(wanted or None) if profiles else None
    stated, origins = chosen.quote(symbol) if chosen else ({}, {})
    if chosen is not None and not chosen.states(symbol):
        log.warning(
            "Cost profile %r states nothing for %s — its fills fall back to [%s.%s.%s] "
            "and QTE_ACCOUNT__*, which are one set of numbers for every symbol",
            chosen.name,
            symbol.upper(),
            PROFILES_TABLE,
            chosen.name,
            DEFAULTS_TABLE,
        )

    given = {key: value for key, value in (flags or {}).items() if value is not None}
    unknown = sorted(set(given) - set(COST_KEYS))
    if unknown:
        raise ValueError(f"resolve_costs: {', '.join(unknown)} is not a cost — {COST_KEYS}")
    negative = sorted(key for key, value in given.items() if value < 0)
    if negative:
        raise ValueError(
            f"--{negative[0].replace('_', '-')} is negative — a cost is paid, never earned"
        )

    account = fallback or {key: getattr(settings.account, key) for key in COST_KEYS}
    values: dict[str, float] = {}
    sources: dict[str, str] = {}
    for key in COST_KEYS:
        if key in given:
            values[key], sources[key] = float(given[key]), FROM_FLAG
        elif key in stated:
            values[key], sources[key] = stated[key], origins[key]
        else:
            values[key], sources[key] = float(account[key]), FROM_ACCOUNT

    origin = CostOrigin(
        profile=chosen.name if chosen else "",
        profile_file=str(profiles.source) if profiles.source else "",
        sources=sources,
    )
    log.info(
        "Pricing %s fills at spread %s, slippage %s, commission %s per unit — %s",
        symbol.upper(),
        values["spread"],
        values["slippage"],
        values["commission_per_unit"],
        origin.summary(),
    )
    return ResolvedCosts(**values, origin=origin)


# ── Parsing ───────────────────────────────────────────────────────────


def _parse(document: dict[str, Any], path: Path) -> CostProfiles:
    """Turn the parsed TOML into profiles, validating as it goes."""
    unknown = sorted(set(document) - {PROFILES_TABLE, DEFAULT_PROFILE_KEY})
    if unknown:
        raise ValueError(
            f"{path}: unknown top-level key(s) {', '.join(unknown)} — the file holds "
            f"{DEFAULT_PROFILE_KEY} and [{PROFILES_TABLE}.<name>]"
        )

    table = document.get(PROFILES_TABLE, {})
    if not isinstance(table, dict):
        raise ValueError(f"{path}: [{PROFILES_TABLE}] must be a table of profiles")

    profiles: dict[str, CostProfile] = {}
    for raw_name, entry in table.items():
        name = str(raw_name)
        where = f"{PROFILES_TABLE}.{name}"
        if not isinstance(entry, dict):
            raise ValueError(f"{path}: [{where}] must be a table")
        unknown = sorted(set(entry) - {DEFAULTS_TABLE, SYMBOLS_TABLE})
        if unknown:
            raise ValueError(
                f"{path}: [{where}] has unknown key(s) {', '.join(unknown)} — a profile "
                f"holds [{where}.{DEFAULTS_TABLE}] and [{where}.{SYMBOLS_TABLE}.<SYMBOL>]"
            )
        profiles[name] = CostProfile(
            name=name,
            defaults=_cost_table(entry.get(DEFAULTS_TABLE, {}), path, f"{where}.{DEFAULTS_TABLE}"),
            symbols=_symbol_tables(entry.get(SYMBOLS_TABLE, {}), path, where),
        )

    default_profile = document.get(DEFAULT_PROFILE_KEY, "")
    if not isinstance(default_profile, str):
        raise ValueError(
            f"{path}: {DEFAULT_PROFILE_KEY} must be a profile name in quotes, "
            f"not {default_profile!r}"
        )
    if default_profile and default_profile not in profiles:
        raise ValueError(
            f"{path}: {DEFAULT_PROFILE_KEY} = {default_profile!r} names no profile. "
            f"The file holds: {', '.join(profiles) or 'none'}"
        )
    return CostProfiles(
        profiles=profiles,
        default_profile=default_profile,
        source=path,
        searched=path,
    )


def _symbol_tables(entry: Any, path: Path, where: str) -> dict[str, dict[str, float]]:
    """``[profiles.<name>.symbols.<SYMBOL>]``, one cost table per symbol."""
    if not isinstance(entry, dict):
        raise ValueError(f"{path}: [{where}.{SYMBOLS_TABLE}] must be a table keyed by symbol")
    symbols: dict[str, dict[str, float]] = {}
    for raw_symbol, costs in entry.items():
        symbol = str(raw_symbol).upper()
        if symbol in symbols:
            raise ValueError(
                f"{path}: [{where}.{SYMBOLS_TABLE}] names {symbol} twice — two cost tables "
                "for one symbol cannot both price a fill"
            )
        symbols[symbol] = _cost_table(costs, path, f"{where}.{SYMBOLS_TABLE}.{raw_symbol}")
    return symbols


def _cost_table(entry: Any, path: Path, where: str) -> dict[str, float]:
    """One table of costs: only :data:`COST_KEYS`, only non-negative numbers."""
    if not isinstance(entry, dict):
        raise ValueError(f"{path}: [{where}] must be a table of costs")
    values: dict[str, float] = {}
    for raw_key, raw_value in entry.items():
        key = str(raw_key)
        if key not in COST_KEYS:
            raise ValueError(
                f"{path}: [{where}] has no key {key!r} — a cost table states {', '.join(COST_KEYS)}"
            )
        if isinstance(raw_value, bool) or not isinstance(raw_value, int | float):
            raise ValueError(f"{path}: [{where}].{key} must be a number, not {raw_value!r}")
        if raw_value < 0:
            raise ValueError(
                f"{path}: [{where}].{key} = {raw_value} is negative — a cost is paid, never earned"
            )
        values[key] = float(raw_value)
    return values


__all__ = [
    "COST_KEYS",
    "CostOrigin",
    "CostProfile",
    "CostProfiles",
    "ResolvedCosts",
    "resolve_costs",
]
