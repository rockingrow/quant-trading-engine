"""Per-strategy settings a mounted repository declares, and what they decide.

A strategy repo publishes two things through its manifest: the alias table
(``load_all``) and this — ``load_settings``, returning ``{alias: settings}``.
The split matters. ``params`` are the knobs an *edge* is tuned on and they
belong to the strategy; what is here is the opposite, properties of the market
the strategy happens to trade, which the engine has to act on whether or not the
strategy remembered to.

Today that is one setting: the weekend flat. A CFD book closes on Friday and
reopens on Sunday evening, and a position left open across that gap is repriced
on Monday against news nobody could stop out of. Before this module the
protection was a strategy's own business — a ``use_weekend_flat`` param and a
Friday cut-off implemented inside one strategy family — which made it something
every future strategy had to reimplement, and something a strategy could simply
forget. So the strategy declares *when* its market closes and the engine does
the closing, in both drivers: :mod:`qte_strategy_engine.runner` and
:mod:`qte_backtest.replay`.

**The declaration is plain data, on purpose.** ``__strategies__/`` is a private
repository with its own lockfile that must never ``import qte_shared`` — see
:mod:`qte_shared.strategies.strategy_base` on why the contract is structural. So
a repo hands over nested dicts of strings and booleans and the parsing lives
here, at the boundary, where a typo becomes one error naming the alias rather
than an exception inside somebody else's package.

**The clock is a weekday and a wall time, not a timestamp.** A market closes
every Friday, not on one Friday, so the window is stated as a pair of weekly
moments and evaluated modulo the week. ``flat_from`` falling later in the week
than ``flat_until`` is the normal case rather than an error: ``FRI 17:00`` to
``SUN 22:00`` wraps past Sunday midnight, which is the shape a weekend has.
Explicit dated ``extra_windows`` can extend that recurring block for holiday
sessions; they use the same market zone and never infer dates from future bars.

**Which zone that wall time is read in is the operator's call**, not the
strategy's — ``QTE_ENGINE__WEEKEND_FLAT_TIMEZONE``, passed in as *market_zone*
on every call here. Default ``UTC``, so a repo that declares UTC times gets them
read literally; an operator whose broker closes on a local exchange calendar
changes one variable instead of every strategy. Nothing in this module reads
settings itself, so the backtest and the live runner cannot end up evaluating
one window in two different zones.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

#: The optional manifest hook, beside ``load_all``. A repo that does not define
#: it declares no settings, which is not an error — it is every strategy that
#: existed before this file, and every loose ``.py`` the directory scan picks up.
STRATEGY_SETTINGS_HOOK = "load_settings"

#: Minutes in a day and in a week — the units :attr:`WeeklyMoment.offset` folds to.
MINUTES_PER_DAY = 24 * 60
MINUTES_PER_WEEK = 7 * MINUTES_PER_DAY

#: Accepted spellings of a weekday. Monday is 0, as ``date.weekday()`` has it.
WEEKDAY_NAMES: dict[str, int] = {
    "MON": 0,
    "MONDAY": 0,
    "TUE": 1,
    "TUESDAY": 1,
    "WED": 2,
    "WEDNESDAY": 2,
    "THU": 3,
    "THURSDAY": 3,
    "FRI": 4,
    "FRIDAY": 4,
    "SAT": 5,
    "SATURDAY": 5,
    "SUN": 6,
    "SUNDAY": 6,
}

#: The spelling :meth:`WeeklyMoment.describe` renders back out.
WEEKDAY_LABELS = ("MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN")

#: The keys :meth:`WeekendFlatPolicy.parse` accepts. Anything else is refused
#: rather than ignored — see the method.
WEEKEND_FLAT_KEYS = frozenset({"enabled", "flat_from", "flat_until", "extra_windows"})


@dataclass(frozen=True, slots=True)
class WeeklyMoment:
    """One instant in the trading week: a weekday plus a wall-clock time.

    Kept as the two fields a human wrote, and compared as :attr:`offset` —
    minutes since Monday 00:00. That is what turns a window spanning Sunday
    midnight into one ordinary comparison instead of a special case.
    """

    weekday: int
    minute_of_day: int

    @classmethod
    def parse(cls, value: Any) -> WeeklyMoment:
        """Read ``"FRI 17:00"``, or ``"4 17:00"`` for a caller generating them.

        The weekday is mandatory. A bare ``"17:00"`` would have to mean "every
        day", and a daily flat is not what any caller of this is asking for.
        """
        if isinstance(value, WeeklyMoment):
            return value
        if not isinstance(value, str):
            raise ValueError(f"{value!r} is not a '<WEEKDAY> HH:MM' string")

        parts = value.replace(",", " ").split()
        if len(parts) != 2:
            raise ValueError(
                f"{value!r} is not '<WEEKDAY> HH:MM' — for example 'FRI 17:00' or 'SUN 22:00'"
            )
        weekday_text, clock_text = parts
        return cls(weekday=_parse_weekday(weekday_text), minute_of_day=_parse_clock(clock_text))

    @classmethod
    def of(cls, moment: datetime, market_zone: ZoneInfo) -> WeeklyMoment:
        """Where *moment* falls in the week, read in *market_zone*.

        A naive datetime is taken as already being in that zone rather than
        refused: both drivers hand over a bar's open time, and a history file
        that lost its ``tzinfo`` should not take the weekend protection down
        with it.
        """
        if moment.tzinfo is not None:
            moment = moment.astimezone(market_zone)
        return cls(weekday=moment.weekday(), minute_of_day=moment.hour * 60 + moment.minute)

    @property
    def offset(self) -> int:
        """Minutes since Monday 00:00 — the week folded onto one number line."""
        return self.weekday * MINUTES_PER_DAY + self.minute_of_day

    def describe(self) -> str:
        """``"FRI 17:00"`` — what the operator wrote, for a log line."""
        return (
            f"{WEEKDAY_LABELS[self.weekday]} "
            f"{self.minute_of_day // 60:02d}:{self.minute_of_day % 60:02d}"
        )


@dataclass(frozen=True, slots=True)
class DatedFlatWindow:
    """A declared holiday closure, read in the same zone as the weekly window.

    These dates come from the repository's calendar, never from looking ahead
    for a gap in replay data. They extend the protection without moving the
    recurring Friday cut-off on ordinary weeks.
    """

    flat_from: datetime
    flat_until: datetime

    @classmethod
    def from_mapping(cls, payload: Any) -> DatedFlatWindow:
        if not isinstance(payload, Mapping) or set(payload) != {"flat_from", "flat_until"}:
            raise ValueError("each extra window needs exactly flat_from and flat_until")
        moments: list[datetime] = []
        for boundary in ("flat_from", "flat_until"):
            declared = payload[boundary]
            try:
                if not isinstance(declared, str):
                    raise ValueError
                moments.append(datetime.strptime(declared, "%Y-%m-%d %H:%M"))
            except ValueError:
                raise ValueError(
                    f"extra window {boundary} must be YYYY-MM-DD HH:MM in the market zone"
                ) from None
        if moments[0] >= moments[1]:
            raise ValueError("extra window flat_until must be after flat_from")
        return cls(flat_from=moments[0], flat_until=moments[1])

    def covers(self, moment: datetime, market_zone: ZoneInfo) -> bool:
        if moment.tzinfo is not None:
            moment = moment.astimezone(market_zone)
        return self.flat_from <= moment.replace(tzinfo=None) < self.flat_until

    def describe(self) -> str:
        return f"{self.flat_from:%Y-%m-%d %H:%M} -> {self.flat_until:%Y-%m-%d %H:%M}"


@dataclass(frozen=True, slots=True)
class WeekendFlatPolicy:
    """A recurring weekly closure, optionally extended by dated holiday windows.

    Half-open, ``[flat_from, flat_until)``: the bar exactly on the cut-off is
    inside the window and the bar exactly on the reopen is outside it, so the
    window and the week that follows it cannot both claim one minute.

    Disabled is the default. A strategy that says nothing keeps the behaviour it
    had before this existed, which is what lets the setting arrive without
    re-auditing every mounted repository on the same day.
    """

    enabled: bool = False
    flat_from: WeeklyMoment | None = None
    flat_until: WeeklyMoment | None = None
    extra_windows: tuple[DatedFlatWindow, ...] = ()

    @classmethod
    def parse(cls, payload: Any) -> WeekendFlatPolicy:
        """Build a policy out of what a repository declared.

        ``ValueError`` on anything that will not read as one; the caller adds
        the alias to the message — see :meth:`StrategySettings.parse`.
        """
        if payload is None:
            return cls()
        if isinstance(payload, bool):
            # `weekend_flat = false` is a repo saying "not for this strategy",
            # which is worth being able to write. `true` carries no window, so
            # there is nothing to act on and guessing one would be worse.
            if payload:
                raise ValueError(
                    "weekend_flat = true declares no window — give it flat_from and "
                    "flat_until, for example {'flat_from': 'FRI 17:00', "
                    "'flat_until': 'SUN 22:00'}"
                )
            return cls()
        if not isinstance(payload, Mapping):
            raise ValueError(f"weekend_flat must be a table, not {type(payload).__name__}")

        unknown = set(payload) - WEEKEND_FLAT_KEYS
        if unknown:
            # Refused rather than ignored: a misspelled `flat_form` would leave
            # the window unset and the strategy trading through the weekend,
            # which is the one failure this module exists to prevent.
            raise ValueError(
                f"weekend_flat has no setting called {', '.join(sorted(unknown))} — "
                "it takes enabled, flat_from, flat_until and extra_windows"
            )

        enabled = payload.get("enabled", True)
        if not isinstance(enabled, bool):
            raise ValueError(f"weekend_flat.enabled must be true or false, not {enabled!r}")
        if not enabled:
            return cls()

        for name in ("flat_from", "flat_until"):
            if payload.get(name) is None:
                raise ValueError(f"weekend_flat is enabled but declares no {name}")
        try:
            flat_from = WeeklyMoment.parse(payload["flat_from"])
            flat_until = WeeklyMoment.parse(payload["flat_until"])
        except ValueError as error:
            raise ValueError(f"weekend_flat: {error}") from None

        if flat_from.offset == flat_until.offset:
            # "Never" or "always" depending on how the comparison happens to be
            # written, and nobody meant either. Saying so beats picking one.
            raise ValueError(
                f"weekend_flat opens and closes at the same moment ({flat_from.describe()}) — "
                "set enabled = false to turn it off instead"
            )
        declared_windows = payload.get("extra_windows", [])
        if not isinstance(declared_windows, (list, tuple)):
            raise ValueError("weekend_flat.extra_windows must be a list of dated windows")
        try:
            extra_windows = tuple(
                DatedFlatWindow.from_mapping(window) for window in declared_windows
            )
        except ValueError as parse_error:
            raise ValueError(f"weekend_flat.extra_windows: {parse_error}") from None
        return cls(
            enabled=True,
            flat_from=flat_from,
            flat_until=flat_until,
            extra_windows=extra_windows,
        )

    def covers(self, moment: datetime, market_zone: ZoneInfo) -> bool:
        """Whether *moment* falls inside the window, read in *market_zone*.

        Compared modulo the week, so ``FRI 17:00`` to ``SUN 22:00`` wrapping
        past Sunday midnight needs no special case: the span from the cut-off,
        measured forward, either reaches this moment or it does not.
        """
        if not self.enabled or self.flat_from is None or self.flat_until is None:
            return False
        if any(window.covers(moment, market_zone) for window in self.extra_windows):
            return True
        opened_at = self.flat_from.offset
        span = (self.flat_until.offset - opened_at) % MINUTES_PER_WEEK
        elapsed = (WeeklyMoment.of(moment, market_zone).offset - opened_at) % MINUTES_PER_WEEK
        return elapsed < span

    def describe(self) -> str:
        """One line for a log or a report: the window, or that there is none."""
        if not self.enabled or self.flat_from is None or self.flat_until is None:
            return "off"
        recurring = f"{self.flat_from.describe()} -> {self.flat_until.describe()}"
        if self.extra_windows:
            closures = "; ".join(window.describe() for window in self.extra_windows)
            return f"{recurring}; extra windows: {closures}"
        return recurring


#: What a strategy that declared no weekend flat gets. Shared rather than
#: rebuilt per strategy, so the common case allocates nothing.
NO_WEEKEND_FLAT = WeekendFlatPolicy()


@dataclass(frozen=True, slots=True)
class StrategySettings:
    """Everything one strategy declared, parsed and validated.

    One field today. It is a class rather than a bare policy because the next
    market-shaped setting — a holiday calendar, a daily maintenance break —
    belongs beside the weekend flat instead of in another parallel table.
    """

    weekend_flat: WeekendFlatPolicy = NO_WEEKEND_FLAT

    @classmethod
    def parse(cls, alias: str, payload: Any) -> StrategySettings:
        """Read one alias's entry, naming the alias in anything it raises.

        The message has to carry the alias: the engine reads every mounted
        repository's settings in one pass, and "flat_until is missing" without
        it leaves an operator grepping several private repos for the typo.
        """
        if payload is None:
            return cls()
        if not isinstance(payload, Mapping):
            raise ValueError(f"{alias}: settings must be a table, not {type(payload).__name__}")

        unknown = set(payload) - {"weekend_flat"}
        if unknown:
            raise ValueError(
                f"{alias}: no setting called {', '.join(sorted(unknown))} — "
                "this engine reads weekend_flat"
            )
        try:
            return cls(weekend_flat=WeekendFlatPolicy.parse(payload.get("weekend_flat")))
        except ValueError as error:
            raise ValueError(f"{alias}: {error}") from None

    def describe(self) -> dict[str, Any]:
        """What the runner logs on start, and the audit prints per strategy."""
        return {"weekend_flat": self.weekend_flat.describe()}


#: What a strategy the settings table never mentioned gets.
DEFAULT_SETTINGS = StrategySettings()


def parse_settings_table(published: Any) -> dict[str, StrategySettings]:
    """Parse a whole ``load_settings()`` return value, alias by alias.

    Raises ``ValueError`` on the first entry that will not read. The loader
    turns that into a refusal to run *that* strategy: a safety setting which did
    not parse must not be quietly replaced by "off".
    """
    if published is None:
        return {}
    if not isinstance(published, Mapping):
        raise ValueError(
            f"{STRATEGY_SETTINGS_HOOK}() must return {{alias: settings}}, "
            f"not {type(published).__name__}"
        )
    return {
        str(alias): StrategySettings.parse(str(alias), payload)
        for alias, payload in published.items()
    }


def _parse_weekday(text: str) -> int:
    """``"FRI"``, ``"Friday"`` or ``"4"`` — Monday is 0, as ``weekday()`` has it."""
    named = WEEKDAY_NAMES.get(text.upper())
    if named is not None:
        return named
    try:
        numbered = int(text)
    except ValueError:
        raise ValueError(f"{text!r} is not a weekday (MON..SUN, or 0..6 from Monday)") from None
    if not 0 <= numbered <= 6:
        raise ValueError(f"weekday {numbered} is out of range — 0 is Monday, 6 is Sunday")
    return numbered


def _parse_clock(text: str) -> int:
    """``"17:00"`` as a minute of the day. 24-hour, and the minutes are required."""
    hours_text, separator, minutes_text = text.partition(":")
    if not separator:
        raise ValueError(f"{text!r} is not a HH:MM time")
    try:
        hours, minutes = int(hours_text), int(minutes_text)
    except ValueError:
        raise ValueError(f"{text!r} is not a HH:MM time") from None
    if not (0 <= hours <= 23 and 0 <= minutes <= 59):
        raise ValueError(f"{text!r} is not a time of day — 00:00 to 23:59")
    return hours * 60 + minutes


__all__ = [
    "DEFAULT_SETTINGS",
    "DatedFlatWindow",
    "NO_WEEKEND_FLAT",
    "STRATEGY_SETTINGS_HOOK",
    "StrategySettings",
    "WeeklyMoment",
    "WeekendFlatPolicy",
    "parse_settings_table",
]
