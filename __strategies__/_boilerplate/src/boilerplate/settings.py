"""What the engine should do *around* each strategy this repo publishes.

The second thing the engine reads, beside the alias table. ``manifest.py`` says
which strategies exist; this one says what the market they trade does, and the
engine acts on it whether or not the strategy remembered to.

That split is the point. A ``param`` is a knob an *edge* is tuned on — a
lookback, a multiple, a threshold — and it belongs to the strategy, which is
the only thing that knows what it means. What is here is the opposite: a
property of the instrument's calendar, which every strategy on that instrument
shares and none of them should have to reimplement. The weekend flat used to
live inside a strategy, and every new strategy either copied it or quietly went
without it.

Everything below is plain data — strings, booleans, dicts. Nothing here imports
the engine, for the same reason nothing else in this repo does: a strategy
checkout has to build, lint and test with only its own lockfile. The engine
parses and validates what this returns at its own boundary, so a typo comes back
as one error naming the alias rather than an exception inside this package.

**A setting that will not parse stops that strategy from loading.** Not a
warning: these are the protections a deployment is trading under, and falling
back to "off" would turn a misspelled key into a position held through a closed
market. Run ``make audit`` from the engine to see what it made of this file.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any

#: alias → settings. The alias is the one ``manifest.py`` publishes under; an
#: entry for anything else applies to nothing and the engine says so.
#:
#: Every key is optional. A strategy absent from this table, and a repo with no
#: ``settings.py`` at all, gets the engine's defaults — which is every strategy
#: that was mounted before this file existed.
SETTINGS: dict[str, dict[str, Any]] = {
    "QTE_BOILERPLATE_M15": {
        # Go flat before the market shuts, and stay out until it reopens.
        #
        # A CFD book closes on Friday and reopens on Sunday evening. A position
        # left open across that gap cannot be stopped out, and Monday reprices
        # it against two days of news — so the engine closes it (action FLAT,
        # every open position on this strategy and symbol) and refuses new
        # entries until the window ends. Exits are never blocked.
        #
        # `flat_from` / `flat_until` are "<WEEKDAY> HH:MM", weekdays MON..SUN,
        # and the window is half-open: the bar exactly on `flat_from` is inside
        # it, the bar exactly on `flat_until` is not. Stating the reopen rather
        # than only the cut-off is what lets the engine know when entries may
        # resume; a window that wraps past Sunday midnight is the normal case.
        #
        # Which clock these times are read on is the *operator's* setting, not
        # this one: QTE_ENGINE__WEEKEND_FLAT_TIMEZONE, default UTC. Write the
        # times in the zone that deployment runs — UTC unless you know better —
        # and pick a cut-off that exists on the broker's Friday calendar with
        # room to spare, because one after the last Friday bar never fires.
        "weekend_flat": {
            "enabled": True,
            "flat_from": "FRI 20:00",
            "flat_until": "SUN 22:00",
        },
    },
}


def load_settings() -> dict[str, dict[str, Any]]:
    """Every strategy's settings, keyed by alias. The engine calls exactly this.

    Deep-copied on the way out, so a caller mutating the result cannot edit the
    table the next call reads — nested tables included, which a shallow copy
    would still be handing over.
    """
    return deepcopy(SETTINGS)


def settings_for(alias: str) -> dict[str, Any]:
    """One strategy's settings, or an empty table when it declares none."""
    return deepcopy(SETTINGS.get(alias, {}))


__all__ = ["SETTINGS", "load_settings", "settings_for"]
