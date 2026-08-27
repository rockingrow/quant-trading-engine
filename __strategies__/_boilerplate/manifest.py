"""The one file the engine needs to know about — copy this repo, keep this file.

``__strategies__/`` is a mount point, not a package. The engine walks one level
into it, finds this ``manifest.py`` (``strategies.py`` works too — pick one, two
is an error), imports it *by file path* and calls :func:`load_all`. What comes
back is the complete list of strategies this repository publishes, keyed by the
name the broker's workers subscribe to.

That is the whole integration. The engine learns no package name, no module
path and no directory layout, so this repo can reorganise itself freely — and
publishing is opt-in: a half-finished experiment sitting in ``src/`` cannot
start trading just because someone left it subclassing a strategy base.

Being imported by path means this module cannot assume it was imported *as*
part of a package: there is no ``boilerplate`` on ``sys.path`` when the engine
reaches it. So it puts ``src/`` there itself, which is also why the checkout
needs no ``pip install`` to be loadable — the mounted directory is the install.
"""

from __future__ import annotations

import sys
from pathlib import Path

#: Root of this checkout, wherever it happens to be mounted.
REPO_ROOT = Path(__file__).resolve().parent
_SOURCE_ROOT = REPO_ROOT / "src"

if str(_SOURCE_ROOT) not in sys.path:
    # Prepended on purpose: if a stale copy of the same package is installed in
    # site-packages, the mounted checkout is the one the operator deployed and
    # the one they will edit. A copy that silently shadowed it would mean
    # editing a strategy, restarting the runner and seeing nothing change.
    sys.path.insert(0, str(_SOURCE_ROOT))

from boilerplate.my_edge import MyEdge  # noqa: E402

#: alias → class. The alias **is** the NATS subject the broker's workers
#: subscribe to (``SIGNALS.<alias>``), so it has to match what they are
#: configured for, and it must be unique across every repo mounted here.
ALIASES = {
    "QTE_BOILERPLATE_M15": MyEdge,
}


def load_all() -> dict[str, type]:
    """Every strategy this repository publishes. The engine calls exactly this.

    A copy is returned so a caller mutating the result — the audit builds
    tables out of it — cannot edit the table the next call reads.
    """
    return dict(ALIASES)


def aliases() -> list[str]:
    """The published names, for a human at a REPL: ``python -c ...``."""
    return sorted(ALIASES)


def load(alias: str) -> type:
    """One strategy class by alias, with a readable error when it is absent."""
    try:
        return ALIASES[alias]
    except KeyError:
        known = ", ".join(aliases()) or "none"
        raise LookupError(f"{alias!r} is not published here (published: {known})") from None


__all__ = ["ALIASES", "REPO_ROOT", "aliases", "load", "load_all"]
