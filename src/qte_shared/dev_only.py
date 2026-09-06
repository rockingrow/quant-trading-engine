"""The one rule a development-only component obeys: it does not run in production.

A market data simulator is a component whose failure mode is silent and
expensive. It looks exactly like a feed — same socket, same ticks, same
candles — so an engine wired to it in production would trade synthetic prices
and report nothing unusual while doing it. There is no log line that reads
"these bars were invented".

So the check is not a comment or a naming convention, it is a call:
:func:`require_dev_env` raises unless ``QTE_ENV=dev``, and both halves of the
simulator (the server, and the provider ingestion connects through) make it
before they do anything else. Deleting the guard is then a diff, which is the
whole point — a reviewer sees it.

There is deliberately no override flag. An escape hatch here would be found and
used by the first person in a hurry, and "we set QTE_SIMULATOR__ALLOW_PROD=1
for a minute" is not a sentence anyone wants to read after a bad fill.
"""

from __future__ import annotations

from qte_shared.config import settings

#: The only value of ``QTE_ENV`` in which a dev-only component may start.
DEV_ENV = "dev"


class DevOnlyError(RuntimeError):
    """Raised when a development-only component is started outside ``QTE_ENV=dev``."""


def is_dev_env() -> bool:
    """Whether this process is running in the development environment."""
    return settings.env == DEV_ENV


def require_dev_env(component: str) -> None:
    """Refuse to continue unless ``QTE_ENV=dev``.

    *component* names what is being refused, because the message is read by
    someone who did not expect the refusal and needs to know which of the
    things they just started is the dev-only one.
    """
    if is_dev_env():
        return
    raise DevOnlyError(
        f"{component} is a development-only component and QTE_ENV is {settings.env!r}. "
        f"It fabricates market data, so it refuses to run anywhere but QTE_ENV={DEV_ENV}. "
        "If a live engine reached this, its QTE_MARKET_DATA__PROVIDER is pointed at the "
        "simulator — fix that before restarting it."
    )


__all__ = ["DEV_ENV", "DevOnlyError", "is_dev_env", "require_dev_env"]
