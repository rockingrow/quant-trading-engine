"""Logger factory for strategy code.

Deliberately thinner than the engine's ``qte_shared.logging_setup``. That one
installs a handler on the *root* logger, which is right for a service that owns
its process and wrong for a plugin loaded into someone else's: a strategy that
reconfigured the root logger would silently reformat, re-level or swallow the
runner's own output.

So this does what a library is supposed to do — hand back a named logger,
attach a ``NullHandler`` once so "No handlers could be found" never appears,
and leave every formatting and level decision to whoever is hosting us.
"""

from __future__ import annotations

import logging

#: Root of this repo's logger tree. Everything a strategy logs lands under it,
#: so the host can raise or silence all of it with one ``setLevel`` call.
PACKAGE_LOGGER = "qss"

logging.getLogger(PACKAGE_LOGGER).addHandler(logging.NullHandler())


def get_logger(name: str) -> logging.Logger:
    """Named logger, re-parented under :data:`PACKAGE_LOGGER`.

    Modules pass ``__name__``. Under the engine's file-path importer that can
    arrive as something like ``__strategies__.strategies``, which would put the
    record outside the ``qss`` tree and out of reach of the host's one knob —
    so anything not already under the package root is re-parented into it.
    """
    if name == PACKAGE_LOGGER or name.startswith(f"{PACKAGE_LOGGER}."):
        return logging.getLogger(name)
    return logging.getLogger(f"{PACKAGE_LOGGER}.{name.rsplit('.', 1)[-1]}")
