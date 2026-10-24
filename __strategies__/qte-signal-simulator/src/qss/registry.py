"""The strategies this repository publishes, by the name the broker knows them.

Same shape as ``quant-trading-strategies``' registry: one hand-written table,
alias → ``"module.path:ClassName"``, imported lazily. The alias is the NATS
subject the broker's workers subscribe to (``SIGNALS.<alias>``), so publishing
a strategy is a visible, reviewable line here and never a side effect of a file
lying in the tree.

:data:`DISABLED` is the same table for strategies kept but taken out of
service: :func:`load_all` never includes them, :func:`load_disabled` hands the
engine their names so it reports them as disabled rather than missing, and
:func:`load` refuses them by name. Moving a line between the two tables is the
whole switch.
"""

from __future__ import annotations

import importlib
from collections.abc import Mapping

from qss.core import StrategyBase

#: alias → ``"module.path:ClassName"``. The alias must equal the class's ``name``.
ALIASES: dict[str, str] = {
    "SIGNAL_SIMULATOR_LIFECYCLE_V1": (
        "qss.strategies.lifecycle.lifecycle_simulator:LifecycleSimulator"
    ),
}

#: The same shape, for strategies kept but not published. Empty: nothing here
#: is out of service. An alias belongs to exactly one of the two tables.
DISABLED: dict[str, str] = {}


def aliases() -> list[str]:
    """Every alias this repo publishes, sorted."""
    return sorted(ALIASES)


def load_disabled() -> list[str]:
    """The aliases kept out of service, sorted. The engine calls this beside load_all."""
    return sorted(DISABLED)


def load(alias: str) -> type[StrategyBase]:
    """Import and return the strategy class registered under *alias*."""
    try:
        target = ALIASES[alias]
    except KeyError:
        known = ", ".join(aliases()) or "none"
        if alias in DISABLED:
            raise LookupError(
                f"Strategy alias {alias!r} is disabled — listed in qss.registry.DISABLED and "
                f"not published (registered: {known})"
            ) from None
        raise LookupError(f"Unknown strategy alias {alias!r} (registered: {known})") from None

    module_path, _, class_name = target.partition(":")
    module = importlib.import_module(module_path)
    try:
        strategy = getattr(module, class_name)
    except AttributeError:
        raise ImportError(f"{module_path} defines no {class_name!r} for alias {alias!r}") from None

    if not (isinstance(strategy, type) and issubclass(strategy, StrategyBase)):
        raise TypeError(f"{target} is not a StrategyBase subclass")
    if getattr(strategy, "name", "") != alias:
        raise ValueError(
            f"Alias {alias!r} resolves to {class_name} whose name is {strategy.name!r}. "
            "The alias is the broker subject workers subscribe to — they must match."
        )
    return strategy


def load_all() -> Mapping[str, type[StrategyBase]]:
    """Every registered strategy class, keyed by alias. The engine calls this."""
    return {alias: load(alias) for alias in aliases()}
