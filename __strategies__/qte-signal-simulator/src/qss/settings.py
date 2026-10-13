"""What the engine should do *around* each strategy this repo publishes.

Empty on purpose. The weekend flat is a property of an instrument's calendar,
and this repository does not know which instrument it will be pointed at — the
engine's mapping table decides that. With no entry, the engine applies its
defaults; a pair that needs the flat declares ``use_weekend_flat`` there.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any

SETTINGS: dict[str, dict[str, Any]] = {}


def load_settings() -> dict[str, dict[str, Any]]:
    """Every strategy's settings, keyed by alias. The engine calls exactly this."""
    return deepcopy(SETTINGS)


__all__ = ["SETTINGS", "load_settings"]
