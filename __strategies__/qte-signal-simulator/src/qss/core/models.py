"""The wire vocabulary a strategy is allowed to speak.

These names are not ours to choose. ``SignalAction`` mirrors the
``SignalActionEnum`` that ``rockingrow/algo-trading-broker`` validates on every
webhook, and ``Scaling`` mirrors the ``scaling`` block of the same payload. The
QTE engine keeps its own copy in ``qte_shared.models``; this file is the second
copy, and it exists on purpose.

**Why a copy rather than an import.** A strategy repository that imports the
engine's package cannot be tested, linted or released without the engine
checked out beside it, and its test run would silently depend on whichever
revision of the engine happens to be on disk. The plugin seam is supposed to
let the two move independently, so the contract is restated here in plain
stdlib types and the engine adapts at the boundary.

**What must never drift**: the seven action names and the three scaling
fields. Everything else here is presentation. ``tests/test_contract.py`` pins
the surface so a rename fails locally instead of as a 422 from the broker.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class SignalAction(str, Enum):
    """Trade actions the broker accepts, spelled exactly as it spells them.

    ``str`` base, not ``StrEnum``: the engine and the broker both compare and
    serialise these by value, and ``StrEnum`` would change what ``str()``
    returns and drift the wire format.
    """

    LONG = "LONG"
    SHORT = "SHORT"
    TP1 = "TP1"
    TP2 = "TP2"
    R_SL = "R_SL"
    SL = "SL"
    FLAT = "FLAT"

    @property
    def is_entry(self) -> bool:
        return self in (SignalAction.LONG, SignalAction.SHORT)

    @property
    def is_close(self) -> bool:
        return not self.is_entry


@dataclass(slots=True)
class Scaling:
    """Levels and size used when adding to an existing position.

    A plain dataclass, not a pydantic model: this repo has no reason to depend
    on pydantic, and the engine validates the block on its own side before it
    reaches the wire.
    """

    tp: float | None = None
    sl: float | None = None
    quantity: float | None = None
