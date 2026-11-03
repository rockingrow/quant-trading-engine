"""Every glyph this repository shows a reader, named and resolved in one place.

The same file exists in `algo-trading-broker` as
``broker/helpers/emoji_constants.py``, and for the same reason: a trade is
reported by both services, so the two have to agree on what a LONG looks like.
Keeping the glyphs as literals in each module is how they drifted — the entry
icon lived in three files and matched in none of them.

Resolved through ``emoji.emojize`` rather than pasted in as characters, so a
name is what the code reads and a reviewer never has to identify a glyph by
eye. The shortcodes are the CLDR names the ``emoji`` package ships; a name it
does not know would come back as the literal ``":name:"``, which
``tests/test_emoji_constants.py`` refuses.

Not everything on screen belongs here. ``qte_bot.tables`` draws its columns
with ``✓`` and ``✗``, which are text marks and not emoji: they occupy one
cell in a monospaced table, where an emoji occupies two and breaks the
alignment the table exists for.
"""

from __future__ import annotations

import emoji


def _glyph(name: str) -> str:
    """Resolve one CLDR shortcode, or fail loudly at import time."""
    resolved = emoji.emojize(f":{name}:")
    if resolved == f":{name}:":
        raise ValueError(f"The emoji package does not know the name {name!r}")
    return resolved


# ── Signal actions ────────────────────────────────────────────────────────
# One per SignalAction. A green and a red dot for the two entries: the chart
# glyphs they replaced (📈 / 📉) were a pixel apart at the size Telegram draws
# an emoji in a message header.
LONG: str = _glyph("green_circle")
SHORT: str = _glyph("red_circle")
TP1: str = _glyph("bullseye")
TP2: str = _glyph("rocket")
R_SL: str = _glyph("shield")
SL: str = _glyph("hollow_red_circle")
FLAT: str = _glyph("white_flag")
DEFAULT_SIGNAL: str = _glyph("satellite_antenna")

# ── Cycle status ──────────────────────────────────────────────────────────
# The lifecycle message's header, which changes in place as the cycle runs.
CYCLE_RUNNING: str = _glyph("hourglass_not_done")
CYCLE_CLOSED: str = _glyph("chequered_flag")

# ── Delivery ──────────────────────────────────────────────────────────────
#: Shadow mode: the signal was built and never sent to a broker.
SHADOW: str = _glyph("test_tube")
#: The broker did not take the signal — the message says so rather than
#: implying a position exists.
UNDELIVERED: str = _glyph("warning")

# ── Flags and sections ────────────────────────────────────────────────────
FLAG_ON: str = _glyph("green_circle")
FLAG_OFF: str = _glyph("red_circle")
BAR_CHART: str = _glyph("bar_chart")

# ── Service lifecycle ─────────────────────────────────────────────────────
SERVICE_UP: str = _glyph("green_circle")
SERVICE_DOWN: str = _glyph("stop_sign")
ERROR_ALERT: str = _glyph("police_car_light")

# ── Operator bot ──────────────────────────────────────────────────────────
CONFIRM: str = _glyph("check_mark_button")
CANCEL: str = _glyph("multiply")
WARNING: str = _glyph("warning")
PACKAGE: str = _glyph("package")
PAUSED: str = _glyph("pause_button")
RESUMED: str = _glyph("play_button")

# ── Backtest report badges ────────────────────────────────────────────────
# Diagnostic severities in the HTML dashboard, read at a glance down a column.
BADGE_CRITICAL: str = _glyph("red_circle")
BADGE_WARNING: str = _glyph("orange_circle")
BADGE_INFO: str = _glyph("blue_circle")
