"""The glyph table: every name resolves, and the two entries stay apart.

A shortcode the ``emoji`` package does not know comes back as the literal
``":name:"``, which would reach a chat as that text. The module raises on it at
import time, so these tests are mostly about keeping the *contract* with
``algo-trading-broker``, whose own table has to hold the same glyphs.
"""

from __future__ import annotations

import pytest

from qte_shared import emoji_constants as em
from qte_shared.emoji_constants import _glyph


def test_no_name_leaked_as_text():
    """Nothing in the table still looks like an unresolved shortcode."""
    for name, value in vars(em).items():
        if name.startswith("_") or not isinstance(value, str):
            continue
        assert not value.startswith(":"), f"{name} did not resolve: {value!r}"
        assert value, f"{name} is empty"


def test_an_unknown_name_is_refused():
    with pytest.raises(ValueError, match="does not know"):
        _glyph("not_an_emoji_name_at_all")


def test_the_two_entries_are_a_green_and_a_red_square():
    """What the operator asked for, and what the broker's table also holds."""
    assert em.LONG == "\U0001f7e9"
    assert em.SHORT == "\U0001f7e5"


def test_long_and_short_are_told_apart():
    assert em.LONG != em.SHORT


def test_an_entry_is_not_drawn_as_a_flag_or_a_service_status():
    """The squares exist so an entry is not read as one of the circles.

    ``FLAG_ON``, ``SERVICE_UP`` and ``BADGE_CRITICAL`` are all circles, and a
    reader scanning a chat should not have to work out which green mark a
    green mark is.
    """
    circles = (em.FLAG_ON, em.FLAG_OFF, em.SERVICE_UP, em.BADGE_CRITICAL)
    assert em.LONG not in circles
    assert em.SHORT not in circles
