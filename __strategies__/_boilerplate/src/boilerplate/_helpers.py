"""Shared helpers — deliberately named with a leading underscore.

If this repository is ever loaded by the *scan* path rather than by its
manifest, every ``.py`` under it is imported and inspected for strategy-shaped
classes. Files whose name starts with ``_`` are skipped by that walk, so helper
modules cannot be mistaken for strategies. Under the manifest path it does not
matter — the manifest is the only list that counts — but the convention costs
nothing and survives a repo that later drops its manifest.
"""

from __future__ import annotations

import pandas as pd


def is_warm(*series: pd.Series) -> bool:
    """True when every indicator has a real value on the last (closed) bar.

    An indicator that has not finished warming up answers ``NaN``. Acting on
    that row means trading on a number that does not exist yet, so every
    decision starts by asking this.
    """
    return not any(pd.isna(one.iloc[-1]) for one in series)


def bracket(price: float, risk: float, direction: int, *, rr: float) -> tuple[float, float, float]:
    """Stop and two targets derived from one risk distance.

    Returned as ``(sl, tp1, tp2)``. Deriving the targets from the same distance
    that sets the stop is what makes the reward-to-risk of a trade a property of
    the strategy rather than of whichever round number looked good on the chart.
    """
    stop = price - direction * risk
    return (
        round(stop, 5),
        round(price + direction * risk * rr, 5),
        round(price + direction * risk * rr * 2, 5),
    )
