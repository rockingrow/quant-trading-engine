"""Shared fixtures. Nothing here touches Redis, Postgres or NATS."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def trending_frame() -> pd.DataFrame:
    """400 M15 bars that trend up then down, with a deterministic wobble.

    Seeded so a strategy test asserting "this many trades" stays stable; the
    shape matters more than the numbers.
    """
    periods = 400
    start = datetime(2026, 1, 1, tzinfo=UTC)
    index = pd.DatetimeIndex(
        [start + timedelta(minutes=15 * i) for i in range(periods)], name="open_time"
    )
    rng = np.random.default_rng(20260101)
    drift = np.concatenate(
        [np.linspace(0, 40, periods // 2), np.linspace(40, 5, periods - periods // 2)]
    )
    close = 2000 + drift + rng.normal(0, 1.5, periods).cumsum() * 0.1
    high = close + rng.uniform(0.5, 2.0, periods)
    low = close - rng.uniform(0.5, 2.0, periods)
    open_ = np.concatenate([[close[0]], close[:-1]])
    return pd.DataFrame(
        {
            "open": open_,
            "high": np.maximum.reduce([high, open_, close]),
            "low": np.minimum.reduce([low, open_, close]),
            "close": close,
            "volume": rng.uniform(100, 1000, periods),
        },
        index=index,
    )
