"""Shared fixtures. Nothing here touches Redis, Postgres or NATS."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd
import pytest
from pydantic_settings import DotEnvSettingsSource


def pytest_configure(config):
    """Choose test configuration before collection imports application globals.

    Deployment secrets, mode and private plans must never become test inputs.
    Individual tests can still set environment variables or patch settings to
    exercise production refusals and configuration precedence explicitly.
    """
    import os

    isolated = pytest.MonkeyPatch()
    config.add_cleanup(isolated.undo)
    directory = TemporaryDirectory(prefix="qte-test-config-")
    config.add_cleanup(directory.cleanup)
    for variable in tuple(os.environ):
        if variable.startswith("QTE_"):
            isolated.delenv(variable)
    isolated.setenv("QTE_ENV", "dev")
    isolated.setenv("QTE_STATE__MODE", "dev")
    isolated.setenv("QTE_MARKET_DATA__CONFIG_FILE", str(Path(directory.name) / "missing.toml"))
    isolated.setenv("QTE_ENGINE__MAPPING_FILE", str(Path(directory.name) / "mapping.toml"))
    isolated.setattr("dotenv.load_dotenv", lambda *arguments, **keywords: False)
    isolated.setattr(DotEnvSettingsSource, "_read_env_files", lambda self: {})


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
