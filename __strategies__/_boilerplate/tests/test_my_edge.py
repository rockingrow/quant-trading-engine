"""The suite a strategy repository keeps: the edge, checked with no engine present.

Nothing here imports the engine, and that is enforced below by
:func:`test_the_repo_imports_nothing_from_the_engine`. Run it with this repo's
own dependencies — pandas, numpy, pytest — and nothing else::

    uv run --project __strategies__/_boilerplate pytest

``tests/`` is also one of the directory names the engine's plugin scan never
walks, so nothing in here can be mistaken for a strategy.
"""

from __future__ import annotations

import re
import sys
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest

SOURCE_ROOT = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SOURCE_ROOT))

from boilerplate.contract import (  # noqa: E402
    REQUIRED_SIGNAL_METHODS,
    SIGNAL_METHODS,
    SignalAction,
    StrategyContext,
)
from boilerplate.my_edge import MyEdge  # noqa: E402


@pytest.fixture
def candles() -> pd.DataFrame:
    """400 rising M15 bars — enough to warm every indicator the strategy reads."""
    index = pd.date_range("2026-01-01", periods=400, freq="15min", tz=UTC)
    # Indexed like the frame it goes into: a Series carrying its own
    # RangeIndex would align to nothing and every column would be NaN.
    close = pd.Series(range(400), dtype="float64", index=index) + 2000.0
    return pd.DataFrame(
        {
            "open": close - 0.5,
            "high": close + 1.0,
            "low": close - 1.0,
            "close": close,
            "volume": 100.0,
        },
        index=index,
    )


@pytest.fixture
def context() -> StrategyContext:
    return StrategyContext(
        symbol="XAUUSD", timeframe="M15", now=datetime(2026, 1, 1, tzinfo=UTC), mode="backtest"
    )


def test_the_engine_can_drive_it() -> None:
    """The loader's structural check, restated: four members and three attributes.

    The engine never asks what this class inherits from — it asks whether these
    exist. Copying the check here is what lets the repo know it is deployable
    without the engine installed to answer.
    """
    for hook in ("on_candle_closed", "on_start", "on_stop", "history_window"):
        assert callable(getattr(MyEdge, hook, None)), hook
    for attribute in ("name", "timeframe", "warmup"):
        assert hasattr(MyEdge, attribute), attribute


def test_it_publishes_the_whole_signal_interface() -> None:
    """Five required methods and two optional ones — what the audit reads."""
    for method in SIGNAL_METHODS:
        assert callable(getattr(MyEdge, method)), method
    for method in REQUIRED_SIGNAL_METHODS:
        # Defined on this class rather than inherited from the abstract base,
        # which is the distinction `qte-strategy-audit` makes.
        assert method in vars(MyEdge), method


def test_it_is_inert_until_the_rule_is_written(
    candles: pd.DataFrame, context: StrategyContext
) -> None:
    """As shipped it decides nothing, in either direction."""
    strategy = MyEdge({})
    assert strategy.long(candles, context) is None
    assert strategy.short(candles, context) is None


def test_the_bracket_is_derived_from_atr(
    candles: pd.DataFrame, context: StrategyContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Write the rule and the entry above it becomes a real signal.

    This is the test your own strategy keeps: fix the input, assert the intent.
    """
    monkeypatch.setattr(MyEdge, "_rule", lambda self, df, *, direction: direction > 0)
    strategy = MyEdge({"atr_sl_mult": 2.0, "min_rr_ratio": 2.0})

    intent = strategy.long(candles, context)
    assert intent is not None
    assert intent.action is SignalAction.LONG
    assert intent.symbol == "XAUUSD"
    assert intent.sl < intent.price < intent.tp1 < intent.tp2
    # Reward is the configured multiple of the risk the stop actually took.
    risk = intent.price - intent.sl
    assert intent.tp1 - intent.price == pytest.approx(risk * 2.0, rel=1e-6)
    # Short still says nothing: the rule above only answers for direction 1.
    assert strategy.short(candles, context) is None


def test_the_dispatcher_asks_exits_while_holding(
    candles: pd.DataFrame, context: StrategyContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A held position routes to the exits, and never to an entry.

    The rule below would open in both directions; ``open_uxid`` being set is
    what keeps ``long``/``short`` from ever being asked.
    """
    monkeypatch.setattr(MyEdge, "_rule", lambda self, df, *, direction: True)
    strategy = MyEdge({})
    context.open_uxid = "cycle-1"

    assert strategy.on_candle_closed(candles, context) == []


def test_the_repo_imports_nothing_from_the_engine() -> None:
    """The isolation rule, enforced rather than documented.

    The strategy logic is the core; the engine is a delivery layer around it.
    An import in this direction would invert that, and it is the kind of thing
    that arrives one convenient helper at a time — so the suite fails on it
    instead of a reviewer having to notice.
    """
    forbidden = re.compile(r"^\s*(?:from|import)\s+(qte_\w+)", re.MULTILINE)
    offenders = {
        path.relative_to(SOURCE_ROOT).as_posix(): sorted(set(forbidden.findall(path.read_text())))
        for path in SOURCE_ROOT.rglob("*.py")
        if forbidden.search(path.read_text())
    }
    assert not offenders, f"engine imports leaked into the strategy core: {offenders}"
