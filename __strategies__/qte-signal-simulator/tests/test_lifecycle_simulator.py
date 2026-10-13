"""Every cycle walks one of three scripted paths to its end, one step per bar.

A small driver below plays the runner's part — it keeps ``open_uxids`` the way
both engine drivers do, dropping a cycle on a terminal action — so whole cycles
are checked here with no engine present.
"""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime

import pandas as pd
import pytest

import manifest
from qss.core import SignalAction, StrategyContext, as_intents
from qss.strategies.lifecycle.lifecycle_simulator import EXIT_PATHS, OUTCOMES, LifecycleSimulator

TERMINAL = {SignalAction.TP2, SignalAction.SL, SignalAction.R_SL, SignalAction.FLAT}
ENTRIES = {SignalAction.LONG, SignalAction.SHORT}


def make_candles(bar_count: int, frequency: str = "15min") -> pd.DataFrame:
    index = pd.date_range("2026-01-01", periods=bar_count, freq=frequency, tz=UTC)
    close = pd.Series(range(bar_count), dtype="float64", index=index) % 7 + 2000.0
    return pd.DataFrame(
        {"open": close, "high": close + 1.0, "low": close - 1.0, "close": close, "volume": 1.0},
        index=index,
    )


def drive(strategy: LifecycleSimulator, bar_count: int = 120, *, max_open_cycles: int = 1):
    """Feed bars one at a time; return every intent tagged with its bar number."""
    candles = make_candles(bar_count)
    open_uxids: list[str] = []
    emitted: list[tuple[int, object]] = []
    for position in range(strategy.warmup, bar_count):
        frame = candles.iloc[: position + 1]
        context = StrategyContext(
            symbol="XAUUSD",
            timeframe=strategy.timeframe,
            now=frame.index[-1].to_pydatetime(),
            open_uxid=open_uxids[-1] if open_uxids else None,
            open_uxids=tuple(open_uxids),
            max_open_cycles=max_open_cycles,
        )
        for intent in as_intents(strategy.on_candle_closed(frame, context)):
            emitted.append((position, intent))
            if intent.action in ENTRIES:
                open_uxids.append(intent.signal_uxid)
            elif intent.action in TERMINAL:
                open_uxids.remove(intent.signal_uxid)
    return emitted


def cycles(emitted) -> dict[str, list[tuple[int, SignalAction]]]:
    by_uxid: dict[str, list[tuple[int, SignalAction]]] = {}
    for position, intent in emitted:
        by_uxid.setdefault(intent.signal_uxid, []).append((position, intent.action))
    return by_uxid


# ── Publication ───────────────────────────────────────────────────────────


def test_the_manifest_publishes_it_under_its_own_name():
    assert manifest.load_all() == {"SIGNAL_SIMULATOR_LIFECYCLE_V1": LifecycleSimulator}
    assert manifest.load_settings() == {}


def test_no_symbol_is_written_into_the_strategy():
    assert LifecycleSimulator.symbols == ()


def test_the_timeframe_comes_from_the_mapping_params():
    assert LifecycleSimulator({"timeframe": "m5"}).timeframe == "M5"
    assert LifecycleSimulator({"timeframe": "1h"}).timeframe == "H1"
    with pytest.raises(ValueError):
        LifecycleSimulator({"timeframe": "M7"})


def test_it_still_instantiates_without_params_for_the_audit():
    assert LifecycleSimulator({}).timeframe  # the contract's fallback


# ── The cycles ────────────────────────────────────────────────────────────


@pytest.mark.parametrize("max_open_cycles", [1, 3])
def test_every_cycle_walks_one_scripted_path_one_bar_per_step(max_open_cycles):
    emitted = drive(LifecycleSimulator(), max_open_cycles=max_open_cycles)
    finished = 0
    for uxid, steps in cycles(emitted).items():
        assert len(uxid) == 16 and uxid == uxid.upper()
        positions = [position for position, _ in steps]
        actions = [action for _, action in steps]
        assert actions[0] in ENTRIES
        assert positions == list(range(positions[0], positions[0] + len(steps)))
        if actions[-1] in TERMINAL:
            assert tuple(actions[1:]) in EXIT_PATHS.values()
            finished += 1
    assert finished > 10


def test_a_single_cycle_pair_reopens_on_the_bar_after_the_exit():
    emitted = drive(LifecycleSimulator(), max_open_cycles=1)
    for (exit_bar, previous), (next_bar, current) in zip(emitted, emitted[1:], strict=False):
        if previous.action in TERMINAL:
            assert current.action in ENTRIES
            assert next_bar == exit_bar + 1


def test_several_cycles_open_one_entry_on_every_bar():
    emitted = drive(LifecycleSimulator(), max_open_cycles=3)
    entry_bars = [position for position, intent in emitted if intent.action in ENTRIES]
    assert entry_bars == list(range(entry_bars[0], entry_bars[0] + len(entry_bars)))


def test_all_three_outcomes_and_both_directions_occur():
    emitted = drive(LifecycleSimulator(), bar_count=300, max_open_cycles=3)
    entries = [intent for _, intent in emitted if intent.action in ENTRIES]
    assert set(Counter(intent.inputs["outcome"] for intent in entries)) == set(OUTCOMES)
    assert {intent.action for intent in entries} == ENTRIES


def test_the_same_seed_replays_the_same_cycles_and_another_does_not():
    def signature(params):
        return [
            (position, intent.action, intent.signal_uxid)
            for position, intent in drive(LifecycleSimulator(params))
        ]

    assert signature({"seed": 7}) == signature({"seed": 7})
    assert signature({"seed": 7}) != signature({"seed": 8})


@pytest.mark.parametrize("outcome", OUTCOMES)
def test_an_outcome_can_be_forced(outcome):
    for steps in cycles(drive(LifecycleSimulator({"outcome": outcome}))).values():
        actions = tuple(action for _, action in steps[1:])
        assert actions == EXIT_PATHS[outcome][: len(actions)]


def test_exits_carry_the_cycle_id_and_their_levels():
    emitted = drive(LifecycleSimulator({"outcome": "tp1_rsl"}), bar_count=40)
    entry = next(intent for _, intent in emitted if intent.action in ENTRIES)
    take_profit = next(intent for _, intent in emitted if intent.action is SignalAction.TP1)
    re_stop = next(intent for _, intent in emitted if intent.action is SignalAction.R_SL)
    assert take_profit.signal_uxid == re_stop.signal_uxid == entry.signal_uxid
    assert re_stop.sl == entry.price
    assert 0 < take_profit.tp1_percent < 100
    assert entry.sl is not None and entry.tp1 is not None and entry.tp2 is not None


def test_a_cycle_it_never_opened_is_closed_once_with_flat():
    strategy = LifecycleSimulator()
    context = StrategyContext(
        symbol="XAUUSD",
        timeframe="M15",
        now=datetime(2026, 1, 1, tzinfo=UTC),
        open_uxid="ABCDEF0123456789",
        open_uxids=("ABCDEF0123456789",),
    )
    first = as_intents(strategy.on_candle_closed(make_candles(40), context))
    again = as_intents(strategy.on_candle_closed(make_candles(41), context))
    assert [(intent.action, intent.signal_uxid) for intent in first] == [
        (SignalAction.FLAT, "ABCDEF0123456789")
    ]
    assert again == []


def test_a_tp1_that_would_close_everything_is_refused():
    with pytest.raises(ValueError, match="tp1_qty_pc"):
        drive(LifecycleSimulator({"tp1_qty_pc": 100}), bar_count=40)
