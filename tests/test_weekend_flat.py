"""A strategy's market shuts and the engine gets it flat, in both drivers.

The weekend flat used to live inside one strategy family. Moving it here means
three separate things have to hold, and each section below pins one of them:

* the window a repository *declares* is read the way it was written, wrap-around
  and non-UTC zones included;
* the plugin loader carries the declaration to the drivers, and refuses to run a
  strategy whose declaration did not parse — falling back to "off" would trade
  through a closed market on the strength of a typo;
* the live runner and the backtest replay both drop entries inside the window
  and close what is open, at the same point in the same bar.

The runner's periodic sweep is the one part with no replay counterpart: it
covers the minutes between bars, which a backtest has no bar for.
"""

from __future__ import annotations

import textwrap
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import pytest
from test_runner_delivery import (
    AcceptingSink,
    FakePositions,
    FakeState,
    RecordingBus,
    RecordingSignals,
)

from qte_backtest.replay import BacktestEngine
from qte_shared.config import settings
from qte_shared.models import SignalAction
from qte_shared.strategies.plugin_loader import StrategyLoader
from qte_shared.strategies.signal_factory import SignalFactory
from qte_shared.strategies.sizing import PositionSizer
from qte_shared.strategies.strategy_base import SignalIntent, StrategyBase
from qte_shared.strategies.strategy_settings import (
    WeekendFlatPolicy,
    WeeklyMoment,
    parse_settings_table,
)
from qte_strategy_engine.runner import StrategyRunner, StrategySlot

UTC_ZONE = ZoneInfo("UTC")

#: The window both mounted gold strategies declare.
GOLD_WINDOW = {"flat_from": "FRI 17:00", "flat_until": "SUN 22:00"}


def gold_policy() -> WeekendFlatPolicy:
    return WeekendFlatPolicy.parse(GOLD_WINDOW)


def moment_at(text: str) -> datetime:
    """A UTC instant from ``"2026-09-18 17:00"``, for readable assertions."""
    return datetime.fromisoformat(text).replace(tzinfo=UTC)


class WatermarkState(FakeState):
    """FakeState plus the decided-bar watermark the candle path writes."""

    def __init__(self) -> None:
        super().__init__()
        self.decided: datetime | None = None

    async def get_decided_open_time(self, strategy, symbol, timeframe):
        return self.decided

    async def set_decided_open_time(self, strategy, symbol, timeframe, open_time):
        self.decided = open_time

    async def get_candles(self, symbol, timeframe, count=0):
        return []


# ── What a repository declared ───────────────────────────────────────────


@pytest.mark.parametrize(
    "text,weekday,minute_of_day",
    [
        ("FRI 17:00", 4, 17 * 60),
        ("fri 17:00", 4, 17 * 60),
        ("Friday 17:30", 4, 17 * 60 + 30),
        ("SUN 22:00", 6, 22 * 60),
        ("MON 00:00", 0, 0),
        ("4 17:00", 4, 17 * 60),
        ("FRI,17:00", 4, 17 * 60),
    ],
)
def test_a_weekly_moment_reads_the_spellings_a_repo_might_write(text, weekday, minute_of_day):
    parsed = WeeklyMoment.parse(text)
    assert (parsed.weekday, parsed.minute_of_day) == (weekday, minute_of_day)


@pytest.mark.parametrize(
    "text",
    ["17:00", "FRI", "FRI 17", "FRI 24:00", "FRI 17:60", "SATURN 17:00", "7 17:00", "FRI 17:00:00"],
)
def test_an_unreadable_weekly_moment_is_refused(text):
    with pytest.raises(ValueError):
        WeeklyMoment.parse(text)


def test_a_weekly_moment_renders_back_out_as_it_was_written():
    assert WeeklyMoment.parse("FRI 17:00").describe() == "FRI 17:00"
    assert gold_policy().describe() == "FRI 17:00 -> SUN 22:00"


@pytest.mark.parametrize(
    "when,inside",
    [
        ("2026-09-18 16:45", False),  # Friday, the last bar before the cut-off
        ("2026-09-18 17:00", True),  # Friday, the cut-off bar itself
        ("2026-09-18 23:45", True),  # Friday night
        ("2026-09-19 12:00", True),  # Saturday
        ("2026-09-20 21:45", True),  # Sunday, the last minute of the window
        ("2026-09-20 22:00", False),  # Sunday, the reopen
        ("2026-09-21 09:00", False),  # Monday
        ("2026-09-16 17:00", False),  # a Wednesday at the same time of day
    ],
)
def test_the_window_wraps_past_sunday_midnight(when, inside):
    """The window is half-open: the cut-off is inside it, the reopen is not."""
    assert gold_policy().covers(moment_at(when), UTC_ZONE) is inside


def test_a_window_inside_one_day_does_not_wrap():
    """The same comparison has to serve a maintenance break, not only a weekend."""
    policy = WeekendFlatPolicy.parse({"flat_from": "WED 21:00", "flat_until": "WED 22:00"})
    assert policy.covers(moment_at("2026-09-16 21:30"), UTC_ZONE)
    assert not policy.covers(moment_at("2026-09-16 20:59"), UTC_ZONE)
    assert not policy.covers(moment_at("2026-09-16 22:00"), UTC_ZONE)


def test_a_disabled_policy_covers_nothing():
    assert not WeekendFlatPolicy().covers(moment_at("2026-09-19 12:00"), UTC_ZONE)
    assert WeekendFlatPolicy.parse(False).describe() == "off"
    assert WeekendFlatPolicy.parse({"enabled": False, **GOLD_WINDOW}).describe() == "off"


@pytest.mark.parametrize(
    "payload,message",
    [
        (True, "declares no window"),
        ({"flat_from": "FRI 17:00"}, "declares no flat_until"),
        ({"flat_until": "SUN 22:00"}, "declares no flat_from"),
        ({"flat_from": "FRI 17:00", "flat_until": "FRI 17:00"}, "same moment"),
        ({"flat_form": "FRI 17:00", "flat_until": "SUN 22:00"}, "no setting called flat_form"),
        ({"enabled": "yes", **GOLD_WINDOW}, "must be true or false"),
        ("FRI 17:00", "must be a table"),
    ],
)
def test_an_unusable_policy_says_what_is_wrong_with_it(payload, message):
    with pytest.raises(ValueError, match=message):
        WeekendFlatPolicy.parse(payload)


def test_a_settings_error_names_the_alias_it_came_from():
    """One pass reads every mounted repo, so the alias is half the answer."""
    with pytest.raises(ValueError, match="MT5_GOLD_M5_V2"):
        parse_settings_table({"MT5_GOLD_M5_V2": {"weekend_flat": {"flat_from": "FRI 17:00"}}})


def test_an_unknown_setting_is_refused_rather_than_ignored():
    with pytest.raises(ValueError, match="no setting called holiday_calendar"):
        parse_settings_table({"PROBE": {"holiday_calendar": "US"}})


# ── The zone the window is read in ───────────────────────────────────────


def test_the_window_is_read_in_the_configured_zone_not_in_utc():
    """``FRI 17:00`` in New York is 21:00 UTC, and the engine must agree."""
    policy = gold_policy()
    new_york = ZoneInfo("America/New_York")
    just_before = moment_at("2026-09-18 20:45")
    just_after = moment_at("2026-09-18 21:00")

    assert policy.covers(just_before, UTC_ZONE)
    assert policy.covers(just_after, UTC_ZONE)
    # Read on New York's clock the same two instants straddle the cut-off.
    assert not policy.covers(just_before, new_york)
    assert policy.covers(just_after, new_york)


def test_the_cut_off_follows_a_daylight_saving_change():
    """A wall-clock declaration means the UTC instant moves, which is the point.

    New York left DST on 2026-11-01, so ``FRI 17:00`` there is 21:00 UTC in
    October and 22:00 UTC in November. A strategy declaring a local market close
    wants exactly that; pinning it to a UTC offset would drift an hour twice a
    year.
    """
    policy = gold_policy()
    new_york = ZoneInfo("America/New_York")

    assert policy.covers(moment_at("2026-10-30 21:00"), new_york)
    assert not policy.covers(moment_at("2026-10-30 20:59"), new_york)

    assert policy.covers(moment_at("2026-11-06 22:00"), new_york)
    assert not policy.covers(moment_at("2026-11-06 21:00"), new_york)


def test_a_naive_moment_is_taken_as_already_being_in_that_zone():
    """A history file that lost its tzinfo must not lose the protection too."""
    naive = datetime(2026, 9, 18, 17, 0)
    assert gold_policy().covers(naive, UTC_ZONE)


def test_the_configured_zone_must_exist():
    from qte_shared.config import EngineSettings

    with pytest.raises(ValueError, match="WEEKEND_FLAT_TIMEZONE"):
        EngineSettings(weekend_flat_timezone="Nowhere/Fake")
    assert EngineSettings(weekend_flat_timezone="Europe/Nicosia").market_zone.key == (
        "Europe/Nicosia"
    )


# ── The loader carries the declaration ───────────────────────────────────

PROBE_STRATEGY = """
class WeekendProbe:
    name = "WEEKEND_PROBE"
    symbols = ("XAUUSD",)
    timeframe = "M15"
    warmup = 1
    params: dict = {}

    def __init__(self, params=None):
        self.params = dict(params or {})

    def on_candle_closed(self, candles_frame, context):
        return None

    def on_start(self, context):
        return None

    def on_stop(self):
        return None

    def history_window(self):
        return 400
"""


def mount_repository(directory, *, settings_body: str | None) -> None:
    """Write a one-strategy plugin repo, with or without a settings hook."""
    repository = directory / "probe-repo"
    repository.mkdir()
    (repository / "probe.py").write_text(PROBE_STRATEGY, encoding="utf-8")
    manifest = [
        "import sys",
        "from pathlib import Path",
        "sys.path.insert(0, str(Path(__file__).resolve().parent))",
        "from probe import WeekendProbe",
        "",
        "def load_all():",
        "    return {'WEEKEND_PROBE': WeekendProbe}",
        "",
    ]
    if settings_body is not None:
        manifest.append(textwrap.dedent(settings_body))
    (repository / "manifest.py").write_text("\n".join(manifest), encoding="utf-8")


def test_a_repo_declaring_a_window_hands_it_to_the_engine(tmp_path):
    mount_repository(
        tmp_path,
        settings_body="""
        def load_settings():
            return {
                'WEEKEND_PROBE': {
                    'weekend_flat': {'flat_from': 'FRI 17:00', 'flat_until': 'SUN 22:00'}
                }
            }
        """,
    )
    discovered = StrategyLoader(tmp_path).discover()

    assert [entry.name for entry in discovered] == ["WEEKEND_PROBE"]
    assert discovered[0].settings.weekend_flat.describe() == "FRI 17:00 -> SUN 22:00"


def test_a_repo_with_no_settings_hook_keeps_the_engine_defaults(tmp_path):
    """Every strategy mounted before the hook existed is in this case."""
    mount_repository(tmp_path, settings_body=None)
    discovered = StrategyLoader(tmp_path).discover()

    assert not discovered[0].settings.weekend_flat.enabled
    assert discovered[0].settings.weekend_flat.describe() == "off"


def test_settings_that_will_not_parse_stop_the_strategy_loading(tmp_path, caplog):
    """Fail closed. "Off" on a typo is a position held through a shut market."""
    mount_repository(
        tmp_path,
        settings_body="""
        def load_settings():
            return {'WEEKEND_PROBE': {'weekend_flat': {'flat_from': 'FRI 17:00'}}}
        """,
    )
    loader = StrategyLoader(tmp_path)
    with caplog.at_level("ERROR"):
        discovered = loader.discover()

    assert discovered == []
    assert [failure.reason for failure in loader.failures] == ["load_settings() did not read"]
    assert any("did not read" in record.getMessage() for record in caplog.records)


def test_a_settings_hook_that_raises_stops_the_strategy_loading(tmp_path):
    mount_repository(
        tmp_path,
        settings_body="""
        def load_settings():
            raise RuntimeError('the settings file is broken')
        """,
    )
    loader = StrategyLoader(tmp_path)

    assert loader.discover() == []
    assert loader.failures and "load_settings" in loader.failures[0].reason


def test_settings_for_an_unpublished_alias_are_reported_but_not_fatal(tmp_path, caplog):
    """A rename that touched one table and not the other."""
    mount_repository(
        tmp_path,
        settings_body="""
        def load_settings():
            return {
                'WEEKEND_PROBE': {'weekend_flat': False},
                'RENAMED_AWAY': {'weekend_flat': False},
            }
        """,
    )
    with caplog.at_level("ERROR"):
        discovered = StrategyLoader(tmp_path).discover()

    assert [entry.name for entry in discovered] == ["WEEKEND_PROBE"]
    assert any("RENAMED_AWAY" in record.getMessage() for record in caplog.records)


def test_find_returns_the_record_and_load_one_still_returns_an_instance(tmp_path):
    """``find`` is how the backtest reaches the settings beside the class."""
    mount_repository(
        tmp_path,
        settings_body="""
        def load_settings():
            return {
                'WEEKEND_PROBE': {
                    'weekend_flat': {'flat_from': 'FRI 17:00', 'flat_until': 'SUN 22:00'}
                }
            }
        """,
    )
    loader = StrategyLoader(tmp_path)

    assert loader.find("WEEKEND_PROBE").settings.weekend_flat.enabled
    assert loader.load_one("WEEKEND_PROBE").name == "WEEKEND_PROBE"
    with pytest.raises(LookupError, match="WEEKEND_PROBE"):
        loader.find("NOPE")


def test_the_mounted_repositories_declare_the_windows_they_trade():
    """The real ``__strategies__/`` tree, so a rename here cannot go unnoticed."""
    declared = {
        entry.name: entry.settings.weekend_flat.describe()
        for entry in StrategyLoader(settings.engine.strategies_dir).discover()
    }
    for alias in ("MT5_GOLD_M5_V2", "MT5_GOLD_M15_V2"):
        assert declared.get(alias) == "FRI 17:00 -> SUN 22:00", declared


def holiday_policy() -> WeekendFlatPolicy:
    """A known shortened Friday, declared before either driver sees any bars."""
    return WeekendFlatPolicy.parse(
        {
            **GOLD_WINDOW,
            "extra_windows": [{"flat_from": "2026-06-19 13:00", "flat_until": "2026-06-21 22:00"}],
        }
    )


@pytest.mark.parametrize(
    "timestamp,expected",
    [
        ("2026-06-19 12:59", False),
        ("2026-06-19 13:00", True),
        ("2026-06-19 16:00", True),
        ("2026-06-21 21:59", True),
        ("2026-06-21 22:00", False),
        ("2026-06-26 13:00", False),
        ("2026-06-26 17:00", True),
    ],
)
def test_dated_closures_extend_only_the_declared_week(timestamp, expected):
    assert holiday_policy().covers(moment_at(timestamp), UTC_ZONE) is expected


def test_dated_closures_follow_the_configured_market_zone():
    policy = holiday_policy()
    timestamp = moment_at("2026-06-19 04:00")
    assert policy.covers(timestamp, ZoneInfo("Asia/Tokyo"))
    assert not policy.covers(timestamp, UTC_ZONE)
    assert policy.covers(datetime(2026, 6, 19, 13), UTC_ZONE)


def test_a_dated_window_cannot_shorten_the_weekly_block():
    policy = WeekendFlatPolicy.parse(
        {
            **GOLD_WINDOW,
            "extra_windows": [{"flat_from": "2026-06-19 13:00", "flat_until": "2026-06-19 16:00"}],
        }
    )
    assert policy.covers(moment_at("2026-06-19 13:00"), UTC_ZONE)
    assert not policy.covers(moment_at("2026-06-19 16:30"), UTC_ZONE)
    assert policy.covers(moment_at("2026-06-19 17:00"), UTC_ZONE)


@pytest.mark.parametrize(
    "declared",
    [
        None,
        "2026-06-19",
        [False],
        [{"flat_from": "2026-06-19 13:00"}],
        [{"flat_from": "2026-06-19", "flat_until": "2026-06-21 22:00"}],
        [{"flat_from": 1300, "flat_until": "2026-06-21 22:00"}],
        [{"flat_from": "2026-06-19 13:00Z", "flat_until": "2026-06-21 22:00"}],
        [{"flat_from": "2026-06-19 13:00", "flat_until": "2026-06-19 13:00"}],
        [{"flat_from": "2026-06-21 22:00", "flat_until": "2026-06-19 13:00"}],
    ],
)
def test_malformed_dated_closures_fail_instead_of_disabling_protection(declared):
    with pytest.raises(ValueError, match="extra_windows"):
        WeekendFlatPolicy.parse({**GOLD_WINDOW, "extra_windows": declared})


def test_malformed_dated_closures_prevent_plugin_loading(tmp_path):
    mount_repository(
        tmp_path,
        settings_body="""
        def load_settings():
            return {'WEEKEND_PROBE': {'weekend_flat': {
                'flat_from': 'FRI 17:00', 'flat_until': 'SUN 22:00',
                'extra_windows': [{'flat_from': '2026-06-19 13:00'}],
            }}}
        """,
    )
    loader = StrategyLoader(tmp_path)
    assert loader.discover() == []
    assert [failure.reason for failure in loader.failures] == ["load_settings() did not read"]


def test_dated_closures_are_visible_in_the_policy_description():
    description = holiday_policy().describe()
    assert description.startswith("FRI 17:00 -> SUN 22:00")
    assert "2026-06-19 13:00 -> 2026-06-21 22:00" in description


# ── The live runner ──────────────────────────────────────────────────────


class EntryEveryBar(StrategyBase):
    """Enters whenever it is flat, so a missing gate shows up immediately."""

    name = "WEEKEND_PROBE"
    timeframe = "M15"
    warmup = 1

    def on_candle_closed(self, candles_frame, context):
        if context.open_uxid is not None:
            return None
        return SignalIntent(action=SignalAction.LONG, price=2000.0, sl=1990.0)


def weekend_runner(*, policy: WeekendFlatPolicy | None = None):
    """A runner holding one slot, wired to the recording transports."""
    runner = StrategyRunner(sink=AcceptingSink())
    runner.signals = RecordingSignals()
    runner.bus = RecordingBus()
    runner.state = WatermarkState()
    runner.state.owner_id = runner._owner_id
    runner._ownership_acquired = True
    runner.positions = FakePositions()
    runner._by_subject = defaultdict(list)
    strategy = EntryEveryBar()
    strategy_slot = StrategySlot(
        strategy,
        "XAUUSD",
        SignalFactory(
            strategy.name,
            timeframe="M15",
            token="test",
            sizer=PositionSizer(capital=10_000.0, risk_percent=1.0),
        ),
        weekend_flat=policy if policy is not None else gold_policy(),
    )
    runner.slots.append(strategy_slot)
    runner._by_subject[("XAUUSD", "M15")].append(strategy_slot)
    return runner, strategy_slot


def emitted_actions(runner) -> list[str]:
    return [signal.position.action.value for signal, _ in runner.signals.rows]


def emitted_signals(runner) -> list:
    return [signal for signal, _ in runner.signals.rows]


async def open_a_position(runner, strategy_slot) -> None:
    await runner._emit(
        strategy_slot,
        SignalIntent(action=SignalAction.LONG, price=2000.0, sl=1990.0),
        fallback_price=2000.0,
        moment=moment_at("2026-09-18 12:00"),
    )
    assert strategy_slot.factory.open_cycle("XAUUSD") is not None


async def test_an_open_position_is_flattened_on_the_cut_off_bar():
    runner, strategy_slot = weekend_runner()
    await open_a_position(runner, strategy_slot)
    runner.signals.rows.clear()

    await runner._flatten_for_the_weekend(strategy_slot, 2001.0, moment_at("2026-09-18 17:00"))

    assert emitted_actions(runner) == ["FLAT"]
    assert emitted_signals(runner)[0].position.quantity is None, "a FLAT carries no size"
    assert strategy_slot.factory.open_cycle("XAUUSD") is None


async def test_flattening_a_pair_that_is_already_flat_sends_nothing():
    """Safe to call on every bar inside the window, and from the sweep."""
    runner, strategy_slot = weekend_runner()

    await runner._flatten_for_the_weekend(strategy_slot, 2001.0, moment_at("2026-09-18 17:00"))

    assert emitted_actions(runner) == []


async def test_the_cut_off_bar_closes_the_position_through_the_candle_path():
    """The whole bar, through the handler the candle subscription calls.

    The strategy holds a position here, so it decides nothing — everything on
    the wire is the runner's own doing.
    """
    runner, strategy_slot = weekend_runner()
    await open_a_position(runner, strategy_slot)
    runner.signals.rows.clear()

    await feed_one_bar(runner, strategy_slot, moment_at("2026-09-18 17:00"))

    assert emitted_actions(runner) == ["FLAT"]
    assert strategy_slot.factory.open_cycle("XAUUSD") is None


async def test_an_entry_inside_the_window_never_reaches_the_outbox(caplog):
    """The pair is flat, so the strategy does offer an entry — and is refused."""
    runner, strategy_slot = weekend_runner()

    with caplog.at_level("INFO"):
        await feed_one_bar(runner, strategy_slot, moment_at("2026-09-18 17:00"))

    assert emitted_actions(runner) == []
    assert strategy_slot.factory.open_cycle("XAUUSD") is None
    blocked = [
        record.getMessage()
        for record in caplog.records
        if "inside the weekend-flat window" in record.getMessage()
    ]
    assert blocked, [record.getMessage() for record in caplog.records]
    assert "LONG" in blocked[0]


async def test_an_entry_outside_the_window_is_untouched():
    runner, strategy_slot = weekend_runner()

    await feed_one_bar(runner, strategy_slot, moment_at("2026-09-18 16:45"))

    assert emitted_actions(runner) == ["LONG"]


async def test_no_entry_is_opened_anywhere_inside_the_window():
    """Bar after bar from the cut-off to the reopen, and nothing is entered."""
    runner, strategy_slot = weekend_runner()
    moment = moment_at("2026-09-18 17:00")
    reopen = moment_at("2026-09-20 22:00")

    while moment < reopen:
        await feed_one_bar(runner, strategy_slot, moment)
        moment += timedelta(hours=1)

    assert emitted_actions(runner) == []
    assert strategy_slot.factory.open_cycle("XAUUSD") is None

    # And the first bar after the reopen trades again.
    await feed_one_bar(runner, strategy_slot, reopen)
    assert emitted_actions(runner) == ["LONG"]


async def test_a_disabled_policy_changes_nothing_in_the_runner():
    runner, strategy_slot = weekend_runner(policy=WeekendFlatPolicy())

    await feed_one_bar(runner, strategy_slot, moment_at("2026-09-19 12:00"))

    assert emitted_actions(runner) == ["LONG"]


async def feed_one_bar(runner, strategy_slot, moment: datetime) -> None:
    """Drive the candle path for one bar, bypassing NATS and Redis history."""
    from qte_shared.models import Candle

    candle = Candle(
        origin=settings.state_scope.origin(),
        symbol="XAUUSD",
        timeframe="M15",
        open_time=moment,
        open=2000.0,
        high=2001.0,
        low=1999.0,
        close=2000.5,
        tick_count=5,
    )
    async with strategy_slot.lock:
        await runner._feed_candle_serialized(strategy_slot, candle)


# ── The sweep between bars ───────────────────────────────────────────────


async def test_the_sweep_flattens_without_a_candle_close(monkeypatch):
    """A feed that stalled at 16:50 on a Friday leaves no bar to act on."""
    runner, strategy_slot = weekend_runner()
    await feed_one_bar(runner, strategy_slot, moment_at("2026-09-18 16:45"))
    runner.signals.rows.clear()
    assert strategy_slot.factory.open_cycle("XAUUSD") is not None

    SweepClock.current_time = moment_at("2026-09-18 17:05")
    monkeypatch.setattr("qte_strategy_engine.runner.datetime", SweepClock)
    await runner._sweep_one_slot(strategy_slot)

    assert emitted_actions(runner) == ["FLAT"]
    assert strategy_slot.factory.open_cycle("XAUUSD") is None


async def test_the_sweep_does_nothing_before_the_cut_off(monkeypatch):
    runner, strategy_slot = weekend_runner()
    await feed_one_bar(runner, strategy_slot, moment_at("2026-09-18 16:45"))
    runner.signals.rows.clear()

    SweepClock.current_time = moment_at("2026-09-18 16:50")
    monkeypatch.setattr("qte_strategy_engine.runner.datetime", SweepClock)
    await runner._sweep_one_slot(strategy_slot)

    assert emitted_actions(runner) == []
    assert strategy_slot.factory.open_cycle("XAUUSD") is not None


async def test_the_sweep_honors_an_early_holiday_cutoff_without_new_bars(monkeypatch):
    runner, strategy_slot = weekend_runner(policy=holiday_policy())
    await feed_one_bar(runner, strategy_slot, moment_at("2026-06-19 12:45"))
    runner.signals.rows.clear()

    SweepClock.current_time = moment_at("2026-06-19 13:00")
    monkeypatch.setattr("qte_strategy_engine.runner.datetime", SweepClock)
    await runner._sweep_one_slot(strategy_slot)
    await runner._sweep_one_slot(strategy_slot)

    assert emitted_actions(runner) == ["FLAT"]
    assert strategy_slot.factory.open_cycle("XAUUSD") is None


class SweepClock(datetime):
    """``datetime.now()`` pinned to one instant, as test_driver_parity does it.

    The sweep reads the wall clock rather than a bar time — the whole point of
    it is that no bar arrived — so pinning the clock is the only way to put it
    inside the window.
    """

    current_time = datetime(2026, 9, 18, 12, tzinfo=UTC)

    @classmethod
    def now(cls, timezone=None):
        return cls.current_time.astimezone(timezone) if timezone else cls.current_time


# ── The backtest replay decides the same way ─────────────────────────────


def bars_across_a_weekend() -> pd.DataFrame:
    """Hourly bars from Friday noon to Monday noon, spanning the window."""
    index = pd.date_range(start="2026-09-18 12:00", end="2026-09-21 12:00", freq="1h", tz="UTC")
    return pd.DataFrame(
        {
            "open": 2000.0,
            "high": 2000.5,
            "low": 1999.5,
            "close": 2000.0,
            "volume": 100.0,
        },
        index=index,
    )


def replay_across_the_weekend(policy: WeekendFlatPolicy | None):
    engine = BacktestEngine(
        EntryEveryBar(),
        symbol="XAUUSD",
        timeframe="H1",
        starting_equity=10_000.0,
        default_quantity=0.1,
        sizer=PositionSizer(capital=10_000.0, risk_percent=1.0),
        weekend_flat=policy,
    )
    return engine, engine.run(bars_across_a_weekend())


def test_the_replay_closes_the_position_on_the_cut_off_bar():
    engine, result = replay_across_the_weekend(gold_policy())
    flats = [signal for signal in result.signals if signal.position.action is SignalAction.FLAT]

    assert flats, "the weekend window closed nothing"
    assert flats[0].timestamp == moment_at("2026-09-18 17:00")
    assert all(signal.position.action is SignalAction.FLAT for signal in flats)


def test_the_replay_opens_nothing_inside_the_window():
    _, result = replay_across_the_weekend(gold_policy())
    entries = [signal for signal in result.signals if signal.position.action.is_entry]

    assert entries, "the strategy should still trade outside the window"
    for signal in entries:
        assert not gold_policy().covers(signal.timestamp, UTC_ZONE), signal.timestamp


def test_the_replay_counts_the_entries_the_window_refused():
    engine, _ = replay_across_the_weekend(gold_policy())

    assert engine._blocked > 0


def test_a_replay_with_no_policy_trades_straight_through_the_weekend():
    """The default, so a run that never declared a window is unchanged."""
    _, result = replay_across_the_weekend(None)
    flats = [signal for signal in result.signals if signal.position.action is SignalAction.FLAT]

    assert flats == []


def test_the_two_drivers_flatten_on_the_same_bar():
    """The parity that matters: a backtest has to predict the Friday evening."""
    _, result = replay_across_the_weekend(gold_policy())
    first_flat = next(
        signal.timestamp for signal in result.signals if signal.position.action is SignalAction.FLAT
    )
    policy = gold_policy()

    # The runner's rule, stated independently: the first bar the window covers.
    bars = bars_across_a_weekend().index
    first_covered = next(
        moment.to_pydatetime() for moment in bars if policy.covers(moment, UTC_ZONE)
    )
    assert first_flat == first_covered


async def test_both_drivers_flatten_before_a_shortened_friday_session_ends():
    policy = holiday_policy()
    timestamps = [
        moment_at("2026-06-19 12:45"),
        moment_at("2026-06-19 13:00"),
        moment_at("2026-06-19 13:45"),
        moment_at("2026-06-21 21:45"),
        moment_at("2026-06-21 22:00"),
        moment_at("2026-06-21 22:15"),
    ]
    candles = pd.DataFrame(
        {"open": 2000.0, "high": 2001.0, "low": 1999.0, "close": 2000.5, "volume": 1.0},
        index=pd.DatetimeIndex(timestamps),
    )
    engine = BacktestEngine(
        EntryEveryBar(),
        symbol="XAUUSD",
        starting_equity=10_000.0,
        sizer=PositionSizer(capital=10_000.0, risk_percent=1.0),
        weekend_flat=policy,
    )
    result = engine.run(candles)
    runner, strategy_slot = weekend_runner(policy=policy)
    for timestamp in timestamps:
        await feed_one_bar(runner, strategy_slot, timestamp)

    expected = [
        (timestamps[0], SignalAction.LONG),
        (timestamps[1], SignalAction.FLAT),
        (timestamps[4], SignalAction.LONG),
    ]
    assert [(signal.timestamp, signal.position.action) for signal in result.signals] == expected
    assert [
        (signal.timestamp, signal.position.action) for signal in emitted_signals(runner)
    ] == expected
    assert result.positions[0].closed_at == timestamps[1]
    assert all(
        not policy.covers(signal.timestamp, UTC_ZONE)
        for signal in result.signals
        if signal.position.action.is_entry
    )


@pytest.fixture(autouse=True)
def live_state_scope(monkeypatch):
    """A live book with fake transports, as the delivery tests use."""
    monkeypatch.setattr(settings, "env", "prod")
    monkeypatch.setattr(settings.state_config, "execution_mode", "live")
    monkeypatch.setattr(settings.market_data, "provider", "tiingo")
