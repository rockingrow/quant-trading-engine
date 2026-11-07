"""``make demo-signal``: a real payload on a subject no worker can reach.

The drill's value rests on two claims, and these pin both. The first is that it
goes somewhere harmless: the demo subject must not sit under the broker's
``SIGNALS`` JetStream filter, or the drill would be fanned out to workers like
any other signal. The second is that what travels is a *real* payload — built
through the live ``SignalFactory``, with the pair's bracket and risk sizing —
because a drill that published something the broker would reject would prove
nothing about the path it is testing.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from qte_shared.bus import Subjects
from qte_shared.config import REPO_ROOT, settings
from qte_shared.models import SignalAction
from qte_shared.timeframes import floor_to_bucket, timeframe_seconds
from qte_strategy_engine import demo_signal
from qte_strategy_engine.demo_signal import (
    REGIMES,
    DemoSignal,
    build_demo_pairs,
    build_demo_signal,
    ideal_candles,
    publish_demo_signals,
    run_demo,
)

EXAMPLE_STRATEGY = "QTE_EXAMPLE_EMA_ATR"
MOMENT = datetime(2026, 6, 30, 12, 0, tzinfo=UTC)


@pytest.fixture
def example_mount(monkeypatch: pytest.MonkeyPatch):
    """Point the drill at the documented sample instead of the private mount."""
    monkeypatch.setattr(
        settings.engine, "strategies_dir", REPO_ROOT / "examples" / "__strategies__"
    )
    return settings.engine.strategies_dir


class RecordingBus:
    """Captures publishes; raises on anything that would need JetStream."""

    def __init__(self) -> None:
        self.published: list[tuple[str, dict]] = []
        self.flushes = 0
        self.nc = self

    async def publish(self, subject: str, payload: dict) -> None:
        self.published.append((subject, payload))

    async def publish_jetstream(self, *arguments, **keywords):
        raise AssertionError("A drill must not be published to a JetStream stream")

    async def flush(self, timeout: float | None = None) -> None:
        self.flushes += 1


# ── The subject ───────────────────────────────────────────────────────


def test_demo_subject_is_not_under_the_broker_signal_stream():
    """``SIGNALS_DEMO.X`` cannot match ``SIGNALS.>``, which is what keeps it safe.

    A wildcard follows whole tokens, so the only thing standing between a drill
    and the buffer the broker fans out from is the first token differing.
    """
    demo = Subjects.broker_signal_demo("ANY_STRATEGY")
    live = Subjects.broker_signal("ANY_STRATEGY")
    assert demo != live
    assert demo.split(".")[0] != live.split(".")[0]
    assert not demo.startswith(f"{live.split('.')[0]}.")


def test_demo_subject_honours_an_explicit_prefix():
    assert Subjects.broker_signal_demo("S1", prefix="DRILL") == "DRILL.S1"


# ── Ideal data ────────────────────────────────────────────────────────


@pytest.mark.parametrize("regime", REGIMES)
def test_ideal_candles_are_deterministic(regime: str):
    first = ideal_candles("XAUUSD", "M15", regime=regime, bar_count=50, end_time=MOMENT)
    second = ideal_candles("XAUUSD", "M15", regime=regime, bar_count=50, end_time=MOMENT)
    assert [candle.close for candle in first] == [candle.close for candle in second]


@pytest.mark.parametrize("regime", REGIMES)
def test_ideal_candles_are_well_formed(regime: str):
    candles = ideal_candles("XAUUSD", "M15", regime=regime, bar_count=40, end_time=MOMENT)
    assert len(candles) == 40
    step = timeframe_seconds("M15")
    for index, candle in enumerate(candles):
        assert candle.low <= min(candle.open, candle.close)
        assert candle.high >= max(candle.open, candle.close)
        assert candle.low < candle.high, "a flat bar leaves range indicators no range"
        assert candle.symbol == "XAUUSD"
        assert candle.timeframe == "M15"
        if index:
            gap = (candle.open_time - candles[index - 1].open_time).total_seconds()
            assert gap == step


def test_ideal_candles_end_on_the_last_closed_bucket():
    candles = ideal_candles("XAUUSD", "M15", regime="uptrend", bar_count=5, end_time=MOMENT)
    expected = floor_to_bucket(MOMENT, "M15").timestamp() - timeframe_seconds("M15")
    assert candles[-1].open_time.timestamp() == expected


def test_trending_regimes_move_the_way_they_are_named():
    up = ideal_candles("XAUUSD", "M15", regime="uptrend", bar_count=60, end_time=MOMENT)
    down = ideal_candles("XAUUSD", "M15", regime="downtrend", bar_count=60, end_time=MOMENT)
    assert up[-1].close > up[0].close
    assert down[-1].close < down[0].close


def test_reversal_regimes_turn_inside_the_window():
    """The extreme is reached before the last bar, so the entry is on the snap back."""
    dip = ideal_candles("XAUUSD", "M15", regime="dip-reversal", bar_count=100, end_time=MOMENT)
    closes = [candle.close for candle in dip]
    trough = closes.index(min(closes))
    assert 0 < trough < len(closes) - 1
    assert closes[-1] > closes[trough]

    spike = ideal_candles("XAUUSD", "M15", regime="spike-reversal", bar_count=100, end_time=MOMENT)
    closes = [candle.close for candle in spike]
    peak = closes.index(max(closes))
    assert 0 < peak < len(closes) - 1
    assert closes[-1] < closes[peak]


@pytest.mark.parametrize("bar_count", [40, 190, 440])
@pytest.mark.parametrize("regime", REGIMES)
def test_every_regime_stays_near_the_symbol_s_own_price(regime: str, bar_count: int):
    """A long warm-up must not compound the drift into a price no market reaches.

    The excursion is spread over the window rather than fixed per bar, so a
    strategy asking for 400 bars still gets a card quoting gold at gold's price.
    A per-bar drift is how this was wrong: 0.18% a bar over a 190-bar warm-up
    quoted gold 34% high.
    """
    candles = ideal_candles("XAUUSD", "M15", regime=regime, bar_count=bar_count, end_time=MOMENT)
    opening = candles[0].open
    for candle in candles:
        assert 0.9 * opening < candle.close < 1.1 * opening


@pytest.mark.parametrize("bar_count", [40, 190])
def test_bars_have_a_realistic_range(bar_count: int):
    """Each bar spans something an ATR floor would accept, at any window length."""
    candles = ideal_candles("XAUUSD", "M15", regime="uptrend", bar_count=bar_count, end_time=MOMENT)
    ranges = [(candle.high - candle.low) / candle.close for candle in candles]
    assert 0.0005 < max(ranges) < 0.02


def test_unknown_regime_is_refused():
    with pytest.raises(ValueError, match="Unknown regime"):
        ideal_candles("XAUUSD", "M15", regime="moonshot", bar_count=10)


@pytest.mark.parametrize("bar_count", [0, -1])
def test_asking_for_no_bars_is_refused_by_name(bar_count: int):
    """Not an IndexError from indexing the empty close list."""
    with pytest.raises(ValueError, match="bar_count must be at least 1"):
        ideal_candles("XAUUSD", "M15", regime="uptrend", bar_count=bar_count)


# ── Pair selection ────────────────────────────────────────────────────


def test_pairs_come_from_the_mount_when_no_mapping_table_exists(example_mount):
    """With no mapping file the strategy keeps the symbols it declares itself."""
    pairs = build_demo_pairs()
    assert [(pair.strategy_name, pair.symbol) for pair in pairs] == [(EXAMPLE_STRATEGY, "XAUUSD")]
    assert pairs[0].timeframe == "M15"
    assert pairs[0].warmup == 220


def test_strategy_filter_is_case_insensitive(example_mount):
    pairs = build_demo_pairs(EXAMPLE_STRATEGY.lower())
    assert len(pairs) == 1


def test_unknown_strategy_is_refused_with_the_names_that_exist(example_mount):
    with pytest.raises(ValueError, match="No mounted strategy named") as refusal:
        build_demo_pairs("NO_SUCH_STRATEGY")
    assert EXAMPLE_STRATEGY in str(refusal.value)


def test_a_mounted_strategy_not_mapped_to_the_symbol_says_so(example_mount):
    """Not "no such strategy" — it is mounted, it just does not trade that symbol.

    Reporting the wrong one of these sends the operator hunting for a strategy
    that the same message lists as mounted.
    """
    with pytest.raises(ValueError, match="is mounted but not mapped to") as refusal:
        build_demo_pairs(EXAMPLE_STRATEGY, symbol="BTCUSDT")
    message = str(refusal.value)
    assert "BTCUSDT" in message
    assert "No mounted strategy named" not in message


def test_symbol_filter_drops_a_strategy_that_does_not_trade_it(example_mount):
    assert build_demo_pairs(symbol="BTCUSDT") == []


# ── The payload ───────────────────────────────────────────────────────


def test_demo_signal_is_a_payload_the_broker_would_accept(example_mount):
    pair = build_demo_pairs()[0]
    demo = build_demo_signal(pair, moment=MOMENT)

    signal = demo.signal
    assert signal.strategy == EXAMPLE_STRATEGY
    assert signal.symbol == "XAUUSD"
    # The broker's contract spells M15 as the bare minute count.
    assert signal.timeframe == "15"
    assert signal.position.action.is_entry
    # validate_shape() inside the factory already refused a naked entry; these
    # restate what it guarantees, because that is what the drill is proving.
    assert signal.position.price is not None
    assert signal.position.quantity is not None and signal.position.quantity > 0
    assert signal.position.sl is not None
    assert demo.source == "synthetic" or demo.source in REGIMES


def test_demo_signal_leaves_no_cycle_behind(example_mount):
    """Nothing may remember the drill: it has no close coming, ever."""
    pair = build_demo_pairs()[0]
    build_demo_signal(pair, moment=MOMENT)
    assert pair.factory.open_cycles("XAUUSD") == ()
    assert pair.factory.open_positions() == {}


def test_each_drill_mints_its_own_cycle(example_mount):
    pair = build_demo_pairs()[0]
    first = build_demo_signal(pair, moment=MOMENT)
    second = build_demo_signal(pair, moment=MOMENT)
    assert first.signal.signal_uxid != second.signal.signal_uxid


def test_a_strategy_that_never_enters_still_gets_a_payload(example_mount, monkeypatch):
    """The transport is the point, so a silent strategy must not silence the drill."""
    monkeypatch.setattr(demo_signal, "_first_entry", lambda pair, candles: (None, ""))
    demo = build_demo_signal(build_demo_pairs()[0], moment=MOMENT)
    assert demo.source == demo_signal.SYNTHETIC
    assert demo.from_strategy is False
    assert demo.signal.position.action is SignalAction.LONG
    assert demo.strategy_error == "", "declining synthetic data is not an error"


def test_a_crashing_strategy_is_reported_but_does_not_stop_the_drill(example_mount, monkeypatch):
    """Declining and crashing must not look the same: one is normal, one is a bug."""
    pair = build_demo_pairs()[0]

    def explode(frame, context):
        raise RuntimeError("bad indicator")

    # Patched on the class, not the instance: the drill replays each regime on a
    # fresh instance so one regime's state cannot colour the next one's bars.
    monkeypatch.setattr(type(pair.strategy), "on_candle_closed", staticmethod(explode))
    demo = build_demo_signal(pair, moment=MOMENT)
    assert demo.source == demo_signal.SYNTHETIC
    assert demo.strategy_error == "RuntimeError: bad indicator"


async def test_run_demo_fails_when_a_strategy_raised(example_mount, monkeypatch, capsys):
    """A crash on plausible bars would be a crash on live bars; say so in the exit code."""
    monkeypatch.setattr(
        demo_signal, "_first_entry", lambda pair, candles: (None, "RuntimeError: boom")
    )

    async def capture(demos, *, bus=None):
        return []

    monkeypatch.setattr(demo_signal, "publish_demo_signals", capture)
    assert await run_demo() == 1
    printed = capsys.readouterr().out
    assert "RAISED" in printed
    assert "RuntimeError: boom" in printed


async def test_run_demo_succeeds_when_a_strategy_merely_declines(example_mount, monkeypatch):
    monkeypatch.setattr(demo_signal, "_first_entry", lambda pair, candles: (None, ""))

    async def capture(demos, *, bus=None):
        return []

    monkeypatch.setattr(demo_signal, "publish_demo_signals", capture)
    assert await run_demo() == 0


# ── Publishing ────────────────────────────────────────────────────────


async def test_publish_wraps_the_payload_and_targets_the_demo_subject(example_mount):
    demo = build_demo_signal(build_demo_pairs()[0], moment=MOMENT)
    bus = RecordingBus()
    deliveries = await publish_demo_signals([demo], bus=bus)

    assert len(bus.published) == 1
    subject, envelope = bus.published[0]
    assert subject == Subjects.broker_signal_demo(EXAMPLE_STRATEGY)
    assert subject != Subjects.broker_signal(EXAMPLE_STRATEGY)
    # The broker reads {"payload": …}, with the credential injected on the way
    # out rather than carried on the audited signal.
    assert set(envelope) == {"payload"}
    assert envelope["payload"]["strategy"] == EXAMPLE_STRATEGY
    assert "token" in envelope["payload"]
    assert deliveries[0].subject == subject
    assert bus.flushes == 1, "an unflushed publish can be lost when the CLI exits"


async def test_publishing_nothing_touches_no_connection():
    assert await publish_demo_signals([]) == []


async def test_a_failed_publish_does_not_discard_the_ones_before_it(example_mount):
    """The operator has to know which cards reached Telegram, or the re-run doubles them."""
    demo = build_demo_signal(build_demo_pairs()[0], moment=MOMENT)

    class HalfBrokenBus(RecordingBus):
        async def publish(self, subject: str, payload: dict) -> None:
            if self.published:
                raise ConnectionResetError("nats went away")
            await super().publish(subject, payload)

    bus = HalfBrokenBus()
    deliveries = await publish_demo_signals([demo, demo, demo], bus=bus)

    assert len(deliveries) == 3
    assert deliveries[0].delivered is True
    assert [delivery.delivered for delivery in deliveries[1:]] == [False, False]
    assert "ConnectionResetError" in deliveries[1].error


async def test_a_failed_flush_leaves_every_delivery_in_doubt(example_mount):
    """An unflushed envelope may never have left the client's write buffer."""
    demo = build_demo_signal(build_demo_pairs()[0], moment=MOMENT)

    class UnflushableBus(RecordingBus):
        async def flush(self, timeout: float | None = None) -> None:
            raise TimeoutError("flush timed out")

    deliveries = await publish_demo_signals([demo], bus=UnflushableBus())
    assert deliveries[0].delivered is False
    assert "unflushed" in deliveries[0].error


async def test_run_demo_fails_when_a_publish_did_not_land(example_mount, monkeypatch, capsys):
    async def half_fail(demos, *, bus=None):
        return [
            demo_signal.DemoDelivery(
                demo=demos[0], subject="SIGNALS_DEMO.X", error="ConnectionResetError: nope"
            )
        ]

    monkeypatch.setattr(demo_signal, "publish_demo_signals", half_fail)
    assert await run_demo() == 1
    printed = capsys.readouterr().out
    assert "NOT DELIVERED" in printed
    assert "Published 0 of 1" in printed


async def test_run_demo_publishes_every_pair(example_mount, monkeypatch):
    published: list[list[DemoSignal]] = []

    async def capture(demos, *, bus=None):
        published.append(demos)
        return []

    monkeypatch.setattr(demo_signal, "publish_demo_signals", capture)
    assert await run_demo() == 0
    assert len(published) == 1
    assert [demo.signal.strategy for demo in published[0]] == [EXAMPLE_STRATEGY]


async def test_run_demo_reports_a_pair_it_could_not_build(example_mount, monkeypatch):
    def explode(pair, *, moment=None):
        raise ValueError("no bracket")

    monkeypatch.setattr(demo_signal, "build_demo_signal", explode)
    assert await run_demo() == 1
