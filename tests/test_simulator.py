"""The dev-only simulator: the guard, the bar↔tick round trip, and the socket.

The load-bearing test here is
:func:`test_a_bar_played_through_the_real_resampler_comes_back_unchanged`. The
simulator's whole claim is that a bar sent into ingestion comes back out as the
same candle; everything else — anchors, cursors, sealing — exists to make that
true at more than one bar. So it is asserted against the *real*
`qte_ingestion.resampler.Resampler`, not against a restatement of what the
synthesis intended.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
import websockets
from qte_ingestion.resampler import Resampler
from qte_shared.dev_only import DevOnlyError, require_dev_env
from qte_shared.models import Candle, Tick
from qte_shared.providers import available_providers, create_provider
from qte_shared.providers.simulator import SimulatorProvider, SimulatorSettings
from qte_shared.providers.simulator.protocol import (
    decode_tick,
    dumps,
    encode_tick,
    loads,
    subscribe_frame,
)
from qte_shared.symbols import build_specs
from qte_shared.timeframes import floor_to_bucket, timeframe_seconds
from qte_simulator.bars import (
    TICKS_PER_BAR,
    BarError,
    BarSpec,
    anchor_open_times,
    bar_ticks,
    expected_candle,
    generate_bars,
    seal_tick,
)
from qte_simulator.client import ControlClient
from qte_simulator.control import CommandError, dispatch
from qte_simulator.hub import SimulatorHub
from qte_simulator.server import SimulatorServer
from qte_simulator.sources import SourceError, load_bars
from qte_simulator.verify import compare

NOW = datetime(2026, 3, 4, 12, 7, 31, tzinfo=UTC)


def a_bar(**overrides) -> BarSpec:
    fields = {
        "symbol": "XAUUSD",
        "timeframe": "M15",
        "open_time": datetime(2026, 3, 4, 11, 0, tzinfo=UTC),
        "open": 2400.0,
        "high": 2410.5,
        "low": 2397.25,
        "close": 2408.75,
        "volume": 123.456,
    }
    return BarSpec(**{**fields, **overrides})


# ── The guard ─────────────────────────────────────────────────────────────


def test_a_dev_only_component_refuses_to_run_outside_dev(monkeypatch):
    from qte_shared import dev_only

    monkeypatch.setattr(dev_only.settings, "env", "prod")
    with pytest.raises(DevOnlyError) as excinfo:
        require_dev_env("The market data simulator")
    assert "QTE_ENV" in str(excinfo.value)


def test_the_provider_itself_is_what_refuses(monkeypatch):
    """Not a comment, not a naming convention: constructing it is the check.

    A live engine reaching the simulator would trade invented prices and log
    nothing unusual doing it, so the refusal has to sit where the wiring
    happens rather than where someone remembers to look.
    """
    from qte_shared import dev_only

    monkeypatch.setattr(dev_only.settings, "env", "staging")
    with pytest.raises(DevOnlyError):
        create_provider("simulator")


def test_the_simulator_is_a_registered_provider_like_any_other():
    assert "simulator" in available_providers()
    provider = create_provider("simulator")
    assert isinstance(provider, SimulatorProvider)
    assert provider.name == "simulator"


def test_it_serves_live_and_deliberately_not_history():
    """Invented history would produce an equity curve that means nothing."""
    from qte_shared.interfaces import Capability, UnsupportedCapability

    provider = create_provider("simulator")
    assert provider.supports(Capability.LIVE)
    assert not provider.supports(Capability.HISTORY)
    with pytest.raises(UnsupportedCapability):
        provider.history_source()


def test_one_socket_carries_every_symbol():
    provider = create_provider("simulator")
    feeds = provider.live_feeds(build_specs(["XAUUSD", "BTCUSDT"]), _noop)
    assert len(feeds) == 1
    assert feeds[0].symbols == ("BTCUSDT", "XAUUSD")


# ── Bars into ticks, and back ─────────────────────────────────────────────


def test_a_bar_played_through_the_real_resampler_comes_back_unchanged():
    bar = a_bar()
    resampler = Resampler(bar.symbol, [bar.timeframe])

    closed: list[Candle] = []
    for tick in bar_ticks(bar):
        closed.extend(resampler.add_tick(tick))
    closed.extend(resampler.add_tick(seal_tick(bar)))

    assert len(closed) == 1
    assert compare(expected_candle(bar), closed[0]).ok


@pytest.mark.parametrize("timeframe", ["M1", "M5", "M15", "H1", "H4", "D1"])
def test_every_tick_of_a_bar_lands_inside_its_own_bucket(timeframe):
    """A tick on the boundary belongs to the next bar and would open it wrong."""
    bar = a_bar(timeframe=timeframe, open_time=datetime(2026, 3, 4, tzinfo=UTC))
    ticks = bar_ticks(bar)

    assert len(ticks) == TICKS_PER_BAR
    for tick in ticks:
        assert floor_to_bucket(tick.ts, timeframe) == bar.open_time
    assert seal_tick(bar).ts == bar.open_time + timedelta(seconds=timeframe_seconds(timeframe))


def test_the_tick_path_matches_the_direction_of_the_bar():
    """A bullish bar that printed its high before its low is a bar that fell."""
    prices = [tick.price for tick in bar_ticks(a_bar(open=2400.0, close=2408.75))]
    assert prices == [2400.0, 2397.25, 2410.5, 2408.75]

    prices = [tick.price for tick in bar_ticks(a_bar(open=2408.75, close=2400.0))]
    assert prices == [2408.75, 2410.5, 2397.25, 2400.0]


def test_the_volume_shares_sum_back_to_the_bar():
    bar = a_bar(volume=123.456)
    assert sum(tick.volume for tick in bar_ticks(bar)) == pytest.approx(bar.volume, rel=1e-12)


def test_price_travels_in_last_so_a_spread_cannot_move_the_candle():
    """Rebuilding from a bid/ask midpoint would drift by half a spread."""
    ticks = bar_ticks(a_bar(), spread=0.5)
    for tick in ticks:
        assert tick.ask - tick.bid == pytest.approx(0.5)
        assert tick.price == tick.last


@pytest.mark.parametrize(
    "overrides",
    [
        {"high": 2395.0},  # high below the body
        {"low": 2412.0},  # low above the body
        {"high": 2390.0, "low": 2400.0},  # inverted
        {"volume": -1.0},
    ],
)
def test_a_bar_that_could_not_have_printed_is_refused(overrides):
    with pytest.raises(BarError):
        a_bar(**overrides)


# ── Anchoring ─────────────────────────────────────────────────────────────


def test_past_anchoring_ends_on_the_last_completed_bucket():
    times = anchor_open_times(3, "M15", mode="past", now=NOW)

    assert times[-1] == datetime(2026, 3, 4, 11, 45, tzinfo=UTC)
    assert times[0] == datetime(2026, 3, 4, 11, 15, tzinfo=UTC)
    assert all(moment + timedelta(minutes=15) <= NOW for moment in times)


def test_next_anchoring_starts_at_the_current_bucket_and_moves_forward():
    """Nothing the wall-clock flush can close, so nothing it can tear in half."""
    times = anchor_open_times(3, "M15", mode="next", now=NOW)

    assert times[0] == datetime(2026, 3, 4, 12, 0, tzinfo=UTC)
    assert all(moment + timedelta(minutes=15) > NOW for moment in times)


def test_next_anchoring_continues_from_the_cursor():
    cursor = datetime(2026, 3, 4, 18, 0, tzinfo=UTC)
    times = anchor_open_times(2, "M15", mode="next", now=NOW, cursor=cursor)
    assert times == [
        datetime(2026, 3, 4, 18, 15, tzinfo=UTC),
        datetime(2026, 3, 4, 18, 30, tzinfo=UTC),
    ]


def test_an_unknown_anchor_is_refused():
    with pytest.raises(BarError):
        anchor_open_times(1, "M15", mode="sideways")


# ── Generated bars ────────────────────────────────────────────────────────


def test_generated_bars_are_gapless_and_reproducible():
    times = anchor_open_times(20, "M15", mode="past", now=NOW)
    first = generate_bars("XAUUSD", "M15", times, start_price=2400.0, seed=7)
    second = generate_bars("XAUUSD", "M15", times, start_price=2400.0, seed=7)

    assert [bar.to_dict() for bar in first] == [bar.to_dict() for bar in second]
    for previous, current in zip(first, first[1:], strict=False):
        # A resampled feed cannot gap: the print that closes one bucket and the
        # print that opens the next are consecutive.
        assert current.open == previous.close
        assert current.open_time == previous.close_time


# ── The hub ───────────────────────────────────────────────────────────────


def test_the_cursor_never_rewinds():
    """`--anchor past` may place a bar behind the series; it must not move it back."""
    hub = SimulatorHub()
    hub.advance_cursor("XAUUSD", "M15", datetime(2026, 3, 4, 12, 0, tzinfo=UTC))
    hub.advance_cursor("XAUUSD", "M15", datetime(2026, 3, 4, 9, 0, tzinfo=UTC))
    assert hub.cursor("XAUUSD", "M15") == datetime(2026, 3, 4, 12, 0, tzinfo=UTC)


async def test_sealing_moves_the_cursor_onto_the_sealed_bucket():
    """Otherwise the next run lands where the seal tick already opened a bar."""
    hub = SimulatorHub()
    result = await dispatch(
        hub,
        {
            "op": "bars",
            "symbol": "XAUUSD",
            "timeframe": "M15",
            "anchor": "next",
            "bars": [{"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5}],
        },
    )
    last_bar = datetime.fromisoformat(result["expected"][-1]["open_time"])
    assert hub.cursor("XAUUSD", "M15") == last_bar + timedelta(minutes=15)


async def test_a_run_of_bars_is_contiguous_across_two_commands():
    hub = SimulatorHub()
    payload = {
        "op": "bars",
        "symbol": "XAUUSD",
        "timeframe": "M15",
        "anchor": "next",
        "seal": False,
        "bars": [{"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5}] * 2,
    }
    first = await dispatch(hub, payload)
    second = await dispatch(hub, payload)

    times = [row["open_time"] for row in first["expected"] + second["expected"]]
    assert times == sorted(set(times))
    assert datetime.fromisoformat(times[-1]) - datetime.fromisoformat(times[0]) == timedelta(
        minutes=45
    )


async def test_a_walk_continues_the_series_rather_than_racing_it():
    """A wall-clock tick behind a forward-anchored bar is discarded as late.

    So a walk that follows a replay picks market time up where the replay left
    it, and runs that clock at whatever multiple of real time was asked for —
    which is also how an M15 bar closes without waiting fifteen minutes.
    """
    hub = SimulatorHub()
    await dispatch(
        hub,
        {
            "op": "bars",
            "symbol": "XAUUSD",
            "timeframe": "M15",
            "anchor": "next",
            "bars": [{"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5}],
        },
    )
    last_print = hub.last_tick_ts["XAUUSD"]

    result = await dispatch(
        hub, {"op": "walk", "symbol": "XAUUSD", "rate": 100, "ticks": 3, "speed": 60}
    )
    await asyncio.wait_for(_drained(hub), timeout=5)

    assert datetime.fromisoformat(result["starts_at"]) > last_print
    # 60 seconds of market time per second of real time, at 100 ticks/s.
    assert hub.last_tick_ts["XAUUSD"] - last_print == timedelta(seconds=0.6 * 3)


async def test_a_walk_is_refused_rather_than_run_at_a_nonsense_rate():
    for bad in ({"speed": 0}, {"rate": 0}, {"price": 0}):
        with pytest.raises(CommandError):
            await dispatch(SimulatorHub(), {"op": "walk", "symbol": "XAUUSD", **bad})


async def _drained(hub: SimulatorHub) -> None:
    while hub.generators:
        await asyncio.sleep(0.01)


async def test_a_loose_tick_does_not_leave_the_next_bar_landing_on_top_of_it():
    """A tick is not a bar, but it does open a bucket in the resampler.

    A bar placed in that same bucket would inherit the tick's price as its
    open, and the candle would not match the bar that was asked for. So "next"
    is the first bucket nothing has touched.
    """
    hub = SimulatorHub()
    await dispatch(hub, {"op": "tick", "symbol": "XAUUSD", "bid": 2400.0, "ask": 2400.4})
    tick_bucket = floor_to_bucket(hub.last_tick_ts["XAUUSD"], "M15")

    result = await dispatch(
        hub,
        {
            "op": "bars",
            "symbol": "XAUUSD",
            "timeframe": "M15",
            "anchor": "next",
            "bars": [{"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5}],
        },
    )
    assert datetime.fromisoformat(result["expected"][0]["open_time"]) > tick_bucket


async def test_an_unstamped_tick_is_never_behind_the_series():
    """Otherwise a tick after a forward replay is dropped as late."""
    hub = SimulatorHub()
    ahead = datetime.now(UTC) + timedelta(days=2)
    await dispatch(hub, {"op": "tick", "symbol": "XAUUSD", "last": 1.0, "ts": ahead.isoformat()})

    result = await dispatch(hub, {"op": "tick", "symbol": "XAUUSD", "last": 2.0})
    assert datetime.fromisoformat(result["tick"]["ts"]) > ahead


async def test_an_unknown_command_is_refused_by_name():
    with pytest.raises(CommandError) as excinfo:
        await dispatch(SimulatorHub(), {"op": "moon"})
    assert "moon" in str(excinfo.value)


async def test_a_tick_needs_a_price_on_some_side():
    with pytest.raises(CommandError):
        await dispatch(SimulatorHub(), {"op": "tick", "symbol": "XAUUSD"})


# ── The protocol ──────────────────────────────────────────────────────────


def test_a_tick_survives_the_wire():
    tick = Tick(symbol="XAUUSD", ts=NOW, bid=2400.0, ask=2400.4, last=2400.2, volume=1.5)
    assert decode_tick(loads(dumps(encode_tick(tick, seq=3)))) == tick


@pytest.mark.parametrize(
    "frame",
    [
        {"type": "welcome"},
        {"type": "tick", "symbol": "XAUUSD"},
        {"type": "tick", "ts": NOW.isoformat()},
        {"type": "tick", "symbol": "XAUUSD", "ts": "not a time"},
        {"type": "tick", "symbol": "XAUUSD", "ts": NOW.isoformat()},
    ],
)
def test_a_frame_that_is_not_a_usable_tick_decodes_to_nothing(frame):
    assert decode_tick(frame) is None


def test_an_empty_subscription_means_every_symbol():
    from qte_simulator.hub import Subscriber

    subscriber = Subscriber(id=1, remote="test", send=None)
    assert subscriber.wants("XAUUSD")
    subscriber.symbols = {"BTCUSDT"}
    assert not subscriber.wants("XAUUSD")


# ── Reading bars off disk ─────────────────────────────────────────────────


def test_a_csv_of_bars_loads_oldest_first(tmp_path):
    path = tmp_path / "bars.csv"
    path.write_text(
        "open_time,open,high,low,close,volume\n"
        "2026-01-01T00:00:00Z,1,2,0.5,1.5,10\n"
        "2026-01-01T00:15:00Z,1.5,2.5,1,2,20\n",
        encoding="utf-8",
    )
    rows = load_bars(path)
    assert [row["close"] for row in rows] == [1.5, 2.0]
    assert load_bars(path, limit=1) == [rows[-1]]


def test_a_file_without_ohlc_says_which_columns_are_missing(tmp_path):
    path = tmp_path / "bad.csv"
    path.write_text("time,price\n1,2\n", encoding="utf-8")
    with pytest.raises(SourceError) as excinfo:
        load_bars(path)
    assert "open" in str(excinfo.value)


# ── Verification ──────────────────────────────────────────────────────────


def test_a_missing_candle_and_a_wrong_one_read_differently():
    expected = expected_candle(a_bar())
    assert compare(expected, None).verdict == "MISSING"

    wrong = expected.model_copy(update={"high": expected.high + 1})
    check = compare(expected, wrong)
    assert check.verdict == "MISMATCH"
    assert "high" in check.mismatches[0]


# ── The server, over a real socket ────────────────────────────────────────


async def test_a_feed_client_receives_the_bars_the_control_plane_plays():
    """The whole fixture, end to end: control in one socket, ticks out another."""
    async with _running_server() as server:
        stream = f"ws://127.0.0.1:{server.bound_port}/stream"
        control = f"ws://127.0.0.1:{server.bound_port}/control"

        async with websockets.connect(stream) as feed:
            assert loads(await feed.recv())["type"] == "welcome"
            await feed.send(dumps(subscribe_frame(["XAUUSD"])))
            assert loads(await feed.recv())["type"] == "subscribed"

            async with ControlClient(control) as client:
                result = await client.send(
                    "bars",
                    symbol="XAUUSD",
                    timeframe="M15",
                    anchor="next",
                    bars=[{"open": 2400, "high": 2410, "low": 2398, "close": 2408, "volume": 8}],
                )
            assert result["delivered"] == 1

            ticks = [
                decode_tick(loads(await asyncio.wait_for(feed.recv(), timeout=2)))
                for _ in range(result["ticks"])
            ]

    # The bar is rebuilt from what actually crossed the socket.
    resampler = Resampler("XAUUSD", ["M15"])
    closed = [candle for tick in ticks for candle in resampler.add_tick(tick)]
    assert len(closed) == 1
    assert compare(Candle.model_validate(result["expected"][0]), closed[0]).ok


async def test_a_feed_only_gets_the_symbols_it_asked_for():
    async with _running_server() as server:
        stream = f"ws://127.0.0.1:{server.bound_port}/stream"
        async with websockets.connect(stream) as feed:
            await feed.recv()
            await feed.send(dumps(subscribe_frame(["BTCUSDT"])))
            await feed.recv()

            async with ControlClient(f"ws://127.0.0.1:{server.bound_port}/control") as client:
                assert (await client.send("tick", symbol="XAUUSD", last=1.0))["delivered"] == 0
                assert (await client.send("tick", symbol="BTCUSDT", last=2.0))["delivered"] == 1

            tick = decode_tick(loads(await asyncio.wait_for(feed.recv(), timeout=2)))
            assert tick.symbol == "BTCUSDT"


async def test_the_provider_feed_reconnects_and_delivers_into_the_tick_handler():
    """What ingestion actually does, minus Redis and NATS."""
    received: list[Tick] = []

    async def on_tick(tick: Tick) -> None:
        received.append(tick)

    async with _running_server() as server:
        config = SimulatorSettings(url=f"ws://127.0.0.1:{server.bound_port}/stream")
        feed = create_provider("simulator", config=config).live_feeds(
            build_specs(["XAUUSD"]), on_tick
        )[0]
        feed.start()
        await _until(lambda: bool(server.hub.subscribers))

        async with ControlClient(f"ws://127.0.0.1:{server.bound_port}/control") as client:
            await client.send("tick", symbol="XAUUSD", bid=2400.0, ask=2400.4)

        await _until(lambda: bool(received))
        await feed.stop()

    assert received[0].price == pytest.approx(2400.2)
    assert feed.ticks_received == 1


async def test_a_command_the_server_refuses_does_not_close_the_session():
    async with _running_server() as server:
        async with ControlClient(f"ws://127.0.0.1:{server.bound_port}/control") as client:
            from qte_simulator.client import ControlError

            with pytest.raises(ControlError):
                await client.send("bars", symbol="XAUUSD", bars=[])
            assert (await client.send("status"))["ticks_sent"] == 0


# ── Fixtures ──────────────────────────────────────────────────────────────


class _running_server:
    """A server on an ephemeral port, torn down however the test exits."""

    async def __aenter__(self) -> SimulatorServer:
        self._server = SimulatorServer("127.0.0.1", 0)
        self._task = asyncio.create_task(self._server.serve_forever())
        await asyncio.wait_for(self._server.ready.wait(), timeout=5)
        return self._server

    async def __aexit__(self, *_exc) -> None:
        self._server.request_stop()
        await asyncio.wait_for(self._task, timeout=5)


async def _until(predicate, timeout: float = 5.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError("condition never became true")
        await asyncio.sleep(0.01)


async def _noop(_tick) -> None:  # pragma: no cover - handler is never invoked
    return None
