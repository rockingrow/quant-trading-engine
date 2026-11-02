"""The end-to-end drill: one open cycle from a real strategy to the broker.

    ideal candles → strategy.on_candle_closed → SignalFactory
        → NATS SIGNALS_DEMO.<strategy> → broker → Telegram

What this answers is a deployment question, not a trading one: *given data a
strategy would act on, does a signal still make it all the way out of this
engine and into the broker's notification path?* Every hop a live signal takes
after the strategy is exercised here with the pair's own configuration — the
mapping table's params and risk, the strategy's own warm-up and timeframe, the
bracket policy, the risk sizing, the broker envelope — so a drill that renders
a sensible Telegram card proves the plumbing for that strategy.

Three things it deliberately does **not** do, because they are what separate a
drill from a trade:

* **It publishes to its own subject.** ``SIGNALS_DEMO.<strategy>`` is not in
  the broker's ``SIGNALS`` JetStream stream, so a demo envelope cannot be
  consumed by the fan-out that commands workers. The broker subscribes to it
  separately and notifies Telegram without publishing anything onward — see
  that repository's ``DemoSignalConsumer``.
* **It touches no engine state.** The ``SignalFactory`` it builds through is a
  throwaway, built fresh per run, and nothing is written to Redis, Postgres or
  the delivery outbox. A running runner's own cycles are unaffected, and the
  drill cannot be confused with a signal it has to reconcile.
* **It ignores shadow mode.** Shadow mode exists to keep orders away from a
  broker that would act on them; the demo subject already guarantees that, and
  a drill that stayed silent in the mode most deployments sit in would never
  be run.

**Ideal data is searched for, not assumed.** A strategy may be trend-following
or mean-reverting, long-only or short-only, and this module cannot read its
mind. So it replays four synthetic regimes — a clean trend each way, and an
exhaustion reversal each way — and takes the first one the strategy answers
with an entry. A strategy that answers none of them still gets a signal: the
drill falls back to a synthetic entry priced off the last bar it generated, and
says so, because the point of the exercise is the transport rather than the
edge.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta

from qte_shared.bus import NatsBus, Subjects
from qte_shared.config import settings
from qte_shared.logging_setup import get_logger
from qte_shared.models import BrokerSignal, Candle, SignalAction, utcnow
from qte_shared.strategies.mapping import SymbolMapping
from qte_shared.strategies.plugin_loader import read_strategy_directory
from qte_shared.strategies.signal_factory import BracketPolicy, SignalFactory
from qte_shared.strategies.signal_serialization import authenticated_signal
from qte_shared.strategies.sizing import PositionSizer
from qte_shared.strategies.strategy_base import (
    SignalIntent,
    StrategyContext,
    StrategyLike,
    as_intents,
    candles_to_frame,
)
from qte_shared.strategies.strategy_settings import resolve_warmup
from qte_shared.timeframes import floor_to_bucket, normalize_timeframe, timeframe_seconds

log = get_logger(__name__)

#: Bars generated past the pair's warm-up, so the strategy gets to decide on
#: several closes of the regime rather than on the first bar it is warm for.
DECISION_BARS = 40

#: Fallback start price for a symbol the drill knows nothing about. The level
#: is immaterial — the bracket and the risk sizing are both proportional — but
#: a figure in the right order of magnitude keeps the Telegram card readable.
DEFAULT_START_PRICE = 1000.0

#: Opening level per symbol, so a gold card does not quote gold at 1000. Only
#: a display nicety; anything unlisted gets :data:`DEFAULT_START_PRICE`.
START_PRICES: dict[str, float] = {
    "XAUUSD": 2400.0,
    "XAGUSD": 30.0,
    "USOIL": 78.0,
    "UKOIL": 82.0,
    "BTCUSDT": 65000.0,
    "ETHUSDT": 3400.0,
    "EURUSD": 1.085,
    "GBPUSD": 1.27,
    "USDJPY": 157.0,
}

#: How far a trending regime travels across the **whole** window, as a fraction
#: of the opening price. Spread over the bars rather than fixed per bar on
#: purpose: a per-bar figure that reads sensibly on a 40-bar window compounds
#: into a move no market makes once a strategy asks for a 200-bar warm-up, and
#: the drill would then quote a price nowhere near the symbol's own.
TREND_EXCURSION = 0.06

#: Amplitude of the deterministic wiggle laid over every regime, as a fraction
#: of price. It is what gives each bar a realistic range once the drift above
#: is spread thin — and without any wiggle the series is monotone, leaving an
#: indicator that divides by the window's range (a Stochastic, a Williams %R)
#: nothing to divide by.
WIGGLE = 0.0012

#: The regimes tried, in order, until one produces an entry. Trends first
#: because a momentum strategy is the common case; the reversals cover the
#: mean-reverting half, where the entry comes from the snap back rather than
#: from the move into it.
REGIMES: tuple[str, ...] = ("uptrend", "downtrend", "dip-reversal", "spike-reversal")


@dataclass(frozen=True, slots=True)
class DemoPair:
    """One (strategy, symbol) the drill will fire, configured as the runner would."""

    strategy_name: str
    symbol: str
    strategy: StrategyLike
    factory: SignalFactory
    warmup: int

    @property
    def timeframe(self) -> str:
        return normalize_timeframe(self.strategy.timeframe)


#: ``DemoSignal.source`` when no regime produced an entry and the payload was
#: invented instead. Not a failure: most strategies gate on more than a price
#: shape, and the transport is what the drill is testing.
SYNTHETIC = "synthetic"


@dataclass(frozen=True, slots=True)
class DemoSignal:
    """A built demo payload and the provenance an operator needs to read it."""

    pair: DemoPair
    signal: BrokerSignal
    #: Regime whose bars produced the intent, or :data:`SYNTHETIC` when none did.
    source: str
    #: How the strategy raised while being driven, if it did. This is the drill's
    #: own finding and is reported separately from the delivery: a strategy that
    #: *declines* synthetic data is ordinary, and one that *crashes* on a frame
    #: of plausible bars would crash the runner on a real one.
    strategy_error: str = ""

    @property
    def from_strategy(self) -> bool:
        """Whether the strategy itself decided this, rather than the fallback."""
        return self.source != SYNTHETIC


@dataclass(frozen=True, slots=True)
class DemoDelivery:
    """Where one demo payload went, or why it did not get there."""

    demo: DemoSignal
    subject: str
    #: Empty when the envelope left. Otherwise what stopped it — kept per
    #: delivery rather than raised, so a run that got some of its cards out
    #: still reports which ones.
    error: str = ""

    @property
    def delivered(self) -> bool:
        return not self.error

    def with_error(self, error: str) -> DemoDelivery:
        return DemoDelivery(demo=self.demo, subject=self.subject, error=error)


# ── Pair selection ────────────────────────────────────────────────────


def build_demo_pairs(strategy: str | None = None, symbol: str | None = None) -> list[DemoPair]:
    """One pair per mounted strategy, configured from the same files the runner reads.

    Narrowed by *strategy* (exact name, case-insensitive) and *symbol* when
    given. Without a symbol a strategy mapped to several contributes its first
    mapped one only: the drill is a per-strategy check, and firing every pair
    would put one Telegram card per symbol into the channel for no extra
    coverage of the path.

    The pair is assembled here rather than borrowed from
    :class:`~qte_strategy_engine.runner.StrategyRunner`, which builds its slots
    as part of starting up: a drill that had to start a runner would claim this
    namespace's ownership lock and fight the one already running it.
    """
    mapping = SymbolMapping.load(settings.engine.mapping_file)
    directory = read_strategy_directory(settings.engine.strategies_dir)
    wanted = strategy.strip().upper() if strategy else None
    wanted_symbol = symbol.strip().upper() if symbol else None

    pairs: list[DemoPair] = []
    for entry in directory.loaded:
        if wanted is not None and entry.name.upper() != wanted:
            continue
        symbols = _symbols_for(mapping, entry.name, entry.cls)
        if wanted_symbol is not None:
            symbols = [name for name in symbols if name == wanted_symbol]
        elif symbols:
            symbols = symbols[:1]
        if not symbols:
            log.warning(
                "Strategy %s is mounted but has no symbol to demo%s",
                entry.name,
                f" matching {wanted_symbol}" if wanted_symbol else "",
            )
            continue

        for pair_symbol in symbols:
            params = {
                **mapping.defaults_for(entry.name),
                **mapping.params_for(pair_symbol, entry.name),
            }
            instance = entry.instantiate(params)
            warmup = resolve_warmup(
                instance.warmup,
                instance.history_window(),
                params,
                subject=f"{entry.name} on {pair_symbol}",
            )
            pairs.append(
                DemoPair(
                    strategy_name=instance.name,
                    symbol=pair_symbol,
                    strategy=instance,
                    factory=SignalFactory(
                        instance.name,
                        timeframe=instance.timeframe,
                        bracket=BracketPolicy(),
                        inputs=instance.params,
                        sizer=PositionSizer.from_settings(params),
                    ),
                    warmup=warmup,
                )
            )

    if wanted is not None and not pairs:
        # Two different mistakes, and saying the wrong one sends the operator
        # looking for a strategy that is right there in the list below.
        names = [entry.name for entry in directory.loaded]
        if wanted in {name.upper() for name in names}:
            raise ValueError(
                f"{strategy!r} is mounted but not mapped to {symbol!r} — "
                f"drop --symbol, or name one it trades"
            )
        raise ValueError(
            f"No mounted strategy named {strategy!r}. Mounted: {', '.join(names) or 'none'}"
        )
    return pairs


def _symbols_for(mapping: SymbolMapping, name: str, cls: StrategyLike) -> list[str]:
    """The symbols one strategy trades: the mapping table's, or its own declaration."""
    if mapping:
        return [entry.upper() for entry in mapping.symbols_for(name)]
    declared = cls.symbols or settings.engine.symbols
    return [entry.upper() for entry in declared]


# ── Ideal data ────────────────────────────────────────────────────────


def ideal_candles(
    symbol: str,
    timeframe: str,
    *,
    regime: str,
    bar_count: int,
    start_price: float | None = None,
    end_time: datetime | None = None,
) -> list[Candle]:
    """A synthetic window of *regime*, oldest bar first and ending on a closed bucket.

    Deterministic: the same arguments always produce the same bars, so a drill
    that fired a signal can be re-run and produce the same one. The shape is
    described by :data:`REGIMES`.
    """
    if bar_count < 1:
        raise ValueError(f"bar_count must be at least 1, got {bar_count}")
    label = normalize_timeframe(timeframe)
    step = timedelta(seconds=timeframe_seconds(label))
    # The most recent *closed* bucket, so a card's timestamp reads like a bar
    # the engine could actually have been handed.
    last_open = floor_to_bucket(end_time or utcnow(), label) - step
    opening = start_price if start_price is not None else START_PRICES.get(symbol.upper())
    base = opening if opening is not None else DEFAULT_START_PRICE

    closes = _regime_closes(regime, bar_count, base)
    candles: list[Candle] = []
    previous_close = closes[0]
    for index, close in enumerate(closes):
        open_price = previous_close
        high = max(open_price, close) * (1.0 + WIGGLE)
        low = min(open_price, close) * (1.0 - WIGGLE)
        candles.append(
            Candle(
                symbol=symbol.upper(),
                timeframe=label,
                open_time=last_open - step * (bar_count - 1 - index),
                open=open_price,
                high=high,
                low=low,
                close=close,
                volume=1000.0,
                tick_count=100,
            )
        )
        previous_close = close
    return candles


def _regime_closes(regime: str, bar_count: int, base: float) -> list[float]:
    """Close prices of one regime, as a multiple of *base*.

    The reversals spend four fifths of the window moving one way and the last
    fifth snapping back at four times that drift, which is what an exhaustion
    entry looks like to an oscillator: the extreme is reached well inside the
    window and the signal fires on the return.
    """
    if regime not in REGIMES:
        raise ValueError(f"Unknown regime {regime!r}; expected one of {', '.join(REGIMES)}")

    turn = max(1, (bar_count * 4) // 5)
    # The excursion is spread over the window, so the shape is the same on a
    # 40-bar window and a 400-bar one and neither ends at an implausible price.
    drift = TREND_EXCURSION / max(bar_count - 1, 1)
    closes: list[float] = []
    for index in range(bar_count):
        if regime == "uptrend":
            factor = 1.0 + drift * index
        elif regime == "downtrend":
            factor = 1.0 - drift * index
        elif regime == "dip-reversal":
            fall = -drift * min(index, turn)
            rise = 4.0 * drift * max(0, index - turn)
            factor = 1.0 + fall + rise
        else:  # spike-reversal
            rise = drift * min(index, turn)
            fall = -4.0 * drift * max(0, index - turn)
            factor = 1.0 + rise + fall
        # Deterministic, period-7 wiggle: what gives each bar its own range once
        # the drift above is spread thin, without reversing the regime it sits on.
        factor += WIGGLE * math.sin(index * 2.0 * math.pi / 7.0)
        closes.append(round(base * factor, 6))
    return closes


# ── Building one demo signal ──────────────────────────────────────────


def build_demo_signal(pair: DemoPair, *, moment: datetime | None = None) -> DemoSignal:
    """Drive *pair* over each regime until it enters, then build the broker payload.

    Raises ``ValueError`` only when even the synthetic fallback cannot be turned
    into a payload a worker could fill, which means the pair's own
    configuration is what is broken — exactly what the drill is for.
    """
    bar_count = max(pair.warmup, 1) + DECISION_BARS
    fallback_candle: Candle | None = None
    first_error = ""
    for regime in REGIMES:
        candles = ideal_candles(
            pair.symbol,
            pair.timeframe,
            regime=regime,
            bar_count=bar_count,
            end_time=moment,
        )
        if fallback_candle is None:
            # The first regime's last bar, so the fallback below is priced off a
            # market going the way the entry it invents is pointing.
            fallback_candle = candles[-1]
        intent, error = _first_entry(pair, candles)
        first_error = first_error or error
        if intent is not None:
            log.info(
                "Demo drill: %s entered %s on %s data",
                pair.strategy_name,
                intent.action.value,
                regime,
            )
            return DemoSignal(
                pair=pair,
                signal=_payload(pair, intent, candles[-1]),
                source=regime,
                strategy_error=first_error,
            )

    assert fallback_candle is not None  # REGIMES is never empty
    log.warning(
        "Demo drill: %s on %s produced no entry on any regime — falling back to a "
        "synthetic LONG so the transport is still exercised",
        pair.strategy_name,
        pair.symbol,
    )
    return DemoSignal(
        pair=pair,
        signal=_payload(pair, SignalIntent(action=SignalAction.LONG), fallback_candle),
        source=SYNTHETIC,
        strategy_error=first_error,
    )


def _first_entry(pair: DemoPair, candles: list[Candle]) -> tuple[SignalIntent | None, str]:
    """The strategy's first entry on *candles*, and how it raised if it did.

    Fed one bar at a time from the warm-up boundary onward, exactly as the
    runner feeds a closed bar, so a strategy carrying state between bars sees
    the same sequence it would live.

    A plugin that raises abandons this regime but not the drill: the payload
    still goes out, and the exception comes back as the second element so the
    run can report it. That distinction is the point — a strategy *declining*
    synthetic data proves nothing is wrong, while one that *crashes* on a frame
    of plausible bars would crash the runner on a real one, and the drill is
    where that should be found.
    """
    # A fresh instance per regime: a strategy that kept state from the previous
    # regime's bars would be deciding on two histories spliced together.
    strategy = type(pair.strategy)(dict(pair.strategy.params))
    started = False
    for index in range(pair.warmup, len(candles) + 1):
        window = candles[:index]
        if not window:
            continue
        moment = window[-1].open_time
        context = StrategyContext(
            symbol=pair.symbol,
            timeframe=pair.timeframe,
            now=moment,
            mode="live",
            params=strategy.params,
        )
        try:
            if not started:
                strategy.on_start(context)
                started = True
            result = strategy.on_candle_closed(candles_to_frame(window), context)
        except Exception as exc:
            log.exception(
                "Demo drill: %s raised on synthetic %s data", pair.strategy_name, pair.symbol
            )
            return None, f"{type(exc).__name__}: {exc}"
        for intent in as_intents(result):
            if intent.action.is_entry:
                if intent.price is None:
                    intent.price = window[-1].close
                return intent, ""
    return None, ""


def _payload(pair: DemoPair, intent: SignalIntent, candle: Candle) -> BrokerSignal:
    """Turn an intent into the broker envelope, through the live factory.

    ``commit=False`` for the same reason the runner uses it: the cycle must not
    be remembered by anything, and here nothing is going to deliver a close for
    it either.
    """
    if intent.price is None:
        intent.price = candle.close
    return pair.factory.build(
        intent,
        symbol=pair.symbol,
        moment=candle.open_time,
        commit=False,
    )


# ── Publishing ────────────────────────────────────────────────────────


async def publish_demo_signals(
    demos: list[DemoSignal], *, bus: NatsBus | None = None
) -> list[DemoDelivery]:
    """Publish each payload on its strategy's demo subject, in order.

    Core NATS, not JetStream: the demo subject is backed by no stream, which is
    the property that keeps a drill out of the buffer the broker's worker
    fan-out consumes. A drill that is not delivered is a drill to re-run, so
    nothing here is worth persisting.

    One failed publish does not discard the ones before it. The deliveries list
    is what the caller prints, and an operator whose NATS dropped after the
    third of six cards needs to know which three reached Telegram — otherwise
    the re-run fires those three a second time and the channel shows each twice.
    The error is attached to the delivery that failed and raised to the caller
    only if *nothing* got out.
    """
    if not demos:
        return []
    connection = bus or NatsBus(
        url=settings.broker_nats_url,
        token=settings.broker_nats_token,
        name="qte-demo-signal",
    )
    owns_bus = bus is None
    if owns_bus:
        await connection.connect()
    deliveries: list[DemoDelivery] = []
    try:
        for demo in demos:
            subject = Subjects.broker_signal_demo(demo.signal.strategy)
            try:
                await connection.publish(
                    subject,
                    {"payload": authenticated_signal(demo.signal, settings.broker.token)},
                )
            except Exception as exc:
                log.exception("Demo signal publish failed subject=%s", subject)
                deliveries.append(
                    DemoDelivery(demo=demo, subject=subject, error=f"{type(exc).__name__}: {exc}")
                )
                continue
            log.info(
                "Demo signal published subject=%s symbol=%s action=%s uxid=%s source=%s",
                subject,
                demo.signal.symbol,
                demo.signal.position.action.value,
                demo.signal.signal_uxid,
                demo.source,
            )
            deliveries.append(DemoDelivery(demo=demo, subject=subject))
        # Core NATS publishes are fire-and-forget; flush so the process cannot
        # exit with the last envelope still in the client's write buffer. A
        # flush that fails leaves every delivery above in doubt rather than any
        # one of them, so it is recorded against the whole run.
        try:
            await connection.nc.flush(timeout=settings.broker.publish_timeout)
        except Exception as exc:
            log.exception("Demo signal flush failed; the last envelopes may not have left")
            deliveries = [
                delivery.with_error(f"unflushed — {type(exc).__name__}: {exc}")
                if delivery.delivered
                else delivery
                for delivery in deliveries
            ]
        return deliveries
    finally:
        if owns_bus:
            await connection.close()


async def run_demo(strategy: str | None = None, symbol: str | None = None) -> int:
    """Build and publish the drill; returns a process exit code.

    Every pair is attempted even when one of them fails to build: an operator
    running this across the whole mount wants the pairs that do work to have
    been fired, and the names of the ones that did not.

    Non-zero for the three things that are genuinely wrong — a pair whose
    payload could not be built, an envelope that did not reach the broker, and a
    strategy that *raised* while being driven. A strategy that merely declined
    every regime exits zero: synthetic bars do not satisfy a session window or
    an ATR floor, and the drill is testing the path rather than the edge.
    """
    pairs = build_demo_pairs(strategy, symbol)
    if not pairs:
        print("No mounted strategy is mapped to a symbol — nothing to demo.")
        return 1

    demos: list[DemoSignal] = []
    failed: list[str] = []
    for pair in pairs:
        try:
            demos.append(build_demo_signal(pair))
        except Exception as exc:
            failed.append(f"{pair.strategy_name} on {pair.symbol}: {exc}")
            log.exception(
                "Demo drill could not build a payload for %s on %s",
                pair.strategy_name,
                pair.symbol,
            )

    deliveries = await publish_demo_signals(demos)
    undelivered = [delivery for delivery in deliveries if not delivery.delivered]
    for delivery in deliveries:
        position = delivery.demo.signal.position
        origin = (
            f"strategy on {delivery.demo.source} data"
            if delivery.demo.from_strategy
            else "synthetic fallback"
        )
        arrow = "<-" if delivery.delivered else "XX"
        print(
            f"{delivery.subject} {arrow} {position.action.value} {delivery.demo.signal.symbol} "
            f"price={position.price} qty={position.quantity} sl={position.sl} "
            f"uxid={delivery.demo.signal.signal_uxid} ({origin})"
            + (f" NOT DELIVERED: {delivery.error}" if delivery.error else "")
        )
    for failure in failed:
        print(f"FAILED {failure}")
    raised = [demo for demo in demos if demo.strategy_error]
    for demo in raised:
        print(
            f"RAISED {demo.signal.strategy} on {demo.signal.symbol}: "
            f"{demo.strategy_error} — the payload still went out, but this "
            "strategy would raise on a live bar too"
        )

    print(
        f"Published {len(deliveries) - len(undelivered)} of {len(pairs)} demo signal(s) to "
        f"{settings.broker.demo_subject_prefix}.* on {settings.broker_nats_url}. "
        "The broker notifies Telegram and forwards nothing to a worker."
    )
    return 1 if failed or raised or undelivered else 0


__all__ = [
    "DECISION_BARS",
    "REGIMES",
    "SYNTHETIC",
    "TREND_EXCURSION",
    "WIGGLE",
    "DemoDelivery",
    "DemoPair",
    "DemoSignal",
    "build_demo_pairs",
    "build_demo_signal",
    "ideal_candles",
    "publish_demo_signals",
    "run_demo",
]
