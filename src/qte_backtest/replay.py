"""The replay loop: parquet history in, filled trades and a report out.

Ordering inside one bar is what keeps a backtest honest, so it is fixed:

1. The bar's range is applied to whatever position is already open (stop and
   target checks).
2. The strategy sees history **up to and including** that bar and decides.
3. Its intents are filled at that bar's close, plus costs.

Step 2 never sees a future bar, and step 1 runs first so a position cannot be
closed by a decision made after the bar that would have stopped it out. Entries
fill at the signal bar's close rather than the next bar's open, which is the
common convention — it is slightly optimistic on a gap, and the gap handling in
:class:`~qte_backtest.execution.FillSimulator` is where that is paid back.

Step 3 has one gate that is not the strategy's. Inside the weekend-flat window
a strategy's repository declared — see
:mod:`qte_shared.strategies.strategy_settings` — entry intents are dropped and
an open cycle is closed with a ``FLAT``. The live runner applies exactly this,
in exactly this position in the bar, which is what keeps a replay a prediction
of what the runner does over a Friday evening rather than a strictly
more-traded version of it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import pandas as pd

from qte_backtest.execution import CostModel, ExitReason, FillSimulator, SimulatedPosition
from qte_backtest.metrics import BacktestMetrics, compute_metrics, format_report
from qte_shared.config import settings
from qte_shared.logging_setup import get_logger
from qte_shared.models import BrokerSignal, SignalAction
from qte_shared.strategies.signal_factory import BracketPolicy, SignalFactory
from qte_shared.strategies.sizing import PositionSizer
from qte_shared.strategies.strategy_base import (
    SignalIntent,
    StrategyContext,
    StrategyLike,
    as_intents,
)
from qte_shared.strategies.strategy_settings import NO_WEEKEND_FLAT, WeekendFlatPolicy
from qte_shared.timeframes import TIMEFRAME_SECONDS, timeframe_seconds

log = get_logger(__name__)


#: Ceiling on the OHLC rows the report carries for drawing.
#:
#: Sized so a normal run ships its bars **at the timeframe it was run on** — a
#: chart of an M15 replay should look like an M15 chart, and the dashboard can
#: roll those bars up to H1 or D1 on its own, which it cannot do from bars that
#: arrived pre-aggregated. A year of M15 is ~35k bars and a year of M1 is ~370k,
#: so the ceiling still has to exist; past it :func:`sample_market` climbs the
#: timeframe ladder instead of the report becoming a copy of the parquet.
MARKET_MAX_ROWS = 20_000


@dataclass(slots=True)
class MarketWindow:
    """An OHLC view of the replayed history — for drawing only.

    Every number in the report is computed from the full series; these rows
    exist so a chart can show *where* the trades happened. Normally they **are**
    the full series, at the timeframe the run was made on: a reader looking at
    an M15 backtest should see M15 candles, and a dashboard can aggregate those
    up to H1 or D1 itself. Only when the file is longer than
    :data:`MARKET_MAX_ROWS` does :func:`sample_market` hand over a higher
    timeframe instead, and :attr:`base_timeframe` says which one it chose.

    Aggregation, when it happens, is by the **calendar**, not by counting bars:
    buckets land on the same boundaries the rest of the engine uses, so a
    candle covers one real hour rather than "the next 32 bars, whatever hours
    those turned out to span across a weekend".

    ``benchmark_close`` is the close of the first bar the strategy could act on
    — the bar completing warm-up — which is what a buy-and-hold comparison has to be
    anchored to. Anchoring it at the first bar of the file would credit or
    charge the benchmark for a stretch the strategy was never allowed to trade.
    """

    #: Nominal bars of the run's timeframe per row: 1 when the rows are the
    #: bars themselves. A calendar bucket spanning a session break holds fewer.
    bucket_bars: int
    #: The timeframe :attr:`rows` are drawn at — the run's own unless the file
    #: was too long to carry at that resolution.
    base_timeframe: str
    rows: list[list[Any]]
    benchmark_close: float
    last_close: float
    benchmark_from: datetime | None = None

    #: Column order of :attr:`rows`, carried into the JSON so a consumer reads
    #: the arrays rather than guessing at them. ``t`` is **epoch seconds**, UTC:
    #: an integer per row rather than a 25-character timestamp, which is a third
    #: of this block's size once the rows are the whole series.
    columns: tuple[str, ...] = ("t", "o", "h", "l", "c")


@dataclass(slots=True)
class BacktestResult:
    strategy: str
    symbol: str
    timeframe: str
    metrics: BacktestMetrics
    positions: list[SimulatedPosition] = field(default_factory=list)
    signals: list[BrokerSignal] = field(default_factory=list)
    rejected: int = 0
    params: dict[str, Any] = field(default_factory=dict)

    # Context the report and the diagnostics need. Captured here because the
    # engine is the only place that still has the frame and the cost model in
    # hand — reconstructing them later invites the two drifting apart.
    costs: CostModel = field(default_factory=CostModel)
    warmup: int = 0
    bars: int = 0
    data_start: datetime | None = None
    data_end: datetime | None = None
    data_gaps: int = 0
    strategy_meta: dict[str, Any] = field(default_factory=dict)
    starting_equity: float = 0.0
    #: Percent of the starting equity risked per entry. Together with the stop
    #: distance it is what produced every ``quantity`` below, so a reader can
    #: re-derive the sizing rather than take the trade list on faith.
    risk_percent: float | None = None
    #: The fallback size an entry that could not be risk-sized — no stop, or a
    #: stop on the entry — was filled at. Also what sizes the buy-and-hold
    #: benchmark, rather than comparing one unit of the market against
    #: whatever the strategy did.
    quantity: float = 1.0
    market: MarketWindow | None = None

    def report(self) -> str:
        header = f"{self.strategy} — {self.symbol} {self.timeframe}"
        body = format_report(self.metrics, header)
        if self.rejected:
            body += f"\nRejected entries  {self.rejected} (position already open)"
        return body

    def trades_as_rows(self) -> list[dict[str, Any]]:
        """Shape the audit table's ``backtest_trades`` insert expects."""
        return [
            {
                "signal_uxid": position.signal_uxid,
                "symbol": position.symbol,
                "direction": "LONG" if position.direction == 1 else "SHORT",
                "opened_at": position.opened_at,
                "closed_at": position.closed_at,
                "entry_price": position.entry_price,
                "exit_price": position.exit_price,
                "quantity": position.quantity,
                "sl": position.sl,
                "tp1": position.tp1,
                "tp2": position.tp2,
                "exit_reason": position.exit_reason,
                "gross_pnl": position.gross_pnl,
                "fees": position.fees,
                "net_pnl": position.net_pnl,
            }
            for position in self.positions
            if position.legs
        ]


class BacktestEngine:
    """Drives one strategy over one symbol's history."""

    def __init__(
        self,
        strategy: StrategyLike,
        *,
        symbol: str,
        timeframe: str | None = None,
        costs: CostModel | None = None,
        starting_equity: float = 0.0,
        bracket: BracketPolicy | None = None,
        default_quantity: float = 1.0,
        sizer: PositionSizer | None = None,
        weekend_flat: WeekendFlatPolicy | None = None,
    ) -> None:
        self.strategy = strategy
        self.symbol = symbol.upper()
        self.timeframe = timeframe or strategy.timeframe
        self.simulator = FillSimulator(costs or CostModel())
        self.starting_equity = starting_equity
        self.default_quantity = default_quantity
        # The calendar the *runner* would enforce around this strategy, read
        # from the same setting in the same zone — see the module docstring.
        # Defaulting to off rather than to the mounted repo's declaration keeps
        # a hand-built engine in a test from needing a strategies directory.
        self.weekend_flat = weekend_flat or NO_WEEKEND_FLAT
        self.market_zone = settings.engine.market_zone
        # The same sizer the live runner builds, so a backtested trade is the
        # size the runner would have sent. Without a caller-supplied one it
        # reads QTE_ACCOUNT__* and the strategy's own params, which is where
        # the mapping table's risk_percent has already landed.
        self.factory = SignalFactory(
            strategy.name,
            timeframe=self.timeframe,
            bracket=bracket,
            inputs=strategy.params,
            sizer=sizer or PositionSizer.from_settings(strategy.params),
            default_quantity=default_quantity,
        )
        self.positions: list[SimulatedPosition] = []
        self.signals: list[BrokerSignal] = []
        self._open: SimulatedPosition | None = None
        self._rejected = 0
        #: Entries the weekend window refused. Counted separately from
        #: ``_rejected`` because these are not malformed — the calendar said no.
        self._blocked = 0

    def run(self, frame: pd.DataFrame) -> BacktestResult:
        if frame.empty:
            raise ValueError(f"No history to replay for {self.symbol} {self.timeframe}")

        warmup = max(self.strategy.warmup, 1)
        if len(frame) < warmup:
            raise ValueError(
                f"{len(frame)} bars is not enough for a strategy needing {warmup} of warm-up"
            )

        # The same bound the live runner keeps its deque at. Passing the whole
        # file instead would be both quadratic and a lie: a strategy would see
        # history in the backtest that it can never see in production.
        window_size = self.strategy.history_window()

        context = StrategyContext(
            symbol=self.symbol,
            timeframe=self.timeframe,
            now=_as_datetime(frame.index[warmup - 1]),
            mode="backtest",
            params=self.strategy.params,
        )
        self.strategy.on_start(context)

        for position in range(warmup - 1, len(frame)):
            bar_time = _as_datetime(frame.index[position])
            bar = frame.iloc[position]

            if self._open is not None and self._open.is_open:
                self.simulator.process_bar(self._open, bar, bar_time)
                if not self._open.is_open:
                    self._open = None
                self._sync_cycle()

            context.now = bar_time
            context.open_uxid = self.factory.open_cycle(self.symbol)
            start = 0 if window_size is None else max(0, position + 1 - window_size)
            window = frame.iloc[start : position + 1]
            close = float(bar["close"])
            market_is_shut = self.weekend_flat.covers(bar_time, self.market_zone)
            for intent in as_intents(self.strategy.on_candle_closed(window, context)):
                if market_is_shut and intent.action.is_entry:
                    self._blocked += 1
                    continue
                self._apply(intent, bar_time, close)
            if market_is_shut:
                # After the strategy's own exits, as in the runner: a bar that
                # hit the stop stopped out, and the flat is what is left.
                self._flatten_for_the_weekend(bar_time, close)

        # A position still open at the last bar is marked out at the final close
        # rather than dropped, so its unrealised P&L cannot silently flatter the
        # report by never being counted.
        if self._open is not None and self._open.is_open:
            self.simulator.close_at(
                self._open,
                _as_datetime(frame.index[-1]),
                float(frame.iloc[-1]["close"]),
                ExitReason.END_OF_DATA,
            )

        self.strategy.on_stop()
        if self.weekend_flat.enabled:
            # Worth one line: a run whose entry count dropped after this setting
            # arrived should be able to show that the calendar is the reason.
            log.info(
                "Weekend flat %s (%s) blocked %d entries",
                self.weekend_flat.describe(),
                self.market_zone.key,
                self._blocked,
            )
        return BacktestResult(
            strategy=self.strategy.name,
            symbol=self.symbol,
            timeframe=self.timeframe,
            metrics=compute_metrics(
                self.positions,
                self.starting_equity,
                total_bars=len(frame),
                costs=self.simulator.costs,
            ),
            positions=self.positions,
            signals=self.signals,
            rejected=self._rejected,
            params=dict(self.strategy.params),
            costs=self.simulator.costs,
            warmup=warmup,
            bars=len(frame),
            data_start=_as_datetime(frame.index[0]),
            data_end=_as_datetime(frame.index[-1]),
            data_gaps=count_gaps(frame, self.timeframe),
            strategy_meta=self.strategy.describe(),
            starting_equity=self.starting_equity,
            risk_percent=self.factory.sizer.risk_percent,
            quantity=self.default_quantity,
            market=sample_market(frame, warmup, self.timeframe),
        )

    # ── Intent handling ───────────────────────────────────────────────

    def _flatten_for_the_weekend(self, bar_time: datetime, close: float) -> None:
        """Close whatever is open, because this strategy's market is shutting.

        Put through :meth:`_apply` like any other intent, so the trade is
        filled, costed and published into ``signals`` exactly as the runner
        would stage and deliver it. A pair already flat produces nothing, which
        is what makes this safe to call on every bar inside the window.
        """
        if self.factory.open_cycle(self.symbol) is None:
            return
        self._apply(
            SignalIntent(
                action=SignalAction.FLAT,
                symbol=self.symbol,
                price=close,
                reason="WEEKEND_FLAT",
            ),
            bar_time,
            close,
        )

    def _apply(self, intent: SignalIntent, bar_time: datetime, close: float) -> None:
        if intent.price is None:
            intent.price = close

        if intent.action.is_entry and self._open is not None and self._open.is_open:
            # The broker's workers refuse a second position on the same
            # symbol+strategy (they answer REJECTED), so a backtest that stacked
            # them would be scoring trades live trading will never take.
            self._rejected += 1
            log.debug("Rejected %s at %s — position already open", intent.action.value, bar_time)
            return

        try:
            signal = self.factory.build(intent, symbol=self.symbol, moment=bar_time)
        except ValueError as exc:
            log.warning("Dropped intent at %s: %s", bar_time, exc)
            return
        self.signals.append(signal)

        # Fill what was *published*, not what the strategy proposed. The factory
        # risk-sizes the entry and rescales the closes, so reading the intent
        # here would simulate a trade of a different size from the payload sat
        # beside it in the report.
        block = signal.position
        if intent.action.is_entry:
            self._open = self.simulator.open_position(
                symbol=self.symbol,
                action=intent.action,
                bar_time=bar_time,
                price=block.price if block.price is not None else close,
                quantity=block.quantity or self.default_quantity,
                sl=block.sl,
                tp1=block.tp1,
                tp2=block.tp2,
                tp1_percent=block.tp1_percent,
                move_sl_to_be=bool(block.move_sl_to_be),
                signal_uxid=signal.signal_uxid,
            )
            self.positions.append(self._open)
        elif self._open is not None and self._open.is_open:
            self.simulator.close_at(
                self._open,
                bar_time,
                block.price if block.price is not None else close,
                _EXIT_REASONS.get(intent.action, ExitReason.FLAT),
                quantity=block.quantity,
                note=intent.reason or None,
            )
            if intent.action is SignalAction.TP1:
                self._open.tp1_filled = True
                if self._open.move_sl_to_be and self._open.is_open:
                    self._open.sl = self._open.entry_price
            if not self._open.is_open:
                self._open = None
        self._sync_cycle()

    def _sync_cycle(self) -> None:
        """Tell the factory what the fill simulator actually did.

        The two keep their own books and only one of them sees a bracket fill:
        a stop or target hit inside :meth:`FillSimulator.process_bar` closes
        size without any signal being emitted. Left alone, the factory would go
        on believing the entry quantity is still open and size the next close
        against it — and, worse, never notice that a ``TP1`` finished the trade.
        """
        position = self.factory.open_position(self.symbol)
        if position is None:
            return
        if self._open is None or not self._open.is_open:
            self.factory.forget_cycle(self.symbol)
            return
        position.remaining = self._open.remaining
        position.tp1_filled = self._open.tp1_filled


_EXIT_REASONS = {
    SignalAction.TP1: ExitReason.TP1,
    SignalAction.TP2: ExitReason.TP2,
    SignalAction.SL: ExitReason.SL,
    SignalAction.R_SL: ExitReason.R_SL,
    SignalAction.FLAT: ExitReason.FLAT,
}


def sample_market(
    frame: pd.DataFrame,
    warmup: int,
    timeframe: str,
    max_rows: int = MARKET_MAX_ROWS,
) -> MarketWindow:
    """The OHLC rows the report carries for drawing, at the finest timeframe that fits.

    The run's own timeframe when the file is short enough to carry whole, which
    is the normal case and the one that matters: a chart drawn from
    pre-aggregated bars can never be shown at a *finer* resolution than it
    arrived at, so handing the viewer coarse bars decides for it. Above
    *max_rows* this climbs the timeframe ladder — M15 to M30 to H1 and so on —
    and takes the first rung that fits.

    Aggregation is by the calendar and never by counting bars. Taking every Nth
    bar would drop the highs and lows a chart is mostly there to show, and
    counting bars into buckets produces candles whose span changes across every
    session break and weekend.
    """
    base_timeframe, drawn = _drawable_series(frame, timeframe, max_rows)
    rows = [
        [
            int(_as_datetime(moment).timestamp()),
            round(float(open_), 8),
            round(float(high), 8),
            round(float(low), 8),
            round(float(close), 8),
        ]
        for moment, open_, high, low, close in zip(
            drawn.index,
            drawn["open"],
            drawn["high"],
            drawn["low"],
            drawn["close"],
            strict=True,
        )
    ]

    anchor = min(max(warmup - 1, 0), len(frame) - 1)
    return MarketWindow(
        bucket_bars=max(1, timeframe_seconds(base_timeframe) // timeframe_seconds(timeframe)),
        base_timeframe=base_timeframe,
        rows=rows,
        benchmark_close=round(float(frame["close"].iloc[anchor]), 8),
        last_close=round(float(frame["close"].iloc[-1]), 8),
        benchmark_from=_as_datetime(frame.index[anchor]),
    )


def _drawable_series(
    frame: pd.DataFrame, timeframe: str, max_rows: int
) -> tuple[str, pd.DataFrame]:
    """*frame* itself, or the coarsest-necessary calendar resampling of it."""
    if len(frame) <= max_rows:
        return timeframe, frame

    seconds = timeframe_seconds(timeframe)
    ladder = sorted(
        (label for label, span in TIMEFRAME_SECONDS.items() if span > seconds),
        key=lambda label: TIMEFRAME_SECONDS[label],
    )
    for label in ladder:
        # ``origin="epoch"`` is the boundary `floor_to_bucket` uses, so a bucket
        # here opens at the same moment the ingestion side would open it.
        resampled = (
            frame.resample(
                pd.Timedelta(seconds=TIMEFRAME_SECONDS[label]),
                label="left",
                closed="left",
                origin="epoch",
            )
            .agg({"open": "first", "high": "max", "low": "min", "close": "last"})
            .dropna()
        )
        if len(resampled) <= max_rows:
            return label, resampled

    # Longer than `max_rows` days. Draw it daily and say so rather than
    # inventing a timeframe the rest of the engine does not have a name for.
    log.warning(
        "History is %d bars; drawing the price chart at D1, the coarsest timeframe there is",
        len(frame),
    )
    return "D1", resampled


def count_gaps(frame: pd.DataFrame, timeframe: str) -> int:
    """How many times consecutive bars are further apart than one timeframe.

    Weekends and session breaks land here too, which is why the diagnostic that
    reads this is informational: the number is a prompt to check the calendar,
    not a defect on its own.
    """
    if len(frame) < 2:
        return 0
    expected = pd.Timedelta(seconds=timeframe_seconds(timeframe))
    return int((frame.index.to_series().diff().dropna() > expected).sum())


def _as_datetime(value: Any) -> datetime:
    return pd.Timestamp(value).to_pydatetime()
