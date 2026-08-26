"""Turning a strategy's :class:`SignalIntent` into a broker-shaped payload.

Both drivers use this — the backtest replay and the live runner — so a signal
recorded in a backtest report is byte-identical to the one a worker would have
executed. That is the whole point: reconciliation compares two rows, not two
implementations.

The factory owns four things a strategy is deliberately not allowed to:

* **Cycle ids.** An entry mints a ``signal_uxid``; every close reuses the one
  its entry got. Get this wrong and the broker renders an exit as a separate
  trade instead of closing the entry's broadcast.
* **The bracket.** SL/TP levels are attached here, from the intent when the
  strategy set them and from ATR/percentage defaults when it did not, so
  "never send a naked entry" is enforced in one place.
* **Size.** ``quantity`` is risk-sized against the configured account — see
  :mod:`qte_shared.sizing` — because a strategy is not told the balance, and a
  strategy that sized itself against a notional one would trade a book nobody
  configured. A strategy's own proposal survives as a *proportion*: the ratio
  between what QTE sent and what it asked for is remembered on the cycle, and
  every close it emits is scaled by that ratio.
* **Timeframe spelling.** QTE says ``M15``; the broker's contract says ``"15"``.

**One cycle per (strategy, symbol) at a time.** The factory refuses a second
entry while one is open, and it is what decides when a cycle is over:
``TP2``/``SL``/``R_SL``/``FLAT`` end it outright, and so does a ``TP1`` that
takes the entry's whole quantity. That state is an
:class:`~qte_shared.models.OpenPosition`, which the live runner persists to
Redis and Postgres so a restart resumes the cycle instead of orphaning it.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from qte_shared.config import settings
from qte_shared.logging_setup import get_logger
from qte_shared.models import (
    QUANTITY_EPSILON,
    TERMINAL_ACTIONS,
    BrokerSignal,
    OpenPosition,
    SignalAction,
    new_uxid,
)
from qte_shared.sizing import PositionSizer, resolve_use_equity_sizing
from qte_shared.strategy_base import SignalIntent
from qte_shared.timeframes import to_broker_timeframe

log = get_logger(__name__)


class BracketPolicy:
    """Fallback stop/target levels for an intent that arrived without them.

    A strategy is free to compute its own levels — most should. This exists so
    that an entry which somehow reaches the wire without a stop still carries
    one: an unbracketed position on a worker is an unbounded loss waiting for
    someone to notice. It is also what makes an entry *sizeable*, since the
    stop distance is the denominator of the risk calculation.
    """

    def __init__(
        self,
        *,
        sl_pct: float = 0.5,
        tp1_r: float = 1.0,
        tp2_r: float = 2.0,
        tp1_percent: float = 50.0,
        move_sl_to_be: bool = True,
        risk_percent: float | None = None,
    ) -> None:
        self.sl_pct = sl_pct
        self.tp1_r = tp1_r
        self.tp2_r = tp2_r
        self.tp1_percent = tp1_percent
        self.move_sl_to_be = move_sl_to_be
        self.risk_percent = risk_percent

    def apply(self, intent: SignalIntent) -> SignalIntent:
        if not intent.action.is_entry or intent.price is None:
            return intent

        direction = 1 if intent.action is SignalAction.LONG else -1
        if intent.sl is None:
            intent.sl = intent.price * (1 - direction * self.sl_pct / 100.0)
            log.warning(
                "Entry for %s had no stop; applied the %.2f%% default at %.5f",
                intent.symbol,
                self.sl_pct,
                intent.sl,
            )

        risk = abs(intent.price - intent.sl)
        if intent.tp1 is None and risk > 0:
            intent.tp1 = intent.price + direction * risk * self.tp1_r
        if intent.tp2 is None and risk > 0:
            intent.tp2 = intent.price + direction * risk * self.tp2_r
        if intent.tp1_percent is None:
            intent.tp1_percent = self.tp1_percent
        if intent.move_sl_to_be is None:
            intent.move_sl_to_be = self.move_sl_to_be
        if intent.risk_percent is None and self.risk_percent is not None:
            intent.risk_percent = self.risk_percent
        return intent


class SignalFactory:
    """Builds :class:`BrokerSignal` objects and remembers the open cycle."""

    def __init__(
        self,
        strategy_name: str,
        *,
        timeframe: str = "M15",
        token: str | None = None,
        bracket: BracketPolicy | None = None,
        inputs: dict[str, Any] | None = None,
        sizer: PositionSizer | None = None,
        default_quantity: float | None = None,
    ) -> None:
        self.strategy_name = strategy_name
        self.timeframe = timeframe
        self.token = token if token is not None else settings.broker.token
        self.bracket = bracket or BracketPolicy()
        self.inputs = dict(inputs or {})
        #: Risk sizing for this pair. Built from ``QTE_ACCOUNT__*`` and the
        #: pair's own params, so the routing table's ``risk_percent`` is
        #: honoured without every caller having to dig it out.
        self.sizer = sizer or PositionSizer.from_settings(
            self.inputs, risk_percent=self.bracket.risk_percent
        )
        #: Size for an entry the sizer could not size — no stop, or a stop
        #: sitting on the entry. ``None`` keeps whatever the strategy proposed.
        self.default_quantity = default_quantity
        #: What the pair's configuration says about equity sizing, mirrored
        #: onto every payload. It never changes the number above.
        self.use_equity_sizing = resolve_use_equity_sizing(self.inputs)
        #: symbol → the one cycle we believe is open on it.
        self._book: dict[str, OpenPosition] = {}
        #: symbol → sizing scale of an entry built but not yet committed.
        self._pending_scale: dict[str, float] = {}

    # ── Cycle bookkeeping ─────────────────────────────────────────────

    def open_cycle(self, symbol: str) -> str | None:
        """Cycle id of the position held on *symbol*, or ``None`` when flat."""
        position = self._book.get(symbol.upper())
        return position.signal_uxid if position else None

    def open_position(self, symbol: str) -> OpenPosition | None:
        """The whole cycle record — what the runner persists and reloads."""
        return self._book.get(symbol.upper())

    def open_positions(self) -> dict[str, OpenPosition]:
        return dict(self._book)

    def restore_cycles(self, cycles: Mapping[str, str]) -> None:
        """Reload bare cycle ids after a restart.

        For a record carrying nothing but a uxid — a legacy Redis value, or a
        caller that only has the id. The size is unknown, so a ``TP1`` restored
        this way cannot be recognised as having closed the whole position and
        the cycle stays open until a terminal action ends it.
        :meth:`restore_positions` is the lossless route.
        """
        for symbol, uxid in cycles.items():
            self.restore_position(OpenPosition(signal_uxid=uxid, symbol=symbol.upper()))

    def restore_positions(self, positions: Mapping[str, OpenPosition]) -> None:
        for symbol, position in positions.items():
            self.restore_position(position, symbol=symbol)

    def restore_position(self, position: OpenPosition, symbol: str | None = None) -> None:
        key = (symbol or position.symbol).upper()
        self._book[key] = position.model_copy(
            update={"symbol": key, "strategy": position.strategy or self.strategy_name}
        )

    def forget_cycle(self, symbol: str) -> None:
        self._book.pop(symbol.upper(), None)
        self._pending_scale.pop(symbol.upper(), None)

    def commit(self, signal: BrokerSignal) -> None:
        """Apply the cycle transition represented by a delivered signal.

        Live delivery is a two-phase operation: first build and validate the
        payload, then publish it, and only then commit the position state. A
        failed publish must not make the runner believe it holds a position the
        broker never opened (or forget one the broker never closed).

        Backtests keep the convenient eager behaviour through :meth:`build`'s
        default ``commit=True``.
        """
        symbol = signal.symbol.upper()
        block = signal.position
        action = block.action

        if action.is_entry:
            self._book[symbol] = OpenPosition(
                signal_uxid=signal.signal_uxid,
                strategy=signal.strategy,
                symbol=symbol,
                action=action,
                opened_at=signal.timestamp,
                updated_at=signal.timestamp,
                price=block.price,
                quantity=block.quantity,
                remaining=block.quantity,
                sl=block.sl,
                tp1=block.tp1,
                tp2=block.tp2,
                risk_percent=block.risk_percent,
                tp1_percent=block.tp1_percent,
                move_sl_to_be=block.move_sl_to_be,
                use_equity_sizing=block.use_equity_sizing,
                is_scale_position=block.is_scale_position,
                scale_strategy=block.scale_strategy,
                scaling=block.scaling,
                scale=self._pending_scale.pop(symbol, 1.0),
            )
            return

        position = self._book.get(symbol)
        if position is None:
            return
        # Do not let a delayed close for an older cycle erase a newer position
        # that happens to use the same strategy and symbol.
        if position.signal_uxid != signal.signal_uxid:
            return
        if position.apply_close(action, block.quantity):
            self._book.pop(symbol, None)

    # ── Building ──────────────────────────────────────────────────────

    def build(
        self,
        intent: SignalIntent,
        *,
        symbol: str,
        moment: datetime,
        indicators: dict[str, Any] | None = None,
        commit: bool = True,
    ) -> BrokerSignal:
        """Materialise *intent* into the payload the broker ingests.

        Mutates *intent* in place with what it settled — the bracket it applied
        and the size it computed — so a driver that also fills the trade itself
        fills exactly what was published.

        Raises ``ValueError`` when the result would be a payload a worker
        cannot act on — an entry with no size, a close with no cycle to close.
        Failing here keeps a malformed signal off the wire entirely.
        """
        target_symbol = (intent.symbol or symbol).upper()
        if intent.action.is_entry:
            uxid = self._prepare_entry(intent, target_symbol)
        else:
            uxid = self._prepare_close(intent, target_symbol)

        if intent.use_equity_sizing is None:
            intent.use_equity_sizing = self.use_equity_sizing

        signal = BrokerSignal(
            strategy=self.strategy_name,
            symbol=target_symbol,
            timeframe=to_broker_timeframe(self.timeframe),
            timestamp=moment,
            signal_uxid=uxid,
            position=intent.to_position_block(),
            indicators={**(indicators or {}), **intent.indicators},
            inputs={**self.inputs, **intent.inputs},
            token=self.token,
        )
        signal.validate_shape()
        if commit:
            self.commit(signal)
        return signal

    # ── Entries ───────────────────────────────────────────────────────

    def _prepare_entry(self, intent: SignalIntent, symbol: str) -> str:
        existing = self._book.get(symbol)
        if existing is not None:
            raise ValueError(
                f"{intent.action.value} for {symbol} would replace open cycle "
                f"{existing.signal_uxid} — close the current position before opening another"
            )
        self.bracket.apply(intent)
        if intent.risk_percent is None:
            intent.risk_percent = self.sizer.risk_percent
        self._size_entry(intent, symbol)
        return intent.signal_uxid or new_uxid()

    def _size_entry(self, intent: SignalIntent, symbol: str) -> None:
        """Replace the strategy's proposal with the account's own size.

        The ratio between the two is stashed rather than written to the book
        straight away: :meth:`build` may still reject this entry, and a scale
        left behind for a cycle that never opened would rescale the *next*
        one's closes. :meth:`commit` picks it up when the entry actually lands.
        """
        proposed = intent.quantity
        if proposed is not None and proposed <= 0:
            # A strategy that says "zero" is declining the trade, not asking to
            # be sized. Manufacturing a position out of it would turn a
            # strategy-side guard into a live entry; leave it, and
            # `validate_shape()` rejects the payload as it always has.
            return
        sized = self.sizer.size(intent.price, intent.sl)
        if sized is None:
            if proposed is None and self.default_quantity is not None:
                intent.quantity = self.default_quantity
            log.debug(
                "Entry for %s could not be risk-sized (price=%s sl=%s); keeping quantity=%s",
                symbol,
                intent.price,
                intent.sl,
                intent.quantity,
            )
            return

        intent.quantity = sized
        self._pending_scale[symbol] = sized / proposed if proposed else 1.0

    # ── Closes ────────────────────────────────────────────────────────

    def _prepare_close(self, intent: SignalIntent, symbol: str) -> str:
        position = self._book.get(symbol)
        uxid = intent.signal_uxid or (position.signal_uxid if position else None)
        if uxid is None:
            raise ValueError(
                f"{intent.action.value} for {symbol} has no cycle to close — "
                "the strategy emitted an exit with no matching entry"
            )
        if position is not None:
            self._size_close(intent, position)
            self._carry_entry_context(intent, position)
        if intent.risk_percent is None:
            intent.risk_percent = self.sizer.risk_percent
        return uxid

    @staticmethod
    def _carry_entry_context(intent: SignalIntent, position: OpenPosition) -> None:
        """Restate on the close what the entry established.

        A worker reading only the close should be able to re-sync the trade
        from it, which is why the broker's own examples carry ``risk_percent``,
        ``tp1_percent`` and the scaling block on a TP as well as on the entry.
        Everything here is filled only where the strategy said nothing — a
        strategy managing its own exit is describing the trade *now*, and now
        is the more recent truth.
        """
        for name in (
            "risk_percent",
            "tp1_percent",
            "move_sl_to_be",
            "use_equity_sizing",
            "is_scale_position",
            "scale_strategy",
            "scaling",
        ):
            if getattr(intent, name) is None:
                setattr(intent, name, getattr(position, name))
        if intent.is_running is None:
            intent.is_running = _still_running(intent, position)

    def _size_close(self, intent: SignalIntent, position: OpenPosition) -> None:
        """Express the strategy's close in the size QTE is actually holding.

        A strategy books partials against its own mirror of the trade, so its
        ``quantity`` is in *its* units; multiplying by the cycle's scale lands
        it back on ours. Clamping to what remains is what stops a close from
        claiming more than the position has left — the negative residual would
        otherwise keep the cycle open forever.

        A close naming no size gets one from the position, with one exception:
        ``FLAT`` is the broker's "close everything on this strategy" directive
        and carries no size by contract, so inventing one would narrow it.
        """
        remaining = position.remaining
        if intent.quantity is not None:
            scaled = self.sizer.rescale(intent.quantity, position.scale)
            intent.quantity = min(scaled, remaining) if remaining is not None else scaled
            return

        if intent.action is SignalAction.FLAT:
            return
        if intent.action is SignalAction.TP1:
            intent.quantity = position.share(intent.tp1_percent or position.tp1_percent)
            return
        intent.quantity = remaining


def _still_running(intent: SignalIntent, position: OpenPosition) -> bool | None:
    """Whether anything is left of the position once *intent* is filled.

    The broker renders a partial differently from a completed trade, and
    ``is_running`` is how it is told which this is — true on the TP1 that banks
    part of a position, false on the exit that finishes it. Derived rather than
    asked of the strategy, because the factory is what knows the sizes.

    ``None`` when the size is unknown (a cycle restored from a bare uxid): the
    broker's schema takes an absent value, and guessing here would state
    something about a position we cannot see.
    """
    if intent.action in TERMINAL_ACTIONS:
        return False
    if position.remaining is None or intent.quantity is None:
        return None
    return position.remaining - abs(intent.quantity) > QUANTITY_EPSILON


__all__ = ["BracketPolicy", "SignalFactory"]
