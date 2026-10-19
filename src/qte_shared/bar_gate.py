"""The switch that stops the runner deciding on closed bars.

``/prevent`` and ``/allow`` in the Telegram bot. It lives in shared because two
services hold opposite ends of it: the bot stores it, the runner enforces it,
and a flag only one of them could name would not be a contract at all.

It exists for the moment an operator wants a pair to stop producing signals
*without* stopping the runner: a restart loses the candle window, the ownership
claim and every in-flight delivery, which is a lot of machinery to disturb when
the actual request is "stop trading gold for an hour".

**A gated bar is still stored.** The close is fed into the strategy's window
and recorded as seen; only the decision is skipped. Dropping the bar instead
would leave a hole in the window exactly as wide as the pause, and the first
decision after ``/allow`` would then be computed on indicators that silently
disagree with the market — the opposite of what pausing was for.

Scope is additive and remembered per name: ``prevent`` on ``XAUUSD`` and then
on ``QTE_EXAMPLE_EMA_ATR`` blocks both, and ``allow`` takes one back without
touching the other. ``all`` is its own flag rather than a wildcard entry, so
"everything is paused" cannot be half-undone by releasing one symbol.

The state lives in Redis under the state namespace, like shadow mode and for
the same reason: a runner that restarts during a pause must come back paused.
The NATS broadcast only tells the running process to re-read it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

#: Redis flag name, under the namespace :class:`~qte_shared.cache.RedisState` adds.
BAR_GATE_FLAG = "bar_gate"


@dataclass(slots=True)
class BarGate:
    """Which pairs may not be decided on right now."""

    #: Nothing is decided on, whatever the sets below hold.
    everything: bool = False
    #: Symbols blocked by name, upper-cased.
    symbols: set[str] = field(default_factory=set)
    #: Strategies blocked by name.
    strategies: set[str] = field(default_factory=set)

    # ── Reading ───────────────────────────────────────────────────────

    @property
    def blocking(self) -> bool:
        """Whether anything at all is blocked."""
        return bool(self.everything or self.symbols or self.strategies)

    def blocks(self, *, symbol: str, strategy: str) -> bool:
        """Whether this pair's closed bars must not be decided on."""
        if self.everything:
            return True
        return symbol.upper() in self.symbols or strategy in self.strategies

    def describe(self) -> str:
        """One line for a log or a chat message."""
        if self.everything:
            return "everything"
        if not self.blocking:
            return "nothing"
        parts = []
        if self.symbols:
            parts.append("symbols=" + ",".join(sorted(self.symbols)))
        if self.strategies:
            parts.append("strategies=" + ",".join(sorted(self.strategies)))
        return " ".join(parts)

    # ── Writing ───────────────────────────────────────────────────────

    def prevent(
        self, *, everything: bool = False, symbol: str | None = None, strategy: str | None = None
    ) -> BarGate:
        """Add a scope to the block. Returns self, so calls read as a pipeline."""
        if everything:
            self.everything = True
        if symbol:
            self.symbols.add(symbol.upper())
        if strategy:
            self.strategies.add(strategy)
        return self

    def allow(
        self, *, everything: bool = False, symbol: str | None = None, strategy: str | None = None
    ) -> BarGate:
        """Release a scope.

        ``everything=True`` clears the whole gate, named sets included: an
        operator who says "allow all" means trading resumes, not "resume
        everything except what somebody paused by name yesterday".
        """
        if everything:
            self.everything = False
            self.symbols.clear()
            self.strategies.clear()
        if symbol:
            self.symbols.discard(symbol.upper())
        if strategy:
            self.strategies.discard(strategy)
        return self

    # ── Serialization ─────────────────────────────────────────────────

    def to_payload(self) -> dict[str, Any]:
        """The JSON shape stored in Redis and broadcast over NATS."""
        return {
            "everything": self.everything,
            "symbols": sorted(self.symbols),
            "strategies": sorted(self.strategies),
        }

    @classmethod
    def from_payload(cls, payload: Any) -> BarGate:
        """Read a stored gate, tolerating anything that is not one.

        A malformed flag reads as "nothing is blocked" rather than raising: the
        alternative is a runner that refuses to decide on any bar because one
        value in Redis is the wrong shape, which is a worse failure than losing
        a pause somebody will notice is missing.
        """
        if not isinstance(payload, dict):
            return cls()
        symbols = payload.get("symbols")
        strategies = payload.get("strategies")
        return cls(
            everything=bool(payload.get("everything")),
            symbols=(
                {str(name).upper() for name in symbols} if isinstance(symbols, list) else set()
            ),
            strategies=(
                {str(name) for name in strategies} if isinstance(strategies, list) else set()
            ),
        )


__all__ = ["BAR_GATE_FLAG", "BarGate"]
