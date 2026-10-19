"""Immutable state identity and market-data provenance shared by every service.

A scope is never changed by a runtime shadow flag. Switching environment,
execution mode or provider selects another book, cache and internal bus.
Unscoped legacy data is not promoted into any new scope automatically.

Several providers share one scope: its provider component is their names,
sorted and joined by ``-`` (``binance-mt5``). Adding or removing one is a
provider switch like any other. Each tick and candle still records the single
provider that produced it, which is what :meth:`StateScope.accepts` checks.
"""

import re
from dataclasses import dataclass
from typing import Literal

from pydantic import BaseModel, ConfigDict

ExecutionMode = Literal["dev", "shadow", "live"]


class MarketDataOrigin(BaseModel):
    """The producer's identity, retained on both ticks and candles."""

    model_config = ConfigDict(frozen=True)

    namespace: str
    #: The one provider that produced the record, even in a multi-provider scope.
    provider: str
    synthetic: bool


@dataclass(frozen=True)
class StateScope:
    environment: str
    execution_mode: ExecutionMode
    #: The provider key: one name, or several sorted and joined by ``-``.
    provider: str

    def __post_init__(self) -> None:
        if self.execution_mode not in ("dev", "shadow", "live"):
            raise ValueError("State mode must be dev, shadow or live")
        for component in (self.environment, self.execution_mode, self.provider):
            if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,47}", component):
                raise ValueError("State identity components must be lowercase identifiers")
        if self.execution_mode == "dev" and self.environment != "dev":
            raise ValueError("The dev state mode requires QTE_ENV=dev")
        if self.execution_mode == "live" and self.environment == "dev":
            raise ValueError("Live state requires a non-development QTE_ENV")
        if "simulator" in self.providers:
            if self.execution_mode != "dev":
                raise ValueError("Synthetic providers require QTE_STATE__MODE=dev")
            if len(self.providers) > 1:
                # One scope, one answer to "may a bar be dated after now": the
                # guard that discards future-dated state exempts synthetic feeds.
                raise ValueError("The simulator cannot share a state scope with another provider")

    @property
    def providers(self) -> tuple[str, ...]:
        """Every provider the scope takes market data from."""
        return tuple(self.provider.split("-"))

    @property
    def namespace(self) -> str:
        return f"{self.environment}:{self.execution_mode}:{self.provider}"

    @property
    def is_paper(self) -> bool:
        return self.execution_mode != "live"

    def origin(
        self, *, provider: str | None = None, synthetic: bool | None = None
    ) -> MarketDataOrigin:
        """The provenance *provider* stamps; optional when the scope has one provider."""
        if provider is None:
            if len(self.providers) > 1:
                raise ValueError(
                    f"State scope {self.provider!r} has several providers; name the producing one"
                )
            provider = self.provider
        if provider not in self.providers:
            raise ValueError(f"Provider {provider!r} is not part of state scope {self.provider!r}")
        synthetic = provider == "simulator" if synthetic is None else synthetic
        if synthetic and self.execution_mode != "dev":
            raise ValueError("Synthetic market data is only allowed in dev state")
        return MarketDataOrigin(namespace=self.namespace, provider=provider, synthetic=synthetic)

    def accepts(self, origin: MarketDataOrigin | None) -> bool:
        return origin is not None and (
            origin.namespace == self.namespace
            and origin.provider in self.providers
            and (not origin.synthetic or self.execution_mode == "dev")
            and (origin.provider != "simulator" or origin.synthetic)
        )


def stamp_market_data[Record: BaseModel](record: Record, origin: MarketDataOrigin) -> Record:
    """Attribute a fresh producer record without relabelling another source."""
    if record.origin is not None and record.origin != origin:
        raise ValueError("Market data belongs to another state scope or provider")
    return record.model_copy(update={"origin": origin})


def stamp_in_scope[Record: BaseModel](record: Record, scope: StateScope) -> Record:
    """Check a record against *scope*, attributing it when it carries no origin yet.

    A stamped record keeps its producing provider, which must belong to the
    scope. An unstamped one can only be attributed when the scope has a single
    provider; with several, its producer has to stamp it first.
    """
    origin = record.origin
    if origin is None:
        return stamp_market_data(record, scope.origin())
    if origin.provider not in scope.providers:
        raise ValueError("Market data belongs to another state scope or provider")
    return stamp_market_data(record, scope.origin(provider=origin.provider))
