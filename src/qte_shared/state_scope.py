"""Immutable state identity and market-data provenance shared by every service.

A scope is never changed by a runtime shadow flag. Switching environment,
execution mode or provider selects another book, cache and internal bus.
Unscoped legacy data is not promoted into any new scope automatically.
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
    provider: str
    synthetic: bool


@dataclass(frozen=True)
class StateScope:
    environment: str
    execution_mode: ExecutionMode
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
        if self.provider == "simulator" and self.execution_mode != "dev":
            raise ValueError("Synthetic providers require QTE_STATE__MODE=dev")

    @property
    def namespace(self) -> str:
        return f"{self.environment}:{self.execution_mode}:{self.provider}"

    @property
    def is_paper(self) -> bool:
        return self.execution_mode != "live"

    def origin(self, *, synthetic: bool | None = None) -> MarketDataOrigin:
        synthetic = self.provider == "simulator" if synthetic is None else synthetic
        if synthetic and self.execution_mode != "dev":
            raise ValueError("Synthetic market data is only allowed in dev state")
        return MarketDataOrigin(
            namespace=self.namespace, provider=self.provider, synthetic=synthetic
        )

    def accepts(self, origin: MarketDataOrigin | None) -> bool:
        return origin is not None and (
            origin.namespace == self.namespace
            and origin.provider == self.provider
            and (not origin.synthetic or self.execution_mode == "dev")
            and (self.provider != "simulator" or origin.synthetic)
        )


def stamp_market_data[Record: BaseModel](record: Record, origin: MarketDataOrigin) -> Record:
    """Attribute a fresh producer record without relabelling another source."""
    if record.origin is not None and record.origin != origin:
        raise ValueError("Market data belongs to another state scope or provider")
    return record.model_copy(update={"origin": origin})
