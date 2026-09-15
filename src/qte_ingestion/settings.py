"""Ingestion-only configuration (prefix ``QTE_INGESTION__``)."""

from __future__ import annotations

from pydantic import Field

from qte_shared.config import MarketStreamSettings, market_data_plan


class IngestionSettings(MarketStreamSettings):
    #: How often the wall-clock flush runs. Must stay well under the shortest
    #: timeframe, or bars close late in a quiet market.
    flush_interval: float = 1.0
    #: Persist the in-progress bar to Redis on each closed candle so a restart
    #: mid-bar resumes rather than losing it.
    persist_open_candles: bool = True
    #: Top Redis up to ``QTE_REDIS__CANDLE_HISTORY`` bars from the provider at
    #: boot, so the runner warms its indicator window on the first close rather
    #: than days later. Only providers that serve history do anything here; see
    #: :mod:`qte_ingestion.backfill`. Set it in ``[provider]`` of the plan,
    #: beside the vendor knobs that decide what a fetch costs.
    backfill_history: bool = Field(
        default_factory=lambda: bool(market_data_plan().option("backfill_history", True))
    )


ingestion_settings = IngestionSettings()
