"""Ingestion-only configuration (prefix ``QTE_INGESTION__``)."""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from qte_shared.config import market_data_plan


class IngestionSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="QTE_INGESTION__", extra="ignore")

    #: Force a symbol onto a market when inference gets it wrong, e.g.
    #: ``{"BTCUSD": "fx"}`` for a CFD desk quoting bitcoin on the FX socket.
    #: Only consulted when there is no market-data plan: a plan states
    #: ``market`` beside the symbol it belongs to, where it can be read.
    market_overrides: dict[str, str] = Field(default_factory=dict)
    #: How often the wall-clock flush runs. Must stay well under the shortest
    #: timeframe, or bars close late in a quiet market.
    flush_interval: float = 1.0
    #: Publish every tick on ``QTE.tick.<symbol>``. Off by default: only a
    #: strategy overriding ``on_tick`` consumes them, and a busy FX feed is a
    #: lot of traffic to move for subscribers that discard it.
    publish_ticks: bool = False
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
