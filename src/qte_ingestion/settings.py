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
    #: Seconds before a series whose history could not be fetched is asked for
    #: again, doubling up to ``history_retry_max_interval``. This is for a
    #: vendor that is connected and said "not now" — an MT5 terminal still
    #: logging in. A vendor that is not connected at all is not asked on a
    #: timer: ingestion waits for it to announce itself.
    history_retry_interval: float = Field(default=5.0, gt=0)
    history_retry_max_interval: float = Field(default=60.0, gt=0)
    #: Seconds between probes while the vendor is not connected. 0, the
    #: default, sends none: the vendor's announcement ends the wait, and an
    #: announcement missed while QTE's own NATS connection was down is covered
    #: by checking once when that connection returns. Set it only for a vendor
    #: that cannot announce itself.
    history_offline_probe_interval: float = Field(default=0.0, ge=0)
    #: Least seconds between two inline top-ups of one series. A live bar that
    #: does not follow the newest stored one triggers a history request before
    #: it is staged; a market's own session breaks look the same, and this
    #: keeps them at one request each.
    history_gap_cooldown: float = Field(default=60.0, ge=0)


ingestion_settings = IngestionSettings()
