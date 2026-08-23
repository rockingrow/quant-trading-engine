"""Strategy-runner configuration (prefix ``QTE_RUNNER__``)."""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class RunnerSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="QTE_RUNNER__", extra="ignore")

    #: Only run these strategies; empty means every one discovered.
    enabled_strategies: list[str] = Field(default_factory=list)
    #: Per-strategy parameter overrides, keyed by strategy name.
    strategy_params: dict[str, dict] = Field(default_factory=dict)
    #: Default size attached to an entry whose strategy did not set one.
    default_quantity: float = 0.01
    #: NATS queue group. Two runner replicas in the same group split candles
    #: between them, so exactly one of them acts on each close.
    queue_group: str = "qte-runners"
    #: Subscribe to ticks. Only worth it when a strategy overrides ``on_tick``;
    #: the runner turns it on automatically when one does.
    subscribe_ticks: bool = False


runner_settings = RunnerSettings()
