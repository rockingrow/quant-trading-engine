"""Control-plane tests that need no infrastructure.

The app is expected to come up with NATS and Postgres unreachable — a control
plane that dies when a dependency dies cannot tell anyone which one died.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from qte_shared.config import settings

STRATEGY_SOURCE = textwrap.dedent(
    """
    from qte_shared.strategy_base import StrategyBase


    class ApiProbe(StrategyBase):
        name = "API_PROBE"
        symbols = ("XAUUSD",)
        timeframe = "M15"
        warmup = 12

        def on_candle_closed(self, df, context):
            return None
    """
)


@pytest.fixture
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    strategies = tmp_path / "user_strategies"
    strategies.mkdir()
    (strategies / "probe.py").write_text(STRATEGY_SOURCE)
    parquet = tmp_path / "parquet"
    parquet.mkdir()

    monkeypatch.setattr(settings.engine, "strategies_dir", strategies)
    monkeypatch.setattr(settings.engine, "parquet_dir", parquet)
    monkeypatch.setattr(settings.postgres, "enabled", False)
    # No NATS in this test run; keep the deliberate startup retry short.
    monkeypatch.setattr(settings.nats, "connect_timeout", 0.3)

    from qte_api.main import create_app

    with TestClient(create_app()) as test_client:
        yield test_client


def test_health_reports_degraded_rather_than_failing(client):
    response = client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] in ("ok", "degraded")
    assert set(body["dependencies"]) == {"redis", "postgres", "nats"}


def test_strategies_lists_whatever_is_in_the_mounted_directory(client):
    body = client.get("/strategies").json()
    assert [entry["name"] for entry in body] == ["API_PROBE"]
    assert body[0]["symbols"] == ["XAUUSD"]
    assert body[0]["warmup"] == 12


def test_backtest_history_is_empty_when_nothing_is_downloaded(client):
    assert client.get("/backtest/history").json() == []


def test_backtest_run_404s_on_an_unknown_strategy(client):
    response = client.post("/backtest/run", json={"strategy": "NOPE", "symbol": "XAUUSD"})
    assert response.status_code == 404
    assert "NOPE" in response.json()["detail"]


def test_backtest_run_404s_when_the_history_is_missing(client):
    response = client.post(
        "/backtest/run", json={"strategy": "API_PROBE", "symbol": "XAUUSD", "persist": False}
    )
    assert response.status_code == 404
    assert "download" in response.json()["detail"]


def test_the_api_key_guard_is_enforced_when_one_is_configured(client, monkeypatch):
    monkeypatch.setattr(settings.api, "api_key", "s3cret")
    assert client.post("/admin/shadow-mode", json={"enabled": True}).status_code == 401
