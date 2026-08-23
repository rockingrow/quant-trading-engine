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
    reports = tmp_path / "reports"
    reports.mkdir()

    monkeypatch.setattr(settings.engine, "strategies_dir", strategies)
    monkeypatch.setattr(settings.engine, "parquet_dir", parquet)
    monkeypatch.setattr(settings.engine, "reports_dir", reports)
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


def test_reports_listing_is_empty_before_anything_runs(client):
    assert client.get("/backtest/reports").json() == []


def test_a_written_report_can_be_listed_and_fetched_whole(client):
    directory = settings.engine.reports_dir
    (directory / "RUN_XAUUSD_M15_20260501T000000Z.json").write_text('{"schema_version": "1.0"}')
    (directory / "RUN_XAUUSD_M15_20260501T000000Z.md").write_text("# Backtest")

    listing = client.get("/backtest/reports").json()
    assert {entry["format"] for entry in listing} == {"json", "md"}

    fetched = client.get("/backtest/reports/RUN_XAUUSD_M15_20260501T000000Z.json")
    assert fetched.status_code == 200
    assert fetched.json() == {"schema_version": "1.0"}
    assert fetched.headers["content-type"].startswith("application/json")


def test_a_markdown_report_is_served_as_markdown(client):
    (settings.engine.reports_dir / "a.md").write_text("# Backtest")
    response = client.get("/backtest/reports/a.md")
    assert response.headers["content-type"].startswith("text/markdown")
    assert response.text == "# Backtest"


@pytest.mark.parametrize("name", ["../../.env", "..%2f..%2f.env", "subdir/../../../etc/passwd"])
def test_a_report_name_cannot_escape_the_reports_directory(client, name):
    # Without the containment check, `../../.env` would be a readable report.
    response = client.get(f"/backtest/reports/{name}")
    assert response.status_code in (400, 404)
    assert "QTE_BROKER__TOKEN" not in response.text


def test_a_non_report_extension_is_refused(client):
    (settings.engine.reports_dir / "secrets.txt").write_text("nope")
    assert client.get("/backtest/reports/secrets.txt").status_code == 400


def test_fetching_a_report_that_does_not_exist_404s(client):
    assert client.get("/backtest/reports/nope.json").status_code == 404
