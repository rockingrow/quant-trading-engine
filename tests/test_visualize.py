"""The dashboard is a rendering of the report, and must stay one.

Two properties are worth pinning down. The first is that every panel is derived
from the JSON alone — a report kept from a run months ago, on a machine with no
history and no strategy installed, still draws. The second is that the page is
self-contained: nothing it needs is fetched when it opens.
"""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime, timedelta

import pytest

from qte_backtest.execution import CostModel
from qte_backtest.replay import BacktestEngine
from qte_backtest.report import build_report
from qte_backtest.visualize import build_view, render_html
from qte_backtest.visualize.view import BENCHMARK_POINTS
from qte_shared.models import SignalAction
from qte_shared.strategies.strategy_base import SignalIntent, StrategyBase

START = datetime(2026, 3, 2, tzinfo=UTC)


class Cadence(StrategyBase):
    """Enters on a fixed cadence, so the panels have something to describe."""

    name = "VIZ_PROBE"
    timeframe = "M15"
    warmup = 20

    def on_candle_closed(self, df, context):
        if context.open_uxid is not None or len(df) % 25 != 0:
            return None
        close = float(df["close"].iloc[-1])
        return SignalIntent(
            action=SignalAction.LONG,
            price=close,
            quantity=1.0,
            sl=close - 4,
            tp1=close + 4,
            tp2=close + 8,
            tp1_percent=50.0,
            move_sl_to_be=True,
        )


@pytest.fixture
def report_dict(trending_frame) -> dict:
    result = BacktestEngine(
        Cadence(),
        symbol="XAUUSD",
        costs=CostModel(spread=0.2, commission_per_unit=0.01, contract_size=10.0),
        starting_equity=10_000,
        default_quantity=1.0,
    ).run(trending_frame)
    return build_report(result).to_dict()


@pytest.fixture
def view(report_dict) -> dict:
    return build_view(report_dict)


def _trade(index: int, net: float, *, closed: datetime, direction: str = "LONG", **extra) -> dict:
    """A trade row with only the fields the derivations read."""
    return {
        "index": index,
        "direction": direction,
        "opened_at": (closed - timedelta(hours=2)).isoformat(),
        "closed_at": closed.isoformat(),
        "bars_held": 8,
        "entry_price": 2000.0,
        "exit_price": 2000.0 + net,
        "quantity": 1.0,
        "net_pnl": net,
        "gross_pnl": net,
        "fees": 0.0,
        "exit_reason": "TP1" if net > 0 else "SL",
        **extra,
    }


def _report(trades: list[dict], *, equity: float = 1000.0, market: dict | None = None) -> dict:
    return {
        "schema_version": "1.1",
        "generated_at": START.isoformat(),
        "run": {"strategy": "S", "symbol": "X", "timeframe": "M15", "starting_equity": equity},
        "data": {"bars": 500, "bars_after_warmup": 480},
        "costs": {"contract_size": 1.0},
        "metrics": {"trades": len(trades)},
        "market": market,
        "trades": trades,
        "diagnostics": {"counts": {"critical": 0, "warning": 0, "info": 0}, "findings": []},
    }


# ── The view is derived from the file and nothing else ────────────────


def test_every_panel_comes_out_of_a_plain_dict(report_dict):
    # Round-tripping through JSON is the point: no dataclass, no engine, no
    # pandas — whatever survives the file is all the dashboard is allowed.
    view = build_view(json.loads(json.dumps(report_dict, default=str)))
    assert set(view) >= {
        "meta",
        "headline",
        "curve",
        "benchmark",
        "market",
        "breakdown",
        "periodical",
        "benchmarking",
        "growth",
        "distribution",
        "streaks",
        "details",
        "excursion",
        "trades",
        "diagnostics",
    }


def test_the_headline_is_the_four_figures_a_tester_leads_with(view):
    assert [stat["label"] for stat in view["headline"]] == [
        "Total P&L",
        "Max drawdown",
        "Profitable trades",
        "Profit factor",
    ]


def test_the_curve_reconciles_with_the_trade_list(view, report_dict):
    points = view["curve"]["points"]
    assert len(points) == len(report_dict["trades"])
    banked = sum(trade["net_pnl"] for trade in report_dict["trades"])
    assert points[-1]["cum"] == pytest.approx(banked, abs=1e-6)
    assert view["curve"]["final_equity"] == pytest.approx(
        report_dict["run"]["starting_equity"] + banked, abs=1e-6
    )


def test_drawdown_on_the_curve_never_reads_positive(view):
    assert all(point["drawdown"] <= 1e-9 for point in view["curve"]["points"])


def test_excursion_is_converted_into_money_by_the_contract_size(report_dict):
    view = build_view(report_dict)
    contract = report_dict["costs"]["contract_size"]
    for trade, point in zip(report_dict["trades"], view["curve"]["points"], strict=True):
        expected = trade["mfe"] * trade["quantity"] * contract
        assert point["runup"] == pytest.approx(expected, abs=1e-6)


# ── The individual derivations ────────────────────────────────────────


def test_growth_segments_alternate_and_agree_with_the_max_drawdown():
    equity = 1000.0
    trades = [
        _trade(1, +100.0, closed=START + timedelta(days=1)),
        _trade(2, -60.0, closed=START + timedelta(days=3)),
        _trade(3, -40.0, closed=START + timedelta(days=6)),
        _trade(4, +150.0, closed=START + timedelta(days=9)),
        _trade(5, -20.0, closed=START + timedelta(days=11)),
    ]
    growth = build_view(_report(trades, equity=equity))["growth"]
    kinds = [segment["kind"] for segment in growth["segments"]]
    assert kinds == ["runup", "drawdown", "runup", "drawdown"], kinds
    # Peak 1100 down to 1000 is the worst episode, and it must be the one the
    # panel calls the maximum.
    assert growth["drawdown"]["max_pct"] == pytest.approx(100.0 * 100.0 / 1100.0, abs=1e-4)
    assert growth["segments"][-1]["ongoing"] is True
    assert growth["drawdown"]["current_pct"] is not None


def test_a_run_that_only_climbs_has_one_open_runup_and_no_drawdown():
    trades = [_trade(index, +10.0, closed=START + timedelta(days=index)) for index in range(1, 5)]
    growth = build_view(_report(trades))["growth"]
    assert [segment["kind"] for segment in growth["segments"]] == ["runup"]
    assert growth["drawdown"]["count"] == 0
    assert growth["runup"]["current_pct"] == growth["runup"]["max_pct"]


def test_streaks_are_runs_of_the_same_sign():
    signs = [+1, +1, +1, -1, -1, +1, -1, -1, -1, -1]
    trades = [
        _trade(index, sign * 5.0, closed=START + timedelta(days=index))
        for index, sign in enumerate(signs, start=1)
    ]
    streaks = build_view(_report(trades))["streaks"]
    assert [run["count"] for run in streaks["runs"]] == [3, 2, 1, 4]
    assert streaks["longest_win"] == 3
    assert streaks["longest_loss"] == 4


def test_periods_file_a_trade_under_the_month_it_closed_in():
    # Opened in March, closed in April: April's result, because that is when the
    # money moved.
    trades = [_trade(1, +12.0, closed=datetime(2026, 4, 1, 2, tzinfo=UTC))]
    trades[0]["opened_at"] = datetime(2026, 3, 30, tzinfo=UTC).isoformat()
    buckets = build_view(_report(trades))["periodical"]["buckets"]
    assert [row["t"] for row in buckets["month"]] == ["2026-04"]
    assert buckets["month"][0]["net"] == pytest.approx(12.0)


def test_outliers_use_tukeys_fence_rather_than_the_biggest_trade():
    ordinary = [_trade(index, 1.0, closed=START + timedelta(days=index)) for index in range(1, 13)]
    distribution = build_view(_report(ordinary))["distribution"]
    assert distribution["outlier_count"] == 0, "a flat set of results has no outliers"

    with_outlier = ordinary + [_trade(13, 500.0, closed=START + timedelta(days=13))]
    distribution = build_view(_report(with_outlier))["distribution"]
    assert distribution["outlier_count"] == 1
    assert distribution["outlier_pnl"] == pytest.approx(500.0)


def test_the_details_table_splits_long_from_short():
    trades = [
        _trade(1, +10.0, closed=START + timedelta(days=1), direction="LONG"),
        _trade(2, -4.0, closed=START + timedelta(days=2), direction="SHORT"),
        _trade(3, +6.0, closed=START + timedelta(days=3), direction="SHORT"),
    ]
    rows = {row["metric"]: row for row in build_view(_report(trades))["details"]["rows"]}
    assert rows["Total trades"]["all"] == 3
    assert rows["Total trades"]["long"] == 1
    assert rows["Total trades"]["short"] == 2
    assert rows["Percent profitable"]["short"] == pytest.approx(50.0)


def test_a_statistic_that_cannot_be_computed_is_absent_rather_than_zero():
    rows = {row["metric"]: row for row in build_view(_report([]))["details"]["rows"]}
    assert rows["Average profit"]["all"] is None
    assert rows["Average profit / average loss"]["all"] is None


# ── The benchmark ─────────────────────────────────────────────────────


def _market(closes: list[float], *, quantity: float = 1.0) -> dict:
    rows = [
        [(START + timedelta(days=index)).isoformat(), close, close, close, close]
        for index, close in enumerate(closes)
    ]
    return {
        "bucket_bars": 1,
        "columns": ["t", "o", "h", "l", "c"],
        "rows": rows,
        "buy_hold": {
            "quantity": quantity,
            "contract_size": 1.0,
            "from": rows[0][0],
            "entry_close": closes[0],
            "exit_close": closes[-1],
            "net_pnl": (closes[-1] - closes[0]) * quantity,
            "return_pct": 100.0 * (closes[-1] - closes[0]) * quantity / 1000.0,
        },
    }


def test_buy_and_hold_is_priced_off_the_market_block():
    trades = [_trade(1, +5.0, closed=START + timedelta(days=2))]
    view = build_view(_report(trades, market=_market([100.0, 110.0, 120.0])))
    assert view["benchmark"]["available"] is True
    assert view["benchmark"]["points"][-1]["equity"] == pytest.approx(1020.0)
    assert view["benchmarking"]["hold_return_pct"] == pytest.approx(2.0)
    assert view["benchmarking"]["outperformance_pct"] == pytest.approx(0.5 - 2.0)


def test_a_report_without_a_market_block_simply_has_no_benchmark_panel():
    view = build_view(_report([_trade(1, +5.0, closed=START)]))
    assert view["benchmark"]["available"] is False
    assert view["market"]["available"] is False
    assert view["benchmarking"] == {"available": False}


def test_the_engine_fills_the_market_block_in(report_dict):
    market = report_dict["market"]
    assert market["columns"] == ["t", "o", "h", "l", "c"]
    assert market["rows"], "the replay should carry a drawable window"
    # Aggregated, not sampled: a bucket's high is the highest of its bars.
    for row in market["rows"]:
        assert row[2] >= max(row[1], row[4]) and row[3] <= min(row[1], row[4])
    assert market["buy_hold"]["entry_close"] > 0


@pytest.mark.parametrize("tp1,tp2", [(None, 2050.0), (2020.0, None)])
def test_price_chart_preserves_absent_targets_as_null(tp1, tp2):
    position = _trade(1, +5.0, closed=START + timedelta(hours=1), tp1=tp1, tp2=tp2)
    rendered = build_view(_report([position], market=_market([2000.0, 2010.0, 2020.0])))
    assert rendered["market"]["trades"][0]["tp1"] == tp1
    assert rendered["market"]["trades"][0]["tp2"] == tp2


def test_price_chart_trades_carry_every_exit_leg_with_its_note():
    closed = START + timedelta(days=2)
    legs = [
        {
            "closed_at": (START + timedelta(days=1)).isoformat(),
            "reason": "TP1",
            "note": None,
            "price": 2004.0,
            "quantity": 0.5,
        },
        {
            "closed_at": closed.isoformat(),
            "reason": "R_SL",
            "note": "trend flipped",
            "price": 2000.0,
            "quantity": 0.5,
        },
    ]
    trades = [_trade(1, +2.0, closed=closed, legs=legs, exit_note="trend flipped")]
    view = build_view(_report(trades, market=_market([100.0, 110.0, 120.0])))
    trade = view["market"]["trades"][0]
    assert trade["quantity"] == 1.0
    assert trade["note"] == "trend flipped"
    assert [(leg["reason"], leg["price"], leg["quantity"]) for leg in trade["legs"]] == [
        ("TP1", 2004.0, 0.5),
        ("R_SL", 2000.0, 0.5),
    ]
    assert trade["legs"][1]["note"] == "trend flipped"


def test_a_trade_without_legs_still_gets_its_own_exit_but_an_open_one_gets_none():
    closed = START + timedelta(days=2)
    finished = _trade(1, +2.0, closed=closed)
    still_open = _trade(2, 0.0, closed=closed, closed_at=None, exit_price=None)
    view = build_view(_report([finished, still_open], market=_market([100.0, 110.0, 120.0])))
    first, second = view["market"]["trades"]
    assert first["legs"] == [
        {"t": closed.isoformat(), "price": 2002.0, "quantity": 1.0, "reason": "TP1", "note": None}
    ]
    assert second["legs"] == []


# ── The page ──────────────────────────────────────────────────────────


def test_the_page_fetches_nothing_when_it_opens(report_dict):
    html = render_html(report_dict)
    found = set(re.findall(r"https?://[^\s\"')]+", html))
    # The SVG namespace is a URI, not a request.
    assert found <= {"http://www.w3.org/2000/svg"}, found
    assert "<link" not in html
    assert "@import" not in html
    assert 'src="' not in html


def test_the_view_travels_with_the_page(report_dict):
    html = render_html(report_dict)
    payload = html.split('<script id="view" type="application/json">')[1].split("</script>")[0]
    assert json.loads(payload)["meta"]["strategy"] == "VIZ_PROBE"


def test_a_value_that_looks_like_markup_cannot_close_the_script(report_dict):
    report_dict["run"]["strategy"] = "</script><img onerror=alert(1)>"
    html = render_html(report_dict)
    body = html.split('<script id="view" type="application/json">')[1]
    assert "</script><img" not in body.split("</script>")[0]
    # And it survives as data rather than being mangled.
    payload = json.loads(body.split("</script>")[0])
    assert payload["meta"]["strategy"] == "</script><img onerror=alert(1)>"


def test_the_page_renders_for_a_run_that_never_traded(trending_frame):
    class Silent(Cadence):
        name = "SILENT"

        def on_candle_closed(self, df, context):
            return None

    report = build_report(BacktestEngine(Silent(), symbol="XAUUSD").run(trending_frame))
    html = report.to_html()
    assert "SILENT" in html
    assert "NO_TRADES" in html  # the diagnostic still has to reach the page


def test_the_stylesheet_and_script_are_inlined_from_the_package(report_dict):
    html = render_html(report_dict)
    assert ".panel {" in html, "the stylesheet did not make it into the page"
    assert "function performanceChart(" in html, "the script did not make it into the page"


def test_html_is_a_report_format_like_the_other_two(report_dict, trending_frame, tmp_path):
    result = BacktestEngine(Cadence(), symbol="XAUUSD", starting_equity=10_000).run(trending_frame)
    written = build_report(result).write(tmp_path, stem="run", formats=("json", "md", "html"))
    assert {path.suffix for path in written} == {".json", ".md", ".html"}
    assert (tmp_path / "run.html").read_text(encoding="utf-8").startswith("<!doctype html>")


def test_the_page_leaves_the_signal_payloads_out(trending_frame):
    # They are a third of the JSON and no chart reads them.
    report = build_report(BacktestEngine(Cadence(), symbol="XAUUSD").run(trending_frame))
    assert report.to_dict()["signals"], "the run should have emitted signals"
    payload = report.to_html().split('<script id="view" type="application/json">')[1]
    assert '"signals"' not in payload.split("</script>")[0]


# ── The price window at full resolution ───────────────────────────────


def _epoch_market(closes: list[float]) -> dict:
    """The same window a schema-2.0 report carries: epoch seconds, not ISO."""
    market = _market(closes)
    market["base_timeframe"] = "M15"
    market["rows"] = [
        [int(datetime.fromisoformat(row[0]).timestamp()), row[1], row[2], row[3], row[4]]
        for row in market["rows"]
    ]
    market["buy_hold"]["from"] = datetime.fromisoformat(market["buy_hold"]["from"]).isoformat()
    return market


def test_the_view_reads_epoch_rows_and_iso_rows_alike():
    """`qte-backtest chart` re-renders reports written before schema 2.0.

    Refusing those would make every archived run unopenable, so the dashboard
    accepts both and the benchmark it derives has to come out the same.
    """
    closes = [100.0, 110.0, 120.0]
    modern = build_view(_report([], market=_epoch_market(closes)))
    legacy = build_view(_report([], market=_market(closes)))

    assert modern["benchmark"]["points"] == legacy["benchmark"]["points"]
    assert modern["market"]["base_timeframe"] == "M15"
    assert legacy["market"]["base_timeframe"] is None


def test_the_benchmark_line_is_thinned_but_still_ends_where_the_series_does():
    """One line on one chart against tens of thousands of bars.

    Copying every row into the benchmark block would duplicate the whole series
    as four-key objects. Thinning is safe for a line read for its level — but
    the last point has to survive, or the line ends short of the real result.
    """
    closes = [100.0 + index for index in range(5000)]
    view = build_view(_report([], market=_epoch_market(closes)))
    points = view["benchmark"]["points"]

    assert len(points) <= BENCHMARK_POINTS + 1
    assert points[0]["close"] == closes[0]
    assert points[-1]["close"] == closes[-1]
    assert [point["close"] for point in points] == sorted(point["close"] for point in points)
