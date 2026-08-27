"""The report is a contract with whoever reads it — mostly an agent."""

from __future__ import annotations

import json

import pytest
from qte_backtest.execution import CostModel
from qte_backtest.replay import BacktestEngine
from qte_backtest.report import SCHEMA_VERSION, build_report
from qte_shared.models import SignalAction
from qte_shared.strategy_base import SignalIntent, StrategyBase


class TwoTradeStrategy(StrategyBase):
    """Enters on a fixed cadence so the report has something to describe."""

    name = "REPORT_PROBE"
    timeframe = "M15"
    warmup = 20

    def on_candle_closed(self, df, context):
        if context.open_uxid is not None or len(df) % 40 != 0:
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
def report(trending_frame):
    result = BacktestEngine(
        TwoTradeStrategy(),
        symbol="XAUUSD",
        costs=CostModel(spread=0.2, commission_per_unit=0.01),
        starting_equity=10_000,
    ).run(trending_frame)
    return build_report(result)


def test_the_json_is_valid_and_self_describing(report):
    payload = json.loads(report.to_json())
    assert payload["schema_version"] == SCHEMA_VERSION
    assert set(payload) == {
        "schema_version",
        "generated_at",
        "reading_guide",
        "run",
        "data",
        "market",
        "costs",
        "metrics",
        "diagnostics",
        "activity",
        "trades",
        "signals",
    }


def test_the_reading_guide_explains_the_conventions_an_agent_would_guess_wrong(report):
    guide = report.to_dict()["reading_guide"]
    # Each of these is a convention an agent cannot infer from the numbers.
    assert set(guide) >= {
        "r_multiple",
        "mae_r_mfe_r",
        "fill_assumptions",
        "exit_reasons",
        "single_position",
        "market_and_benchmark",
    }


def test_the_run_block_records_what_was_actually_tested(report):
    run = report.to_dict()["run"]
    assert run["strategy"] == "REPORT_PROBE"
    assert run["symbol"] == "XAUUSD"
    assert run["warmup_bars"] == 20
    assert run["strategy_meta"]["class"] == "TwoTradeStrategy"


def test_the_data_block_states_the_span_and_its_gaps(report):
    data = report.to_dict()["data"]
    assert data["bars"] == 400
    assert data["bars_after_warmup"] == 380
    assert data["first_bar"] < data["last_bar"]
    assert data["gaps"] == 0  # the fixture is a contiguous M15 series


def test_the_market_block_carries_a_drawable_window_and_a_benchmark(report):
    """The one thing the report cannot re-derive later: what price did.

    Every other block is computed from the trades, but a chart of the run — and
    any comparison against holding the instrument — needs the series itself. It
    is aggregated rather than sampled, because a candle drawn from one surviving
    bar claims a range that was never traded.
    """
    market = report.to_dict()["market"]
    assert market["columns"] == ["t", "o", "h", "l", "c"]
    assert market["rows"], "a replayed run should carry a window to draw"
    assert market["bucket_bars"] >= 1
    for _, open_, high, low, close in market["rows"]:
        assert high >= max(open_, close) and low <= min(open_, close)

    hold = market["buy_hold"]
    # Anchored at the first bar the strategy could act on, not at the first bar
    # of the file: the benchmark must not be credited with the warm-up.
    assert (
        hold["from"] == report.to_dict()["data"]["first_bar"]
        or hold["from"] > (report.to_dict()["data"]["first_bar"])
    )
    assert hold["net_pnl"] == pytest.approx(
        (hold["exit_close"] - hold["entry_close"]) * hold["quantity"] * hold["contract_size"]
    )


def test_the_run_block_records_the_size_an_entry_defaults_to(report):
    # The buy-and-hold benchmark is sized the same way; without this it would be
    # comparing one unit of the market against whatever the strategy traded.
    assert report.to_dict()["run"]["default_quantity"] == 1.0


def test_costs_are_recorded_so_a_result_can_be_re_priced(report):
    costs = report.to_dict()["costs"]
    assert costs["spread"] == 0.2
    assert costs["round_trip_cost"] == 0.2


def test_every_trade_carries_what_the_aggregates_were_derived_from(report):
    trades = report.to_dict()["trades"]
    assert trades, "the probe strategy should have traded"
    for trade in trades:
        assert set(trade) >= {
            "index",
            "signal_uxid",
            "direction",
            "opened_at",
            "closed_at",
            "bars_held",
            "entry_price",
            "exit_price",
            "initial_sl",
            "initial_risk",
            "exit_reason",
            "net_pnl",
            "r_multiple",
            "mae_r",
            "mfe_r",
            "legs",
        }
        assert trade["legs"], "a closed trade must record how it closed"


def test_partial_exits_are_visible_leg_by_leg(report):
    # A trade that took TP1 then stopped at breakeven is a different lesson
    # from one that ran to TP2, and only the legs distinguish them.
    trades = report.to_dict()["trades"]
    multi_leg = [trade for trade in trades if len(trade["legs"]) > 1]
    assert multi_leg, "tp1_percent=50 should produce at least one partial exit"
    reasons = [leg["reason"] for leg in multi_leg[0]["legs"]]
    assert reasons[0] == "TP1"


def test_the_emitted_broker_payloads_travel_with_the_report(report):
    signals = report.to_dict()["signals"]
    assert signals
    assert signals[0]["strategy"] == "REPORT_PROBE"
    # Same shape the live runner would publish — that is what makes the report
    # comparable against the audit trail.
    assert set(signals[0]) >= {"strategy", "symbol", "timeframe", "position", "signal_uxid"}


def test_signals_can_be_left_out_when_the_caller_does_not_want_them(report):
    assert report.to_dict(include_signals=False)["signals"] == []


def test_diagnostics_are_summarised_and_listed(report):
    diagnostics = report.to_dict()["diagnostics"]
    assert set(diagnostics["counts"]) == {"critical", "warning", "info"}
    assert diagnostics["counts"]["critical"] == len(
        [f for f in report.findings if f.severity == "critical"]
    )


def test_trustworthiness_tracks_the_critical_findings(report):
    assert report.is_trustworthy == (report.severity_counts()["critical"] == 0)


def test_the_markdown_leads_with_the_warning_when_something_critical_fired(report):
    report.findings[:] = [finding for finding in report.findings]
    markdown = report.to_markdown()
    assert markdown.startswith("# Backtest — REPORT_PROBE on XAUUSD M15")
    if not report.is_trustworthy:
        assert "critical finding" in markdown


def test_the_markdown_renders_without_a_single_trade(trending_frame):
    class Silent(TwoTradeStrategy):
        name = "SILENT"

        def on_candle_closed(self, df, context):
            return None

    empty = build_report(BacktestEngine(Silent(), symbol="XAUUSD").run(trending_frame))
    markdown = empty.to_markdown()
    assert "NO_TRADES" in markdown
    assert "n/a" in markdown  # the undefined ratios must not crash the renderer
    json.loads(empty.to_json())


def test_write_produces_both_files_named_by_run_and_timestamp(report, tmp_path):
    written = report.write(tmp_path)
    assert {path.suffix for path in written} == {".json", ".md"}
    for path in written:
        assert path.exists() and path.stat().st_size > 0
        assert path.stem.startswith("REPORT_PROBE_XAUUSD_M15_")


def test_two_runs_do_not_overwrite_each_other(report, tmp_path):
    first = report.write(tmp_path, stem="run-a", formats=("json",))
    second = report.write(tmp_path, stem="run-b", formats=("json",))
    assert first != second
    assert len(list(tmp_path.glob("*.json"))) == 2


def test_an_unknown_format_is_refused_rather_than_silently_skipped(report, tmp_path):
    with pytest.raises(ValueError, match="Unknown report format"):
        report.write(tmp_path, formats=("pdf",))
