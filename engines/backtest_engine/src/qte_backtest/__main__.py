"""``qte-backtest`` CLI — download history, list it, replay a strategy, draw one."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import UTC, date, datetime
from pathlib import Path

from qte_shared.config import settings
from qte_shared.logging_setup import configure_logging, get_logger

from qte_backtest.data_store import ParquetStore
from qte_backtest.downloader import DownloadRequest, HistoryDownloader
from qte_backtest.runner import BacktestRequest, run_backtest
from qte_backtest.visualize import render_html

log = get_logger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="qte-backtest", description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    download = subparsers.add_parser("download", help="Fetch provider history into parquet")
    download.add_argument(
        "--symbol",
        action="append",
        default=None,
        help="Repeatable; defaults to QTE_ENGINE__SYMBOLS",
    )
    download.add_argument(
        "--timeframe",
        action="append",
        default=None,
        help="Repeatable; defaults to QTE_ENGINE__TIMEFRAMES",
    )
    download.add_argument("--start", type=_as_date, default=None, help="YYYY-MM-DD")
    download.add_argument("--end", type=_as_date, default=None, help="YYYY-MM-DD")
    download.add_argument(
        "--market", choices=["fx", "crypto"], default=None, help="Override the inferred market"
    )
    download.add_argument(
        "--replace",
        action="store_true",
        help="Overwrite the parquet instead of merging into it (discards bars outside the range)",
    )

    subparsers.add_parser("list", help="Show the history already on disk")

    run = subparsers.add_parser("run", help="Replay a strategy over stored history")
    run.add_argument("--strategy", required=True)
    run.add_argument("--symbol", required=True)
    run.add_argument("--timeframe", default="M15")
    run.add_argument("--start", type=_as_datetime, default=None)
    run.add_argument("--end", type=_as_datetime, default=None)
    run.add_argument(
        "--quantity",
        type=float,
        default=1.0,
        help="Fallback size for an entry the risk sizer cannot size (no stop)",
    )
    run.add_argument(
        "--spread", type=float, default=0.0, help="Full bid/ask distance in price units"
    )
    run.add_argument("--slippage", type=float, default=0.0)
    run.add_argument(
        "--commission",
        type=float,
        default=settings.account.commission_per_unit,
        help="Per unit, charged each side (default: QTE_ACCOUNT__COMMISSION_PER_UNIT)",
    )
    run.add_argument("--contract-size", type=float, default=settings.account.contract_size)
    run.add_argument(
        "--equity",
        type=float,
        default=settings.account.capital,
        help="Starting capital, and what entries are risk-sized against "
        "(default: QTE_ACCOUNT__CAPITAL)",
    )
    run.add_argument(
        "--risk-percent",
        type=float,
        default=None,
        help="Percent of --equity risked per entry. Defaults to this pair's "
        "risk_percent in config/strategies_mapping.toml, then QTE_ACCOUNT__RISK_PERCENT",
    )
    run.add_argument("--persist", action="store_true", help="Write the run into Postgres")
    run.add_argument(
        "--report",
        nargs="?",
        const="",
        default=None,
        metavar="DIR",
        help="Write the machine-readable report (default dir: QTE_ENGINE__REPORTS_DIR)",
    )
    run.add_argument(
        "--report-format",
        default="json,md",
        help="Comma-separated: json, md, html (default: json,md)",
    )
    run.add_argument(
        "--no-report-signals",
        action="store_true",
        help="Omit the emitted broker payloads from the JSON report",
    )
    run.add_argument(
        "--chart",
        action="store_true",
        help="Also write the interactive HTML dashboard (same as adding html to --report-format)",
    )
    run.add_argument(
        "--param",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Repeatable strategy parameter override",
    )
    chart = subparsers.add_parser(
        "chart", help="Render a report JSON into an interactive HTML dashboard"
    )
    chart.add_argument("report", type=Path, help="A *.json report written by `run --report`")
    chart.add_argument(
        "--out",
        type=Path,
        default=None,
        metavar="FILE",
        help="Where to write the page (default: the report's name with .html)",
    )
    chart.add_argument("--title", default=None, help="Override the page heading")
    return parser


async def _download(args: argparse.Namespace) -> None:
    symbols = args.symbol or settings.engine.symbols
    timeframes = args.timeframe or settings.engine.timeframes
    downloader = HistoryDownloader()
    for symbol in symbols:
        for timeframe in timeframes:
            await downloader.download(
                DownloadRequest(
                    symbol=symbol,
                    timeframe=timeframe,
                    start=args.start,
                    end=args.end,
                    market=args.market,
                ),
                replace=args.replace,
            )


async def _run(args: argparse.Namespace) -> None:
    report_dir = None
    if args.report is not None:
        report_dir = Path(args.report) if args.report else settings.engine.reports_dir

    report = await run_backtest(
        BacktestRequest(
            strategy=args.strategy,
            symbol=args.symbol,
            timeframe=args.timeframe,
            start=args.start,
            end=args.end,
            params=_parse_params(args.param),
            spread=args.spread,
            slippage=args.slippage,
            commission_per_unit=args.commission,
            contract_size=args.contract_size,
            quantity=args.quantity,
            starting_equity=args.equity,
            risk_percent=args.risk_percent,
            persist=args.persist,
            report_dir=report_dir,
            report_formats=_report_formats(args),
            report_include_signals=not args.no_report_signals,
        )
    )
    print()
    print(report.result.report())
    print()
    _print_findings(report)


def _report_formats(args: argparse.Namespace) -> tuple[str, ...]:
    formats = [fmt.strip() for fmt in args.report_format.split(",") if fmt.strip()]
    if args.chart and "html" not in formats:
        formats.append("html")
    return tuple(formats)


def _chart(args: argparse.Namespace) -> None:
    """Draw a report that already exists, without re-running anything.

    The replay is the expensive part and the JSON is the artefact, so rendering
    is a separate command rather than a flag you have to have remembered: a run
    from last month can be drawn today, and a report someone sent you can be
    drawn without its parquet history, its strategy, or this engine's config.
    """
    report = json.loads(args.report.read_text(encoding="utf-8"))
    if "trades" not in report or "metrics" not in report:
        raise SystemExit(
            f"{args.report} does not look like a qte-backtest report "
            "(no 'trades'/'metrics' block). Pass the JSON written by `run --report`."
        )
    destination = args.out or args.report.with_suffix(".html")
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(render_html(report, title=args.title), encoding="utf-8")
    size = destination.stat().st_size / 1024
    print(f"Wrote {destination} ({size:,.0f} KB) — open it in a browser")


def _print_findings(report) -> None:
    """Surface the diagnostics on the terminal, not only in the file.

    A report nobody opens is a report nobody acts on, and the critical findings
    are exactly the ones that decide whether the numbers just printed mean
    anything.
    """
    if not report.findings:
        print("Diagnostics       no findings")
        print()
        return

    counts = report.severity_counts()
    summary = ", ".join(f"{count} {name}" for name, count in counts.items() if count)
    print(f"Diagnostics       {summary}")
    for finding in report.findings:
        print(f"  [{finding.severity.upper():<8}] {finding.code}: {finding.title}")
        print(f"             → {finding.suggestion}")
    print()


def _list() -> None:
    store = ParquetStore()
    pairs = store.available()
    if not pairs:
        print(f"No parquet history in {store.directory}. Run `qte-backtest download` first.")
        return
    print(f"History in {store.directory}:")
    for symbol, timeframe in pairs:
        path = store.path_for(symbol, timeframe)
        print(f"  {symbol:<12} {timeframe:<5} {path.stat().st_size / 1_048_576:>8.2f} MB")


def _parse_params(pairs: list[str]) -> dict[str, object]:
    """Parse ``--param key=value`` with light type coercion."""
    params: dict[str, object] = {}
    for pair in pairs:
        key, _, raw = pair.partition("=")
        if not key or not _:
            raise SystemExit(f"--param expects KEY=VALUE, got {pair!r}")
        params[key.strip()] = _coerce(raw.strip())
    return params


def _coerce(raw: str) -> object:
    lowered = raw.lower()
    if lowered in ("true", "false"):
        return lowered == "true"
    if lowered in ("none", "null", ""):
        return None
    try:
        return int(raw)
    except ValueError:
        pass
    try:
        return float(raw)
    except ValueError:
        return raw


def _as_date(raw: str) -> date:
    return datetime.strptime(raw, "%Y-%m-%d").date()


def _as_datetime(raw: str) -> datetime:
    return datetime.strptime(raw, "%Y-%m-%d").replace(tzinfo=UTC)


def _widen_console() -> None:
    """Let the report's box drawing survive a legacy console code page.

    Windows terminals still default to cp1252, which has no ``─`` and no ``→``.
    The replay itself is fine; it is the final ``print`` that raises, so a run
    that took ten minutes dies at the last line with its numbers already
    computed and nowhere to go. Replacement characters are a far better outcome
    than that, and on a UTF-8 console nothing changes.
    """
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")


def main() -> None:
    _widen_console()
    configure_logging()
    args = build_parser().parse_args()
    if args.command == "download":
        asyncio.run(_download(args))
    elif args.command == "run":
        asyncio.run(_run(args))
    elif args.command == "chart":
        _chart(args)
    else:
        _list()


if __name__ == "__main__":
    main()
