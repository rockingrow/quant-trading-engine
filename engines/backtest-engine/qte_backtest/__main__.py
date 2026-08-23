"""``qte-backtest`` CLI — download history, list it, replay a strategy."""

from __future__ import annotations

import argparse
import asyncio
from datetime import UTC, date, datetime
from pathlib import Path

from qte_shared.config import settings
from qte_shared.logging_setup import configure_logging, get_logger

from qte_backtest.data_store import ParquetStore
from qte_backtest.downloader import DownloadRequest, TiingoDownloader
from qte_backtest.runner import BacktestRequest, run_backtest

log = get_logger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="qte-backtest", description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    download = subparsers.add_parser("download", help="Fetch Tiingo history into parquet")
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

    subparsers.add_parser("list", help="Show the history already on disk")

    run = subparsers.add_parser("run", help="Replay a strategy over stored history")
    run.add_argument("--strategy", required=True)
    run.add_argument("--symbol", required=True)
    run.add_argument("--timeframe", default="M15")
    run.add_argument("--start", type=_as_datetime, default=None)
    run.add_argument("--end", type=_as_datetime, default=None)
    run.add_argument("--quantity", type=float, default=1.0)
    run.add_argument(
        "--spread", type=float, default=0.0, help="Full bid/ask distance in price units"
    )
    run.add_argument("--slippage", type=float, default=0.0)
    run.add_argument("--commission", type=float, default=0.0, help="Per unit, charged each side")
    run.add_argument("--contract-size", type=float, default=1.0)
    run.add_argument("--equity", type=float, default=10_000.0)
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
        help="Comma-separated: json, md (default: both)",
    )
    run.add_argument(
        "--no-report-signals",
        action="store_true",
        help="Omit the emitted broker payloads from the JSON report",
    )
    run.add_argument(
        "--param",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Repeatable strategy parameter override",
    )
    return parser


async def _download(args: argparse.Namespace) -> None:
    symbols = args.symbol or settings.engine.symbols
    timeframes = args.timeframe or settings.engine.timeframes
    downloader = TiingoDownloader()
    for symbol in symbols:
        for timeframe in timeframes:
            await downloader.download(
                DownloadRequest(
                    symbol=symbol,
                    timeframe=timeframe,
                    start=args.start,
                    end=args.end,
                    market=args.market,
                )
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
            persist=args.persist,
            report_dir=report_dir,
            report_formats=tuple(
                fmt.strip() for fmt in args.report_format.split(",") if fmt.strip()
            ),
            report_include_signals=not args.no_report_signals,
        )
    )
    print()
    print(report.result.report())
    print()
    _print_findings(report)


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


def main() -> None:
    configure_logging()
    args = build_parser().parse_args()
    if args.command == "download":
        asyncio.run(_download(args))
    elif args.command == "run":
        asyncio.run(_run(args))
    else:
        _list()


if __name__ == "__main__":
    main()
