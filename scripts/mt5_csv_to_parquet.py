"""MT5 bar export (CSV) → the canonical QTE parquet history.

MetaTrader 5's "Save as CSV" from the Symbols/History dialog writes a
tab-separated file with bracketed headers and the date and time split across
two columns::

    <DATE>	<TIME>	<OPEN>	<HIGH>	<LOW>	<CLOSE>	<TICKVOL>	<VOL>	<SPREAD>
    2023.01.02	23:00:00	1825.968	1828.169	1825.663	1828.157	233	0	926

The rest of QTE only knows one shape — a UTC ``open_time`` index over
``open/high/low/close/volume`` — so this converts into exactly what
:class:`~qte_backtest.downloader.HistoryDownloader` writes, and drops the file
where :class:`~qte_backtest.data_store.ParquetStore` looks for it. A file
converted here and one downloaded through a market data provider are interchangeable to a replay.

Two things the CSV does not carry and the caller must get right:

* **The clock.** MT5 timestamps are *broker server time*, not UTC. Most MT5
  brokers run EET (UTC+2/+3 with DST); pass ``--tz`` with the server's zone so
  the bars land on the same UTC instants as downloaded history. The default is
  UTC — correct only if the terminal itself is UTC.
* **The symbol.** Brokers suffix their tickers (``XAUUSDm``, ``XAUUSD.pro``).
  The suffix is stripped by default so the parquet matches the symbol the
  engine trades; ``--symbol`` overrides the guess outright.

Usage::

    uv run python scripts/mt5_csv_to_parquet.py data/csv/XAUUSDm_M5_*.csv --tz EET
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pandas as pd
from qte_shared.config import settings
from qte_shared.timeframes import normalize_timeframe

OHLCV = ["open", "high", "low", "close", "volume"]

#: ``<SYMBOL>_<TF>_<from>_<to>.csv`` — how MT5 names its exports.
_EXPORT_NAME = re.compile(r"^(?P<symbol>[A-Za-z0-9.#_-]+?)_(?P<timeframe>[A-Za-z]\d+)_\d+_\d+$")

#: A broker suffix is whatever trails the six-character FX/metal pair.
_BROKER_SUFFIX = re.compile(r"^(?P<base>[A-Z]{6})[a-z.\-_].*$")


def parse_export_name(path: Path) -> tuple[str | None, str | None]:
    """Pull the symbol and timeframe out of an MT5 export filename."""
    match = _EXPORT_NAME.match(path.stem)
    if not match:
        return None, None
    try:
        timeframe = normalize_timeframe(match.group("timeframe"))
    except ValueError:
        timeframe = None
    return match.group("symbol"), timeframe


def strip_broker_suffix(symbol: str) -> str:
    """``XAUUSDm`` → ``XAUUSD``. Leaves anything unrecognised untouched."""
    match = _BROKER_SUFFIX.match(symbol.upper() if symbol.isupper() else symbol)
    return match.group("base") if match else symbol.upper()


def read_mt5_csv(path: Path, tz: str) -> pd.DataFrame:
    """Read one MT5 export into the canonical OHLCV frame."""
    frame = pd.read_csv(path, sep=None, engine="python")
    frame.columns = [str(c).strip().strip("<>").lower() for c in frame.columns]

    missing = {"date", "open", "high", "low", "close"} - set(frame.columns)
    if missing:
        raise ValueError(f"{path.name} is missing column(s) {sorted(missing)} — not an MT5 export?")

    stamps = frame["date"].astype(str)
    if "time" in frame.columns:
        stamps = stamps + " " + frame["time"].astype(str)
    # D1 exports carry no <TIME>; both forms parse under the same format string
    # once the missing half is absent, so let pandas infer rather than branch.
    naive = pd.to_datetime(stamps, format="mixed")

    try:
        zone = ZoneInfo(tz)
    except ZoneInfoNotFoundError as exc:  # pragma: no cover - depends on host tzdata
        raise ValueError(f"Unknown timezone {tz!r}") from exc
    # A DST fall-back repeats an hour of local time. `ambiguous="infer"` reads
    # the offset off the surrounding bars, which works because the series is
    # dense and monotonic; `nonexistent` cannot occur in exchange data but is
    # shifted rather than raising so a broker's odd bar does not abort the run.
    localized = naive.dt.tz_localize(zone, ambiguous="infer", nonexistent="shift_forward")
    frame["open_time"] = localized.dt.tz_convert("UTC")

    # <VOL> is real volume and is 0 for most FX/CFD feeds; <TICKVOL> counts
    # ticks and is the only volume MT5 actually has there. Prefer the real one
    # when the broker reports it, so a volume-aware strategy is not fed ticks.
    real = pd.to_numeric(frame.get("vol", 0), errors="coerce").fillna(0.0)
    ticks = pd.to_numeric(frame.get("tickvol", 0), errors="coerce").fillna(0.0)
    frame["volume"] = real if float(real.sum()) > 0 else ticks

    frame = frame.set_index("open_time")[OHLCV].astype(float).sort_index()
    return frame[~frame.index.duplicated(keep="last")]


def convert(
    path: Path,
    *,
    out_dir: Path,
    tz: str,
    symbol: str | None = None,
    timeframe: str | None = None,
    keep_suffix: bool = False,
    overwrite: bool = False,
) -> Path:
    name_symbol, name_timeframe = parse_export_name(path)
    symbol = symbol or name_symbol
    timeframe = timeframe or name_timeframe
    if not symbol or not timeframe:
        raise ValueError(
            f"Cannot tell the symbol/timeframe from {path.name}; "
            f"pass --symbol and --timeframe explicitly"
        )
    symbol = symbol.upper() if keep_suffix else strip_broker_suffix(symbol)
    timeframe = normalize_timeframe(timeframe)

    target = out_dir / f"{symbol}_{timeframe}.parquet"
    if target.exists() and not overwrite:
        raise FileExistsError(f"{target} already exists — pass --overwrite to replace it")

    frame = read_mt5_csv(path, tz)
    if frame.empty:
        raise ValueError(f"{path.name} holds no bars")
    frame.attrs["symbol"] = symbol
    frame.attrs["timeframe"] = timeframe

    out_dir.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(target, engine="pyarrow", compression="snappy")
    print(
        f"{path.name} -> {target}  rows={len(frame)}  "
        f"{frame.index[0].isoformat()} .. {frame.index[-1].isoformat()}"
    )
    return target


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Convert MetaTrader 5 CSV exports into QTE parquet history.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("csv", nargs="+", type=Path, help="MT5 CSV export(s)")
    parser.add_argument(
        "--tz",
        default="UTC",
        help="Timezone of the timestamps in the CSV, i.e. the MT5 server's "
        "clock (EET for most brokers). Default: UTC",
    )
    parser.add_argument("--symbol", help="Override the symbol read from the filename")
    parser.add_argument("--timeframe", help="Override the timeframe read from the filename")
    parser.add_argument(
        "--keep-suffix",
        action="store_true",
        help="Keep the broker's ticker suffix (XAUUSDm stays XAUUSDm)",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=settings.engine.parquet_dir,
        help=f"Where to write. Default: {settings.engine.parquet_dir}",
    )
    parser.add_argument("--overwrite", action="store_true", help="Replace an existing parquet")
    args = parser.parse_args(argv)

    failures = 0
    for path in args.csv:
        try:
            convert(
                path,
                out_dir=args.out_dir,
                tz=args.tz,
                symbol=args.symbol,
                timeframe=args.timeframe,
                keep_suffix=args.keep_suffix,
                overwrite=args.overwrite,
            )
        except (OSError, ValueError) as exc:
            print(f"error: {exc}", file=sys.stderr)
            failures += 1
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
