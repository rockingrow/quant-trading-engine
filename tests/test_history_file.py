"""A replay reads the file it was told to read, and nothing else.

History now lands under ``data/parquet/<source>/`` — one directory per provider,
plus ``mt5/`` for a broker CSV import. Two files for the same pair disagree
about session times, weekend gaps and volume, so the pair alone can no longer
name a file: picking one by convention is how a run measures the wrong book
while still printing a plausible equity curve.
"""

from __future__ import annotations

import importlib.util
from datetime import UTC, datetime

import pandas as pd
import pytest

from qte_backtest.data_store import load_history
from qte_shared.config import REPO_ROOT, settings


def write_history(path, closes: list[float]) -> None:
    index = pd.date_range(datetime(2024, 1, 1, tzinfo=UTC), periods=len(closes), freq="15min")
    frame = pd.DataFrame(
        {
            "open": closes,
            "high": [close + 1 for close in closes],
            "low": [close - 1 for close in closes],
            "close": closes,
            "volume": [100.0] * len(closes),
        },
        index=index,
    )
    frame.index.name = "open_time"
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, engine="pyarrow", compression="snappy")


def test_two_sources_for_one_pair_stay_distinct(tmp_path):
    """The path decides, not the symbol: same pair, different bars."""
    vendor_file = tmp_path / "tiingo" / "XAUUSD_M15.parquet"
    broker_file = tmp_path / "mt5" / "XAUUSD_M15.parquet"
    write_history(vendor_file, [2400.0, 2401.0])
    write_history(broker_file, [2500.0, 2501.0])

    vendor = load_history(vendor_file, "XAUUSD", "M15")
    broker = load_history(broker_file, "XAUUSD", "M15")

    assert vendor["close"].tolist() == [2400.0, 2401.0]
    assert broker["close"].tolist() == [2500.0, 2501.0]
    assert vendor.attrs["source_file"] == str(vendor_file)


def test_a_missing_file_names_itself(tmp_path):
    """The error has to say which path was wrong, not just that one was."""
    missing = tmp_path / "tiingo" / "XAUUSD_M15.parquet"
    with pytest.raises(FileNotFoundError, match="XAUUSD_M15.parquet"):
        load_history(missing, "XAUUSD", "M15")


def test_the_window_is_trimmed_to_the_requested_range(tmp_path):
    path = tmp_path / "tiingo" / "XAUUSD_M15.parquet"
    write_history(path, [2400.0, 2401.0, 2402.0, 2403.0])

    frame = load_history(
        path,
        "XAUUSD",
        "M15",
        start=datetime(2024, 1, 1, 0, 15, tzinfo=UTC),
        end=datetime(2024, 1, 1, 0, 30, tzinfo=UTC),
    )

    assert frame["close"].tolist() == [2401.0, 2402.0]


def test_the_backtest_cli_demands_a_history_file():
    """`run` without --file must fail at parse time, not fall back to a guess."""
    from qte_backtest.__main__ import build_parser

    with pytest.raises(SystemExit):
        build_parser().parse_args(
            ["run", "--strategy", "EXAMPLE", "--symbol", "XAUUSD", "--timeframe", "M15"]
        )

    args = build_parser().parse_args(
        [
            "run",
            "--strategy",
            "EXAMPLE",
            "--symbol",
            "XAUUSD",
            "--timeframe",
            "M15",
            "--file",
            "data/parquet/tiingo/XAUUSD_M15.parquet",
        ]
    )
    assert args.history_file.name == "XAUUSD_M15.parquet"


def test_the_csv_importer_writes_its_own_source_directory():
    """An MT5 import is a source like any vendor, and says so in its path."""
    module_spec = importlib.util.spec_from_file_location(
        "mt5_csv_to_parquet", REPO_ROOT / "scripts" / "mt5_csv_to_parquet.py"
    )
    converter = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(converter)

    assert converter.MT5_PARQUET_DIR == settings.engine.parquet_dir / "mt5"
