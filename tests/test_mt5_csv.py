"""MT5 volume columns may be absent without making OHLC history unreadable."""

import importlib.util

import pytest

from qte_shared.config import REPO_ROOT


@pytest.mark.parametrize(
    ("headers", "volumes", "expected"),
    [("", "", 0.0), ("\t<TICKVOL>", "\t12", 12.0), ("\t<VOL>", "\t5", 5.0)],
)
def test_missing_optional_volume_columns(tmp_path, headers, volumes, expected):
    module_spec = importlib.util.spec_from_file_location(
        "mt5_csv_to_parquet", REPO_ROOT / "scripts" / "mt5_csv_to_parquet.py"
    )
    converter = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(converter)
    source_file = tmp_path / "export.csv"
    source_file.write_text(
        f"<DATE>\t<TIME>\t<OPEN>\t<HIGH>\t<LOW>\t<CLOSE>{headers}\n"
        f"2026.01.01\t00:00:00\t2400\t2402\t2399\t2401{volumes}\n",
        encoding="utf-8",
    )
    candles = converter.read_mt5_csv(source_file, "UTC")
    assert candles["volume"].tolist() == [expected]
    assert candles["close"].tolist() == [2401.0]
