"""Reading bars off disk, so a rehearsal can run on data that really printed.

Generated bars are enough to warm an indicator window; they are not enough to
reproduce the Tuesday your strategy did something you did not expect. For that
you replay the file — the same parquet the backtest engine reads, an MT5 CSV
export, or a hand-written JSONL scenario:

    data/parquet/XAUUSD_M15.parquet     what `make download` wrote
    data/csv/gold.csv                   open,high,low,close,volume (+ any index)
    scenario.jsonl                      one JSON object per line

Only OHLCV is taken. Timestamps in the file are deliberately **ignored**: the
simulator re-anchors the run onto live buckets, because a bar whose bucket
closed months ago is one ingestion's wall-clock flush would tear in half — see
:func:`~qte_simulator.bars.anchor_open_times`. What is preserved is the shape
and the order, which is what the strategy reads.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

#: What every reader normalises to. The order is the bar's, not the file's.
BAR_COLUMNS = ("open", "high", "low", "close", "volume")


class SourceError(ValueError):
    """The file could not be read as a series of bars."""


def load_bars(path: str | Path, *, limit: int | None = None) -> list[dict[str, Any]]:
    """Read *path* into OHLCV rows, oldest first, optionally the last *limit*."""
    file = Path(path)
    if not file.is_file():
        raise SourceError(f"No such file: {file}")

    suffix = file.suffix.lower()
    try:
        if suffix == ".parquet":
            frame = pd.read_parquet(file)
        elif suffix in (".jsonl", ".ndjson"):
            frame = pd.read_json(file, lines=True)
        elif suffix == ".json":
            frame = pd.read_json(file)
        else:
            frame = pd.read_csv(file)
    except ImportError as exc:  # pragma: no cover - depends on the environment
        raise SourceError(
            f"Reading {file.name} needs a library this environment does not have ({exc}). "
            "Parquet needs pyarrow, which only the backtest engine installs — run the "
            "replay from the workspace venv, or export the file to CSV first."
        ) from exc
    except Exception as exc:
        raise SourceError(f"Could not read {file}: {exc}") from exc

    frame.columns = [str(column).strip().lower() for column in frame.columns]
    missing = [column for column in BAR_COLUMNS[:4] if column not in frame.columns]
    if missing:
        raise SourceError(
            f"{file.name} is missing {', '.join(missing)}. Columns found: "
            f"{', '.join(map(str, frame.columns))}"
        )
    if "volume" not in frame.columns:
        # FX history has no volume at all; a zero column keeps the shape uniform.
        frame["volume"] = 0.0

    frame = frame[list(BAR_COLUMNS)].astype(float).dropna()
    if frame.empty:
        raise SourceError(f"{file.name} contains no usable bars")
    if limit:
        frame = frame.tail(limit)
    return frame.to_dict(orient="records")


__all__ = ["BAR_COLUMNS", "SourceError", "load_bars"]
