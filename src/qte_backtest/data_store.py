"""Reading a parquet history file back off disk.

The caller names the file. Nothing here derives a path from a symbol and a
timeframe: history now lands under ``data/parquet/<source>/`` — ``tiingo/`` for
what the vendor served, ``mt5/`` for a broker CSV import — and two files for
the same pair routinely disagree about session times, weekend gaps and volume.
Choosing between them by convention is how a run silently measures the wrong
book, so the run has to say which file it replayed.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd

from qte_shared.logging_setup import get_logger
from qte_shared.timeframes import normalize_timeframe

log = get_logger(__name__)


def load_history(
    path: Path,
    symbol: str,
    timeframe: str,
    start: datetime | None = None,
    end: datetime | None = None,
) -> pd.DataFrame:
    """Load *path* and trim it to ``[start, end]``.

    *symbol* and *timeframe* are stated by the caller rather than parsed out of
    the filename: they decide the pip size, the market calendar and the bar
    bucket, and a mistake there changes the result instead of failing.
    """
    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(
            f"No history at {path}. Download it first: "
            f"`uv run qte-backtest download --symbol {symbol} --timeframe {timeframe} "
            "--market fx`"
        )
    frame = pd.read_parquet(path, engine="pyarrow")
    if start is not None:
        frame = frame[frame.index >= pd.Timestamp(start).tz_convert("UTC")]
    if end is not None:
        frame = frame[frame.index <= pd.Timestamp(end).tz_convert("UTC")]
    frame.attrs["symbol"] = symbol.upper()
    frame.attrs["timeframe"] = normalize_timeframe(timeframe)
    frame.attrs["source_file"] = str(path)
    return frame
