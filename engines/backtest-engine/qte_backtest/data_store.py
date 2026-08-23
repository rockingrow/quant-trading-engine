"""Reading the parquet history back off disk."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
from qte_shared.config import settings
from qte_shared.logging_setup import get_logger
from qte_shared.timeframes import normalize_timeframe

log = get_logger(__name__)


class ParquetStore:
    """Loads ``<SYMBOL>_<TF>.parquet`` files written by the downloader."""

    def __init__(self, directory: Path | None = None) -> None:
        self.directory = Path(directory or settings.engine.parquet_dir)

    def path_for(self, symbol: str, timeframe: str) -> Path:
        return self.directory / f"{symbol.upper()}_{normalize_timeframe(timeframe)}.parquet"

    def exists(self, symbol: str, timeframe: str) -> bool:
        return self.path_for(symbol, timeframe).exists()

    def available(self) -> list[tuple[str, str]]:
        """Every (symbol, timeframe) pair on disk — what the API lists."""
        pairs = []
        for path in sorted(self.directory.glob("*.parquet")):
            symbol, _, timeframe = path.stem.rpartition("_")
            if symbol and timeframe:
                pairs.append((symbol, timeframe))
        return pairs

    def load(
        self,
        symbol: str,
        timeframe: str,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> pd.DataFrame:
        path = self.path_for(symbol, timeframe)
        if not path.exists():
            raise FileNotFoundError(
                f"No history at {path}. Download it first: "
                f"`uv run qte-backtest download --symbol {symbol} --timeframe {timeframe}`"
            )
        frame = pd.read_parquet(path, engine="pyarrow")
        if start is not None:
            frame = frame[frame.index >= pd.Timestamp(start).tz_convert("UTC")]
        if end is not None:
            frame = frame[frame.index <= pd.Timestamp(end).tz_convert("UTC")]
        frame.attrs["symbol"] = symbol.upper()
        frame.attrs["timeframe"] = normalize_timeframe(timeframe)
        return frame
