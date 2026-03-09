from __future__ import annotations

from datetime import date, datetime

import pandas as pd
from rich.console import Console

from quant_data.config import load_config
from quant_data.service import load_candles


class Candles:
    @staticmethod
    def load(
        exchange: str,
        symbol: str,
        timeframe: str,
        *,
        from_date: str | date | datetime | None = None,
        to_date: str | date | datetime | None = None,
    ) -> pd.DataFrame:
        config = load_config()
        return load_candles(
            config,
            exchange,
            symbol,
            timeframe,
            from_date=str(from_date) if from_date is not None else None,
            to_date=str(to_date) if to_date is not None else None,
            show_progress=True,
            console=Console(),
        )
