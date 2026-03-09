from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import pytest

from quant_data.models import AppConfig


def candle(ts: int, price: float) -> list[float]:
    return [ts, price, price + 1, price - 1, price + 0.5, price * 10]


def stored_row(ts: int, price: float) -> dict[str, float | int]:
    return {
        "ts": ts,
        "open": price,
        "high": price + 1,
        "low": price - 0.5,
        "close": price + 0.5,
        "volume": price * 10,
    }


class FakeExchange:
    def __init__(
        self,
        candles_by_market: dict[tuple[str, str], list[list[float]]],
        *,
        markets: dict[str, dict[str, str]] | None = None,
        failures_by_since: dict[int, int] | None = None,
    ) -> None:
        self._candles_by_market = candles_by_market
        self._markets = markets or {}
        self._failures_by_since = defaultdict(int, failures_by_since or {})

    def load_markets(self) -> dict[str, dict[str, str]]:
        return self._markets

    def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        since: int | None = None,
        limit: int | None = None,
    ) -> list[list[float]]:
        current_since = since or 0
        if self._failures_by_since[current_since] > 0:
            self._failures_by_since[current_since] -= 1
            raise RuntimeError("temporary exchange failure")

        rows = self._candles_by_market.get((symbol, timeframe), [])
        filtered = [row for row in rows if row[0] >= current_since]
        return filtered[: limit or len(filtered)]

    def close(self) -> None:
        return None


@pytest.fixture
def config(tmp_path: Path) -> AppConfig:
    return AppConfig(data_path=tmp_path / "storage", gap_warning_threshold=30, exchanges={})
