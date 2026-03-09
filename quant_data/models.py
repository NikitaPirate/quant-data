from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol

CANDLE_COLUMNS = ["ts", "open", "high", "low", "close", "volume"]


@dataclass(frozen=True)
class ExchangeConfig:
    type: str = "spot"


@dataclass(frozen=True)
class AppConfig:
    data_path: Path
    gap_warning_threshold: int
    exchanges: dict[str, ExchangeConfig]

    def exchange_type(self, exchange_id: str) -> str:
        exchange_config = self.exchanges.get(exchange_id)
        return exchange_config.type if exchange_config else "spot"


@dataclass(frozen=True)
class DatasetKey:
    exchange: str
    symbol: str
    timeframe: str


@dataclass(frozen=True)
class DatasetInfo:
    exchange: str
    symbol: str
    timeframe: str
    from_ts: int
    to_ts: int
    rows: int
    warnings: int


@dataclass(frozen=True)
class GapWarning:
    start_ts: int
    end_ts: int
    candles: int


@dataclass(frozen=True)
class TimeRange:
    start_ms: int
    end_ms: int

    @property
    def candles(self) -> int:
        return max(0, self.end_ms - self.start_ms)


@dataclass(frozen=True)
class DownloadStats:
    dataset: DatasetKey
    added_rows: int
    warnings: int


@dataclass(frozen=True)
class MarketInfo:
    symbol: str
    base: str
    quote: str
    type: str


ConfigSource = Literal["explicit", "env", "local", "global", "defaults"]
DataPathMode = Literal["global", "local", "absolute"]


@dataclass(frozen=True)
class LoadedConfig:
    config: AppConfig
    config_path: Path
    config_source: ConfigSource
    config_exists: bool
    data_path_mode: DataPathMode


@dataclass(frozen=True)
class CapabilityInfo:
    supported_data_flows: list[str]
    supported_market_types: list[str]
    configured_exchanges: dict[str, str]
    exchange_support_model: str
    unsupported_data_flows: list[str]
    unsupported_market_types: list[str]
    cli_output_formats: list[str]
    commands: list[str]
    library_api: list[str]


class ExchangeProtocol(Protocol):
    def load_markets(self) -> dict[str, dict[str, Any]]: ...

    def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        since: int | None = None,
        limit: int | None = None,
    ) -> list[list[float]]: ...

    def close(self) -> None: ...
