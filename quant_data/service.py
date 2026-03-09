from __future__ import annotations

from contextlib import suppress
from typing import Iterable

import pandas as pd
from rich.console import Console
from rich.progress import BarColumn, Progress, TaskProgressColumn, TextColumn, TimeRemainingColumn

from quant_data import downloader, storage, utils
from quant_data.models import (
    AppConfig,
    CapabilityInfo,
    DatasetInfo,
    DatasetKey,
    DownloadStats,
    ExchangeProtocol,
    GapWarning,
    MarketInfo,
    TimeRange,
)

SUPPORTED_DATA_FLOWS = ["candles"]
SUPPORTED_MARKET_TYPES = ["spot"]
UNSUPPORTED_DATA_FLOWS = [
    "trades",
    "orderbook",
    "funding",
    "open-interest",
    "liquidations",
    "websocket-live",
]
UNSUPPORTED_MARKET_TYPES = ["futures", "swap", "options"]
CLI_COMMANDS = [
    "list",
    "markets",
    "download",
    "update",
    "check",
    "remove",
    "capabilities",
    "config show",
]
CLI_OUTPUT_FORMATS = ["human", "json"]
LIBRARY_API = ["quant_data.Candles.load"]


def list_datasets(
    config: AppConfig,
    *,
    exchange: str | None = None,
    symbol: str | None = None,
) -> list[DatasetInfo]:
    items: list[DatasetInfo] = []
    for key in _filtered_keys(storage.list_dataset_keys(config), exchange=exchange, symbol=symbol):
        dataset = storage.get_dataset_info(config, key)
        if dataset is not None:
            items.append(dataset)
    return sorted(items, key=lambda item: (item.exchange, item.symbol, item.timeframe))


def list_markets(
    config: AppConfig,
    exchange: str,
    *,
    quote: str | None = None,
    base: str | None = None,
) -> list[MarketInfo]:
    with _exchange_client(config, exchange) as client:
        markets = client.load_markets()

    items: list[MarketInfo] = []
    for market in markets.values():
        symbol_name = market.get("symbol")
        market_type = market.get("type", "")
        market_base = market.get("base")
        market_quote = market.get("quote")
        if not symbol_name or not market_base or not market_quote:
            continue
        if quote and market_quote != quote:
            continue
        if base and market_base != base:
            continue
        items.append(
            MarketInfo(
                symbol=str(symbol_name),
                base=str(market_base),
                quote=str(market_quote),
                type=str(market_type),
            )
        )
    return sorted(items, key=lambda item: item.symbol)


def describe_capabilities(config: AppConfig) -> CapabilityInfo:
    return CapabilityInfo(
        supported_data_flows=SUPPORTED_DATA_FLOWS.copy(),
        supported_market_types=SUPPORTED_MARKET_TYPES.copy(),
        configured_exchanges={
            exchange_id: exchange_config.type
            for exchange_id, exchange_config in sorted(config.exchanges.items())
        },
        exchange_support_model=(
            "Attempt any CCXT exchange id in spot mode. A dataset is supported only when the "
            "exchange provides spot OHLCV through fetch_ohlcv."
        ),
        unsupported_data_flows=UNSUPPORTED_DATA_FLOWS.copy(),
        unsupported_market_types=UNSUPPORTED_MARKET_TYPES.copy(),
        cli_output_formats=CLI_OUTPUT_FORMATS.copy(),
        commands=CLI_COMMANDS.copy(),
        library_api=LIBRARY_API.copy(),
    )


def plan_missing_ranges(
    config: AppConfig,
    key: DatasetKey,
    *,
    start_ms: int,
    end_ms: int,
) -> list[TimeRange]:
    step_ms = utils.timeframe_to_milliseconds(key.timeframe)
    timestamps = storage.dataset_timestamps(config, key, start_ms=start_ms, end_ms=end_ms)
    ranges: list[TimeRange] = []
    cursor = start_ms
    for ts in timestamps:
        if ts < cursor:
            continue
        if ts > cursor:
            ranges.append(TimeRange(cursor, ts))
        cursor = ts + step_ms
    if cursor < end_ms:
        ranges.append(TimeRange(cursor, end_ms))
    return ranges


def download_dataset(
    config: AppConfig,
    exchange: str,
    symbol: str,
    timeframe: str,
    *,
    from_date: str | None = None,
    to_date: str | None = None,
    show_progress: bool = False,
    console: Console | None = None,
) -> DownloadStats:
    key = DatasetKey(exchange=exchange, symbol=symbol, timeframe=timeframe)
    start_ms = _resolve_download_start(config, key, from_date)
    end_ms = utils.parse_to_bound_exclusive(to_date) or utils.default_to_exclusive_utc()
    if start_ms >= end_ms:
        raise ValueError("The requested range is empty.")

    missing_ranges = plan_missing_ranges(config, key, start_ms=start_ms, end_ms=end_ms)
    total_missing_candles = sum(
        (time_range.end_ms - time_range.start_ms) // utils.timeframe_to_milliseconds(timeframe)
        for time_range in missing_ranges
    )

    progress: Progress | None = None
    task_id: int | None = None
    if show_progress and total_missing_candles > 0:
        progress = Progress(
            TextColumn("{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeRemainingColumn(),
            console=console or Console(),
        )
        progress.start()
        task_id = progress.add_task(f"{exchange} {symbol} {timeframe}", total=total_missing_candles)

    added_rows = 0
    warnings_count = 0

    try:
        if not missing_ranges:
            return DownloadStats(dataset=key, added_rows=0, warnings=0)

        with _exchange_client(config, exchange) as client:
            for missing_range in missing_ranges:
                previous_row = storage.read_previous_row(config, key, missing_range.start_ms)
                rows, warnings = downloader.fetch_range_rows(
                    client,
                    symbol,
                    timeframe,
                    missing_range.start_ms,
                    missing_range.end_ms,
                    gap_warning_threshold_minutes=config.gap_warning_threshold,
                    previous_row=previous_row,
                    progress_callback=(
                        (lambda amount: progress.advance(task_id, advance=amount))
                        if progress is not None and task_id is not None
                        else None
                    ),
                )
                added_rows += storage.write_rows(config, key, rows)
                warnings_count += len(warnings)
                storage.append_notes(config, key, warnings)
    finally:
        if progress is not None:
            progress.stop()

    return DownloadStats(dataset=key, added_rows=added_rows, warnings=warnings_count)


def update_datasets(
    config: AppConfig,
    *,
    exchange: str | None = None,
    symbol: str | None = None,
    show_progress: bool = False,
    console: Console | None = None,
) -> list[DownloadStats]:
    results: list[DownloadStats] = []
    for key in _filtered_keys(storage.list_dataset_keys(config), exchange=exchange, symbol=symbol):
        info = storage.get_dataset_info(config, key)
        if info is None:
            continue
        from_ms = info.to_ts + utils.timeframe_to_milliseconds(key.timeframe)
        to_ms = utils.default_to_exclusive_utc()
        if from_ms >= to_ms:
            results.append(DownloadStats(dataset=key, added_rows=0, warnings=0))
            continue
        from_date = utils.utc_date_string(from_ms)
        to_date = utils.utc_date_string(to_ms - 1)
        results.append(
            download_dataset(
                config,
                key.exchange,
                key.symbol,
                key.timeframe,
                from_date=from_date,
                to_date=to_date,
                show_progress=show_progress,
                console=console,
            )
        )
    return results


def check_datasets(
    config: AppConfig,
    *,
    exchange: str | None = None,
    symbol: str | None = None,
    timeframe: str | None = None,
) -> list[tuple[DatasetKey, list[GapWarning]]]:
    items: list[tuple[DatasetKey, list[GapWarning]]] = []
    for key in _filtered_keys(
        storage.list_dataset_keys(config),
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
    ):
        items.append((key, storage.read_notes(config, key)))
    return items


def remove_datasets(
    config: AppConfig,
    *,
    exchange: str,
    symbol: str,
    timeframe: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
) -> int:
    if timeframe is None and (from_date is not None or to_date is not None):
        raise ValueError("Partial remove requires --timeframe.")

    start_ms = utils.parse_from_bound(from_date)
    end_ms = utils.parse_to_bound_exclusive(to_date)
    keys = _filtered_keys(
        storage.list_dataset_keys(config),
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
    )
    if not keys:
        return 0

    removed_rows = 0
    for key in keys:
        removed_rows += storage.remove_range(config, key, start_ms=start_ms, end_ms=end_ms)
    return removed_rows


def load_candles(
    config: AppConfig,
    exchange: str,
    symbol: str,
    timeframe: str,
    *,
    from_date: str | None = None,
    to_date: str | None = None,
    show_progress: bool = True,
    console: Console | None = None,
) -> pd.DataFrame:
    download_dataset(
        config,
        exchange,
        symbol,
        timeframe,
        from_date=from_date,
        to_date=to_date,
        show_progress=show_progress,
        console=console,
    )
    start_ms = _resolve_download_start(config, DatasetKey(exchange, symbol, timeframe), from_date)
    end_ms = utils.parse_to_bound_exclusive(to_date) or utils.default_to_exclusive_utc()
    frame = storage.read_frame(
        config,
        DatasetKey(exchange=exchange, symbol=symbol, timeframe=timeframe),
        start_ms=start_ms,
        end_ms=end_ms,
    )
    if frame.empty:
        empty = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
        empty.index = pd.DatetimeIndex([], tz="UTC", name="ts")
        return empty

    frame = frame.copy()
    frame["ts"] = pd.to_datetime(frame["ts"], unit="ms", utc=True)
    frame.set_index("ts", inplace=True)
    frame.index.name = "ts"
    return frame.loc[:, ["open", "high", "low", "close", "volume"]].copy()


def _resolve_download_start(config: AppConfig, key: DatasetKey, from_date: str | None) -> int:
    explicit_start = utils.parse_from_bound(from_date)
    if explicit_start is not None:
        return explicit_start

    info = storage.get_dataset_info(config, key)
    if info is not None:
        return info.from_ts
    return 0


def _filtered_keys(
    keys: Iterable[DatasetKey],
    *,
    exchange: str | None = None,
    symbol: str | None = None,
    timeframe: str | None = None,
) -> list[DatasetKey]:
    return sorted(
        [
            key
            for key in keys
            if (exchange is None or key.exchange == exchange)
            and (symbol is None or key.symbol == symbol)
            and (timeframe is None or key.timeframe == timeframe)
        ],
        key=lambda item: (item.exchange, item.symbol, item.timeframe),
    )


class _ExchangeClient:
    def __init__(self, config: AppConfig, exchange_id: str) -> None:
        self._client = downloader.instantiate_exchange(
            exchange_id,
            config.exchange_type(exchange_id),
        )

    def __enter__(self) -> ExchangeProtocol:
        return self._client

    def __exit__(self, *_: object) -> None:
        with suppress(Exception):
            self._client.close()


def _exchange_client(config: AppConfig, exchange_id: str) -> _ExchangeClient:
    return _ExchangeClient(config, exchange_id)
