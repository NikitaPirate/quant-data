from __future__ import annotations

import typer
from rich.console import Console
from rich.table import Table

from quant_data import service
from quant_data.config import ConfigError, load_config, load_config_details
from quant_data.models import (
    CapabilityInfo,
    DatasetInfo,
    DatasetKey,
    DownloadStats,
    GapWarning,
    LoadedConfig,
    MarketInfo,
)
from quant_data.utils import utc_date_string, utc_datetime_string

app = typer.Typer(no_args_is_help=True, add_completion=False)
config_app = typer.Typer(no_args_is_help=True, add_completion=False)
app.add_typer(config_app, name="config")


def main() -> None:
    app()


def _load_app_config(console: Console):
    try:
        return load_config()
    except ConfigError as error:
        console.print(str(error))
        raise typer.Exit(code=1) from error


def _load_app_config_details(console: Console) -> LoadedConfig:
    try:
        return load_config_details()
    except ConfigError as error:
        console.print(str(error))
        raise typer.Exit(code=1) from error


def _print_json(console: Console, payload: object) -> None:
    console.print_json(data=payload)


def _dataset_key_payload(key: DatasetKey) -> dict[str, str]:
    return {
        "exchange": key.exchange,
        "symbol": key.symbol,
        "timeframe": key.timeframe,
    }


def _dataset_payload(item: DatasetInfo) -> dict[str, str | int]:
    return {
        "exchange": item.exchange,
        "symbol": item.symbol,
        "timeframe": item.timeframe,
        "from": utc_date_string(item.from_ts),
        "to": utc_date_string(item.to_ts),
        "from_ts": item.from_ts,
        "to_ts": item.to_ts,
        "rows": item.rows,
        "warnings": item.warnings,
    }


def _market_payload(item: MarketInfo) -> dict[str, str]:
    return {
        "symbol": item.symbol,
        "base": item.base,
        "quote": item.quote,
        "type": item.type,
    }


def _download_payload(stats: DownloadStats) -> dict[str, str | int]:
    return {
        **_dataset_key_payload(stats.dataset),
        "added_rows": stats.added_rows,
        "warnings": stats.warnings,
    }


def _warning_payload(warning: GapWarning) -> dict[str, str | int]:
    return {
        "start": utc_datetime_string(warning.start_ts),
        "end": utc_datetime_string(warning.end_ts),
        "start_ts": warning.start_ts,
        "end_ts": warning.end_ts,
        "candles": warning.candles,
    }


def _check_payload(key: DatasetKey, warnings: list[GapWarning]) -> dict[str, object]:
    return {
        **_dataset_key_payload(key),
        "status": "ok" if not warnings else "warnings",
        "warnings": [_warning_payload(warning) for warning in warnings],
    }


def _config_payload(details: LoadedConfig) -> dict[str, object]:
    return {
        "config_source": details.config_source,
        "config_path": str(details.config_path),
        "config_exists": details.config_exists,
        "data_path_mode": details.data_path_mode,
        "data_path": str(details.config.data_path),
        "gap_warning_threshold": details.config.gap_warning_threshold,
        "exchanges": {
            exchange_id: {"type": exchange_config.type}
            for exchange_id, exchange_config in sorted(details.config.exchanges.items())
        },
    }


def _capabilities_payload(capabilities: CapabilityInfo) -> dict[str, object]:
    return {
        "supported_data_flows": capabilities.supported_data_flows,
        "supported_market_types": capabilities.supported_market_types,
        "configured_exchanges": capabilities.configured_exchanges,
        "exchange_support_model": capabilities.exchange_support_model,
        "unsupported_data_flows": capabilities.unsupported_data_flows,
        "unsupported_market_types": capabilities.unsupported_market_types,
        "cli_output_formats": capabilities.cli_output_formats,
        "commands": capabilities.commands,
        "library_api": capabilities.library_api,
    }


def _print_config_summary(console: Console, details: LoadedConfig) -> None:
    table = Table("field", "value")
    table.add_row("config_source", details.config_source)
    table.add_row("config_path", str(details.config_path))
    table.add_row("config_exists", "yes" if details.config_exists else "no")
    table.add_row("data_path_mode", details.data_path_mode)
    table.add_row("data_path", str(details.config.data_path))
    table.add_row("gap_warning_threshold", str(details.config.gap_warning_threshold))
    console.print(table)

    if not details.config.exchanges:
        console.print("Configured exchanges: none")
        return

    exchange_table = Table("exchange", "type")
    for exchange_id, exchange_config in sorted(details.config.exchanges.items()):
        exchange_table.add_row(exchange_id, exchange_config.type)
    console.print(exchange_table)


def _print_capabilities_summary(console: Console, capabilities: CapabilityInfo) -> None:
    table = Table("field", "value")
    table.add_row("supported_data_flows", ", ".join(capabilities.supported_data_flows))
    table.add_row("supported_market_types", ", ".join(capabilities.supported_market_types))
    table.add_row("exchange_support_model", capabilities.exchange_support_model)
    table.add_row("unsupported_data_flows", ", ".join(capabilities.unsupported_data_flows))
    table.add_row("unsupported_market_types", ", ".join(capabilities.unsupported_market_types))
    table.add_row("cli_output_formats", ", ".join(capabilities.cli_output_formats))
    table.add_row("commands", ", ".join(capabilities.commands))
    table.add_row("library_api", ", ".join(capabilities.library_api))
    console.print(table)

    if not capabilities.configured_exchanges:
        console.print("Configured exchanges: none")
        return

    exchange_table = Table("exchange", "type")
    for exchange_id, exchange_type in capabilities.configured_exchanges.items():
        exchange_table.add_row(exchange_id, exchange_type)
    console.print(exchange_table)


@app.command("list")
def list_command(
    exchange: str | None = typer.Option(None, "--exchange"),
    symbol: str | None = typer.Option(None, "--symbol"),
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    console = Console()
    items = service.list_datasets(_load_app_config(console), exchange=exchange, symbol=symbol)
    if json_output:
        _print_json(console, {"datasets": [_dataset_payload(item) for item in items]})
        return
    if not items:
        console.print("No local datasets found.")
        return

    table = Table("exchange", "symbol", "timeframe", "from", "to", "rows", "warnings")
    for item in items:
        table.add_row(
            item.exchange,
            item.symbol,
            item.timeframe,
            utc_date_string(item.from_ts),
            utc_date_string(item.to_ts),
            str(item.rows),
            str(item.warnings),
        )
    console.print(table)


@app.command("markets")
def markets_command(
    exchange: str = typer.Option(..., "--exchange"),
    quote: str | None = typer.Option(None, "--quote"),
    base: str | None = typer.Option(None, "--base"),
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    console = Console()
    items = service.list_markets(_load_app_config(console), exchange, quote=quote, base=base)
    if json_output:
        _print_json(console, {"markets": [_market_payload(item) for item in items]})
        return
    if not items:
        console.print("No markets found.")
        return

    table = Table("symbol", "base", "quote", "type")
    for item in items:
        table.add_row(item.symbol, item.base, item.quote, item.type)
    console.print(table)


@app.command("download")
def download_command(
    exchange: str = typer.Option(..., "--exchange"),
    symbol: str = typer.Option(..., "--symbol"),
    timeframe: str = typer.Option(..., "--timeframe"),
    from_date: str | None = typer.Option(None, "--from"),
    to_date: str | None = typer.Option(None, "--to"),
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    console = Console()
    try:
        stats = service.download_dataset(
            _load_app_config(console),
            exchange,
            symbol,
            timeframe,
            from_date=from_date,
            to_date=to_date,
            show_progress=not json_output,
            console=console,
        )
    except ValueError as error:
        console.print(str(error))
        raise typer.Exit(code=1) from error

    if json_output:
        _print_json(console, _download_payload(stats))
        return

    console.print(
        f"Downloaded {stats.added_rows} candles for {exchange} {symbol} {timeframe}. "
        f"Warnings: {stats.warnings}."
    )


@app.command("update")
def update_command(
    exchange: str | None = typer.Option(None, "--exchange"),
    symbol: str | None = typer.Option(None, "--symbol"),
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    console = Console()
    results = service.update_datasets(
        _load_app_config(console),
        exchange=exchange,
        symbol=symbol,
        show_progress=not json_output,
        console=console,
    )
    if json_output:
        _print_json(console, {"datasets": [_download_payload(result) for result in results]})
        return
    if not results:
        console.print("No local datasets found.")
        return

    table = Table("exchange", "symbol", "timeframe", "added", "warnings")
    for result in results:
        table.add_row(
            result.dataset.exchange,
            result.dataset.symbol,
            result.dataset.timeframe,
            str(result.added_rows),
            str(result.warnings),
        )
    console.print(table)


@app.command("check")
def check_command(
    exchange: str | None = typer.Option(None, "--exchange"),
    symbol: str | None = typer.Option(None, "--symbol"),
    timeframe: str | None = typer.Option(None, "--timeframe"),
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    console = Console()
    items = service.check_datasets(
        _load_app_config(console),
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
    )
    if json_output:
        _print_json(
            console, {"datasets": [_check_payload(key, warnings) for key, warnings in items]}
        )
        return
    if not items:
        console.print("No local datasets found.")
        return

    for key, warnings in items:
        if not warnings:
            console.print(f"{key.exchange} {key.symbol} {key.timeframe} - OK")
            continue

        console.print(
            f"{key.exchange} {key.symbol} {key.timeframe} - {len(warnings)} interpolation warnings:"
        )
        for warning in warnings:
            warning_range = (
                f"{utc_datetime_string(warning.start_ts)} - {utc_datetime_string(warning.end_ts)}"
            )
            console.print(f"  {warning_range} ({warning.candles} candles interpolated)")


@app.command("remove")
def remove_command(
    exchange: str = typer.Option(..., "--exchange"),
    symbol: str = typer.Option(..., "--symbol"),
    timeframe: str | None = typer.Option(None, "--timeframe"),
    from_date: str | None = typer.Option(None, "--from"),
    to_date: str | None = typer.Option(None, "--to"),
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    console = Console()
    if not typer.confirm("Delete matching data from storage?"):
        raise typer.Abort()

    try:
        removed_rows = service.remove_datasets(
            _load_app_config(console),
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            from_date=from_date,
            to_date=to_date,
        )
    except ValueError as error:
        console.print(str(error))
        raise typer.Exit(code=1) from error

    if json_output:
        _print_json(console, {"removed_rows": removed_rows})
        return

    console.print(f"Removed {removed_rows} candles.")


@app.command("capabilities")
def capabilities_command(
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    console = Console()
    capabilities = service.describe_capabilities(_load_app_config(console))
    if json_output:
        _print_json(console, _capabilities_payload(capabilities))
        return

    _print_capabilities_summary(console, capabilities)


@config_app.command("show")
def config_show_command(
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    console = Console()
    details = _load_app_config_details(console)
    if json_output:
        _print_json(console, _config_payload(details))
        return

    _print_config_summary(console, details)
