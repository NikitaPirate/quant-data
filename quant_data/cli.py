from __future__ import annotations

import typer
from rich.console import Console
from rich.table import Table

from quant_data import service
from quant_data.config import ConfigError, load_config
from quant_data.utils import utc_date_string, utc_datetime_string

app = typer.Typer(no_args_is_help=True, add_completion=False)


def main() -> None:
    app()


def _load_app_config(console: Console):
    try:
        return load_config()
    except ConfigError as error:
        console.print(str(error))
        raise typer.Exit(code=1) from error


@app.command("list")
def list_command(
    exchange: str | None = typer.Option(None, "--exchange"),
    symbol: str | None = typer.Option(None, "--symbol"),
) -> None:
    console = Console()
    items = service.list_datasets(_load_app_config(console), exchange=exchange, symbol=symbol)
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
) -> None:
    console = Console()
    items = service.list_markets(_load_app_config(console), exchange, quote=quote, base=base)
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
            show_progress=True,
            console=console,
        )
    except ValueError as error:
        console.print(str(error))
        raise typer.Exit(code=1) from error

    console.print(
        f"Downloaded {stats.added_rows} candles for {exchange} {symbol} {timeframe}. "
        f"Warnings: {stats.warnings}."
    )


@app.command("update")
def update_command(
    exchange: str | None = typer.Option(None, "--exchange"),
    symbol: str | None = typer.Option(None, "--symbol"),
) -> None:
    console = Console()
    results = service.update_datasets(
        _load_app_config(console),
        exchange=exchange,
        symbol=symbol,
        show_progress=True,
        console=console,
    )
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
) -> None:
    console = Console()
    items = service.check_datasets(
        _load_app_config(console),
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
    )
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

    console.print(f"Removed {removed_rows} candles.")
