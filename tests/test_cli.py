from __future__ import annotations

from typer.testing import CliRunner

from quant_data import cli, downloader, storage
from quant_data.models import DatasetKey
from tests.conftest import FakeExchange, stored_row

runner = CliRunner()


def test_markets_command_filters_quote(config, monkeypatch) -> None:
    fake = FakeExchange(
        {},
        markets={
            "BTC/USDT": {"symbol": "BTC/USDT", "base": "BTC", "quote": "USDT", "type": "spot"},
            "ETH/BTC": {"symbol": "ETH/BTC", "base": "ETH", "quote": "BTC", "type": "spot"},
        },
    )
    monkeypatch.setattr(downloader, "instantiate_exchange", lambda *_: fake)
    monkeypatch.setattr(cli, "load_config", lambda: config)

    result = runner.invoke(cli.app, ["markets", "--exchange", "binance", "--quote", "USDT"])

    assert result.exit_code == 0
    assert "BTC/USDT" in result.stdout
    assert "ETH/BTC" not in result.stdout


def test_list_and_remove_commands(config, monkeypatch) -> None:
    key = DatasetKey("binance", "BTC/USDT", "1m")
    storage.write_rows(
        config,
        key,
        [
            stored_row(1_704_067_200_000, 1.0),
            stored_row(1_704_067_260_000, 2.0),
        ],
    )
    monkeypatch.setattr(cli, "load_config", lambda: config)

    list_result = runner.invoke(cli.app, ["list", "--exchange", "binance"])
    remove_result = runner.invoke(
        cli.app,
        ["remove", "--exchange", "binance", "--symbol", "BTC/USDT", "--timeframe", "1m"],
        input="y\n",
    )

    assert list_result.exit_code == 0
    assert "BTC/USDT" in list_result.stdout
    assert remove_result.exit_code == 0
    assert "Removed 2 candles." in remove_result.stdout
