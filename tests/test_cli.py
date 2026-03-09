from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

from typer.testing import CliRunner

from quant_data import cli, downloader, service, storage
from quant_data.models import DatasetKey, ExchangeConfig, LoadedConfig
from tests.conftest import FakeExchange, candle, stored_row

runner = CliRunner()


def _extract_json(stdout: str) -> object:
    for marker in ("{", "["):
        position = stdout.find(marker)
        if position != -1:
            return json.loads(stdout[position:])
    raise AssertionError(f"No JSON payload found in output: {stdout!r}")


def _loaded_config(config) -> LoadedConfig:
    return LoadedConfig(
        config=config,
        config_path=Path("/tmp/qd_config.toml"),
        config_source="local",
        config_exists=True,
        data_path_mode="local",
    )


def test_markets_command_filters_quote_and_supports_json(config, monkeypatch) -> None:
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
    json_result = runner.invoke(
        cli.app,
        ["markets", "--exchange", "binance", "--quote", "USDT", "--json"],
    )

    assert result.exit_code == 0
    assert "BTC/USDT" in result.stdout
    assert "ETH/BTC" not in result.stdout
    assert json_result.exit_code == 0
    assert _extract_json(json_result.stdout) == {
        "markets": [{"symbol": "BTC/USDT", "base": "BTC", "quote": "USDT", "type": "spot"}]
    }


def test_list_and_remove_commands_support_json(config, monkeypatch) -> None:
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
    list_json_result = runner.invoke(cli.app, ["list", "--exchange", "binance", "--json"])
    remove_result = runner.invoke(
        cli.app,
        ["remove", "--exchange", "binance", "--symbol", "BTC/USDT", "--timeframe", "1m"],
        input="y\n",
    )

    storage.write_rows(
        config,
        key,
        [
            stored_row(1_704_067_200_000, 1.0),
            stored_row(1_704_067_260_000, 2.0),
        ],
    )
    remove_json_result = runner.invoke(
        cli.app,
        [
            "remove",
            "--exchange",
            "binance",
            "--symbol",
            "BTC/USDT",
            "--timeframe",
            "1m",
            "--json",
        ],
        input="y\n",
    )

    assert list_result.exit_code == 0
    assert "BTC/USDT" in list_result.stdout
    assert list_json_result.exit_code == 0
    assert _extract_json(list_json_result.stdout) == {
        "datasets": [
            {
                "exchange": "binance",
                "symbol": "BTC/USDT",
                "timeframe": "1m",
                "from": "2024-01-01",
                "to": "2024-01-01",
                "from_ts": 1_704_067_200_000,
                "to_ts": 1_704_067_260_000,
                "rows": 2,
                "warnings": 0,
            }
        ]
    }
    assert remove_result.exit_code == 0
    assert "Removed 2 candles." in remove_result.stdout
    assert remove_json_result.exit_code == 0
    assert _extract_json(remove_json_result.stdout) == {"removed_rows": 2}


def test_download_and_check_commands_support_json(config, monkeypatch) -> None:
    fake = FakeExchange(
        {
            ("BTC/USDT", "1m"): [
                candle(1_704_067_200_000, 100.0),
                candle(1_704_067_260_000, 101.0),
            ]
        }
    )
    monkeypatch.setattr(downloader, "instantiate_exchange", lambda *_: fake)
    monkeypatch.setattr(cli, "load_config", lambda: config)

    download_result = runner.invoke(
        cli.app,
        [
            "download",
            "--exchange",
            "binance",
            "--symbol",
            "BTC/USDT",
            "--timeframe",
            "1m",
            "--from",
            "2024-01-01",
            "--to",
            "2024-01-01",
            "--json",
        ],
    )
    check_result = runner.invoke(
        cli.app,
        [
            "check",
            "--exchange",
            "binance",
            "--symbol",
            "BTC/USDT",
            "--timeframe",
            "1m",
            "--json",
        ],
    )

    assert download_result.exit_code == 0
    assert _extract_json(download_result.stdout) == {
        "exchange": "binance",
        "symbol": "BTC/USDT",
        "timeframe": "1m",
        "added_rows": 2,
        "warnings": 0,
    }
    assert check_result.exit_code == 0
    assert _extract_json(check_result.stdout) == {
        "datasets": [
            {
                "exchange": "binance",
                "symbol": "BTC/USDT",
                "timeframe": "1m",
                "status": "ok",
                "warnings": [],
            }
        ]
    }


def test_update_command_supports_json(config, monkeypatch) -> None:
    key = DatasetKey("binance", "BTC/USDT", "1m")
    storage.write_rows(
        config,
        key,
        [
            stored_row(1_704_067_200_000, 1.0),
            stored_row(1_704_067_260_000, 2.0),
        ],
    )
    fake = FakeExchange(
        {
            ("BTC/USDT", "1m"): [
                candle(1_704_067_320_000, 102.0),
                candle(1_704_067_380_000, 103.0),
            ]
        }
    )
    monkeypatch.setattr(downloader, "instantiate_exchange", lambda *_: fake)
    monkeypatch.setattr(cli, "load_config", lambda: config)
    monkeypatch.setattr(service.utils, "default_to_exclusive_utc", lambda: 1_704_067_440_000)

    result = runner.invoke(
        cli.app,
        ["update", "--exchange", "binance", "--symbol", "BTC/USDT", "--json"],
    )

    assert result.exit_code == 0
    assert _extract_json(result.stdout) == {
        "datasets": [
            {
                "exchange": "binance",
                "symbol": "BTC/USDT",
                "timeframe": "1m",
                "added_rows": 2,
                "warnings": 0,
            }
        ]
    }


def test_capabilities_and_config_show_commands_support_human_and_json(config, monkeypatch) -> None:
    config = replace(
        config,
        exchanges={
            "binance": ExchangeConfig(type="spot"),
            "bybit": ExchangeConfig(type="spot"),
        },
    )
    monkeypatch.setattr(cli, "load_config", lambda: config)
    monkeypatch.setattr(cli, "load_config_details", lambda: _loaded_config(config))

    capabilities_result = runner.invoke(cli.app, ["capabilities"])
    capabilities_json_result = runner.invoke(cli.app, ["capabilities", "--json"])
    config_result = runner.invoke(cli.app, ["config", "show"])
    config_json_result = runner.invoke(cli.app, ["config", "show", "--json"])

    assert capabilities_result.exit_code == 0
    assert "supported_data_flows" in capabilities_result.stdout
    assert "binance" in capabilities_result.stdout
    assert capabilities_json_result.exit_code == 0
    assert _extract_json(capabilities_json_result.stdout) == {
        "supported_data_flows": ["candles"],
        "supported_market_types": ["spot"],
        "configured_exchanges": {"binance": "spot", "bybit": "spot"},
        "exchange_support_model": (
            "Attempt any CCXT exchange id in spot mode. A dataset is supported only when the "
            "exchange provides spot OHLCV through fetch_ohlcv."
        ),
        "unsupported_data_flows": [
            "trades",
            "orderbook",
            "funding",
            "open-interest",
            "liquidations",
            "websocket-live",
        ],
        "unsupported_market_types": ["futures", "swap", "options"],
        "cli_output_formats": ["human", "json"],
        "commands": [
            "list",
            "markets",
            "download",
            "update",
            "check",
            "remove",
            "capabilities",
            "config show",
        ],
        "library_api": ["quant_data.Candles.load"],
    }

    assert config_result.exit_code == 0
    assert "config_source" in config_result.stdout
    assert "data_path" in config_result.stdout
    assert config_json_result.exit_code == 0
    assert _extract_json(config_json_result.stdout) == {
        "config_source": "local",
        "config_path": "/tmp/qd_config.toml",
        "config_exists": True,
        "data_path_mode": "local",
        "data_path": str(config.data_path),
        "gap_warning_threshold": 30,
        "exchanges": {"binance": {"type": "spot"}, "bybit": {"type": "spot"}},
    }
