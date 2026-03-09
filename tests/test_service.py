from __future__ import annotations

from dataclasses import replace

import pandas as pd

from quant_data import api, downloader, service, storage
from quant_data.models import DatasetKey
from tests.conftest import FakeExchange, candle, stored_row


def test_plan_missing_ranges_detects_internal_gap(config) -> None:
    key = DatasetKey("binance", "BTC/USDT", "1m")
    storage.write_rows(
        config,
        key,
        [
            stored_row(1_704_067_200_000, 1.0),
            stored_row(1_704_067_260_000, 2.0),
            stored_row(1_704_067_440_000, 5.0),
        ],
    )

    missing = service.plan_missing_ranges(
        config,
        key,
        start_ms=1_704_067_200_000,
        end_ms=1_704_067_500_000,
    )

    assert [(item.start_ms, item.end_ms) for item in missing] == [
        (1_704_067_320_000, 1_704_067_440_000)
    ]


def test_download_interpolates_gaps_and_writes_notes(config, monkeypatch) -> None:
    config = replace(config, gap_warning_threshold=1)
    fake = FakeExchange(
        {
            ("BTC/USDT", "1m"): [
                candle(1_704_067_200_000, 100.0),
                candle(1_704_067_260_000, 101.0),
                candle(1_704_067_440_000, 104.0),
            ]
        }
    )
    monkeypatch.setattr(downloader, "instantiate_exchange", lambda *_: fake)

    stats = service.download_dataset(
        config,
        "binance",
        "BTC/USDT",
        "1m",
        from_date="2024-01-01",
        to_date="2024-01-01",
    )

    frame = storage.read_frame(config, DatasetKey("binance", "BTC/USDT", "1m"))
    assert stats.added_rows == 5
    assert frame["ts"].tolist()[:5] == [
        1_704_067_200_000,
        1_704_067_260_000,
        1_704_067_320_000,
        1_704_067_380_000,
        1_704_067_440_000,
    ]
    warnings = storage.read_notes(config, DatasetKey("binance", "BTC/USDT", "1m"))
    assert len(warnings) == 1
    assert warnings[0].candles == 2


def test_update_adds_only_tail(config, monkeypatch) -> None:
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
    monkeypatch.setattr(service.utils, "default_to_exclusive_utc", lambda: 1_704_067_440_000)

    results = service.update_datasets(config, exchange="binance", symbol="BTC/USDT")

    assert len(results) == 1
    assert results[0].added_rows == 2
    assert storage.read_frame(config, key)["ts"].tolist() == [
        1_704_067_200_000,
        1_704_067_260_000,
        1_704_067_320_000,
        1_704_067_380_000,
    ]


def test_candles_load_returns_utc_index(config, monkeypatch) -> None:
    fake = FakeExchange(
        {
            ("BTC/USDT", "1m"): [
                candle(1_704_067_200_000, 100.0),
                candle(1_704_067_260_000, 101.0),
            ]
        }
    )
    monkeypatch.setattr(downloader, "instantiate_exchange", lambda *_: fake)
    monkeypatch.setattr(api, "load_config", lambda: config)

    frame = api.Candles.load(
        "binance",
        "BTC/USDT",
        "1m",
        from_date="2024-01-01",
        to_date="2024-01-01",
    )

    assert isinstance(frame.index, pd.DatetimeIndex)
    assert str(frame.index.tz) == "UTC"
    assert list(frame.columns) == ["open", "high", "low", "close", "volume"]
