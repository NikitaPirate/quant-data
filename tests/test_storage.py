from __future__ import annotations

from quant_data import storage
from quant_data.models import DatasetKey
from tests.conftest import stored_row


def test_write_rows_deduplicates_and_splits_years(config) -> None:
    key = DatasetKey("binance", "BTC/USDT", "1m")
    storage.write_rows(
        config,
        key,
        [
            stored_row(1_704_067_140_000, 1.0),
            stored_row(1_704_067_200_000, 2.0),
            stored_row(1_704_067_140_000, 1.1),
        ],
    )
    storage.write_rows(
        config,
        key,
        [
            stored_row(1_704_067_260_000, 3.0),
            stored_row(1_704_153_600_000, 4.0),
        ],
    )

    frame = storage.read_frame(config, key)
    assert frame["ts"].tolist() == [
        1_704_067_140_000,
        1_704_067_200_000,
        1_704_067_260_000,
        1_704_153_600_000,
    ]
    assert frame.iloc[0]["open"] == 1.1
    assert storage.year_file_path(config, key, 2024).exists()


def test_remove_range_prunes_rows(config) -> None:
    key = DatasetKey("binance", "BTC/USDT", "1m")
    storage.write_rows(
        config,
        key,
        [
            stored_row(1_704_067_140_000, 1.0),
            stored_row(1_704_067_200_000, 2.0),
            stored_row(1_704_067_260_000, 3.0),
        ],
    )

    removed = storage.remove_range(
        config,
        key,
        start_ms=1_704_067_200_000,
        end_ms=1_704_067_260_000,
    )

    assert removed == 1
    assert storage.read_frame(config, key)["ts"].tolist() == [1_704_067_140_000, 1_704_067_260_000]
