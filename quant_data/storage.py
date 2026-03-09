from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import cast

import pandas as pd

from quant_data.models import AppConfig, DatasetInfo, DatasetKey, GapWarning
from quant_data.utils import storage_name_to_symbol, symbol_to_storage_name


def _empty_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ts": pd.Series(dtype="int64"),
            "open": pd.Series(dtype="float64"),
            "high": pd.Series(dtype="float64"),
            "low": pd.Series(dtype="float64"),
            "close": pd.Series(dtype="float64"),
            "volume": pd.Series(dtype="float64"),
        }
    )


def dataset_dir(config: AppConfig, key: DatasetKey) -> Path:
    return config.storage_path / key.exchange / symbol_to_storage_name(key.symbol) / key.timeframe


def notes_path(config: AppConfig, key: DatasetKey) -> Path:
    return dataset_dir(config, key) / "notes.json"


def year_file_path(config: AppConfig, key: DatasetKey, year: int) -> Path:
    return dataset_dir(config, key) / f"{year}.parquet"


def _read_year_frame(path: Path) -> pd.DataFrame:
    if not path.exists():
        return _empty_frame()

    frame = pd.read_parquet(path)
    if frame.empty:
        return _empty_frame()

    ordered = cast(
        pd.DataFrame,
        frame.loc[:, ["ts", "open", "high", "low", "close", "volume"]].copy(),
    )
    ordered["ts"] = ordered["ts"].astype("int64")
    for column in ["open", "high", "low", "close", "volume"]:
        ordered[column] = ordered[column].astype("float64")
    return ordered


def _normalize_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return _empty_frame()

    normalized = cast(
        pd.DataFrame,
        frame.loc[:, ["ts", "open", "high", "low", "close", "volume"]].copy(),
    )
    normalized["ts"] = normalized["ts"].astype("int64")
    for column in ["open", "high", "low", "close", "volume"]:
        normalized[column] = normalized[column].astype("float64")
    normalized = normalized.drop_duplicates(subset=["ts"], keep="last").sort_values("ts")
    normalized.reset_index(drop=True, inplace=True)
    return normalized


def list_dataset_keys(config: AppConfig) -> list[DatasetKey]:
    keys: list[DatasetKey] = []
    if not config.storage_path.exists():
        return keys

    for exchange_dir in sorted(path for path in config.storage_path.iterdir() if path.is_dir()):
        for symbol_dir in sorted(path for path in exchange_dir.iterdir() if path.is_dir()):
            for timeframe_dir in sorted(path for path in symbol_dir.iterdir() if path.is_dir()):
                has_parquet = any(path.suffix == ".parquet" for path in timeframe_dir.iterdir())
                if has_parquet:
                    keys.append(
                        DatasetKey(
                            exchange=exchange_dir.name,
                            symbol=storage_name_to_symbol(symbol_dir.name),
                            timeframe=timeframe_dir.name,
                        )
                    )
    return keys


def write_rows(config: AppConfig, key: DatasetKey, rows: list[dict[str, float | int]]) -> int:
    if not rows:
        return 0

    directory = dataset_dir(config, key)
    directory.mkdir(parents=True, exist_ok=True)
    frame = _normalize_frame(pd.DataFrame(rows))
    frame["_year"] = pd.to_datetime(frame["ts"], unit="ms", utc=True).dt.year.astype("int64")

    written_rows = 0
    for _, group in frame.groupby("_year"):
        year = int(group["_year"].iloc[0])
        year_path = year_file_path(config, key, year)
        existing = _read_year_frame(year_path)
        merged = _normalize_frame(
            pd.concat(
                [existing, group.drop(columns="_year")],
                ignore_index=True,
            )
        )
        merged.to_parquet(year_path, index=False)
        written_rows += len(group.index)

    return written_rows


def _candidate_year_paths(config: AppConfig, key: DatasetKey) -> list[Path]:
    directory = dataset_dir(config, key)
    if not directory.exists():
        return []
    return sorted(path for path in directory.iterdir() if path.suffix == ".parquet")


def read_frame(
    config: AppConfig,
    key: DatasetKey,
    *,
    start_ms: int | None = None,
    end_ms: int | None = None,
) -> pd.DataFrame:
    frames = [_read_year_frame(path) for path in _candidate_year_paths(config, key)]
    if not frames:
        return _empty_frame()

    frame = _normalize_frame(pd.concat(frames, ignore_index=True))
    if start_ms is not None:
        frame = cast(pd.DataFrame, frame.loc[frame["ts"] >= start_ms].copy())
    if end_ms is not None:
        frame = cast(pd.DataFrame, frame.loc[frame["ts"] < end_ms].copy())
    return cast(pd.DataFrame, frame.reset_index(drop=True))


def dataset_timestamps(
    config: AppConfig,
    key: DatasetKey,
    *,
    start_ms: int | None = None,
    end_ms: int | None = None,
) -> list[int]:
    frame = read_frame(config, key, start_ms=start_ms, end_ms=end_ms)
    return frame["ts"].astype("int64").tolist()


def read_previous_row(
    config: AppConfig,
    key: DatasetKey,
    before_ms: int,
) -> dict[str, float | int] | None:
    frame = read_frame(config, key, end_ms=before_ms)
    if frame.empty:
        return None

    row = frame.iloc[-1]
    return {
        "ts": int(row["ts"]),
        "open": float(row["open"]),
        "high": float(row["high"]),
        "low": float(row["low"]),
        "close": float(row["close"]),
        "volume": float(row["volume"]),
    }


def get_dataset_info(config: AppConfig, key: DatasetKey) -> DatasetInfo | None:
    frame = read_frame(config, key)
    if frame.empty:
        return None

    warnings = read_notes(config, key)
    return DatasetInfo(
        exchange=key.exchange,
        symbol=key.symbol,
        timeframe=key.timeframe,
        from_ts=int(frame.iloc[0]["ts"]),
        to_ts=int(frame.iloc[-1]["ts"]),
        rows=len(frame),
        warnings=len(warnings),
    )


def read_notes(config: AppConfig, key: DatasetKey) -> list[GapWarning]:
    path = notes_path(config, key)
    if not path.exists():
        return []

    payload = json.loads(path.read_text())
    warnings: list[GapWarning] = []
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                warnings.append(
                    GapWarning(
                        start_ts=int(item["start_ts"]),
                        end_ts=int(item["end_ts"]),
                        candles=int(item["candles"]),
                    )
                )
    return warnings


def replace_notes(config: AppConfig, key: DatasetKey, warnings: list[GapWarning]) -> None:
    path = notes_path(config, key)
    if not warnings:
        path.unlink(missing_ok=True)
        return

    unique = {
        (warning.start_ts, warning.end_ts, warning.candles): warning
        for warning in sorted(warnings, key=lambda item: (item.start_ts, item.end_ts, item.candles))
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            [
                {
                    "start_ts": warning.start_ts,
                    "end_ts": warning.end_ts,
                    "candles": warning.candles,
                }
                for warning in unique.values()
            ],
            indent=2,
        )
    )


def append_notes(config: AppConfig, key: DatasetKey, warnings: list[GapWarning]) -> None:
    replace_notes(config, key, [*read_notes(config, key), *warnings])


def remove_range(
    config: AppConfig,
    key: DatasetKey,
    *,
    start_ms: int | None = None,
    end_ms: int | None = None,
) -> int:
    directory = dataset_dir(config, key)
    if not directory.exists():
        return 0

    if start_ms is None and end_ms is None:
        frame = read_frame(config, key)
        shutil.rmtree(directory)
        _prune_empty_parents(directory)
        return len(frame)

    removed_rows = 0
    for path in _candidate_year_paths(config, key):
        frame = _read_year_frame(path)
        keep_mask = pd.Series([True] * len(frame))
        if start_ms is not None:
            keep_mask &= frame["ts"] < start_ms
        if end_ms is not None:
            keep_mask |= frame["ts"] >= end_ms

        removed_rows += int((~keep_mask).sum())
        kept = cast(pd.DataFrame, frame.loc[keep_mask].copy())
        if kept.empty:
            path.unlink()
        else:
            _normalize_frame(kept).to_parquet(path, index=False)

    remaining_warnings = [
        warning
        for warning in read_notes(config, key)
        if not (
            (start_ms is None or warning.end_ts >= start_ms)
            and (end_ms is None or warning.start_ts < end_ms)
        )
    ]
    replace_notes(config, key, remaining_warnings)
    _prune_empty_parents(directory)
    return removed_rows


def _prune_empty_parents(directory: Path) -> None:
    current = directory
    while current.name and current.exists():
        if any(current.iterdir()):
            break
        current.rmdir()
        current = current.parent
