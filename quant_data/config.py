from __future__ import annotations

import os
import tomllib
from pathlib import Path

from quant_data.models import AppConfig, ExchangeConfig

DEFAULT_STORAGE_PATH = Path("~/.quant-data/storage").expanduser()
DEFAULT_CONFIG_PATH = Path("~/.quant-data/config.toml").expanduser()


def _resolve_storage_path(raw_path: str | None, config_path: Path) -> Path:
    if not raw_path:
        return DEFAULT_STORAGE_PATH

    storage_path = Path(raw_path).expanduser()
    if storage_path.is_absolute():
        return storage_path
    return (config_path.parent / storage_path).resolve()


def load_config(config_path: str | Path | None = None) -> AppConfig:
    configured_path = (
        Path(config_path).expanduser()
        if config_path is not None
        else Path(os.environ.get("QUANT_DATA_CONFIG", DEFAULT_CONFIG_PATH)).expanduser()
    )

    raw_config: dict[str, object] = {}
    if configured_path.is_file():
        raw_config = tomllib.loads(configured_path.read_text())

    exchanges_config = raw_config.get("exchanges", {})
    exchanges: dict[str, ExchangeConfig] = {}
    if isinstance(exchanges_config, dict):
        for exchange_id, exchange_settings in exchanges_config.items():
            if isinstance(exchange_settings, dict):
                exchanges[exchange_id] = ExchangeConfig(
                    type=str(exchange_settings.get("type", "spot"))
                )

    storage_value = raw_config.get("storage_path")
    gap_warning_value = raw_config.get("gap_warning_threshold", 30)

    storage_path = str(storage_value) if isinstance(storage_value, str) else None
    gap_warning_threshold = (
        int(gap_warning_value)
        if isinstance(
            gap_warning_value,
            int | str,
        )
        else 30
    )

    return AppConfig(
        storage_path=_resolve_storage_path(storage_path, configured_path),
        gap_warning_threshold=gap_warning_threshold,
        exchanges=exchanges,
    )
