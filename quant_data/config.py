from __future__ import annotations

import os
import tomllib
from pathlib import Path

from quant_data.models import AppConfig, ExchangeConfig

QD_HOME = Path("~/.qd").expanduser()
GLOBAL_CONFIG_PATH = QD_HOME / "qd_config.toml"
GLOBAL_DATA_PATH = QD_HOME / "data"
LOCAL_CONFIG_NAME = "qd_config.toml"


class ConfigError(ValueError):
    pass


def load_config(config_path: str | Path | None = None) -> AppConfig:
    configured_path = _discover_config_path(config_path)

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

    data_path_value = raw_config.get("data_path", "global")
    gap_warning_value = raw_config.get("gap_warning_threshold", 30)

    if not isinstance(data_path_value, str):
        raise ConfigError("data_path must be set to 'global', 'local', or an absolute path.")

    gap_warning_threshold = (
        int(gap_warning_value)
        if isinstance(
            gap_warning_value,
            int | str,
        )
        else 30
    )

    return AppConfig(
        data_path=_resolve_data_path(data_path_value, configured_path),
        gap_warning_threshold=gap_warning_threshold,
        exchanges=exchanges,
    )


def _discover_config_path(config_path: str | Path | None) -> Path:
    if config_path is not None:
        return Path(config_path).expanduser().resolve()

    env_path = os.environ.get("QD_CONFIG")
    if env_path:
        return Path(env_path).expanduser().resolve()

    local_config = _find_local_config()
    if local_config is not None:
        return local_config

    return GLOBAL_CONFIG_PATH.resolve()


def _find_local_config(start_dir: Path | None = None) -> Path | None:
    current_dir = (start_dir or Path.cwd()).resolve()
    for directory in [current_dir, *current_dir.parents]:
        candidate = directory / LOCAL_CONFIG_NAME
        if candidate.is_file():
            return candidate.resolve()
    return None


def _resolve_data_path(raw_value: str, config_path: Path) -> Path:
    if raw_value == "global":
        return GLOBAL_DATA_PATH.resolve()

    if raw_value == "local":
        if _is_global_config_path(config_path):
            raise ConfigError(
                "data_path='local' is not allowed in the global ~/.qd/qd_config.toml."
            )
        return (config_path.parent / ".qd" / "data").resolve()

    explicit_path = Path(raw_value).expanduser()
    if not explicit_path.is_absolute():
        raise ConfigError("data_path must be 'global', 'local', or an absolute path.")
    return explicit_path.resolve()


def _is_global_config_path(config_path: Path) -> bool:
    return config_path.resolve() == GLOBAL_CONFIG_PATH.resolve()
