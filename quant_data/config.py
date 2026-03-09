from __future__ import annotations

import os
import tomllib
from pathlib import Path

from quant_data.models import AppConfig, ConfigSource, DataPathMode, ExchangeConfig, LoadedConfig

QD_HOME = Path("~/.qd").expanduser()
GLOBAL_CONFIG_PATH = QD_HOME / "qd_config.toml"
GLOBAL_DATA_PATH = QD_HOME / "data"
LOCAL_CONFIG_NAME = "qd_config.toml"


class ConfigError(ValueError):
    pass


def load_config(config_path: str | Path | None = None) -> AppConfig:
    return load_config_details(config_path).config


def load_config_details(config_path: str | Path | None = None) -> LoadedConfig:
    configured_path, config_source = _discover_config_path(config_path)

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

    data_path, data_path_mode = _resolve_data_path(data_path_value, configured_path)

    return LoadedConfig(
        config=AppConfig(
            data_path=data_path,
            gap_warning_threshold=gap_warning_threshold,
            exchanges=exchanges,
        ),
        config_path=configured_path,
        config_source=config_source,
        config_exists=configured_path.is_file(),
        data_path_mode=data_path_mode,
    )


def _discover_config_path(config_path: str | Path | None) -> tuple[Path, ConfigSource]:
    if config_path is not None:
        return Path(config_path).expanduser().resolve(), "explicit"

    env_path = os.environ.get("QD_CONFIG")
    if env_path:
        return Path(env_path).expanduser().resolve(), "env"

    local_config = _find_local_config()
    if local_config is not None:
        return local_config, "local"

    global_config = GLOBAL_CONFIG_PATH.resolve()
    if global_config.is_file():
        return global_config, "global"
    return global_config, "defaults"


def _find_local_config(start_dir: Path | None = None) -> Path | None:
    current_dir = (start_dir or Path.cwd()).resolve()
    for directory in [current_dir, *current_dir.parents]:
        candidate = directory / LOCAL_CONFIG_NAME
        if candidate.is_file():
            return candidate.resolve()
    return None


def _resolve_data_path(raw_value: str, config_path: Path) -> tuple[Path, DataPathMode]:
    if raw_value == "global":
        return GLOBAL_DATA_PATH.resolve(), "global"

    if raw_value == "local":
        if _is_global_config_path(config_path):
            raise ConfigError(
                "data_path='local' is not allowed in the global ~/.qd/qd_config.toml."
            )
        return (config_path.parent / ".qd" / "data").resolve(), "local"

    explicit_path = Path(raw_value).expanduser()
    if not explicit_path.is_absolute():
        raise ConfigError("data_path must be 'global', 'local', or an absolute path.")
    return explicit_path.resolve(), "absolute"


def _is_global_config_path(config_path: Path) -> bool:
    return config_path.resolve() == GLOBAL_CONFIG_PATH.resolve()
