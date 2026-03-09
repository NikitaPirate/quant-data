from __future__ import annotations

from pathlib import Path

import pytest

from quant_data import config as config_module
from quant_data.config import ConfigError, load_config, load_config_details


def test_explicit_config_path_beats_env_and_local(monkeypatch, tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    project = workspace / "project"
    project.mkdir(parents=True)

    explicit_config = workspace / "explicit.toml"
    explicit_config.write_text('data_path = "local"\n')

    env_config = workspace / "env.toml"
    env_config.write_text('data_path = "global"\n')

    (project / "qd_config.toml").write_text('data_path = "/tmp/ignored-local"\n')

    monkeypatch.chdir(project)
    monkeypatch.setenv("QD_CONFIG", str(env_config))

    loaded = load_config(explicit_config)
    details = load_config_details(explicit_config)

    assert loaded.data_path == (workspace / ".qd" / "data").resolve()
    assert details.config_source == "explicit"
    assert details.data_path_mode == "local"


def test_env_config_beats_local_and_global(monkeypatch, tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    project = workspace / "nested" / "project"
    project.mkdir(parents=True)

    env_config = workspace / "env.toml"
    env_config.write_text('data_path = "/tmp/env-data"\n')

    (project / "qd_config.toml").write_text('data_path = "/tmp/local-data"\n')

    global_config = tmp_path / ".qd" / "qd_config.toml"
    global_config.parent.mkdir(parents=True)
    global_config.write_text('data_path = "/tmp/global-data"\n')

    monkeypatch.setattr(config_module, "GLOBAL_CONFIG_PATH", global_config)
    monkeypatch.setattr(config_module, "GLOBAL_DATA_PATH", tmp_path / ".qd" / "data")
    monkeypatch.chdir(project)
    monkeypatch.setenv("QD_CONFIG", str(env_config))

    loaded = load_config()
    details = load_config_details()

    assert loaded.data_path == Path("/tmp/env-data").resolve()
    assert details.config_source == "env"
    assert details.data_path_mode == "absolute"


def test_nearest_local_config_is_preferred(monkeypatch, tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    parent = workspace / "parent"
    child = parent / "child"
    child.mkdir(parents=True)

    (parent / "qd_config.toml").write_text('data_path = "/tmp/parent-data"\n')
    (child / "qd_config.toml").write_text('data_path = "local"\n')

    global_config = tmp_path / ".qd" / "qd_config.toml"
    global_config.parent.mkdir(parents=True)
    global_config.write_text('data_path = "/tmp/global-data"\n')

    monkeypatch.setattr(config_module, "GLOBAL_CONFIG_PATH", global_config)
    monkeypatch.setattr(config_module, "GLOBAL_DATA_PATH", tmp_path / ".qd" / "data")
    monkeypatch.chdir(child)
    monkeypatch.delenv("QD_CONFIG", raising=False)

    loaded = load_config()
    details = load_config_details()

    assert loaded.data_path == (child / ".qd" / "data").resolve()
    assert details.config_source == "local"
    assert details.data_path_mode == "local"


def test_builtin_defaults_use_global_qd_home(monkeypatch, tmp_path: Path) -> None:
    global_config = tmp_path / ".qd" / "qd_config.toml"
    global_data = tmp_path / ".qd" / "data"
    workspace = tmp_path / "workspace"
    workspace.mkdir()

    monkeypatch.setattr(config_module, "GLOBAL_CONFIG_PATH", global_config)
    monkeypatch.setattr(config_module, "GLOBAL_DATA_PATH", global_data)
    monkeypatch.chdir(workspace)
    monkeypatch.delenv("QD_CONFIG", raising=False)

    loaded = load_config()
    details = load_config_details()

    assert loaded.data_path == global_data.resolve()
    assert details.config_source == "defaults"
    assert not details.config_exists


def test_absolute_data_path_is_used_as_is(tmp_path: Path) -> None:
    config_path = tmp_path / "custom.toml"
    target_path = tmp_path / "custom-data"
    config_path.write_text(f'data_path = "{target_path}"\n')

    loaded = load_config(config_path)

    assert loaded.data_path == target_path.resolve()


def test_relative_data_path_raises_clear_error(tmp_path: Path) -> None:
    config_path = tmp_path / "custom.toml"
    config_path.write_text('data_path = "./data"\n')

    with pytest.raises(
        ConfigError,
        match="data_path must be 'global', 'local', or an absolute path.",
    ):
        load_config(config_path)


def test_local_in_global_config_raises_clear_error(monkeypatch, tmp_path: Path) -> None:
    global_config = tmp_path / ".qd" / "qd_config.toml"
    global_config.parent.mkdir(parents=True)
    global_config.write_text('data_path = "local"\n')
    workspace = tmp_path / "workspace"
    workspace.mkdir()

    monkeypatch.setattr(config_module, "GLOBAL_CONFIG_PATH", global_config)
    monkeypatch.setattr(config_module, "GLOBAL_DATA_PATH", tmp_path / ".qd" / "data")
    monkeypatch.delenv("QD_CONFIG", raising=False)
    monkeypatch.chdir(workspace)

    with pytest.raises(
        ConfigError,
        match="data_path='local' is not allowed in the global ~/.qd/qd_config.toml.",
    ):
        load_config()
