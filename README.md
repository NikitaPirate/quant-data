# quant-data

Utility, library, and bundled agent skill for local OHLCV storage on top of ccxt: download data once and then reuse it across projects without repeated loading.

The project has three parts:

- CLI `qd` for preparing and maintaining local data: download, update, inspect, and remove.
- Python API `Candles` for notebooks and backtests: request data and get a `DataFrame`, with missing ranges downloaded automatically when needed.
- Bundled agent skill for installing the `quant-data` workflow into supported runtimes.

## Installation

CLI from the current directory:

```bash
uv tool install .
qd --help
```

CLI from GitHub:

```bash
uv tool install git+https://github.com/NikitaPirate/quant-data.git
qd --help
```

Install the bundled skill into a runtime root:

```bash
qd install skill ~/.claude
qd install skill ~/.openclaw
qd install skill ~/.codex --codex
```

`<runtime-root>` can also be relative to the current working directory, for example:

```bash
qd install skill .codex --codex
```

Use `--codex` only for Codex runtimes, where the tool must also write the extra agents metadata file.

Library from the current directory:

```bash
uv add .
```

Library from GitHub:

```bash
uv add git+https://github.com/NikitaPirate/quant-data.git
```

```python
from quant_data import Candles
```

## Storage

By default, global data is stored in `~/.qd/data` as yearly Parquet files:

```text
~/.qd/data/
`-- <exchange>/
    `-- <symbol-with-slashes-replaced-by-dashes>/
        `-- <timeframe>/
            |-- 2022.parquet
            |-- 2023.parquet
            `-- notes.json
```

Candle schema:

- `ts` - candle open timestamp in Unix milliseconds
- `open`
- `high`
- `low`
- `close`
- `volume`

Downloaded ranges are repaired to stay gap-free. If the exchange still does not return missing candles after a retry, the gap is filled with linear interpolation. Interpolations longer than the warning threshold are also recorded in `notes.json`. Partial `qd remove` operations can intentionally create gaps again.

## Configuration

Config discovery order is:

1. explicit config path passed to the loader
2. `QD_CONFIG`
3. nearest `qd_config.toml` found by walking upward from the current working directory
4. global `~/.qd/qd_config.toml`
5. built-in defaults

For repo-local runs, copy `qd_config.example.toml` to `qd_config.toml`. If the file is in the
current working directory or one of its parents, no environment variable is required.

```toml
data_path = "global"
gap_warning_threshold = 30

[exchanges.binance]
type = "spot"
```

`data_path` accepts exactly:

- `global` -> `~/.qd/data`
- `local` -> `.qd/data` next to the active local `qd_config.toml`
- a home-relative or absolute path such as `~/qd-data`

Invalid configurations:

- `data_path = "local"` inside the global `~/.qd/qd_config.toml`
- any relative explicit path such as `./data`

## CLI

Every CLI command has two output modes:

- default human-readable output
- `--json` machine-readable output

```bash
qd config show
qd capabilities
qd install skill ~/.claude
qd install skill ~/.openclaw
qd install skill ~/.codex --codex
qd list
qd markets --exchange binance --quote USDT
qd download --exchange binance --symbol BTC/USDT --timeframe 1m --from 2024-01-01 --to 2024-01-31
qd update --exchange binance
qd check --exchange binance --symbol BTC/USDT --timeframe 1m
qd remove --exchange binance --symbol BTC/USDT --timeframe 1m
```

Agent-oriented introspection:

```bash
qd config show --json
qd capabilities --json
qd install skill ~/.claude --json
qd install skill ~/.codex --codex --json
qd list --json
```

## Python API

```python
from quant_data import Candles

df = Candles.load(
    "binance",
    "BTC/USDT",
    "1m",
    from_date="2024-01-01",
    to_date="2024-01-31",
)
```

The return value is a `pandas.DataFrame` with a UTC `DatetimeIndex` and `open`, `high`, `low`, `close`, `volume` columns.
