# quant-data

Standalone tooling for downloading, storing, and maintaining OHLCV data on top of ccxt.

The project has two parts:

- CLI `qd` for preparing and maintaining local data: download, update, inspect, and remove.
- Python API `Candles` for notebooks and backtests: request data and get a `DataFrame`, with missing ranges downloaded automatically when needed.

## Installation

CLI from the current directory:

```bash
uv tool install .
qd --help
```

CLI from GitHub:

```bash
uv tool install git+ssh://git@github.com/NikitaPirate/quant-data.git
qd --help
```

Library from the current directory:

```bash
uv add .
```

Library from GitHub:

```bash
uv add git+ssh://git@github.com/NikitaPirate/quant-data.git
```

```python
from quant_data import Candles
```

## Storage

By default, global data is stored in `~/.qd/data` as yearly Parquet files:

```text
~/.qd/data/
`-- <exchange>/
    `-- <symbol>/
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

System invariant: locally stored data stays continuous and gap-free. If the exchange still does not return missing candles after a retry, the gap is filled with linear interpolation. Interpolations longer than the warning threshold are also recorded in `notes.json`.

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
- an absolute path

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
