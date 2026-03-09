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

By default, data is stored in `~/.quant-data/storage` as yearly Parquet files:

```text
~/.quant-data/storage/
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

The project reads `~/.quant-data/config.toml` by default, or the path from `QUANT_DATA_CONFIG`.
For repo-local runs, copy `config.example.toml` to `config.local.toml` and point
`QUANT_DATA_CONFIG` to it.

```toml
storage_path = "~/.quant-data/storage"
gap_warning_threshold = 30

[exchanges.binance]
type = "spot"
```

## CLI

```bash
qd list
qd markets --exchange binance --quote USDT
qd download --exchange binance --symbol BTC/USDT --timeframe 1m --from 2024-01-01 --to 2024-01-31
qd update --exchange binance
qd check --exchange binance --symbol BTC/USDT --timeframe 1m
qd remove --exchange binance --symbol BTC/USDT --timeframe 1m
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
