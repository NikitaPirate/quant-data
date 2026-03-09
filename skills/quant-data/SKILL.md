---
name: quant-data
description: Work with the quant-data CLI and Python library for local OHLCV candle storage, inspection, download, update, validation, and configuration. Use when Codex needs to operate `qd`, inspect `qd_config.toml`, load candles through `quant_data.Candles`, or determine which exchanges, data flows, and market types quant-data currently supports.
---

# Quant Data

## Overview

Use the CLI as the operational source of truth and the Python API as the notebook/backtest interface. Prefer asking the tool what it supports instead of hardcoding assumptions in the answer.

## Quick Start

Run commands with `uv run` from the repository root.

Use these commands first when support boundaries or config location matter:

- `uv run qd capabilities --json`
- `uv run qd config show --json`
- `uv run qd install skill ~/.claude --json`
- `uv run qd install skill ~/.codex --codex --json`

Use the default human-readable output when the user should be able to visually verify the result. Use `--json` when another agent step needs machine-readable output.

## Workflow

1. Inspect runtime context.
Run `uv run qd config show --json` to discover the active config source, resolved data path, and configured exchanges.

2. Inspect product scope.
Run `uv run qd capabilities --json` before answering questions about supported exchanges, supported market types, or unsupported data flows.

3. Choose the interface.
Use CLI commands for storage management and operator-facing tasks. Use `from quant_data import Candles` and `Candles.load(...)` when the user needs a `pandas.DataFrame`.

4. Prefer JSON for chaining.
For scripted or agent-to-agent workflows, add `--json` to CLI commands and parse the payload. For human review, rerun the same command without `--json`.

5. Keep support claims dynamic.
Do not maintain a handwritten list of supported exchanges in the answer. Read `configured_exchanges` and `exchange_support_model` from `qd capabilities`, then explain the result.

## Core Commands

- `uv run qd config show [--json]`
- `uv run qd capabilities [--json]`
- `uv run qd install skill <runtime-root> [--codex] [--json]`
- `uv run qd list [--exchange <id>] [--symbol <symbol>] [--json]`
- `uv run qd markets --exchange <id> [--quote <quote>] [--base <base>] [--json]`
- `uv run qd download --exchange <id> --symbol <symbol> --timeframe <tf> [--from <date>] [--to <date>] [--json]`
- `uv run qd update [--exchange <id>] [--symbol <symbol>] [--json]`
- `uv run qd check [--exchange <id>] [--symbol <symbol>] [--timeframe <tf>] [--json]`
- `uv run qd remove --exchange <id> --symbol <symbol> [--timeframe <tf>] [--from <date>] [--to <date>] [--json]`

`qd remove` is interactive and asks for confirmation.
`qd install skill` copies the bundled skill into `<runtime-root>/skills/quant-data`.
Use `--codex` only for Codex runtimes; it also writes `<runtime-root>/agents/quant-data.yaml`.
`<runtime-root>` may be absolute, home-relative like `~/.claude`, or relative to the current working directory like `.codex`.

## Python API

Use the library when a dataframe is needed inside Python code:

```python
from quant_data import Candles

frame = Candles.load(
    "binance",
    "BTC/USDT",
    "1h",
    from_date="2024-01-01",
    to_date="2024-01-07",
)
```

## Guardrails

- Do not claim support for trades, order books, futures, options, funding, or other non-candle flows unless `qd capabilities` says that changed.
- Do not re-derive config discovery rules when `qd config show` can answer directly.
- Keep the skill aligned with the actual CLI flags, JSON payloads, and `Candles.load` behavior.
