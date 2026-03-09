from __future__ import annotations

import time
from typing import Callable

from quant_data.models import ExchangeProtocol, GapWarning
from quant_data.utils import timeframe_to_milliseconds

BatchProgress = Callable[[int], None]
CandleRow = dict[str, float | int]
BATCH_LIMIT = 1_000


def instantiate_exchange(exchange_id: str, market_type: str) -> ExchangeProtocol:
    import ccxt

    try:
        exchange_class = getattr(ccxt, exchange_id)
    except AttributeError as error:
        raise ValueError(f"Unknown exchange: {exchange_id}") from error

    return exchange_class({"enableRateLimit": True, "options": {"defaultType": market_type}})


def fetch_range_rows(
    exchange: ExchangeProtocol,
    symbol: str,
    timeframe: str,
    start_ms: int,
    end_ms: int,
    *,
    gap_warning_threshold_minutes: int,
    previous_row: CandleRow | None = None,
    progress_callback: BatchProgress | None = None,
) -> tuple[list[CandleRow], list[GapWarning]]:
    step_ms = timeframe_to_milliseconds(timeframe)
    current = start_ms
    emitted_rows: list[CandleRow] = []
    warnings: list[GapWarning] = []

    while current < end_ms:
        raw_batch = _fetch_range_once(exchange, symbol, timeframe, current, end_ms)
        if not raw_batch:
            break

        if previous_row is None and raw_batch[0]["ts"] > current:
            current = int(raw_batch[0]["ts"])

        repaired_rows, batch_warnings = _repair_batch(
            exchange,
            symbol,
            timeframe,
            previous_row,
            raw_batch,
            gap_warning_threshold_minutes,
        )

        repaired_rows = [row for row in repaired_rows if current <= int(row["ts"]) < end_ms]
        if emitted_rows:
            last_ts = int(emitted_rows[-1]["ts"])
            repaired_rows = [row for row in repaired_rows if int(row["ts"]) > last_ts]

        if not repaired_rows:
            break

        emitted_rows.extend(repaired_rows)
        warnings.extend(batch_warnings)
        previous_row = repaired_rows[-1]
        current = int(previous_row["ts"]) + step_ms

        if progress_callback:
            progress_callback(len(repaired_rows))

        if len(raw_batch) < BATCH_LIMIT:
            break

    return emitted_rows, warnings


def _fetch_range_once(
    exchange: ExchangeProtocol,
    symbol: str,
    timeframe: str,
    start_ms: int,
    end_ms: int,
    *,
    limit: int = BATCH_LIMIT,
) -> list[CandleRow]:
    rows = _normalize_rows(_fetch_ohlcv_with_retry(exchange, symbol, timeframe, start_ms, limit))
    return [row for row in rows if start_ms <= int(row["ts"]) < end_ms]


def _fetch_raw_range(
    exchange: ExchangeProtocol,
    symbol: str,
    timeframe: str,
    start_ms: int,
    end_ms: int,
) -> list[CandleRow]:
    step_ms = timeframe_to_milliseconds(timeframe)
    current = start_ms
    collected: list[CandleRow] = []

    while current < end_ms:
        batch = _fetch_range_once(exchange, symbol, timeframe, current, end_ms)
        if not batch:
            break

        if collected:
            last_ts = int(collected[-1]["ts"])
            batch = [row for row in batch if int(row["ts"]) > last_ts]

        if not batch:
            break

        collected.extend(batch)
        next_current = int(batch[-1]["ts"]) + step_ms
        if next_current <= current:
            break
        current = next_current

        if len(batch) < BATCH_LIMIT:
            break

    return collected


def _repair_batch(
    exchange: ExchangeProtocol,
    symbol: str,
    timeframe: str,
    previous_row: CandleRow | None,
    raw_batch: list[CandleRow],
    gap_warning_threshold_minutes: int,
) -> tuple[list[CandleRow], list[GapWarning]]:
    step_ms = timeframe_to_milliseconds(timeframe)
    repaired: list[CandleRow] = []
    warnings: list[GapWarning] = []
    cursor = previous_row

    for row in raw_batch:
        if cursor is not None and int(row["ts"]) > int(cursor["ts"]) + step_ms:
            gap_rows, gap_warnings = _fill_gap(
                exchange,
                symbol,
                timeframe,
                cursor,
                row,
                gap_warning_threshold_minutes,
            )
            repaired.extend(gap_rows)
            warnings.extend(gap_warnings)

        repaired.append(row)
        cursor = row

    return repaired, warnings


def _fill_gap(
    exchange: ExchangeProtocol,
    symbol: str,
    timeframe: str,
    left: CandleRow,
    right: CandleRow,
    gap_warning_threshold_minutes: int,
) -> tuple[list[CandleRow], list[GapWarning]]:
    step_ms = timeframe_to_milliseconds(timeframe)
    between = _fetch_raw_range(
        exchange,
        symbol,
        timeframe,
        int(left["ts"]) + step_ms,
        int(right["ts"]),
    )
    candidates = [row for row in between if int(left["ts"]) < int(row["ts"]) < int(right["ts"])]
    repaired: list[CandleRow] = []
    warnings: list[GapWarning] = []
    anchor = left

    for candidate in [*candidates, right]:
        missing = (int(candidate["ts"]) - int(anchor["ts"])) // step_ms - 1
        if missing > 0:
            interpolated_rows = _interpolate_rows(anchor, candidate, step_ms, missing)
            repaired.extend(interpolated_rows)
            duration_minutes = missing * step_ms / 60_000
            if duration_minutes > gap_warning_threshold_minutes:
                warnings.append(
                    GapWarning(
                        start_ts=int(interpolated_rows[0]["ts"]),
                        end_ts=int(interpolated_rows[-1]["ts"]),
                        candles=len(interpolated_rows),
                    )
                )

        if candidate is not right:
            repaired.append(candidate)
        anchor = candidate

    return repaired, warnings


def _interpolate_rows(
    left: CandleRow,
    right: CandleRow,
    step_ms: int,
    missing: int,
) -> list[CandleRow]:
    rows: list[CandleRow] = []
    for index in range(1, missing + 1):
        ratio = index / (missing + 1)
        rows.append(
            {
                "ts": int(left["ts"]) + step_ms * index,
                "open": _interpolate_value(float(left["open"]), float(right["open"]), ratio),
                "high": _interpolate_value(float(left["high"]), float(right["high"]), ratio),
                "low": _interpolate_value(float(left["low"]), float(right["low"]), ratio),
                "close": _interpolate_value(float(left["close"]), float(right["close"]), ratio),
                "volume": _interpolate_value(float(left["volume"]), float(right["volume"]), ratio),
            }
        )
    return rows


def _interpolate_value(start: float, end: float, ratio: float) -> float:
    return start + (end - start) * ratio


def _fetch_ohlcv_with_retry(
    exchange: ExchangeProtocol,
    symbol: str,
    timeframe: str,
    since_ms: int,
    limit: int,
) -> list[list[float]]:
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            return exchange.fetch_ohlcv(symbol, timeframe, since=since_ms, limit=limit)
        except Exception as error:  # noqa: BLE001
            last_error = error
            if attempt == 2:
                break
            time.sleep(0.1 * (2**attempt))

    if last_error is None:
        return []
    raise last_error


def _normalize_rows(raw_rows: list[list[float]]) -> list[CandleRow]:
    unique: dict[int, CandleRow] = {}
    for raw in raw_rows:
        if len(raw) < 6:
            continue
        ts = int(raw[0])
        unique[ts] = {
            "ts": ts,
            "open": float(raw[1]),
            "high": float(raw[2]),
            "low": float(raw[3]),
            "close": float(raw[4]),
            "volume": float(raw[5]),
        }
    return [unique[ts] for ts in sorted(unique)]
