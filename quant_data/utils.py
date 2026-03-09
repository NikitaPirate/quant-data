from __future__ import annotations

from datetime import UTC, date, datetime, timedelta


def timeframe_to_milliseconds(timeframe: str) -> int:
    units = {"m": 60_000, "h": 3_600_000, "d": 86_400_000}
    unit = timeframe[-1]
    if unit not in units:
        raise ValueError(f"Unsupported timeframe: {timeframe}")

    amount = int(timeframe[:-1])
    return amount * units[unit]


def symbol_to_storage_name(symbol: str) -> str:
    return symbol.replace("/", "-")


def storage_name_to_symbol(symbol: str) -> str:
    return symbol.replace("-", "/")


def ensure_utc_datetime(value: str | date | datetime) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time(), tzinfo=UTC)

    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        if "T" in value or " " in value:
            return parsed.replace(tzinfo=UTC)
        return datetime.combine(parsed.date(), datetime.min.time(), tzinfo=UTC)
    return parsed.astimezone(UTC)


def parse_from_bound(value: str | date | datetime | None) -> int | None:
    if value is None:
        return None

    moment = ensure_utc_datetime(value)
    boundary = datetime.combine(moment.date(), datetime.min.time(), tzinfo=UTC)
    return int(boundary.timestamp() * 1000)


def parse_to_bound_exclusive(value: str | date | datetime | None) -> int | None:
    if value is None:
        return None

    moment = ensure_utc_datetime(value)
    boundary = datetime.combine(moment.date(), datetime.min.time(), tzinfo=UTC) + timedelta(days=1)
    return int(boundary.timestamp() * 1000)


def default_to_exclusive_utc() -> int:
    yesterday = datetime.now(UTC).date() - timedelta(days=1)
    boundary = datetime.combine(yesterday + timedelta(days=1), datetime.min.time(), tzinfo=UTC)
    return int(boundary.timestamp() * 1000)


def utc_date_string(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000, tz=UTC).strftime("%Y-%m-%d")


def utc_datetime_string(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000, tz=UTC).strftime("%Y-%m-%d %H:%M")


def year_from_timestamp(ts: int) -> int:
    return datetime.fromtimestamp(ts / 1000, tz=UTC).year
