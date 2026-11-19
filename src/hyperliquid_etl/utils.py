from datetime import date, timedelta
from typing import Iterator


def date_range(start: date, end: date) -> Iterator[date]:
    if end < start:
        raise ValueError("End date must not be earlier than start date")
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def format_bytes(value: int) -> str:
    size = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if size < 1024 or unit == "TiB":
            return "%.2f %s" % (size, unit)
        size /= 1024
    return "%s B" % value
