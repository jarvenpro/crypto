from __future__ import annotations

from datetime import timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


FALLBACK_TIMEZONES = {
    "Asia/Shanghai": timezone(timedelta(hours=8)),
    "UTC": timezone.utc,
}


def resolve_timezone(name: str):
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError:
        return FALLBACK_TIMEZONES.get(name, timezone.utc)
