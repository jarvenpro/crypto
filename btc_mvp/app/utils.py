from __future__ import annotations

from datetime import datetime, timezone
import json
from typing import Any
from urllib import parse, request


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def http_get_json(
    url: str,
    params: dict[str, Any] | None = None,
    timeout: int = 20,
    headers: dict[str, str] | None = None,
) -> Any:
    final_url = url
    if params:
        final_url = f"{url}?{parse.urlencode(params)}"

    req = request.Request(final_url, headers=headers or {})
    with request.urlopen(req, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def http_post_json(
    url: str,
    payload: dict[str, Any],
    timeout: int = 20,
    headers: dict[str, str] | None = None,
) -> Any:
    data = json.dumps(payload).encode("utf-8")
    final_headers = {"Content-Type": "application/json"}
    if headers:
        final_headers.update(headers)
    req = request.Request(
        url,
        data=data,
        headers=final_headers,
        method="POST",
    )
    with request.urlopen(req, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


def escape_markdown_text(text: str) -> str:
    escaped = text
    for token in ("\\", "_", "*", "`", "["):
        escaped = escaped.replace(token, f"\\{token}")
    return escaped
