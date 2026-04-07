from __future__ import annotations

from dataclasses import dataclass
from time import monotonic, sleep
from typing import Any
import json

import httpx

from app.gateway.config import GatewayConfig


RETRIABLE_STATUS_CODES = {408, 425, 500, 502, 503, 504}


class UpstreamServiceError(RuntimeError):
    def __init__(self, source: str, detail: str, status_code: int = 502) -> None:
        super().__init__(detail)
        self.source = source
        self.detail = detail
        self.status_code = status_code


@dataclass(slots=True)
class _CacheEntry:
    expires_at: float
    value: Any


class TtlCache:
    def __init__(self) -> None:
        self._items: dict[str, _CacheEntry] = {}

    def get(self, key: str) -> Any | None:
        entry = self._items.get(key)
        if entry is None:
            return None
        if entry.expires_at <= monotonic():
            self._items.pop(key, None)
            return None
        return entry.value

    def set(self, key: str, value: Any, ttl_seconds: int) -> None:
        self._items[key] = _CacheEntry(expires_at=monotonic() + ttl_seconds, value=value)


class GatewayHttpClient:
    def __init__(self, config: GatewayConfig) -> None:
        self._config = config
        self._cache = TtlCache()
        self._source_backoff_until: dict[str, float] = {}
        self._client = httpx.Client(
            timeout=config.http_timeout_seconds,
            follow_redirects=True,
            headers={
                "Accept": "application/json, text/plain, */*",
                "User-Agent": config.user_agent,
            },
        )

    def close(self) -> None:
        self._client.close()

    def get_json(
        self,
        source: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        ttl_seconds: int = 0,
        cache_key: str | None = None,
    ) -> Any:
        key = cache_key or self._build_cache_key(source, url, params)
        if ttl_seconds > 0:
            cached = self._cache.get(key)
            if cached is not None:
                return cached

        response = self._request(source, "GET", url, params=params, headers=headers)
        try:
            payload = response.json()
        except json.JSONDecodeError as exc:
            raise UpstreamServiceError(source, f"{source} returned non-JSON data: {exc}") from exc

        if ttl_seconds > 0:
            self._cache.set(key, payload, ttl_seconds)
        return payload

    def get_text(
        self,
        source: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        ttl_seconds: int = 0,
        cache_key: str | None = None,
    ) -> str:
        key = cache_key or self._build_cache_key(source, url, params)
        if ttl_seconds > 0:
            cached = self._cache.get(key)
            if cached is not None:
                return str(cached)

        response = self._request(source, "GET", url, params=params, headers=headers)
        text = response.text
        if ttl_seconds > 0:
            self._cache.set(key, text, ttl_seconds)
        return text

    def _request(
        self,
        source: str,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        attempts = self._config.http_max_retries + 1
        last_error: Exception | None = None
        blocked_until = self._source_backoff_until.get(source)
        now = monotonic()
        if blocked_until and blocked_until > now:
            retry_after = max(1, int(blocked_until - now))
            raise UpstreamServiceError(
                source,
                f"{source} is temporarily cooling down after upstream rate limiting. Retry after {retry_after}s.",
                status_code=503,
            )

        for attempt in range(1, attempts + 1):
            try:
                response = self._client.request(method, url, params=params, headers=headers)
                if response.status_code in RETRIABLE_STATUS_CODES and attempt < attempts:
                    sleep(0.35 * attempt)
                    continue
                response.raise_for_status()
                return response
            except httpx.HTTPStatusError as exc:
                last_error = exc
                status_code = exc.response.status_code
                body_preview = exc.response.text[:400].strip()
                if status_code in {418, 429}:
                    retry_after = self._extract_retry_after_seconds(exc.response)
                    default_cooldown = 120 if status_code == 418 else 60
                    self._source_backoff_until[source] = monotonic() + (retry_after or default_cooldown)
                if status_code in RETRIABLE_STATUS_CODES and attempt < attempts:
                    sleep(0.35 * attempt)
                    continue
                raise UpstreamServiceError(
                    source,
                    f"{source} request failed with HTTP {status_code}: {body_preview}",
                    status_code=502,
                ) from exc
            except httpx.RequestError as exc:
                last_error = exc
                if attempt < attempts:
                    sleep(0.35 * attempt)
                    continue
                raise UpstreamServiceError(source, f"{source} request failed: {exc}") from exc

        raise UpstreamServiceError(source, f"{source} request failed: {last_error}")

    @staticmethod
    def _build_cache_key(source: str, url: str, params: dict[str, Any] | None) -> str:
        if not params:
            return f"{source}:{url}"
        sorted_pairs = "&".join(f"{key}={params[key]}" for key in sorted(params))
        return f"{source}:{url}?{sorted_pairs}"

    @staticmethod
    def _extract_retry_after_seconds(response: httpx.Response) -> int | None:
        value = response.headers.get("Retry-After")
        if value is None:
            return None
        try:
            return max(1, int(float(value)))
        except (TypeError, ValueError):
            return None
