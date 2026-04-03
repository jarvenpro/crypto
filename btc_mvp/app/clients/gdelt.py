from __future__ import annotations

from datetime import datetime, timezone
from urllib.parse import urlencode

from app.clients.cryptocompare import RawNewsItem
from app.telegram_tools import run_powershell_json


BASE_URL = "https://api.gdeltproject.org/api/v2/doc/doc"


class GdeltClient:
    def get_latest_news(
        self,
        query: str,
        timespan_hours: int = 12,
        max_records: int = 15,
    ) -> list[RawNewsItem]:
        params = {
            "query": query,
            "mode": "ArtList",
            "format": "json",
            "sort": "DateDesc",
            "maxrecords": str(max_records),
            "timespan": f"{timespan_hours}h",
        }
        url = f"{BASE_URL}?{urlencode(params)}"
        payload = self._get_via_powershell(url)
        if isinstance(payload, str):
            raise RuntimeError(f"GDELT error: {payload}")
        raw_items = payload.get("articles", [])
        if not isinstance(raw_items, list):
            raise RuntimeError("GDELT returned an unexpected response shape for article data.")
        filtered_items = [item for item in raw_items if str(item.get("language") or "").lower() == "english"]
        return [self._parse_item(item) for item in filtered_items[:max_records]]

    @staticmethod
    def _get_via_powershell(url: str) -> dict:
        script = (
            "[Console]::OutputEncoding = [System.Text.Encoding]::UTF8; "
            "$ProgressPreference = 'SilentlyContinue'; "
            f"$headers = @{{ 'User-Agent' = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' }}; "
            f"$resp = Invoke-RestMethod -Method Get -Uri '{url}' -Headers $headers; "
            "$resp | ConvertTo-Json -Depth 8 -Compress"
        )
        return run_powershell_json(script, timeout=40)

    @staticmethod
    def _parse_item(item: dict) -> RawNewsItem:
        published_at = _parse_gdelt_time(str(item.get("seendate") or ""))
        title = str(item.get("title") or "").strip()
        url = str(item.get("url") or "").strip()
        domain = str(item.get("domain") or "GDELT").strip() or "GDELT"

        return RawNewsItem(
            item_id=url or title,
            title=title,
            body="",
            url=url,
            source=domain,
            published_at=published_at,
            categories="gdelt",
        )


def _parse_gdelt_time(value: str) -> datetime:
    try:
        return datetime.strptime(value, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return datetime.now(timezone.utc)
