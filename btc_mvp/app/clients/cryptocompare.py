from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from app.utils import http_get_json


BASE_URL = "https://min-api.cryptocompare.com/data/v2/news/"


@dataclass(slots=True)
class RawNewsItem:
    item_id: str
    title: str
    body: str
    url: str
    source: str
    published_at: datetime
    categories: str


class CryptoCompareClient:
    def __init__(self, api_key: str) -> None:
        self.api_key = api_key

    def get_latest_news(self, lang: str = "EN", limit: int = 40) -> list[RawNewsItem]:
        if not self.api_key:
            raise RuntimeError("CRYPTOCOMPARE_API_KEY is missing.")

        payload = http_get_json(
            BASE_URL,
            params={"lang": lang, "api_key": self.api_key},
        )

        if payload.get("Type") not in (100, "100"):
            message = payload.get("Message", "Unknown CryptoCompare error.")
            raise RuntimeError(f"CryptoCompare error: {message}")

        raw_items = payload.get("Data", [])
        if not isinstance(raw_items, list):
            raise RuntimeError("CryptoCompare returned an unexpected response shape for news data.")

        return [self._parse_item(item) for item in raw_items[:limit]]

    @staticmethod
    def _parse_item(item: dict) -> RawNewsItem:
        return RawNewsItem(
            item_id=str(item.get("id") or item.get("guid") or item.get("url")),
            title=str(item.get("title") or "").strip(),
            body=str(item.get("body") or "").strip(),
            url=str(item.get("url") or "").strip(),
            source=str(item.get("source_info", {}).get("name") or item.get("source") or "Unknown").strip(),
            published_at=datetime.fromtimestamp(int(item.get("published_on", 0)), tz=timezone.utc),
            categories=str(item.get("categories") or "").strip(),
        )
