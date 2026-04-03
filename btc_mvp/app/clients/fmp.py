from __future__ import annotations

from app.utils import http_get_json


BASE_URL = "https://financialmodelingprep.com/stable/economic-calendar"


class FmpClient:
    def __init__(self, api_key: str) -> None:
        self.api_key = api_key

    def get_economic_calendar(self, date_from: str, date_to: str) -> list[dict]:
        if not self.api_key:
            raise RuntimeError("FMP_API_KEY is missing.")

        payload = http_get_json(
            BASE_URL,
            params={
                "from": date_from,
                "to": date_to,
                "apikey": self.api_key,
            },
            timeout=20,
        )
        if not isinstance(payload, list):
            raise RuntimeError(f"Unexpected FMP economic calendar response: {payload}")
        return payload
