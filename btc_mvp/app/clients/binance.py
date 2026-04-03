from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from app.utils import http_get_json


SPOT_BASE = "https://api.binance.com"
FUTURES_BASE = "https://fapi.binance.com"


@dataclass(slots=True)
class Kline:
    open_time: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float


class BinanceClient:
    def get_spot_klines(self, symbol: str, interval: str, limit: int) -> list[Kline]:
        raw = http_get_json(
            f"{SPOT_BASE}/api/v3/klines",
            {"symbol": symbol, "interval": interval, "limit": limit},
        )
        return [self._parse_kline(item) for item in raw]

    def get_book_ticker(self, symbol: str) -> dict[str, float]:
        raw = http_get_json(
            f"{SPOT_BASE}/api/v3/ticker/bookTicker",
            {"symbol": symbol},
        )
        return {
            "bid_price": float(raw["bidPrice"]),
            "ask_price": float(raw["askPrice"]),
            "bid_qty": float(raw["bidQty"]),
            "ask_qty": float(raw["askQty"]),
        }

    def get_open_interest(self, symbol: str) -> float:
        raw = http_get_json(
            f"{FUTURES_BASE}/fapi/v1/openInterest",
            {"symbol": symbol},
        )
        return float(raw["openInterest"])

    def get_open_interest_hist(self, symbol: str, period: str = "5m", limit: int = 12) -> list[dict[str, Any]]:
        raw = http_get_json(
            f"{FUTURES_BASE}/futures/data/openInterestHist",
            {"symbol": symbol, "period": period, "limit": limit},
        )
        return raw

    def get_taker_ratio(self, symbol: str, period: str = "5m", limit: int = 12) -> list[dict[str, Any]]:
        raw = http_get_json(
            f"{FUTURES_BASE}/futures/data/takerlongshortRatio",
            {"symbol": symbol, "period": period, "limit": limit},
        )
        return raw

    def get_global_long_short_ratio(self, symbol: str, period: str = "5m", limit: int = 12) -> list[dict[str, Any]]:
        raw = http_get_json(
            f"{FUTURES_BASE}/futures/data/globalLongShortAccountRatio",
            {"symbol": symbol, "period": period, "limit": limit},
        )
        return raw

    def get_funding_rate(self, symbol: str, limit: int = 3) -> list[dict[str, Any]]:
        raw = http_get_json(
            f"{FUTURES_BASE}/fapi/v1/fundingRate",
            {"symbol": symbol, "limit": limit},
        )
        return raw

    @staticmethod
    def _parse_kline(item: list[Any]) -> Kline:
        return Kline(
            open_time=datetime.fromtimestamp(item[0] / 1000, tz=timezone.utc),
            open_price=float(item[1]),
            high_price=float(item[2]),
            low_price=float(item[3]),
            close_price=float(item[4]),
            volume=float(item[5]),
        )
