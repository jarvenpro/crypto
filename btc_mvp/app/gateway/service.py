from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from time import monotonic
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
import html
import re
from xml.etree import ElementTree

from app.gateway.config import GatewayConfig
from app.gateway.http import GatewayHttpClient, UpstreamServiceError
from app.gateway.liquidity import LiquidityContextBuilder


BINANCE_SPOT_BASE = "https://api.binance.com"
BINANCE_FUTURES_BASE = "https://fapi.binance.com"
OKX_BASE = "https://www.okx.com"
TREASURY_BASE = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od"
BLS_BASE = "https://api.bls.gov/publicAPI/v2/timeseries/data"
FRED_BASE = "https://api.stlouisfed.org/fred"
BEA_BASE = "https://apps.bea.gov/api/data"
SEC_FILES_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions"
CFTC_COT_URL = "https://publicreporting.cftc.gov/resource/6dca-aqww.json"
FED_MONETARY_FEED = "https://www.federalreserve.gov/feeds/press_monetary.xml"
FOMC_SCHEDULE_URL = "https://www.federalreserve.gov/newsevents/pressreleases/monetary20240809a.htm"
MEMPOOL_FEES_URL = "https://mempool.space/api/v1/fees/recommended"
FEAR_GREED_URL = "https://api.alternative.me/fng/"
COINGECKO_SIMPLE_PRICE_URL = "https://api.coingecko.com/api/v3/simple/price"
COINGLASS_BASE = "https://open-api-v4.coinglass.com"
BYBIT_BASE = "https://api.bybit.com"
DERIBIT_BASE = "https://www.deribit.com/api/v2"
BLS_RELEASE_CALENDAR_ICS_URL = "https://www.bls.gov/schedule/news_release/bls.ics"
BEA_RELEASE_DATES_URL = "https://apps.bea.gov/API/signup/release_dates.json"
TREASURY_UPCOMING_AUCTIONS_URL = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/upcoming_auctions"

COINGECKO_IDS = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "SOL": "solana",
}

DEFAULT_FRED_SERIES = ("FEDFUNDS", "DGS10", "UNRATE")
DEFAULT_BITCOIN_ETF_ENTITIES = ("IBIT", "FBTC", "GBTC")
NY_TZ = "America/New_York"


@dataclass(slots=True)
class _MacroEvent:
    source: str
    event_name: str
    category: str
    importance: str
    scheduled_at_utc: datetime | None
    scheduled_date: date | None
    time_precision: str
    metadata: dict[str, Any]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@dataclass(slots=True)
class _MemoEntry:
    expires_at: float
    stale_until: float
    refreshed_at_monotonic: float
    refreshed_at_iso: str
    value: Any


class GatewayService:
    def __init__(self, config: GatewayConfig, http_client: GatewayHttpClient) -> None:
        self._config = config
        self._http = http_client
        self._memo_cache: dict[str, _MemoEntry] = {}

    def health(self) -> dict[str, Any]:
        return {
            "ok": True,
            "service": "crypto-gpt-aggregator",
            "generated_at": utc_now_iso(),
            "configured_sources": {
                "fred": self._config.fred_enabled,
                "bls": self._config.bls_enabled,
                "bea": self._config.bea_enabled,
                "coingecko_key": self._config.coingecko_auth_enabled,
                "coinglass": self._config.coinglass_enabled,
                "sec": True,
                "cftc": True,
                "fed_rss": True,
                "binance": True,
                "okx": True,
                "mempool": True,
                "fear_greed": True,
                "treasury": True,
            },
        }

    def crypto_overview(self, symbol: str) -> dict[str, Any]:
        symbol = symbol.upper()
        return self._memoize(
            f"crypto_overview:{symbol}",
            ttl_seconds=20,
            stale_if_error_seconds=120,
            builder=lambda: self._build_crypto_overview(symbol),
        )

    def _build_crypto_overview(self, symbol: str) -> dict[str, Any]:
        root_symbol = self._extract_root_asset(symbol)
        coingecko_id = COINGECKO_IDS.get(root_symbol)
        with ThreadPoolExecutor(max_workers=6) as executor:
            okx_market_future = executor.submit(self._capture, lambda: self.get_okx_market_overview(symbol))
            okx_derivatives_future = executor.submit(self._capture, lambda: self.get_okx_derivatives_overview(symbol))
            bybit_future = executor.submit(self._capture, lambda: self.get_bybit_market_structure(symbol))
            fear_greed_future = executor.submit(self._capture, self.get_fear_greed_latest)
            coingecko_future = (
                executor.submit(self._capture, lambda: self.get_coingecko_simple_price(coingecko_id))
                if coingecko_id
                else None
            )

            okx_market = okx_market_future.result()
            reference_price = None
            if okx_market.get("ok"):
                reference_price = okx_market.get("data", {}).get("last_price")

            okx_structure_future = executor.submit(
                self._capture,
                lambda: self.get_okx_multi_timeframe_overview(symbol, reference_price=reference_price),
            )

            okx_derivatives = okx_derivatives_future.result()
            bybit = bybit_future.result()
            fear_greed = fear_greed_future.result()
            coingecko = (
                coingecko_future.result()
                if coingecko_future is not None
                else self._skipped(f"No CoinGecko asset mapping configured for {root_symbol}.")
            )
            okx_structure = okx_structure_future.result()

        payload = {
            "generated_at": utc_now_iso(),
            "symbol": symbol,
            "sources": {
                "okx": okx_market,
                "okx_structure": okx_structure,
                "okx_derivatives": okx_derivatives,
                "bybit": bybit,
                "fear_greed": fear_greed,
                "mempool": self._skipped("Excluded from default overview to keep the endpoint fast and stable. Call /v1/sources/mempool/fees separately when needed."),
            },
        }

        payload["sources"]["coingecko"] = coingecko

        return payload

    def _memoize(
        self,
        key: str,
        ttl_seconds: int,
        builder,
        *,
        stale_if_error_seconds: int = 0,
    ) -> Any:
        now = monotonic()
        cached = self._memo_cache.get(key)
        if cached and cached.expires_at > now:
            return self._with_cache_metadata(cached, now=now, status="cached")

        try:
            value = builder()
        except UpstreamServiceError as exc:
            if cached and cached.stale_until > now:
                return self._with_cache_metadata(
                    cached,
                    now=now,
                    status="stale",
                    reason=exc.detail,
                    source=exc.source,
                )
            raise

        refreshed_at_iso = utc_now_iso()
        entry = _MemoEntry(
            expires_at=now + ttl_seconds,
            stale_until=now + ttl_seconds + max(0, stale_if_error_seconds),
            refreshed_at_monotonic=now,
            refreshed_at_iso=refreshed_at_iso,
            value=self._strip_cache_metadata(value),
        )
        self._memo_cache[key] = entry
        return self._with_cache_metadata(entry, now=now, status="live")

    @staticmethod
    def _strip_cache_metadata(value: Any) -> Any:
        sanitized = deepcopy(value)

        def scrub(item: Any) -> None:
            if isinstance(item, dict):
                item.pop("_cache", None)
                for nested in item.values():
                    scrub(nested)
                return
            if isinstance(item, list):
                for nested in item:
                    scrub(nested)

        scrub(sanitized)
        return sanitized

    @staticmethod
    def _with_cache_metadata(
        entry: _MemoEntry,
        *,
        now: float,
        status: str,
        reason: str | None = None,
        source: str | None = None,
    ) -> Any:
        value = deepcopy(entry.value)
        if not isinstance(value, dict):
            return value

        cache = {
            "status": status,
            "refreshed_at": entry.refreshed_at_iso,
            "age_seconds": max(0, int(now - entry.refreshed_at_monotonic)),
            "expires_in_seconds": max(0, int(entry.expires_at - now)),
        }
        if status == "stale":
            cache["stale_if_error_expires_in_seconds"] = max(0, int(entry.stale_until - now))
        if reason:
            cache["reason"] = reason
        if source:
            cache["source"] = source

        value["_cache"] = cache
        return value

    def macro_overview(self, fred_series_ids: list[str] | None = None) -> dict[str, Any]:
        series_ids = fred_series_ids or list(DEFAULT_FRED_SERIES)
        return {
            "generated_at": utc_now_iso(),
            "sources": {
                "fed": self._capture(self.get_fed_monetary_feed),
                "treasury": self._capture(self.get_treasury_latest_avg_rates),
                "bls": self._capture(lambda: self.get_bls_series("CUUR0000SA0", limit=12)),
                "fred": self._capture(lambda: self.get_fred_series_bundle(series_ids)),
                "bea": self._capture(self.get_bea_gdp),
            },
        }

    def regulatory_overview(self, entities: list[str] | None = None) -> dict[str, Any]:
        sec_entities = entities or list(DEFAULT_BITCOIN_ETF_ENTITIES)
        return {
            "generated_at": utc_now_iso(),
            "sources": {
                "cftc": self._capture(self.get_cftc_bitcoin_cot),
                "sec": self._capture(lambda: self.get_sec_recent_filings_for_entities(sec_entities)),
            },
        }

    def get_macro_event_calendar(self, horizon_hours: int = 72, major_only: bool = True) -> dict[str, Any]:
        horizon_hours = max(8, min(int(horizon_hours), 336))
        cache_key = f"macro_event_calendar:{horizon_hours}:{major_only}"
        return self._memoize(
            cache_key,
            ttl_seconds=1800,
            stale_if_error_seconds=14400,
            builder=lambda: self._build_macro_event_calendar(horizon_hours=horizon_hours, major_only=major_only),
        )

    def _build_macro_event_calendar(self, *, horizon_hours: int, major_only: bool) -> dict[str, Any]:
        now_utc = datetime.now(timezone.utc)
        horizon_end = now_utc + timedelta(hours=horizon_hours)

        events: list[_MacroEvent] = []
        source_status: dict[str, Any] = {}
        for source_name, fetcher in (
            ("bls", self._fetch_bls_release_events),
            ("bea", self._fetch_bea_release_events),
            ("fomc", self._fetch_fomc_schedule_events),
            ("treasury", self._fetch_treasury_auction_events),
        ):
            try:
                fetched = fetcher()
                events.extend(fetched)
                source_status[source_name] = {"ok": True, "count": len(fetched)}
            except UpstreamServiceError as exc:
                source_status[source_name] = {"ok": False, "reason": exc.detail}

        allowed_importance = {"very_high", "high", "medium"} if major_only else {"very_high", "high", "medium", "low"}
        upcoming_events: list[_MacroEvent] = []
        for event in events:
            if event.importance not in allowed_importance:
                continue
            if event.scheduled_at_utc is not None:
                if now_utc <= event.scheduled_at_utc <= horizon_end:
                    upcoming_events.append(event)
                continue
            if event.scheduled_date is None:
                continue
            if now_utc.date() <= event.scheduled_date <= horizon_end.date():
                upcoming_events.append(event)

        upcoming_events = self._dedupe_macro_events(upcoming_events)
        normalized_events = [self._serialize_macro_event(event, now_utc=now_utc) for event in upcoming_events]

        next_8h = [event for event in normalized_events if event.get("is_within_next_8h")]
        next_24h = [event for event in normalized_events if event.get("starts_in_hours") is not None and event["starts_in_hours"] <= 24]
        date_only_events = [event for event in normalized_events if event.get("time_precision") == "date"]

        return {
            "generated_at": utc_now_iso(),
            "current_time_utc": now_utc.replace(microsecond=0).isoformat(),
            "horizon_hours": horizon_hours,
            "major_only": major_only,
            "summary": {
                "events_within_horizon": len(normalized_events),
                "events_within_next_8h": len(next_8h),
                "events_within_next_24h": len(next_24h),
                "date_only_events_within_horizon": len(date_only_events),
                "highest_importance_within_next_8h": self._highest_event_importance(next_8h),
                "sources_covered": sorted({event["source"] for event in normalized_events}),
            },
            "source_status": source_status,
            "events": normalized_events,
        }

    def get_liquidity_summary(
        self,
        symbol: str = "BTCUSDT",
        depth_limit: int = 100,
        include_binance: bool = False,
        include_okx: bool = True,
        include_bybit: bool = True,
    ) -> dict[str, Any]:
        symbol = symbol.upper()
        requested_depth_limit = max(50, min(int(depth_limit), 500))
        cache_key = f"liquidity_summary:{symbol}:{requested_depth_limit}:{include_binance}:{include_okx}:{include_bybit}"
        return self._memoize(
            cache_key,
            ttl_seconds=20,
            stale_if_error_seconds=120,
            builder=lambda: self._build_liquidity_summary(
                symbol=symbol,
                depth_limit=requested_depth_limit,
                include_binance=include_binance,
                include_okx=include_okx,
                include_bybit=include_bybit,
            ),
        )

    def _build_liquidity_summary(
        self,
        *,
        symbol: str,
        depth_limit: int,
        include_binance: bool,
        include_okx: bool,
        include_bybit: bool,
    ) -> dict[str, Any]:
        liquidity = LiquidityContextBuilder(self._http)
        venues: dict[str, Any] = {}
        source_status: dict[str, Any] = {}
        depth_limits: dict[str, int | None] = {
            "requested": depth_limit,
            "binance": liquidity._normalize_binance_depth_limit(depth_limit) if include_binance else None,
            "okx": min(depth_limit, 200) if include_okx else None,
            "bybit": min(depth_limit, 200) if include_bybit else None,
        }

        reference_price: float | None = None
        if include_okx:
            try:
                okx_orderbook = liquidity._get_okx_orderbook(symbol, depth_limit=depth_limits["okx"] or 100)
                reference_price = okx_orderbook.get("mid_price") or reference_price
                venues["okx"] = {
                    "ok": True,
                    "orderbook": liquidity._build_orderbook_summary("OKX", okx_orderbook, reference_price),
                }
                source_status["okx"] = {"ok": True}
            except UpstreamServiceError as exc:
                source_status["okx"] = {"ok": False, "reason": exc.detail}
        if include_bybit:
            try:
                bybit_orderbook = liquidity._get_bybit_orderbook(symbol, depth_limit=depth_limits["bybit"] or 100)
                reference_price = reference_price or bybit_orderbook.get("mid_price")
                venues["bybit"] = {
                    "ok": True,
                    "orderbook": liquidity._build_orderbook_summary("BYBIT", bybit_orderbook, reference_price),
                }
                source_status["bybit"] = {"ok": True}
            except UpstreamServiceError as exc:
                source_status["bybit"] = {"ok": False, "reason": exc.detail}
        if include_binance:
            try:
                binance_orderbook = liquidity._get_binance_orderbook(symbol, depth_limit=depth_limits["binance"] or 50)
                reference_price = reference_price or binance_orderbook.get("mid_price")
                venues["binance"] = {
                    "ok": True,
                    "orderbook": liquidity._build_orderbook_summary("BINANCE", binance_orderbook, reference_price),
                }
                source_status["binance"] = {"ok": True}
            except UpstreamServiceError as exc:
                source_status["binance"] = {"ok": False, "reason": exc.detail}

        if not venues:
            raise UpstreamServiceError("liquidity", "At least one exchange must be enabled for liquidity summary.")

        combined = self._build_orderbook_liquidity_view(venues, reference_price)
        return {
            "generated_at": utc_now_iso(),
            "symbol": symbol,
            "reference_price": reference_price,
            "depth_limits": depth_limits,
            "venues": venues,
            "source_status": source_status,
            "combined": combined,
            "limitations": {
                "liquidation_heatmap": {
                    "available": False,
                    "reason": "No stable free REST source provides a complete liquidation heatmap.",
                    "replacement": "Use cross-exchange orderbook walls, band depth imbalance, and sweep-risk summary as the free proxy.",
                },
                "hanging_order_map": {
                    "available": True,
                    "scope": "Snapshot-based orderbook walls and near-price heatmap bands.",
                },
            },
        }

    def get_liquidity_context(
        self,
        symbol: str = "BTCUSDT",
        depth_limit: int = 100,
        liquidation_sample_seconds: int = 4,
        include_binance: bool = False,
        include_okx: bool = True,
        include_bybit: bool = True,
    ) -> dict[str, Any]:
        symbol = symbol.upper()
        requested_depth_limit = max(50, min(int(depth_limit), 500))
        liquidation_sample_seconds = max(2, min(int(liquidation_sample_seconds), 8))
        cache_key = (
            f"liquidity_context:{symbol}:{requested_depth_limit}:{liquidation_sample_seconds}:"
            f"{include_binance}:{include_okx}:{include_bybit}"
        )
        return self._memoize(
            cache_key,
            ttl_seconds=15,
            stale_if_error_seconds=60,
            builder=lambda: self._build_liquidity_context(
                symbol=symbol,
                depth_limit=requested_depth_limit,
                liquidation_sample_seconds=liquidation_sample_seconds,
                include_binance=include_binance,
                include_okx=include_okx,
                include_bybit=include_bybit,
            ),
        )

    def _build_liquidity_context(
        self,
        *,
        symbol: str,
        depth_limit: int,
        liquidation_sample_seconds: int,
        include_binance: bool,
        include_okx: bool,
        include_bybit: bool,
    ) -> dict[str, Any]:
        liquidity = LiquidityContextBuilder(self._http)
        payload = asyncio.run(
            liquidity.build_context(
                symbol=symbol,
                depth_limit=depth_limit,
                liquidation_sample_seconds=liquidation_sample_seconds,
                include_binance=include_binance,
                include_bybit=include_bybit,
                include_okx=include_okx,
            )
        )
        payload["limitations"] = {
            "scope": "Short sampled liquidation flow and current orderbook structure.",
            "not_included": [
                "long-history liquidation heatmap",
                "full orderbook replay",
                "commercial liquidation clustering models",
            ],
        }
        return payload

    def get_okx_market_overview(self, symbol: str = "BTCUSDT") -> dict[str, Any]:
        symbol = symbol.upper()
        return self._memoize(
            f"okx_market_overview:{symbol}",
            ttl_seconds=30,
            stale_if_error_seconds=180,
            builder=lambda: self._build_okx_market_overview(symbol),
        )

    def _build_okx_market_overview(self, symbol: str = "BTCUSDT") -> dict[str, Any]:
        symbol = symbol.upper()
        swap_inst_id = self._to_okx_swap_inst_id(symbol)
        index_inst_id = self._to_okx_index_inst_id(symbol)

        ticker = self._okx_get_first_row(
            "/api/v5/market/ticker",
            params={"instId": swap_inst_id},
            ttl_seconds=20,
        )
        mark_price_row = self._okx_get_first_row(
            "/api/v5/public/mark-price",
            params={"instType": "SWAP", "instId": swap_inst_id},
            ttl_seconds=20,
        )
        index_ticker = self._okx_get_first_row(
            "/api/v5/market/index-tickers",
            params={"instId": index_inst_id},
            ttl_seconds=20,
        )

        last_price = self._safe_float(ticker.get("last"))
        open_24h = self._safe_float(ticker.get("open24h"))
        mark_price = self._safe_float(mark_price_row.get("markPx"))
        index_price = self._safe_float(index_ticker.get("idxPx"))

        return {
            "symbol": symbol,
            "exchange": "OKX",
            "inst_id": swap_inst_id,
            "index_inst_id": index_inst_id,
            "last_price": last_price,
            "book_ticker": {
                "bid_price": self._safe_float(ticker.get("bidPx")),
                "ask_price": self._safe_float(ticker.get("askPx")),
                "bid_qty": self._safe_float(ticker.get("bidSz")),
                "ask_qty": self._safe_float(ticker.get("askSz")),
            },
            "ticker_24h": {
                "price_change_pct": self._compute_change_pct(open_24h, last_price),
                "open_price": open_24h,
                "high_price": self._safe_float(ticker.get("high24h")),
                "low_price": self._safe_float(ticker.get("low24h")),
                "volume": self._safe_float(ticker.get("vol24h")),
                "base_volume": self._safe_float(ticker.get("volCcy24h")),
            },
            "mark_price": mark_price,
            "index_price": index_price,
            "mark_index_spread": self._mark_index_spread(mark_price, index_price),
            "swap_index_basis": None if last_price is None or index_price is None else round(last_price - index_price, 6),
            "swap_index_basis_pct": self._pct_of_value(None if last_price is None or index_price is None else round(last_price - index_price, 6), index_price),
        }

    def get_okx_multi_timeframe_overview(self, symbol: str = "BTCUSDT", reference_price: float | None = None) -> dict[str, Any]:
        symbol = symbol.upper()
        cache_key = f"okx_multi_timeframe_overview:{symbol}:{reference_price if reference_price is not None else 'na'}"
        return self._memoize(
            cache_key,
            ttl_seconds=120,
            stale_if_error_seconds=900,
            builder=lambda: self._build_okx_multi_timeframe_overview(symbol, reference_price=reference_price),
        )

    def _build_okx_multi_timeframe_overview(self, symbol: str = "BTCUSDT", reference_price: float | None = None) -> dict[str, Any]:
        symbol = symbol.upper()
        swap_inst_id = self._to_okx_swap_inst_id(symbol)
        if reference_price is None:
            market = self.get_okx_market_overview(symbol)
            reference_price = market.get("last_price")

        interval_limits = {
            "1h": 24,
            "4h": 18,
            "1d": 30,
            "1w": 24,
            "1M": 12,
        }

        timeframe_candles: dict[str, list[dict[str, Any]]] = {}
        with ThreadPoolExecutor(max_workers=len(interval_limits)) as executor:
            future_map = {
                interval: executor.submit(
                    self._okx_get_rows,
                    "/api/v5/market/candles",
                    params={"instId": swap_inst_id, "bar": self._to_okx_bar(interval), "limit": limit},
                    ttl_seconds=self._okx_kline_ttl(interval),
                )
                for interval, limit in interval_limits.items()
            }
            for interval, future in future_map.items():
                timeframe_candles[interval] = self._normalize_okx_candle_rows(future.result())

        derived_levels = self._build_multi_timeframe_levels(timeframe_candles)
        support_resistance = self._build_support_resistance_levels(timeframe_candles, reference_price)
        fibonacci_levels = self._build_multi_timeframe_fibonacci_levels(timeframe_candles)

        return {
            "symbol": symbol,
            "exchange": "OKX",
            "inst_id": swap_inst_id,
            "reference_price": reference_price,
            "timeframes": {
                interval: {
                    "summary": self._build_candle_structure_summary(candles),
                }
                for interval, candles in timeframe_candles.items()
            },
            "derived_levels": derived_levels,
            "support_resistance": support_resistance,
            "fibonacci_levels": fibonacci_levels,
        }

    def get_okx_derivatives_overview(self, symbol: str = "BTCUSDT", period: str = "1h", limit: int = 8) -> dict[str, Any]:
        symbol = symbol.upper()
        period = period.lower()
        return self._memoize(
            f"okx_derivatives_overview:{symbol}:{period}:{limit}",
            ttl_seconds=120,
            stale_if_error_seconds=900,
            builder=lambda: self._build_okx_derivatives_overview(symbol, period=period, limit=limit),
        )

    def _build_okx_derivatives_overview(self, symbol: str = "BTCUSDT", period: str = "1h", limit: int = 8) -> dict[str, Any]:
        symbol = symbol.upper()
        swap_inst_id = self._to_okx_swap_inst_id(symbol)
        index_inst_id = self._to_okx_index_inst_id(symbol)
        okx_period = self._to_okx_period(period)
        root_asset = self._extract_root_asset(symbol)

        with ThreadPoolExecutor(max_workers=7) as executor:
            ticker_future = executor.submit(
                self._okx_get_first_row,
                "/api/v5/market/ticker",
                params={"instId": swap_inst_id},
                ttl_seconds=20,
            )
            mark_price_future = executor.submit(
                self._okx_get_first_row,
                "/api/v5/public/mark-price",
                params={"instType": "SWAP", "instId": swap_inst_id},
                ttl_seconds=20,
            )
            index_ticker_future = executor.submit(
                self._okx_get_first_row,
                "/api/v5/market/index-tickers",
                params={"instId": index_inst_id},
                ttl_seconds=20,
            )
            funding_current_future = executor.submit(
                self._okx_get_first_row,
                "/api/v5/public/funding-rate",
                params={"instId": swap_inst_id},
                ttl_seconds=30,
            )
            funding_history_future = executor.submit(
                self._okx_get_rows,
                "/api/v5/public/funding-rate-history",
                params={"instId": swap_inst_id, "limit": limit},
                ttl_seconds=60,
            )
            open_interest_history_future = executor.submit(
                self._okx_get_rows,
                "/api/v5/rubik/stat/contracts/open-interest-volume",
                params={"ccy": root_asset, "period": okx_period},
                ttl_seconds=60,
            )
            long_short_history_future = executor.submit(
                self._okx_get_rows,
                "/api/v5/rubik/stat/contracts/long-short-account-ratio",
                params={"ccy": root_asset, "period": okx_period},
                ttl_seconds=60,
            )
            open_interest_current_future = executor.submit(
                self._okx_get_first_row,
                "/api/v5/public/open-interest",
                params={"instType": "SWAP", "instId": swap_inst_id},
                ttl_seconds=30,
            )

            ticker = ticker_future.result()
            mark_price_row = mark_price_future.result()
            index_ticker = index_ticker_future.result()
            funding_current = funding_current_future.result()
            funding_history = self._normalize_okx_funding_rows(funding_history_future.result())
            open_interest_history = self._normalize_okx_open_interest_rows(open_interest_history_future.result())
            long_short_history = self._normalize_okx_long_short_ratio_rows(long_short_history_future.result())
            open_interest_current = open_interest_current_future.result()

        last_price = self._safe_float(ticker.get("last"))
        mark_price = self._safe_float(mark_price_row.get("markPx"))
        index_price = self._safe_float(index_ticker.get("idxPx"))
        swap_index_basis = None if last_price is None or index_price is None else round(last_price - index_price, 6)
        swap_index_basis_pct = self._pct_of_value(swap_index_basis, index_price)
        if swap_index_basis is None:
            basis_state = None
        elif swap_index_basis > 0:
            basis_state = "contango"
        elif swap_index_basis < 0:
            basis_state = "backwardation"
        else:
            basis_state = "flat"

        funding_summary = self._build_ohlc_summary(funding_history, value_key_candidates=("fundingRate", "value", "close"))
        open_interest_summary = self._build_ohlc_summary(open_interest_history, value_key_candidates=("sumOpenInterestValue", "value", "close"))
        if open_interest_summary is not None:
            open_interest_summary["latest_open_interest"] = self._safe_float(open_interest_current.get("oi"))
            open_interest_summary["latest_open_interest_value"] = self._safe_float(open_interest_current.get("oiUsd"))
        long_short_summary = self._build_ohlc_summary(long_short_history, value_key_candidates=("longShortRatio", "value", "close"))

        basis_summary = {
            "latest_basis": swap_index_basis,
            "latest_basis_rate": swap_index_basis_pct,
            "premium_rate": self._safe_float(funding_current.get("premium")),
            "latest_funding_rate": self._safe_float(funding_current.get("fundingRate")),
            "state": basis_state,
        }

        return {
            "symbol": symbol,
            "exchange": "OKX",
            "inst_id": swap_inst_id,
            "current": {
                "last_price": last_price,
                "mark_price": mark_price,
                "index_price": index_price,
                "last_funding_rate": self._safe_float(funding_current.get("fundingRate")),
                "interest_rate": self._safe_float(funding_current.get("interestRate")),
                "premium_rate": self._safe_float(funding_current.get("premium")),
                "next_funding_time": funding_current.get("nextFundingTime"),
                "mark_index_spread": self._mark_index_spread(mark_price, index_price),
                "mark_index_spread_pct": self._pct_of_value(self._mark_index_spread(mark_price, index_price), index_price),
                "open_interest": self._safe_float(open_interest_current.get("oi")),
                "open_interest_usd": self._safe_float(open_interest_current.get("oiUsd")),
            },
            "summary": {
                "basis": basis_summary,
                "open_interest": open_interest_summary,
                "global_long_short_ratio": long_short_summary,
                "funding_rate": funding_summary,
                "composite_view": self._build_binance_derivatives_composite_view(
                    basis_summary=basis_summary,
                    open_interest_summary=open_interest_summary,
                    top_position_summary=None,
                    top_account_summary=None,
                    global_ratio_summary=long_short_summary,
                    taker_volume_summary=None,
                ),
            },
            "raw": {
                "funding_rate_history": funding_history[-8:],
                "open_interest_history": open_interest_history[-8:],
                "long_short_ratio_history": long_short_history[-8:],
            },
        }

    def get_binance_market(self, symbol: str) -> dict[str, Any]:
        symbol = symbol.upper()
        return self._memoize(
            f"binance_market:{symbol}",
            ttl_seconds=20,
            stale_if_error_seconds=120,
            builder=lambda: self._build_binance_market(symbol),
        )

    def _build_binance_market(self, symbol: str) -> dict[str, Any]:
        symbol = symbol.upper()
        price = self._http.get_json(
            "binance",
            f"{BINANCE_SPOT_BASE}/api/v3/ticker/price",
            params={"symbol": symbol},
            ttl_seconds=15,
        )
        book = self._http.get_json(
            "binance",
            f"{BINANCE_SPOT_BASE}/api/v3/ticker/bookTicker",
            params={"symbol": symbol},
            ttl_seconds=15,
        )
        open_interest = self._http.get_json(
            "binance",
            f"{BINANCE_FUTURES_BASE}/fapi/v1/openInterest",
            params={"symbol": symbol},
            ttl_seconds=30,
        )
        funding = self._http.get_json(
            "binance",
            f"{BINANCE_FUTURES_BASE}/fapi/v1/fundingRate",
            params={"symbol": symbol, "limit": 1},
            ttl_seconds=30,
        )
        taker_ratio = self._http.get_json(
            "binance",
            f"{BINANCE_FUTURES_BASE}/futures/data/takerlongshortRatio",
            params={"symbol": symbol, "period": "5m", "limit": 1},
            ttl_seconds=30,
        )
        long_short_ratio = self._http.get_json(
            "binance",
            f"{BINANCE_FUTURES_BASE}/futures/data/globalLongShortAccountRatio",
            params={"symbol": symbol, "period": "5m", "limit": 1},
            ttl_seconds=30,
        )
        candles = self._http.get_json(
            "binance",
            f"{BINANCE_SPOT_BASE}/api/v3/klines",
            params={"symbol": symbol, "interval": "5m", "limit": 3},
            ttl_seconds=15,
        )

        return {
            "symbol": symbol,
            "spot_price": self._safe_float(price.get("price")),
            "book_ticker": {
                "bid_price": self._safe_float(book.get("bidPrice")),
                "ask_price": self._safe_float(book.get("askPrice")),
                "bid_qty": self._safe_float(book.get("bidQty")),
                "ask_qty": self._safe_float(book.get("askQty")),
            },
            "open_interest": self._safe_float(open_interest.get("openInterest")),
            "latest_funding_rate": self._safe_float(self._first_item_value(funding, "fundingRate")),
            "latest_taker_long_short_ratio": self._safe_float(self._first_item_value(taker_ratio, "buySellRatio")),
            "latest_global_long_short_account_ratio": self._safe_float(self._first_item_value(long_short_ratio, "longShortRatio")),
            "recent_5m_candles": [self._normalize_binance_candle(item) for item in candles],
        }

    def get_binance_market_overview(self, symbol: str) -> dict[str, Any]:
        symbol = symbol.upper()
        return self._memoize(
            f"binance_market_overview:{symbol}",
            ttl_seconds=30,
            stale_if_error_seconds=180,
            builder=lambda: self._build_binance_market_overview(symbol),
        )

    def _build_binance_market_overview(self, symbol: str) -> dict[str, Any]:
        symbol = symbol.upper()
        price = self._http.get_json(
            "binance",
            f"{BINANCE_SPOT_BASE}/api/v3/ticker/price",
            params={"symbol": symbol},
            ttl_seconds=20,
        )
        ticker_24h = self._http.get_json(
            "binance",
            f"{BINANCE_SPOT_BASE}/api/v3/ticker/24hr",
            params={"symbol": symbol},
            ttl_seconds=30,
        )
        return {
            "symbol": symbol,
            "spot_price": self._safe_float(price.get("price")),
            "ticker_24h": {
                "price_change_pct": self._safe_float(ticker_24h.get("priceChangePercent")),
                "high_price": self._safe_float(ticker_24h.get("highPrice")),
                "low_price": self._safe_float(ticker_24h.get("lowPrice")),
                "weighted_avg_price": self._safe_float(ticker_24h.get("weightedAvgPrice")),
                "volume": self._safe_float(ticker_24h.get("volume")),
                "quote_volume": self._safe_float(ticker_24h.get("quoteVolume")),
            },
        }

    def get_binance_multi_timeframe_structure(self, symbol: str = "BTCUSDT") -> dict[str, Any]:
        symbol = symbol.upper()
        return self._memoize(
            f"binance_multi_timeframe_structure:{symbol}",
            ttl_seconds=120,
            stale_if_error_seconds=900,
            builder=lambda: self._build_binance_multi_timeframe_structure(symbol),
        )

    def _build_binance_multi_timeframe_structure(self, symbol: str = "BTCUSDT") -> dict[str, Any]:
        symbol = symbol.upper()
        interval_limits = {
            "15m": 32,
            "1h": 32,
            "4h": 30,
            "8h": 21,
            "1d": 30,
            "1w": 24,
            "1M": 12,
        }

        ticker_24h = self._http.get_json(
            "binance",
            f"{BINANCE_SPOT_BASE}/api/v3/ticker/24hr",
            params={"symbol": symbol},
            ttl_seconds=60,
        )
        current_price = self._http.get_json(
            "binance",
            f"{BINANCE_SPOT_BASE}/api/v3/ticker/price",
            params={"symbol": symbol},
            ttl_seconds=15,
        )

        timeframe_candles: dict[str, list[dict[str, Any]]] = {}
        for interval, limit in interval_limits.items():
            rows = self._http.get_json(
                "binance",
                f"{BINANCE_SPOT_BASE}/api/v3/klines",
                params={"symbol": symbol, "interval": interval, "limit": limit},
                ttl_seconds=self._binance_kline_ttl(interval),
            )
            timeframe_candles[interval] = [self._normalize_binance_candle(item) for item in rows]

        spot_price = self._safe_float(current_price.get("price"))
        derived_levels = self._build_multi_timeframe_levels(timeframe_candles)
        support_resistance = self._build_support_resistance_levels(timeframe_candles, spot_price)
        fibonacci_levels = self._build_multi_timeframe_fibonacci_levels(timeframe_candles)

        return {
            "symbol": symbol,
            "spot_price": spot_price,
            "ticker_24h": {
                "price_change_pct": self._safe_float(ticker_24h.get("priceChangePercent")),
                "high_price": self._safe_float(ticker_24h.get("highPrice")),
                "low_price": self._safe_float(ticker_24h.get("lowPrice")),
                "volume": self._safe_float(ticker_24h.get("volume")),
                "quote_volume": self._safe_float(ticker_24h.get("quoteVolume")),
                "weighted_avg_price": self._safe_float(ticker_24h.get("weightedAvgPrice")),
            },
            "timeframes": {
                interval: {
                    "summary": self._build_candle_structure_summary(candles),
                    "raw_candles": candles[-self._raw_candle_tail_size(interval):],
                }
                for interval, candles in timeframe_candles.items()
            },
            "derived_levels": derived_levels,
            "support_resistance": support_resistance,
            "fibonacci_levels": fibonacci_levels,
        }

    def get_binance_multi_timeframe_overview(self, symbol: str = "BTCUSDT", spot_price: float | None = None) -> dict[str, Any]:
        symbol = symbol.upper()
        overview_key = f"binance_multi_timeframe_overview:{symbol}:{spot_price if spot_price is not None else 'na'}"
        return self._memoize(
            overview_key,
            ttl_seconds=120,
            stale_if_error_seconds=900,
            builder=lambda: self._build_binance_multi_timeframe_overview(symbol, spot_price=spot_price),
        )

    def _build_binance_multi_timeframe_overview(self, symbol: str = "BTCUSDT", spot_price: float | None = None) -> dict[str, Any]:
        symbol = symbol.upper()
        if spot_price is None:
            current_price = self._http.get_json(
                "binance",
                f"{BINANCE_SPOT_BASE}/api/v3/ticker/price",
                params={"symbol": symbol},
                ttl_seconds=20,
            )
            spot_price = self._safe_float(current_price.get("price"))

        interval_limits = {
            "1h": 24,
            "4h": 18,
            "1d": 30,
            "1w": 24,
            "1M": 12,
        }

        timeframe_candles: dict[str, list[dict[str, Any]]] = {}
        for interval, limit in interval_limits.items():
            rows = self._http.get_json(
                "binance",
                f"{BINANCE_SPOT_BASE}/api/v3/klines",
                params={"symbol": symbol, "interval": interval, "limit": limit},
                ttl_seconds=self._binance_kline_ttl(interval),
            )
            timeframe_candles[interval] = [self._normalize_binance_candle(item) for item in rows]

        derived_levels = self._build_multi_timeframe_levels(timeframe_candles)
        support_resistance = self._build_support_resistance_levels(timeframe_candles, spot_price)
        fibonacci_levels = self._build_multi_timeframe_fibonacci_levels(timeframe_candles)

        return {
            "symbol": symbol,
            "spot_price": spot_price,
            "timeframes": {
                interval: {
                    "summary": self._build_candle_structure_summary(candles),
                }
                for interval, candles in timeframe_candles.items()
            },
            "derived_levels": derived_levels,
            "support_resistance": support_resistance,
            "fibonacci_levels": fibonacci_levels,
        }

    def get_binance_derivatives_structure(self, symbol: str = "BTCUSDT", period: str = "1h", limit: int = 12) -> dict[str, Any]:
        symbol = symbol.upper()
        period = period.lower()
        return self._memoize(
            f"binance_derivatives_structure:{symbol}:{period}:{limit}",
            ttl_seconds=120,
            stale_if_error_seconds=900,
            builder=lambda: self._build_binance_derivatives_structure(symbol, period=period, limit=limit),
        )

    def _build_binance_derivatives_structure(self, symbol: str = "BTCUSDT", period: str = "1h", limit: int = 12) -> dict[str, Any]:
        symbol = symbol.upper()
        period = period.lower()
        premium_index = self._http.get_json(
            "binance",
            f"{BINANCE_FUTURES_BASE}/fapi/v1/premiumIndex",
            params={"symbol": symbol},
            ttl_seconds=20,
        )
        basis_rows = self._http.get_json(
            "binance",
            f"{BINANCE_FUTURES_BASE}/futures/data/basis",
            params={"pair": symbol, "contractType": "PERPETUAL", "period": period, "limit": limit},
            ttl_seconds=60,
        )
        open_interest_rows = self._http.get_json(
            "binance",
            f"{BINANCE_FUTURES_BASE}/futures/data/openInterestHist",
            params={"symbol": symbol, "period": period, "limit": limit},
            ttl_seconds=60,
        )
        top_position_rows = self._http.get_json(
            "binance",
            f"{BINANCE_FUTURES_BASE}/futures/data/topLongShortPositionRatio",
            params={"symbol": symbol, "period": period, "limit": limit},
            ttl_seconds=60,
        )
        top_account_rows = self._http.get_json(
            "binance",
            f"{BINANCE_FUTURES_BASE}/futures/data/topLongShortAccountRatio",
            params={"symbol": symbol, "period": period, "limit": limit},
            ttl_seconds=60,
        )
        global_ratio_rows = self._http.get_json(
            "binance",
            f"{BINANCE_FUTURES_BASE}/futures/data/globalLongShortAccountRatio",
            params={"symbol": symbol, "period": period, "limit": limit},
            ttl_seconds=60,
        )
        taker_volume_rows = self._http.get_json(
            "binance",
            f"{BINANCE_FUTURES_BASE}/futures/data/takerlongshortRatio",
            params={"symbol": symbol, "period": period, "limit": limit},
            ttl_seconds=60,
        )

        normalized_basis = [self._normalize_binance_basis_row(row) for row in basis_rows if isinstance(row, dict)]
        normalized_open_interest = [self._normalize_binance_open_interest_hist_row(row) for row in open_interest_rows if isinstance(row, dict)]
        normalized_top_position = [self._normalize_binance_ratio_row(row, "longShortRatio") for row in top_position_rows if isinstance(row, dict)]
        normalized_top_account = [self._normalize_binance_ratio_row(row, "longShortRatio") for row in top_account_rows if isinstance(row, dict)]
        normalized_global_ratio = [self._normalize_binance_ratio_row(row, "longShortRatio") for row in global_ratio_rows if isinstance(row, dict)]
        normalized_taker_volume = [self._normalize_binance_taker_volume_row(row) for row in taker_volume_rows if isinstance(row, dict)]

        current_mark_price = self._safe_float(premium_index.get("markPrice"))
        current_index_price = self._safe_float(premium_index.get("indexPrice"))
        current_spread = self._mark_index_spread(current_mark_price, current_index_price)

        top_position_summary = self._build_ratio_summary(normalized_top_position, "top_trader_position_ratio")
        top_account_summary = self._build_ratio_summary(normalized_top_account, "top_trader_account_ratio")
        global_ratio_summary = self._build_ratio_summary(normalized_global_ratio, "global_long_short_ratio")
        taker_volume_summary = self._build_taker_volume_summary(normalized_taker_volume)
        open_interest_summary = self._build_open_interest_hist_summary(normalized_open_interest)
        basis_summary = self._build_binance_basis_summary(normalized_basis)

        return {
            "symbol": symbol,
            "period": period,
            "limit": limit,
            "current": {
                "mark_price": current_mark_price,
                "index_price": current_index_price,
                "estimated_settlement_price": self._safe_float(premium_index.get("estimatedSettlePrice")),
                "last_funding_rate": self._safe_float(premium_index.get("lastFundingRate")),
                "interest_rate": self._safe_float(premium_index.get("interestRate")),
                "next_funding_time": premium_index.get("nextFundingTime"),
                "time": premium_index.get("time"),
                "mark_index_spread": current_spread,
                "mark_index_spread_pct": self._pct_of_value(current_spread, current_index_price),
            },
            "summary": {
                "basis": basis_summary,
                "open_interest": open_interest_summary,
                "top_trader_position_ratio": top_position_summary,
                "top_trader_account_ratio": top_account_summary,
                "global_long_short_ratio": global_ratio_summary,
                "taker_buy_sell_volume": taker_volume_summary,
                "composite_view": self._build_binance_derivatives_composite_view(
                    basis_summary=basis_summary,
                    open_interest_summary=open_interest_summary,
                    top_position_summary=top_position_summary,
                    top_account_summary=top_account_summary,
                    global_ratio_summary=global_ratio_summary,
                    taker_volume_summary=taker_volume_summary,
                ),
            },
            "raw": {
                "basis_history": normalized_basis[-8:],
                "open_interest_history": normalized_open_interest[-8:],
                "top_trader_position_ratio_history": normalized_top_position[-8:],
                "top_trader_account_ratio_history": normalized_top_account[-8:],
                "global_long_short_ratio_history": normalized_global_ratio[-8:],
                "taker_buy_sell_volume_history": normalized_taker_volume[-8:],
            },
        }

    def get_binance_derivatives_overview(self, symbol: str = "BTCUSDT", period: str = "1h", limit: int = 8) -> dict[str, Any]:
        symbol = symbol.upper()
        period = period.lower()
        return self._memoize(
            f"binance_derivatives_overview:{symbol}:{period}:{limit}",
            ttl_seconds=120,
            stale_if_error_seconds=900,
            builder=lambda: self._build_binance_derivatives_overview(symbol, period=period, limit=limit),
        )

    def _build_binance_derivatives_overview(self, symbol: str = "BTCUSDT", period: str = "1h", limit: int = 8) -> dict[str, Any]:
        symbol = symbol.upper()
        period = period.lower()
        premium_index = self._http.get_json(
            "binance",
            f"{BINANCE_FUTURES_BASE}/fapi/v1/premiumIndex",
            params={"symbol": symbol},
            ttl_seconds=20,
        )
        basis_rows = self._http.get_json(
            "binance",
            f"{BINANCE_FUTURES_BASE}/futures/data/basis",
            params={"pair": symbol, "contractType": "PERPETUAL", "period": period, "limit": limit},
            ttl_seconds=60,
        )
        open_interest_rows = self._http.get_json(
            "binance",
            f"{BINANCE_FUTURES_BASE}/futures/data/openInterestHist",
            params={"symbol": symbol, "period": period, "limit": limit},
            ttl_seconds=60,
        )
        top_position_rows = self._http.get_json(
            "binance",
            f"{BINANCE_FUTURES_BASE}/futures/data/topLongShortPositionRatio",
            params={"symbol": symbol, "period": period, "limit": limit},
            ttl_seconds=60,
        )
        global_ratio_rows = self._http.get_json(
            "binance",
            f"{BINANCE_FUTURES_BASE}/futures/data/globalLongShortAccountRatio",
            params={"symbol": symbol, "period": period, "limit": limit},
            ttl_seconds=60,
        )
        taker_volume_rows = self._http.get_json(
            "binance",
            f"{BINANCE_FUTURES_BASE}/futures/data/takerlongshortRatio",
            params={"symbol": symbol, "period": period, "limit": limit},
            ttl_seconds=60,
        )

        normalized_basis = [self._normalize_binance_basis_row(row) for row in basis_rows if isinstance(row, dict)]
        normalized_open_interest = [self._normalize_binance_open_interest_hist_row(row) for row in open_interest_rows if isinstance(row, dict)]
        normalized_top_position = [self._normalize_binance_ratio_row(row, "longShortRatio") for row in top_position_rows if isinstance(row, dict)]
        normalized_global_ratio = [self._normalize_binance_ratio_row(row, "longShortRatio") for row in global_ratio_rows if isinstance(row, dict)]
        normalized_taker_volume = [self._normalize_binance_taker_volume_row(row) for row in taker_volume_rows if isinstance(row, dict)]

        basis_summary = self._build_binance_basis_summary(normalized_basis)
        open_interest_summary = self._build_open_interest_hist_summary(normalized_open_interest)
        top_position_summary = self._build_ratio_summary(normalized_top_position, "top_trader_position_ratio")
        global_ratio_summary = self._build_ratio_summary(normalized_global_ratio, "global_long_short_ratio")
        taker_volume_summary = self._build_taker_volume_summary(normalized_taker_volume)

        current_mark_price = self._safe_float(premium_index.get("markPrice"))
        current_index_price = self._safe_float(premium_index.get("indexPrice"))
        current_spread = self._mark_index_spread(current_mark_price, current_index_price)

        return {
            "symbol": symbol,
            "period": period,
            "current": {
                "mark_price": current_mark_price,
                "index_price": current_index_price,
                "last_funding_rate": self._safe_float(premium_index.get("lastFundingRate")),
                "mark_index_spread": current_spread,
                "mark_index_spread_pct": self._pct_of_value(current_spread, current_index_price),
            },
            "summary": {
                "basis": basis_summary,
                "open_interest": open_interest_summary,
                "top_trader_position_ratio": top_position_summary,
                "global_long_short_ratio": global_ratio_summary,
                "taker_buy_sell_volume": taker_volume_summary,
                "composite_view": self._build_binance_derivatives_composite_view(
                    basis_summary=basis_summary,
                    open_interest_summary=open_interest_summary,
                    top_position_summary=top_position_summary,
                    top_account_summary=None,
                    global_ratio_summary=global_ratio_summary,
                    taker_volume_summary=taker_volume_summary,
                ),
            },
        }

    def get_bybit_market_structure(self, symbol: str = "BTCUSDT") -> dict[str, Any]:
        symbol = symbol.upper()
        return self._memoize(
            f"bybit_market_structure:{symbol}",
            ttl_seconds=90,
            stale_if_error_seconds=600,
            builder=lambda: self._build_bybit_market_structure(symbol),
        )

    def _build_bybit_market_structure(self, symbol: str = "BTCUSDT") -> dict[str, Any]:
        symbol = symbol.upper()
        ticker_result = self._bybit_get_result(
            "/v5/market/tickers",
            params={"category": "linear", "symbol": symbol},
        )
        ticker_rows = ticker_result.get("list", [])
        ticker = ticker_rows[0] if ticker_rows else {}

        price_1h_rows = self._normalize_bybit_kline_rows(
            self._bybit_get_result(
                "/v5/market/kline",
                params={"category": "linear", "symbol": symbol, "interval": "60", "limit": 32},
            ).get("list", [])
        )
        price_4h_rows = self._normalize_bybit_kline_rows(
            self._bybit_get_result(
                "/v5/market/kline",
                params={"category": "linear", "symbol": symbol, "interval": "240", "limit": 24},
            ).get("list", [])
        )
        mark_price_rows = self._normalize_bybit_kline_rows(
            self._bybit_get_result(
                "/v5/market/mark-price-kline",
                params={"category": "linear", "symbol": symbol, "interval": "60", "limit": 24},
            ).get("list", [])
        )
        open_interest_rows = self._normalize_bybit_open_interest_rows(
            self._bybit_get_result(
                "/v5/market/open-interest",
                params={"category": "linear", "symbol": symbol, "intervalTime": "1h", "limit": 24},
            ).get("list", [])
        )
        funding_rows = self._normalize_bybit_funding_rows(
            self._bybit_get_result(
                "/v5/market/funding/history",
                params={"category": "linear", "symbol": symbol, "limit": 20},
            ).get("list", [])
        )
        long_short_rows = self._normalize_bybit_account_ratio_rows(
            self._bybit_get_result(
                "/v5/market/account-ratio",
                params={"category": "linear", "symbol": symbol, "period": "1h", "limit": 24},
            ).get("list", [])
        )

        return {
            "symbol": symbol,
            "exchange": "BYBIT",
            "ticker": {
                "last_price": self._safe_float(ticker.get("lastPrice")),
                "mark_price": self._safe_float(ticker.get("markPrice")),
                "index_price": self._safe_float(ticker.get("indexPrice")),
                "funding_rate": self._safe_float(ticker.get("fundingRate")),
                "open_interest": self._safe_float(ticker.get("openInterest")),
                "open_interest_value": self._safe_float(ticker.get("openInterestValue")),
                "price_24h_pct": self._safe_float(ticker.get("price24hPcnt")),
                "turnover_24h": self._safe_float(ticker.get("turnover24h")),
                "volume_24h": self._safe_float(ticker.get("volume24h")),
            },
            "summary": {
                "price_1h": self._build_candle_structure_summary(price_1h_rows),
                "price_4h": self._build_candle_structure_summary(price_4h_rows),
                "mark_price_1h": self._build_candle_structure_summary(mark_price_rows),
                "open_interest": self._build_ohlc_summary(open_interest_rows, value_key_candidates=("close", "value", "openInterest")),
                "funding_rate": self._build_ohlc_summary(funding_rows, value_key_candidates=("close", "value", "fundingRate")),
                "long_short_ratio": self._build_ohlc_summary(long_short_rows, value_key_candidates=("close", "value", "longShortRatio")),
            },
            "raw": {
                "price_1h_candles": price_1h_rows[-8:],
                "price_4h_candles": price_4h_rows[-8:],
                "mark_price_1h_candles": mark_price_rows[-8:],
                "open_interest_history": open_interest_rows[-8:],
                "funding_rate_history": funding_rows[-8:],
                "long_short_ratio_history": long_short_rows[-8:],
            },
        }

    def get_coingecko_simple_price(self, asset_id: str, vs_currency: str = "usd") -> dict[str, Any]:
        asset_id = asset_id.strip().lower()
        vs_currency = vs_currency.strip().lower()
        return self._memoize(
            f"coingecko_simple_price:{asset_id}:{vs_currency}",
            ttl_seconds=180,
            stale_if_error_seconds=1200,
            builder=lambda: self._build_coingecko_simple_price(asset_id, vs_currency),
        )

    def _build_coingecko_simple_price(self, asset_id: str, vs_currency: str = "usd") -> dict[str, Any]:
        headers: dict[str, str] = {}
        if self._config.coingecko_pro_api_key:
            headers["x-cg-pro-api-key"] = self._config.coingecko_pro_api_key
        elif self._config.coingecko_demo_api_key:
            headers["x-cg-demo-api-key"] = self._config.coingecko_demo_api_key

        payload = self._http.get_json(
            "coingecko",
            COINGECKO_SIMPLE_PRICE_URL,
            params={
                "ids": asset_id,
                "vs_currencies": vs_currency,
                "include_market_cap": "true",
                "include_24hr_vol": "true",
                "include_24hr_change": "true",
                "include_last_updated_at": "true",
            },
            headers=headers or None,
            ttl_seconds=60,
        )

        if asset_id not in payload:
            raise UpstreamServiceError("coingecko", f"CoinGecko did not return data for asset '{asset_id}'.")

        return {
            "asset_id": asset_id,
            "vs_currency": vs_currency,
            "metrics": payload[asset_id],
        }

    def get_deribit_options_context(self, currency: str = "BTC") -> dict[str, Any]:
        currency = currency.strip().upper() or "BTC"
        return self._memoize(
            f"deribit_options_context:{currency}",
            ttl_seconds=120,
            stale_if_error_seconds=900,
            builder=lambda: self._build_deribit_options_context(currency),
        )

    def _build_deribit_options_context(self, currency: str) -> dict[str, Any]:
        now_utc = datetime.now(timezone.utc)
        end_timestamp = int(now_utc.timestamp() * 1000)
        start_timestamp = int((now_utc - timedelta(hours=24)).timestamp() * 1000)

        option_rows = self._deribit_public_get(
            "get_book_summary_by_currency",
            params={"currency": currency, "kind": "option"},
            ttl_seconds=30,
        )
        future_rows = self._deribit_public_get(
            "get_book_summary_by_currency",
            params={"currency": currency, "kind": "future"},
            ttl_seconds=30,
        )
        historical_vol_rows = self._deribit_public_get(
            "get_historical_volatility",
            params={"currency": currency},
            ttl_seconds=300,
        )
        vol_index_payload = self._deribit_public_get(
            "get_volatility_index_data",
            params={
                "currency": currency,
                "resolution": 60,
                "start_timestamp": start_timestamp,
                "end_timestamp": end_timestamp,
            },
            ttl_seconds=120,
        )

        if not isinstance(option_rows, list) or not option_rows:
            raise UpstreamServiceError("deribit", f"Deribit returned no option data for {currency}.")
        if not isinstance(future_rows, list):
            raise UpstreamServiceError("deribit", f"Deribit returned invalid futures data for {currency}.")

        return {
            "generated_at": utc_now_iso(),
            "currency": currency,
            "options": self._build_deribit_option_summary(option_rows),
            "nearest_expiries": self._build_deribit_nearest_expiry_summaries(option_rows)[:3],
            "futures": self._build_deribit_futures_summary(future_rows),
            "historical_volatility": self._build_deribit_historical_vol_summary(historical_vol_rows if isinstance(historical_vol_rows, list) else []),
            "volatility_index": self._build_deribit_vol_index_summary(
                vol_index_payload.get("data", []) if isinstance(vol_index_payload, dict) else []
            ),
        }

    def get_coinglass_market_structure(self, symbol: str = "BTCUSDT", exchange: str = "OKX", interval: str = "1h") -> dict[str, Any]:
        if not self._config.coinglass_api_key:
            raise UpstreamServiceError("coinglass", "COINGLASS_API_KEY is not configured.", status_code=424)

        normalized_pair_symbol = self._normalize_coinglass_pair_symbol(symbol)
        normalized_asset_symbol = self._extract_root_asset(symbol)
        normalized_exchange = exchange.upper()
        resolved_pair_symbol = self._resolve_coinglass_supported_pair(exchange=normalized_exchange, symbol=normalized_pair_symbol)
        symbol_candidates = self._unique_strings(
            [
                resolved_pair_symbol,
                normalized_pair_symbol,
                normalized_asset_symbol,
            ]
        )

        unavailable: list[dict[str, Any]] = []

        open_interest = self._coinglass_capture_component(
            unavailable,
            "open_interest",
            lambda: self._get_coinglass_open_interest(
                symbol_candidates=symbol_candidates,
                asset_symbol=normalized_asset_symbol,
                exchange=normalized_exchange,
                interval=interval,
            ),
        )
        funding = self._coinglass_capture_component(
            unavailable,
            "funding_rate",
            lambda: self._coinglass_get_first_available_data(
                "/api/futures/funding-rate/history",
                self._build_coinglass_param_variants(symbol_candidates, normalized_exchange, interval),
            ),
        )
        oi_weighted_funding = self._coinglass_capture_component(
            unavailable,
            "oi_weighted_funding_rate",
            lambda: self._coinglass_get_first_available_data(
                "/api/futures/funding-rate/oi-weight-history",
                self._build_coinglass_param_variants(symbol_candidates, normalized_exchange, interval),
            ),
        )
        long_short_ratio = self._coinglass_capture_component(
            unavailable,
            "long_short_ratio",
            lambda: self._coinglass_get_first_available_data(
                "/api/futures/global-long-short-account-ratio/history",
                self._build_coinglass_param_variants(symbol_candidates, normalized_exchange, interval),
            ),
        )
        liquidation = self._coinglass_capture_component(
            unavailable,
            "liquidation",
            lambda: self._coinglass_get_first_available_data(
                "/api/futures/liquidation/history",
                self._build_coinglass_param_variants(symbol_candidates, normalized_exchange, interval),
            ),
        )
        exchange_rank = self._coinglass_capture_component(
            unavailable,
            "exchange_rank",
            lambda: self._coinglass_get_data(
                "/api/futures/exchange-rank",
                params=None,
            ),
        )

        if all(component is None for component in (open_interest, funding, oi_weighted_funding, long_short_ratio, liquidation, exchange_rank)):
            detail = "CoinGlass returned no usable market-structure components."
            if unavailable:
                detail += f" Components: {', '.join(item['component'] for item in unavailable)}."
            raise UpstreamServiceError("coinglass", detail)

        return {
            "symbol": resolved_pair_symbol,
            "requested_symbol": normalized_pair_symbol,
            "asset_symbol": normalized_asset_symbol,
            "symbol_candidates": symbol_candidates,
            "exchange": normalized_exchange,
            "interval": interval,
            "unavailable_components": unavailable,
            "summary": {
                "open_interest": self._build_ohlc_summary(open_interest, value_key_candidates=("close", "value", "openInterest")),
                "funding_rate": self._build_ohlc_summary(funding, value_key_candidates=("close", "value", "fundingRate")),
                "oi_weighted_funding_rate": self._build_ohlc_summary(oi_weighted_funding, value_key_candidates=("close", "value", "fundingRate")),
                "long_short_ratio": self._build_ohlc_summary(long_short_ratio, value_key_candidates=("close", "value", "longShortRadio", "longShortRatio")),
                "liquidation": self._build_liquidation_summary(liquidation),
                "exchange_rank": self._build_exchange_rank_summary(exchange_rank, normalized_exchange),
            },
            "raw": {
                "open_interest_history": self._truncate_rows(open_interest),
                "funding_rate_history": self._truncate_rows(funding),
                "oi_weighted_funding_rate_history": self._truncate_rows(oi_weighted_funding),
                "long_short_ratio_history": self._truncate_rows(long_short_ratio),
                "liquidation_history": self._truncate_rows(liquidation),
            },
        }

    def get_fear_greed_latest(self) -> dict[str, Any]:
        return self._memoize(
            "fear_greed_latest",
            ttl_seconds=300,
            stale_if_error_seconds=3600,
            builder=self._build_fear_greed_latest,
        )

    def _build_fear_greed_latest(self) -> dict[str, Any]:
        payload = self._http.get_json(
            "fear_greed",
            FEAR_GREED_URL,
            params={"limit": 1},
            ttl_seconds=60,
        )
        data = payload.get("data", [])
        if not data:
            raise UpstreamServiceError("fear_greed", "Fear & Greed API returned no rows.")
        latest = data[0]
        return {
            "value": self._safe_int(latest.get("value")),
            "classification": latest.get("value_classification"),
            "timestamp": latest.get("timestamp"),
            "time_until_update_seconds": self._safe_int(latest.get("time_until_update")),
        }

    def get_mempool_recommended_fees(self) -> dict[str, Any]:
        return self._memoize(
            "mempool_recommended_fees",
            ttl_seconds=60,
            stale_if_error_seconds=300,
            builder=lambda: self._http.get_json(
                "mempool",
                MEMPOOL_FEES_URL,
                ttl_seconds=30,
            ),
        )

    def get_treasury_latest_avg_rates(self) -> dict[str, Any]:
        payload = self._http.get_json(
            "treasury",
            f"{TREASURY_BASE}/avg_interest_rates",
            params={"sort": "-record_date", "page[size]": 200},
            ttl_seconds=900,
        )
        rows = payload.get("data", [])
        if not rows:
            raise UpstreamServiceError("treasury", "Treasury Fiscal Data returned no rows.")

        latest_date = rows[0]["record_date"]
        latest_rows = [row for row in rows if row.get("record_date") == latest_date][:12]
        return {
            "record_date": latest_date,
            "rates": [
                {
                    "security_type": row.get("security_type_desc"),
                    "security": row.get("security_desc"),
                    "avg_interest_rate_pct": self._safe_float(row.get("avg_interest_rate_amt")),
                }
                for row in latest_rows
            ],
        }

    def get_bls_series(self, series_id: str, limit: int = 12) -> dict[str, Any]:
        params: dict[str, Any] = {}
        if self._config.bls_api_key:
            params["registrationkey"] = self._config.bls_api_key

        payload = self._http.get_json(
            "bls",
            f"{BLS_BASE}/{series_id}",
            params=params or None,
            ttl_seconds=3600,
        )
        if payload.get("status") != "REQUEST_SUCCEEDED":
            raise UpstreamServiceError("bls", f"BLS returned non-success status: {payload}")

        series_list = payload.get("Results", {}).get("series", [])
        if not series_list:
            raise UpstreamServiceError("bls", f"BLS returned no series for {series_id}.")
        series = series_list[0]

        return {
            "series_id": series.get("seriesID", series_id),
            "observations": [
                {
                    "year": item.get("year"),
                    "period": item.get("period"),
                    "period_name": item.get("periodName"),
                    "value": item.get("value"),
                    "latest": item.get("latest") == "true",
                }
                for item in series.get("data", [])[:limit]
            ],
        }

    def get_fred_series(self, series_id: str, limit: int = 12) -> dict[str, Any]:
        if not self._config.fred_api_key:
            raise UpstreamServiceError("fred", "FRED_API_KEY is not configured.", status_code=424)

        payload = self._http.get_json(
            "fred",
            f"{FRED_BASE}/series/observations",
            params={
                "series_id": series_id,
                "api_key": self._config.fred_api_key,
                "file_type": "json",
                "sort_order": "desc",
                "limit": limit,
            },
            ttl_seconds=3600,
        )

        observations = payload.get("observations", [])
        return {
            "series_id": series_id,
            "observations": [
                {
                    "date": item.get("date"),
                    "value": item.get("value"),
                }
                for item in observations
            ],
        }

    def get_fred_series_bundle(self, series_ids: list[str]) -> dict[str, Any]:
        return {
            "series": [self.get_fred_series(series_id.strip().upper()) for series_id in series_ids if series_id.strip()]
        }

    def get_bea_datasets(self) -> dict[str, Any]:
        if not self._config.bea_api_key:
            raise UpstreamServiceError("bea", "BEA_API_KEY is not configured.", status_code=424)

        payload = self._http.get_json(
            "bea",
            BEA_BASE,
            params={
                "UserID": self._config.bea_api_key,
                "method": "GETDATASETLIST",
                "ResultFormat": "json",
            },
            ttl_seconds=86400,
        )
        datasets = payload.get("BEAAPI", {}).get("Results", {}).get("Dataset", [])
        return {"datasets": datasets}

    def get_bea_gdp(self, year: str = "LAST5") -> dict[str, Any]:
        if not self._config.bea_api_key:
            raise UpstreamServiceError("bea", "BEA_API_KEY is not configured.", status_code=424)

        payload = self._http.get_json(
            "bea",
            BEA_BASE,
            params={
                "UserID": self._config.bea_api_key,
                "method": "GETDATA",
                "datasetname": "NIPA",
                "TableName": "T10101",
                "LineNumber": 1,
                "Frequency": "Q",
                "Year": year,
                "ResultFormat": "json",
            },
            ttl_seconds=86400,
        )
        rows = payload.get("BEAAPI", {}).get("Results", {}).get("Data", [])
        return {
            "series": "Gross domestic product",
            "observations": [
                {
                    "time_period": row.get("TimePeriod"),
                    "value": row.get("DataValue"),
                    "unit": row.get("CL_UNIT"),
                }
                for row in rows[:12]
            ],
        }

    def get_fed_monetary_feed(self, limit: int = 5) -> dict[str, Any]:
        xml_text = self._http.get_text(
            "fed",
            FED_MONETARY_FEED,
            ttl_seconds=900,
        )
        root = ElementTree.fromstring(xml_text)
        channel = root.find("channel")
        if channel is None:
            raise UpstreamServiceError("fed", "Federal Reserve RSS feed returned an unexpected shape.")

        items = []
        for item in channel.findall("item")[:limit]:
            items.append(
                {
                    "title": self._xml_text(item, "title"),
                    "link": self._xml_text(item, "link"),
                    "category": self._xml_text(item, "category"),
                    "published_at": self._xml_text(item, "pubDate"),
                }
            )

        return {
            "title": self._xml_text(channel, "title"),
            "items": items,
        }

    def _fetch_bls_release_events(self) -> list[_MacroEvent]:
        ics_text = self._http.get_text(
            "bls_schedule",
            BLS_RELEASE_CALENDAR_ICS_URL,
            ttl_seconds=21600,
        )
        events: list[_MacroEvent] = []
        for raw_block in self._parse_ics_events(ics_text):
            summary = str(raw_block.get("SUMMARY") or "").strip()
            dtstart_key = next((key for key in raw_block if key.startswith("DTSTART")), None)
            starts_at = self._parse_ics_start(
                f"{dtstart_key}:{raw_block[dtstart_key]}" if dtstart_key else raw_block.get("DTSTART")
            )
            if not summary or starts_at is None:
                continue
            category, importance = self._classify_bls_release(summary)
            events.append(
                _MacroEvent(
                    source="bls",
                    event_name=summary,
                    category=category,
                    importance=importance,
                    scheduled_at_utc=starts_at.astimezone(timezone.utc),
                    scheduled_date=None,
                    time_precision="datetime",
                    metadata={
                        "location": raw_block.get("LOCATION"),
                    },
                )
            )
        return events

    def _fetch_bea_release_events(self) -> list[_MacroEvent]:
        payload = self._http.get_json(
            "bea_schedule",
            BEA_RELEASE_DATES_URL,
            ttl_seconds=21600,
        )
        if not isinstance(payload, dict):
            raise UpstreamServiceError("bea_schedule", "BEA release schedule returned an unexpected payload.")

        events: list[_MacroEvent] = []
        for name, item in payload.items():
            if not isinstance(item, dict):
                continue
            release_dates = item.get("release_dates")
            if not isinstance(release_dates, list):
                continue
            category, importance = self._classify_bea_release(name)
            to_be_rescheduled = {
                str(value)
                for value in item.get("to_be_rescheduled", [])
                if isinstance(value, str)
            }
            for raw_date in release_dates:
                if not isinstance(raw_date, str):
                    continue
                scheduled_at = self._parse_iso_datetime(raw_date)
                if scheduled_at is None:
                    continue
                events.append(
                    _MacroEvent(
                        source="bea",
                        event_name=name,
                        category=category,
                        importance=importance,
                        scheduled_at_utc=scheduled_at.astimezone(timezone.utc),
                        scheduled_date=None,
                        time_precision="datetime",
                        metadata={
                            "reschedule_flag": raw_date in to_be_rescheduled,
                        },
                    )
                )
        return events

    def _fetch_fomc_schedule_events(self) -> list[_MacroEvent]:
        html_text = self._http.get_text(
            "fomc_schedule",
            FOMC_SCHEDULE_URL,
            ttl_seconds=86400,
        )
        events: list[_MacroEvent] = []
        for section_year, section_html in re.findall(r"<p>For (\d{4}):</p>\s*<ul>(.*?)</ul>", html_text, flags=re.I | re.S):
            base_year = self._safe_int(section_year)
            if base_year is None:
                continue
            for raw_item in re.findall(r"<li>(.*?)</li>", section_html, flags=re.I | re.S):
                line = html.unescape(re.sub(r"<[^>]+>", "", raw_item)).strip()
                if not line:
                    continue
                scheduled_at, meeting_start = self._parse_fomc_schedule_line(line=line, base_year=base_year)
                if scheduled_at is None:
                    continue
                events.append(
                    _MacroEvent(
                        source="fomc",
                        event_name="FOMC statement and Chair press conference",
                        category="monetary_policy",
                        importance="very_high",
                        scheduled_at_utc=scheduled_at.astimezone(timezone.utc),
                        scheduled_date=None,
                        time_precision="datetime",
                        metadata={
                            "meeting_start_date": meeting_start.isoformat() if meeting_start else None,
                            "meeting_schedule_line": line,
                        },
                    )
                )
        return events

    def _fetch_treasury_auction_events(self) -> list[_MacroEvent]:
        payload = self._http.get_json(
            "treasury_auctions",
            TREASURY_UPCOMING_AUCTIONS_URL,
            params={"page[size]": 100, "sort": "auction_date"},
            ttl_seconds=21600,
        )
        rows = payload.get("data", [])
        if not isinstance(rows, list):
            raise UpstreamServiceError("treasury_auctions", "Treasury upcoming auctions returned an unexpected payload.")

        events: list[_MacroEvent] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            auction_date = self._parse_date_only(row.get("auction_date"))
            if auction_date is None:
                continue
            security_type = str(row.get("security_type") or "").strip()
            security_term = str(row.get("security_term") or "").strip()
            importance = self._classify_treasury_auction_importance(security_type, security_term)
            events.append(
                _MacroEvent(
                    source="treasury",
                    event_name=f"Treasury {security_term} {security_type} auction".strip(),
                    category="treasury_auction",
                    importance=importance,
                    scheduled_at_utc=None,
                    scheduled_date=auction_date,
                    time_precision="date",
                    metadata={
                        "announcement_date": row.get("announcemt_date"),
                        "issue_date": row.get("issue_date"),
                        "offering_amount_usd": self._safe_float(row.get("offering_amt")),
                        "security_type": security_type or None,
                        "security_term": security_term or None,
                    },
                )
            )
        return events

    def _build_orderbook_liquidity_view(self, venues: dict[str, Any], reference_price: float | None) -> dict[str, Any]:
        orderbook_venues = [
            venue_data.get("orderbook")
            for venue_data in venues.values()
            if isinstance(venue_data, dict) and isinstance(venue_data.get("orderbook"), dict)
        ]
        upper_wall_strength = 0.0
        lower_wall_strength = 0.0
        for venue in orderbook_venues:
            for wall in venue.get("top_ask_walls", [])[:3]:
                upper_wall_strength += wall.get("notional", 0.0)
            for wall in venue.get("top_bid_walls", [])[:3]:
                lower_wall_strength += wall.get("notional", 0.0)

        if upper_wall_strength > lower_wall_strength * 1.15:
            wall_bias = "upside_heavier"
        elif lower_wall_strength > upper_wall_strength * 1.15:
            wall_bias = "downside_heavier"
        else:
            wall_bias = "balanced"

        band_biases: dict[str, str] = {}
        for band_label in ("0.10%", "0.25%", "0.50%", "1.00%"):
            bid_total = 0.0
            ask_total = 0.0
            for venue in orderbook_venues:
                band = venue.get("band_depth", {}).get(band_label, {})
                bid_total += band.get("bid_notional", 0.0) or 0.0
                ask_total += band.get("ask_notional", 0.0) or 0.0
            if abs(bid_total - ask_total) < max(bid_total, ask_total, 1.0) * 0.05:
                bias = "balanced"
            elif bid_total > ask_total:
                bias = "bid_heavier"
            else:
                bias = "ask_heavier"
            band_biases[band_label] = bias

        narratives: list[str] = []
        if wall_bias == "upside_heavier":
            narratives.append("Near-price ask walls are thicker than bid walls, so upside sweeps face heavier resting liquidity.")
        elif wall_bias == "downside_heavier":
            narratives.append("Near-price bid walls are thicker than ask walls, so downside sweeps face heavier resting liquidity.")
        else:
            narratives.append("Near-price resting liquidity looks relatively balanced across sides.")

        if band_biases.get("0.25%") == "ask_heavier" and wall_bias == "upside_heavier":
            sweep_risk = "upside_sweep_risk_high"
        elif band_biases.get("0.25%") == "bid_heavier" and wall_bias == "downside_heavier":
            sweep_risk = "downside_sweep_risk_high"
        else:
            sweep_risk = "two_sided_or_mixed"

        return {
            "reference_price": reference_price,
            "wall_bias": wall_bias,
            "band_biases": band_biases,
            "upside_wall_strength_usd": round(upper_wall_strength, 2),
            "downside_wall_strength_usd": round(lower_wall_strength, 2),
            "liquidity_sweep_risk": sweep_risk,
            "narratives": narratives,
        }

    @staticmethod
    def _parse_ics_events(ics_text: str) -> list[dict[str, str]]:
        unfolded_lines: list[str] = []
        for line in ics_text.splitlines():
            if line.startswith((" ", "\t")) and unfolded_lines:
                unfolded_lines[-1] += line[1:]
            else:
                unfolded_lines.append(line)

        events: list[dict[str, str]] = []
        current: dict[str, str] | None = None
        for line in unfolded_lines:
            if line == "BEGIN:VEVENT":
                current = {}
                continue
            if line == "END:VEVENT":
                if current:
                    events.append(current)
                current = None
                continue
            if current is None or ":" not in line:
                continue
            raw_key, value = line.split(":", 1)
            key = raw_key.split(";", 1)[0]
            current[raw_key] = value.strip()
            current[key] = value.strip()
        return events

    def _parse_ics_start(self, raw_value: str | None) -> datetime | None:
        if not raw_value:
            return None
        timezone_match = re.search(r"TZID=([^:;]+)", raw_value)
        value = raw_value.split(":", 1)[-1]
        if timezone_match:
            return self._parse_named_timezone_datetime(value, timezone_match.group(1))
        if value.endswith("Z"):
            try:
                return datetime.strptime(value, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
            except ValueError:
                return None
        try:
            return datetime.strptime(value, "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
        except ValueError:
            return None

    def _parse_named_timezone_datetime(self, value: str, tz_name: str) -> datetime | None:
        parsed_formats = ("%Y%m%dT%H%M%S", "%Y%m%dT%H%M")
        naive: datetime | None = None
        for fmt in parsed_formats:
            with_timezone = value.split(":")[-1]
            try:
                naive = datetime.strptime(with_timezone, fmt)
                break
            except ValueError:
                continue
        if naive is None:
            return None

        timezone_candidates = [tz_name, "America/New_York" if tz_name == "US-Eastern" else tz_name]
        for candidate in timezone_candidates:
            try:
                return naive.replace(tzinfo=ZoneInfo(candidate))
            except ZoneInfoNotFoundError:
                continue

        fallback_offset = -4 if 3 <= naive.month <= 11 else -5
        return naive.replace(tzinfo=timezone(timedelta(hours=fallback_offset)))

    @staticmethod
    def _parse_iso_datetime(raw_value: str | None) -> datetime | None:
        if not raw_value:
            return None
        try:
            return datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
        except ValueError:
            return None

    @staticmethod
    def _parse_date_only(raw_value: Any) -> date | None:
        if not raw_value:
            return None
        try:
            return date.fromisoformat(str(raw_value))
        except ValueError:
            return None

    def _parse_fomc_schedule_line(self, *, line: str, base_year: int) -> tuple[datetime | None, date | None]:
        months_pattern = (
            "January|February|March|April|May|June|July|August|September|October|November|December"
        )
        matches = list(re.finditer(rf"({months_pattern})\s+(\d{{1,2}})(?:,\s*(\d{{4}}))?", line))
        if len(matches) < 2:
            return None, None

        start_month, start_day, start_year = matches[0].groups()
        end_month, end_day, end_year = matches[1].groups()
        meeting_start = datetime.strptime(
            f"{start_month} {start_day} {start_year or base_year}",
            "%B %d %Y",
        ).date()
        meeting_end = datetime.strptime(
            f"{end_month} {end_day} {end_year or base_year}",
            "%B %d %Y",
        )
        try:
            eastern = ZoneInfo(NY_TZ)
        except ZoneInfoNotFoundError:
            eastern = timezone(timedelta(hours=-5))
        scheduled_local = datetime.combine(meeting_end.date(), time(hour=14, minute=0), tzinfo=eastern)
        return scheduled_local, meeting_start

    @staticmethod
    def _classify_bls_release(event_name: str) -> tuple[str, str]:
        name = event_name.lower()
        if "consumer price index" in name:
            return "inflation", "very_high"
        if "employment situation" in name:
            return "employment", "very_high"
        if "producer price index" in name or "import and export prices" in name:
            return "inflation", "high"
        if "job openings" in name or "jolts" in name:
            return "labor_demand", "high"
        if "employment cost index" in name or "productivity" in name:
            return "labor_costs", "medium"
        return "macro_release", "medium"

    @staticmethod
    def _classify_bea_release(event_name: str) -> tuple[str, str]:
        name = event_name.lower()
        if "gross domestic product" in name:
            return "growth", "very_high"
        if "personal income and outlays" in name:
            return "consumption", "high"
        if "trade" in name:
            return "trade", "medium"
        return "bea_release", "medium"

    @staticmethod
    def _classify_treasury_auction_importance(security_type: str, security_term: str) -> str:
        combined = f"{security_term} {security_type}".lower()
        if any(keyword in combined for keyword in ("20-year", "30-year", "bond", "10-year", "7-year", "5-year", "3-year", "note")):
            return "medium"
        if any(keyword in combined for keyword in ("17-week", "26-week", "52-week")):
            return "medium"
        return "low"

    @staticmethod
    def _dedupe_macro_events(events: list[_MacroEvent]) -> list[_MacroEvent]:
        seen: set[tuple[Any, ...]] = set()
        deduped: list[_MacroEvent] = []
        for event in sorted(
            events,
            key=lambda item: (
                item.scheduled_at_utc or datetime.max.replace(tzinfo=timezone.utc),
                item.scheduled_date or date.max,
                item.source,
                item.event_name,
            ),
        ):
            key = (
                event.source,
                event.event_name,
                event.scheduled_at_utc.isoformat() if event.scheduled_at_utc else None,
                event.scheduled_date.isoformat() if event.scheduled_date else None,
            )
            if key in seen:
                continue
            seen.add(key)
            deduped.append(event)
        return deduped

    def _serialize_macro_event(self, event: _MacroEvent, *, now_utc: datetime) -> dict[str, Any]:
        pre_event_hours, post_event_hours = self._event_risk_window_hours(event.importance)
        starts_in_hours: float | None = None
        is_within_next_8h = False
        if event.scheduled_at_utc is not None:
            starts_in_hours = round((event.scheduled_at_utc - now_utc).total_seconds() / 3600, 3)
            is_within_next_8h = 0 <= starts_in_hours <= 8

        payload = {
            "source": event.source,
            "event_name": event.event_name,
            "category": event.category,
            "importance": event.importance,
            "time_precision": event.time_precision,
            "scheduled_time_utc": event.scheduled_at_utc.replace(microsecond=0).isoformat() if event.scheduled_at_utc else None,
            "scheduled_date": event.scheduled_date.isoformat() if event.scheduled_date else None,
            "starts_in_hours": starts_in_hours,
            "is_within_next_8h": is_within_next_8h,
            "risk_window": {
                "pre_event_hours": pre_event_hours,
                "post_event_hours": post_event_hours,
            },
        }
        if event.metadata:
            payload["metadata"] = event.metadata
        return payload

    @staticmethod
    def _event_risk_window_hours(importance: str) -> tuple[int, int]:
        mapping = {
            "very_high": (24, 8),
            "high": (12, 4),
            "medium": (8, 2),
            "low": (4, 1),
        }
        return mapping.get(importance, (8, 2))

    @staticmethod
    def _highest_event_importance(events: list[dict[str, Any]]) -> str | None:
        if not events:
            return None
        ranking = {"very_high": 4, "high": 3, "medium": 2, "low": 1}
        top = max(events, key=lambda item: ranking.get(str(item.get("importance")), 0))
        return top.get("importance")

    def get_sec_company_tickers(self, query: str | None = None, exchange: str | None = None, limit: int = 25) -> dict[str, Any]:
        payload = self._http.get_json(
            "sec",
            SEC_FILES_URL,
            headers={"User-Agent": self._config.user_agent},
            ttl_seconds=86400,
        )
        rows = [self._zip_fields(payload["fields"], item) for item in payload.get("data", [])]

        if query:
            normalized = query.strip().upper()
            filtered_rows = [
                row
                for row in rows
                if normalized in str(row.get("ticker", "")).upper() or normalized in str(row.get("name", "")).upper()
            ]
            rows = sorted(
                filtered_rows,
                key=lambda row: (
                    str(row.get("ticker", "")).upper() != normalized,
                    not str(row.get("name", "")).upper().startswith(normalized),
                    str(row.get("ticker", "")),
                ),
            )
        if exchange:
            rows = [row for row in rows if str(row.get("exchange", "")).lower() == exchange.lower()]

        return {
            "count": min(len(rows), limit),
            "results": rows[:limit],
        }

    def get_sec_submissions(self, entity: str, forms_limit: int = 10) -> dict[str, Any]:
        cik = self._resolve_sec_entity_to_cik(entity)
        payload = self._http.get_json(
            "sec",
            f"{SEC_SUBMISSIONS_URL}/CIK{cik}.json",
            headers={"User-Agent": self._config.user_agent},
            ttl_seconds=1800,
        )
        filings = payload.get("filings", {}).get("recent", {})
        accession_numbers = filings.get("accessionNumber", [])
        filing_dates = filings.get("filingDate", [])
        forms = filings.get("form", [])
        primary_documents = filings.get("primaryDocument", [])
        descriptions = filings.get("primaryDocDescription", [])

        recent_filings = []
        for index in range(min(forms_limit, len(accession_numbers))):
            accession_number = accession_numbers[index]
            primary_document = primary_documents[index]
            accession_path = accession_number.replace("-", "")
            archive_url = (
                f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession_path}/{primary_document}"
                if primary_document
                else None
            )
            recent_filings.append(
                {
                    "filing_date": filing_dates[index] if index < len(filing_dates) else None,
                    "form": forms[index] if index < len(forms) else None,
                    "accession_number": accession_number,
                    "primary_document": primary_document,
                    "description": descriptions[index] if index < len(descriptions) else None,
                    "archive_url": archive_url,
                }
            )

        return {
            "cik": cik,
            "name": payload.get("name"),
            "tickers": payload.get("tickers", []),
            "exchanges": payload.get("exchanges", []),
            "recent_filings": recent_filings,
        }

    def get_sec_recent_filings_for_entities(self, entities: list[str]) -> dict[str, Any]:
        return {
            "entities": [self.get_sec_submissions(entity, forms_limit=3) for entity in entities]
        }

    def get_cftc_bitcoin_cot(self, exchange: str = "cme", limit: int = 4) -> dict[str, Any]:
        where_clause = "commodity_name='BITCOIN'"
        if exchange.lower() == "cme":
            where_clause += " AND market_and_exchange_names like '%CHICAGO MERCANTILE EXCHANGE%'"

        payload = self._http.get_json(
            "cftc",
            CFTC_COT_URL,
            params={
                "$select": (
                    "report_date_as_yyyy_mm_dd,contract_market_name,market_and_exchange_names,"
                    "open_interest_all,noncomm_positions_long_all,noncomm_positions_short_all,"
                    "comm_positions_long_all,comm_positions_short_all"
                ),
                "$where": where_clause,
                "$order": "report_date_as_yyyy_mm_dd DESC",
                "$limit": limit,
            },
            ttl_seconds=86400,
        )
        return {
            "exchange_filter": exchange,
            "records": [
                {
                    "report_date": row.get("report_date_as_yyyy_mm_dd"),
                    "contract_market_name": row.get("contract_market_name"),
                    "market_and_exchange_names": row.get("market_and_exchange_names"),
                    "open_interest": self._safe_int(row.get("open_interest_all")),
                    "noncommercial_long": self._safe_int(row.get("noncomm_positions_long_all")),
                    "noncommercial_short": self._safe_int(row.get("noncomm_positions_short_all")),
                    "commercial_long": self._safe_int(row.get("comm_positions_long_all")),
                    "commercial_short": self._safe_int(row.get("comm_positions_short_all")),
                }
                for row in payload
            ],
        }

    def _coinglass_get_data(self, path: str, params: dict[str, Any] | None) -> Any:
        payload = self._http.get_json(
            "coinglass",
            f"{COINGLASS_BASE}{path}",
            params=params,
            headers={"CG-API-KEY": self._config.coinglass_api_key},
            ttl_seconds=30,
        )

        if not isinstance(payload, dict):
            raise UpstreamServiceError("coinglass", f"CoinGlass returned an unexpected payload: {payload}")

        success = payload.get("success")
        if success is False:
            message = payload.get("msg") or payload.get("message") or "CoinGlass returned success=false."
            raise UpstreamServiceError("coinglass", f"CoinGlass error: {message}")

        data = payload.get("data")
        if data is None:
            raise UpstreamServiceError("coinglass", f"CoinGlass returned no data for {path}.")
        return data

    def _bybit_get_result(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        payload = self._http.get_json(
            "bybit",
            f"{BYBIT_BASE}{path}",
            params=params,
            ttl_seconds=20,
        )
        if not isinstance(payload, dict):
            raise UpstreamServiceError("bybit", f"Bybit returned an unexpected payload: {payload}")

        if payload.get("retCode") not in (0, "0", None):
            raise UpstreamServiceError(
                "bybit",
                f"Bybit error {payload.get('retCode')}: {payload.get('retMsg') or payload.get('retExtInfo') or 'unknown error'}",
            )

        result = payload.get("result")
        if not isinstance(result, dict):
            raise UpstreamServiceError("bybit", f"Bybit returned no result for {path}.")
        return result

    def _coinglass_get_first_available_data(self, path: str, param_variants: list[dict[str, Any]]) -> Any:
        attempts: list[str] = []
        last_error: UpstreamServiceError | None = None

        for params in param_variants:
            try:
                return self._coinglass_get_data(path, params=params)
            except UpstreamServiceError as exc:
                last_error = exc
                symbol = params.get("symbol")
                attempts.append(str(symbol))
                if exc.source != "coinglass":
                    raise
                if "returned no data" in exc.detail:
                    continue
                raise UpstreamServiceError(
                    "coinglass",
                    f"{exc.detail} Path: {path}. Params: {params}",
                    status_code=exc.status_code,
                ) from exc

        attempted_symbols = ", ".join(self._unique_strings(attempts)) or "none"
        detail = f"CoinGlass returned no data for {path}. Tried symbols: {attempted_symbols}."
        raise UpstreamServiceError(
            "coinglass",
            detail,
            status_code=last_error.status_code if last_error else 502,
        )

    @staticmethod
    def _coinglass_capture_component(unavailable: list[dict[str, Any]], component: str, func) -> Any:
        try:
            return func()
        except UpstreamServiceError as exc:
            unavailable.append(
                {
                    "component": component,
                    "reason": exc.detail,
                }
            )
            return None

    def _get_coinglass_open_interest(
        self,
        *,
        symbol_candidates: list[str],
        asset_symbol: str,
        exchange: str,
        interval: str,
    ) -> Any:
        try:
            return self._coinglass_get_first_available_data(
                "/api/futures/open-interest/history",
                self._build_coinglass_param_variants(symbol_candidates, exchange, interval),
            )
        except UpstreamServiceError as exc:
            primary_error = exc

        exchange_history = self._coinglass_get_data(
            "/api/futures/open-interest/exchange-history-chart",
            params={
                "symbol": asset_symbol,
                "interval": interval,
            },
        )
        exchange_rows = self._extract_exchange_history_rows(exchange_history, exchange)
        if exchange_rows:
            return exchange_rows

        raise primary_error

    @staticmethod
    def _build_coinglass_param_variants(symbol_candidates: list[str], exchange: str, interval: str) -> list[dict[str, Any]]:
        variants: list[dict[str, Any]] = []
        seen: set[tuple[str, str, str]] = set()
        for symbol in symbol_candidates:
            key = (symbol, exchange, interval)
            if key in seen:
                continue
            seen.add(key)
            variants.append(
                {
                    "symbol": symbol,
                    "exchange": exchange,
                    "interval": interval,
                }
            )
        return variants

    def _resolve_coinglass_supported_pair(self, exchange: str, symbol: str) -> str:
        try:
            data = self._coinglass_get_data(
                "/api/futures/supported-exchange-pairs",
                params=None,
            )
        except Exception:
            return symbol

        pairs = self._extract_supported_pairs_for_exchange(data, exchange)
        if not pairs:
            return symbol

        normalized_target = self._normalize_coinglass_pair_symbol(symbol)
        exact_match = next((pair for pair in pairs if self._normalize_coinglass_pair_symbol(pair) == normalized_target), None)
        if exact_match:
            return exact_match

        substring_match = next(
            (
                pair
                for pair in pairs
                if normalized_target in self._normalize_coinglass_pair_symbol(pair)
                or self._normalize_coinglass_pair_symbol(pair) in normalized_target
            ),
            None,
        )
        if substring_match:
            return substring_match

        root_asset = self._extract_root_asset(symbol)
        root_match = next(
            (
                pair
                for pair in pairs
                if self._normalize_coinglass_pair_symbol(pair).startswith(root_asset)
            ),
            None,
        )
        return root_match or symbol

    def _capture(self, func) -> dict[str, Any]:
        try:
            return {"ok": True, "data": func()}
        except UpstreamServiceError as exc:
            return {"ok": False, "reason": exc.detail, "source": exc.source}
        except Exception as exc:
            return {"ok": False, "reason": str(exc), "source": "internal"}

    @staticmethod
    def _skipped(reason: str) -> dict[str, Any]:
        return {"ok": False, "reason": reason, "source": "gateway"}

    def _resolve_sec_entity_to_cik(self, entity: str) -> str:
        normalized = entity.strip().upper()
        if normalized.isdigit():
            return normalized.zfill(10)

        matches = self.get_sec_company_tickers(query=normalized, limit=10)["results"]
        for match in matches:
            if str(match.get("ticker", "")).upper() == normalized:
                return str(match["cik"]).zfill(10)
        raise UpstreamServiceError("sec", f"Unable to resolve SEC entity '{entity}' to a CIK.", status_code=404)

    @staticmethod
    def _extract_root_asset(symbol: str) -> str:
        symbol = symbol.upper()
        for suffix in ("USDT", "USD", "USDC", "BUSD"):
            if symbol.endswith(suffix):
                return symbol[: -len(suffix)]
        return symbol

    @staticmethod
    def _first_item_value(rows: Any, key: str) -> Any:
        if isinstance(rows, list) and rows:
            return rows[0].get(key)
        return None

    @staticmethod
    def _normalize_binance_candle(item: list[Any]) -> dict[str, Any]:
        return {
            "open_time": datetime.fromtimestamp(item[0] / 1000, tz=timezone.utc).replace(microsecond=0).isoformat(),
            "close_time": datetime.fromtimestamp(item[6] / 1000, tz=timezone.utc).replace(microsecond=0).isoformat(),
            "open": float(item[1]),
            "high": float(item[2]),
            "low": float(item[3]),
            "close": float(item[4]),
            "volume": float(item[5]),
            "quote_volume": float(item[7]),
            "trade_count": int(item[8]),
        }

    @staticmethod
    def _normalize_binance_basis_row(row: dict[str, Any]) -> dict[str, Any]:
        basis = GatewayService._safe_float(row.get("basis"))
        basis_rate = GatewayService._safe_float(row.get("basisRate"))
        annualized_basis_rate = GatewayService._safe_float(row.get("annualizedBasisRate"))
        futures_price = GatewayService._safe_float(row.get("futuresPrice"))
        index_price = GatewayService._safe_float(row.get("indexPrice"))
        return {
            "time": row.get("timestamp"),
            "contract_type": row.get("contractType"),
            "basis": basis,
            "basisRate": basis_rate,
            "annualizedBasisRate": annualized_basis_rate,
            "futuresPrice": futures_price,
            "indexPrice": index_price,
            "value": basis_rate if basis_rate is not None else basis,
            "close": basis_rate if basis_rate is not None else basis,
        }

    @staticmethod
    def _normalize_binance_open_interest_hist_row(row: dict[str, Any]) -> dict[str, Any]:
        open_interest = GatewayService._safe_float(row.get("sumOpenInterest"))
        open_interest_value = GatewayService._safe_float(row.get("sumOpenInterestValue"))
        return {
            "time": row.get("timestamp"),
            "sumOpenInterest": open_interest,
            "sumOpenInterestValue": open_interest_value,
            "value": open_interest_value if open_interest_value is not None else open_interest,
            "close": open_interest_value if open_interest_value is not None else open_interest,
            "openInterest": open_interest,
        }

    @staticmethod
    def _normalize_binance_ratio_row(row: dict[str, Any], value_key: str) -> dict[str, Any]:
        ratio = GatewayService._safe_float(row.get(value_key))
        long_account = GatewayService._safe_float(row.get("longAccount"))
        short_account = GatewayService._safe_float(row.get("shortAccount"))
        return {
            "time": row.get("timestamp"),
            "longAccount": long_account,
            "shortAccount": short_account,
            "longShortRatio": ratio,
            "value": ratio,
            "close": ratio,
        }

    @staticmethod
    def _normalize_binance_taker_volume_row(row: dict[str, Any]) -> dict[str, Any]:
        buy_vol = GatewayService._safe_float(row.get("buyVol"))
        sell_vol = GatewayService._safe_float(row.get("sellVol"))
        ratio = GatewayService._safe_float(row.get("buySellRatio"))
        return {
            "time": row.get("timestamp"),
            "buyVol": buy_vol,
            "sellVol": sell_vol,
            "buySellRatio": ratio,
            "value": ratio,
            "close": ratio,
        }

    @staticmethod
    def _mark_index_spread(mark_price: float | None, index_price: float | None) -> float | None:
        if mark_price is None or index_price is None:
            return None
        return round(mark_price - index_price, 6)

    def _build_candle_structure_summary(self, candles: list[dict[str, Any]]) -> dict[str, Any] | None:
        if not candles:
            return None

        latest = candles[-1]
        previous = candles[-2] if len(candles) > 1 else None
        highs = [candle["high"] for candle in candles]
        lows = [candle["low"] for candle in candles]
        closes = [candle["close"] for candle in candles]
        total_volume = sum(candle.get("volume", 0.0) for candle in candles)
        total_quote_volume = sum(candle.get("quote_volume", 0.0) for candle in candles)

        atr = self._compute_average_true_range(candles)
        latest_close = latest["close"]
        window_high = max(highs)
        window_low = min(lows)
        window_open = candles[0]["open"]

        return {
            "bars": len(candles),
            "latest_open_time": latest.get("open_time"),
            "latest_close": latest_close,
            "previous_close": previous["close"] if previous else None,
            "window_open": window_open,
            "window_change_pct": self._pct_change(window_open, latest_close),
            "last_bar_change_pct": self._pct_change(latest["open"], latest_close),
            "window_high": window_high,
            "window_low": window_low,
            "window_range_pct": self._range_pct(window_low, window_high, latest_close),
            "distance_to_window_high_pct": self._distance_pct(latest_close, window_high),
            "distance_to_window_low_pct": self._distance_pct(window_low, latest_close),
            "atr": atr,
            "atr_pct": self._pct_of_value(atr, latest_close),
            "approx_vwap": round(total_quote_volume / total_volume, 4) if total_volume else None,
            "volume_total": round(total_volume, 4),
            "trend": self._classify_trend(window_open, latest_close),
            "close_position_in_range": self._close_position_in_range(window_low, window_high, latest_close),
        }

    def _build_multi_timeframe_levels(self, timeframe_candles: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
        candles_15m = timeframe_candles.get("15m", [])
        candles_1h = timeframe_candles.get("1h", [])
        candles_4h = timeframe_candles.get("4h", [])
        candles_8h = timeframe_candles.get("8h", [])
        candles_1d = timeframe_candles.get("1d", [])
        candles_1w = timeframe_candles.get("1w", [])
        candles_1m = timeframe_candles.get("1M", [])

        return {
            "range_4h_high": self._max_high(candles_15m[-16:]),
            "range_4h_low": self._min_low(candles_15m[-16:]),
            "range_8h_high": self._max_high(candles_1h[-8:]),
            "range_8h_low": self._min_low(candles_1h[-8:]),
            "range_24h_high": self._max_high(candles_1h[-24:]),
            "range_24h_low": self._min_low(candles_1h[-24:]),
            "swing_3d_high": self._max_high(candles_4h[-18:]),
            "swing_3d_low": self._min_low(candles_4h[-18:]),
            "daily_7d_high": self._max_high(candles_1d[-7:]),
            "daily_7d_low": self._min_low(candles_1d[-7:]),
            "daily_30d_high": self._max_high(candles_1d[-30:]),
            "daily_30d_low": self._min_low(candles_1d[-30:]),
            "recent_8h_bar_high": self._max_high(candles_8h[-3:]),
            "recent_8h_bar_low": self._min_low(candles_8h[-3:]),
            "weekly_12w_high": self._max_high(candles_1w[-12:]),
            "weekly_12w_low": self._min_low(candles_1w[-12:]),
            "weekly_24w_high": self._max_high(candles_1w[-24:]),
            "weekly_24w_low": self._min_low(candles_1w[-24:]),
            "monthly_12m_high": self._max_high(candles_1m[-12:]),
            "monthly_12m_low": self._min_low(candles_1m[-12:]),
        }

    @staticmethod
    def _raw_candle_tail_size(interval: str) -> int:
        return {
            "15m": 12,
            "1h": 12,
            "4h": 12,
            "8h": 10,
            "1d": 20,
            "1w": 20,
            "1M": 12,
        }.get(interval, 10)

    @staticmethod
    def _binance_kline_ttl(interval: str) -> int:
        return {
            "15m": 30,
            "1h": 60,
            "4h": 180,
            "8h": 300,
            "1d": 900,
            "1w": 3600,
            "1M": 21600,
        }.get(interval, 60)

    def _build_support_resistance_levels(
        self,
        timeframe_candles: dict[str, list[dict[str, Any]]],
        current_price: float | None,
    ) -> dict[str, Any]:
        candidates: list[dict[str, Any]] = []

        def add_level(label: str, value: float | None, category: str) -> None:
            if value is None:
                return
            candidates.append({"label": label, "value": round(value, 4), "category": category})

        candles_15m = timeframe_candles.get("15m", [])
        candles_1h = timeframe_candles.get("1h", [])
        candles_4h = timeframe_candles.get("4h", [])
        candles_1d = timeframe_candles.get("1d", [])
        candles_1w = timeframe_candles.get("1w", [])
        candles_1m = timeframe_candles.get("1M", [])

        add_level("range_8h_high", self._max_high(candles_1h[-8:]), "intraday")
        add_level("range_8h_low", self._min_low(candles_1h[-8:]), "intraday")
        add_level("range_24h_high", self._max_high(candles_1h[-24:]), "intraday")
        add_level("range_24h_low", self._min_low(candles_1h[-24:]), "intraday")
        add_level("swing_3d_high", self._max_high(candles_4h[-18:]), "swing")
        add_level("swing_3d_low", self._min_low(candles_4h[-18:]), "swing")
        add_level("daily_7d_high", self._max_high(candles_1d[-7:]), "weekly_filter")
        add_level("daily_7d_low", self._min_low(candles_1d[-7:]), "weekly_filter")
        add_level("previous_week_high", self._candle_value(candles_1w, -2, "high"), "weekly_filter")
        add_level("previous_week_low", self._candle_value(candles_1w, -2, "low"), "weekly_filter")
        add_level("weekly_12w_high", self._max_high(candles_1w[-12:]), "multi_week")
        add_level("weekly_12w_low", self._min_low(candles_1w[-12:]), "multi_week")
        add_level("weekly_24w_high", self._max_high(candles_1w[-24:]), "multi_week")
        add_level("weekly_24w_low", self._min_low(candles_1w[-24:]), "multi_week")
        add_level("monthly_12m_high", self._max_high(candles_1m[-12:]), "monthly")
        add_level("monthly_12m_low", self._min_low(candles_1m[-12:]), "monthly")

        for fib_label, fib_map in self._build_multi_timeframe_fibonacci_levels(timeframe_candles).items():
            if not isinstance(fib_map, dict):
                continue
            for ratio, value in fib_map.items():
                if ratio in {"swing_high", "swing_low"}:
                    continue
                add_level(f"{fib_label}_{ratio}", self._safe_float(value), "fibonacci")

        deduped = self._dedupe_levels(candidates)
        supports = sorted(
            [item for item in deduped if current_price is None or item["value"] <= current_price],
            key=lambda item: (abs((current_price or item["value"]) - item["value"]), -item["value"]),
        )[:8]
        resistances = sorted(
            [item for item in deduped if current_price is None or item["value"] >= current_price],
            key=lambda item: (abs(item["value"] - (current_price or item["value"])), item["value"]),
        )[:8]

        return {
            "spot_price": current_price,
            "supports": supports,
            "resistances": resistances,
        }

    def _build_multi_timeframe_fibonacci_levels(self, timeframe_candles: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
        fib_sources = {
            "swing_3d": timeframe_candles.get("4h", [])[-18:],
            "weekly_30d": timeframe_candles.get("1d", [])[-30:],
            "multi_week_24w": timeframe_candles.get("1w", [])[-24:],
        }

        result: dict[str, Any] = {}
        for label, candles in fib_sources.items():
            levels = self._build_fibonacci_levels(candles)
            if levels:
                result[label] = levels
        return result

    def _build_fibonacci_levels(self, candles: list[dict[str, Any]]) -> dict[str, Any] | None:
        if not candles:
            return None
        swing_high = self._max_high(candles)
        swing_low = self._min_low(candles)
        if swing_high is None or swing_low is None or swing_high == swing_low:
            return None
        diff = swing_high - swing_low
        return {
            "swing_high": round(swing_high, 4),
            "swing_low": round(swing_low, 4),
            "0.236": round(swing_high - diff * 0.236, 4),
            "0.382": round(swing_high - diff * 0.382, 4),
            "0.5": round(swing_high - diff * 0.5, 4),
            "0.618": round(swing_high - diff * 0.618, 4),
            "0.786": round(swing_high - diff * 0.786, 4),
        }

    @staticmethod
    def _dedupe_levels(levels: list[dict[str, Any]]) -> list[dict[str, Any]]:
        seen: set[float] = set()
        deduped: list[dict[str, Any]] = []
        for item in levels:
            rounded_value = round(float(item["value"]), 2)
            if rounded_value in seen:
                continue
            seen.add(rounded_value)
            deduped.append(item)
        return deduped

    @staticmethod
    def _candle_value(candles: list[dict[str, Any]], index: int, key: str) -> float | None:
        if not candles:
            return None
        try:
            return candles[index].get(key)
        except IndexError:
            return None

    def _build_binance_basis_summary(self, rows: list[dict[str, Any]]) -> dict[str, Any] | None:
        if not rows:
            return None
        latest = rows[-1]
        previous = rows[-2] if len(rows) > 1 else None
        latest_basis_rate = latest.get("basisRate")
        previous_basis_rate = previous.get("basisRate") if previous else None
        latest_basis = latest.get("basis")

        state = "neutral"
        if latest_basis is not None:
            if latest_basis > 0:
                state = "contango"
            elif latest_basis < 0:
                state = "backwardation"

        return {
            "latest_time": latest.get("time"),
            "latest_basis": latest_basis,
            "latest_basis_rate": latest_basis_rate,
            "latest_annualized_basis_rate": latest.get("annualizedBasisRate"),
            "futures_price": latest.get("futuresPrice"),
            "index_price": latest.get("indexPrice"),
            "basis_rate_change_pct": self._compute_change_pct(previous_basis_rate, latest_basis_rate),
            "trend": self._classify_trend(previous_basis_rate, latest_basis_rate),
            "state": state,
        }

    def _build_open_interest_hist_summary(self, rows: list[dict[str, Any]]) -> dict[str, Any] | None:
        if not rows:
            return None
        summary = self._build_ohlc_summary(rows, value_key_candidates=("sumOpenInterestValue", "sumOpenInterest", "value"))
        if summary is None:
            return None
        latest = rows[-1]
        summary["latest_open_interest"] = latest.get("sumOpenInterest")
        summary["latest_open_interest_value"] = latest.get("sumOpenInterestValue")
        return summary

    def _build_ratio_summary(self, rows: list[dict[str, Any]], label: str) -> dict[str, Any] | None:
        summary = self._build_ohlc_summary(rows, value_key_candidates=("longShortRatio", "value", "close"))
        if summary is None:
            return None
        latest_value = summary.get("latest_value")
        summary["label"] = label
        summary["bias"] = self._classify_ratio_bias(latest_value)
        if rows:
            latest = rows[-1]
            summary["latest_long_account"] = latest.get("longAccount")
            summary["latest_short_account"] = latest.get("shortAccount")
        return summary

    def _build_taker_volume_summary(self, rows: list[dict[str, Any]]) -> dict[str, Any] | None:
        summary = self._build_ohlc_summary(rows, value_key_candidates=("buySellRatio", "value", "close"))
        if summary is None:
            return None
        latest = rows[-1]
        buy_vol = latest.get("buyVol")
        sell_vol = latest.get("sellVol")
        summary["latest_buy_volume"] = buy_vol
        summary["latest_sell_volume"] = sell_vol
        summary["net_buy_minus_sell_volume"] = round((buy_vol or 0.0) - (sell_vol or 0.0), 4)
        summary["bias"] = self._classify_taker_flow(summary.get("latest_value"))
        return summary

    def _build_binance_derivatives_composite_view(
        self,
        *,
        basis_summary: dict[str, Any] | None,
        open_interest_summary: dict[str, Any] | None,
        top_position_summary: dict[str, Any] | None,
        top_account_summary: dict[str, Any] | None,
        global_ratio_summary: dict[str, Any] | None,
        taker_volume_summary: dict[str, Any] | None,
    ) -> dict[str, Any]:
        components: list[str] = []

        if open_interest_summary and open_interest_summary.get("trend") == "up":
            components.append("open_interest_building")
        elif open_interest_summary and open_interest_summary.get("trend") == "down":
            components.append("open_interest_unwinding")

        if basis_summary and basis_summary.get("state") == "contango":
            components.append("positive_basis")
        elif basis_summary and basis_summary.get("state") == "backwardation":
            components.append("negative_basis")

        for summary in (top_position_summary, top_account_summary, global_ratio_summary, taker_volume_summary):
            if not summary:
                continue
            bias = summary.get("bias")
            if bias and bias != "balanced":
                components.append(bias)

        bullish_count = sum(1 for item in components if item in {"positive_basis", "long_crowded", "buyers_aggressive"})
        bearish_count = sum(1 for item in components if item in {"negative_basis", "short_crowded", "sellers_aggressive"})

        if bullish_count > bearish_count:
            direction_bias = "bullish_bias"
        elif bearish_count > bullish_count:
            direction_bias = "bearish_bias"
        else:
            direction_bias = "mixed_bias"

        return {
            "direction_bias": direction_bias,
            "signals": components,
            "bullish_signal_count": bullish_count,
            "bearish_signal_count": bearish_count,
        }

    @staticmethod
    def _classify_ratio_bias(value: float | None) -> str | None:
        if value is None:
            return None
        if value >= 1.1:
            return "long_crowded"
        if value <= 0.9:
            return "short_crowded"
        return "balanced"

    @staticmethod
    def _classify_taker_flow(value: float | None) -> str | None:
        if value is None:
            return None
        if value >= 1.1:
            return "buyers_aggressive"
        if value <= 0.9:
            return "sellers_aggressive"
        return "balanced"

    @staticmethod
    def _safe_float(value: Any) -> float | None:
        if value in (None, "", "-"):
            return None
        if isinstance(value, str) and value.strip().lower() in {"null", "none", "nan", "n/a"}:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _safe_int(value: Any) -> int | None:
        if value in (None, "", "-"):
            return None
        if isinstance(value, str) and value.strip().lower() in {"null", "none", "nan", "n/a"}:
            return None
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _normalize_coinglass_pair_symbol(symbol: str) -> str:
        return symbol.strip().upper().replace("-", "").replace("_", "").replace("/", "")

    @staticmethod
    def _normalize_bybit_kline_rows(rows: list[Any]) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for item in rows:
            if not isinstance(item, list) or len(item) < 7:
                continue
            normalized.append(
                {
                    "open_time": datetime.fromtimestamp(int(item[0]) / 1000, tz=timezone.utc).replace(microsecond=0).isoformat(),
                    "open": float(item[1]),
                    "high": float(item[2]),
                    "low": float(item[3]),
                    "close": float(item[4]),
                    "volume": float(item[5]),
                    "quote_volume": float(item[6]),
                }
            )
        return sorted(normalized, key=lambda row: row["open_time"])

    @staticmethod
    def _normalize_okx_candle_rows(rows: list[Any]) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for item in rows:
            if not isinstance(item, list) or len(item) < 8:
                continue
            normalized.append(
                {
                    "open_time": datetime.fromtimestamp(int(item[0]) / 1000, tz=timezone.utc).replace(microsecond=0).isoformat(),
                    "open": float(item[1]),
                    "high": float(item[2]),
                    "low": float(item[3]),
                    "close": float(item[4]),
                    "volume": float(item[5]),
                    "base_volume": float(item[6]),
                    "quote_volume": float(item[7]),
                    "confirm": str(item[8]) if len(item) > 8 else None,
                }
            )
        return sorted(normalized, key=lambda row: row["open_time"])

    @staticmethod
    def _normalize_bybit_open_interest_rows(rows: list[Any]) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for item in rows:
            if not isinstance(item, dict):
                continue
            value = GatewayService._safe_float(item.get("openInterest"))
            if value is None:
                continue
            normalized.append(
                {
                    "time": item.get("timestamp"),
                    "value": value,
                    "close": value,
                    "openInterest": value,
                }
            )
        return sorted(normalized, key=lambda row: str(row.get("time") or ""))

    @staticmethod
    def _normalize_okx_funding_rows(rows: list[Any]) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for item in rows:
            if not isinstance(item, dict):
                continue
            value = GatewayService._safe_float(item.get("fundingRate"))
            if value is None:
                continue
            normalized.append(
                {
                    "time": item.get("fundingTime"),
                    "value": value,
                    "close": value,
                    "fundingRate": value,
                }
            )
        return sorted(normalized, key=lambda row: str(row.get("time") or ""))

    @staticmethod
    def _normalize_okx_open_interest_rows(rows: list[Any]) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for item in rows:
            if not isinstance(item, list) or len(item) < 2:
                continue
            value = GatewayService._safe_float(item[1])
            if value is None:
                continue
            normalized.append(
                {
                    "time": item[0],
                    "sumOpenInterestValue": value,
                    "value": value,
                    "close": value,
                }
            )
        return sorted(normalized, key=lambda row: str(row.get("time") or ""))

    @staticmethod
    def _normalize_okx_long_short_ratio_rows(rows: list[Any]) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for item in rows:
            if not isinstance(item, list) or len(item) < 2:
                continue
            value = GatewayService._safe_float(item[1])
            if value is None:
                continue
            normalized.append(
                {
                    "time": item[0],
                    "longShortRatio": value,
                    "value": value,
                    "close": value,
                }
            )
        return sorted(normalized, key=lambda row: str(row.get("time") or ""))

    @staticmethod
    def _normalize_bybit_funding_rows(rows: list[Any]) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for item in rows:
            if not isinstance(item, dict):
                continue
            value = GatewayService._safe_float(item.get("fundingRate"))
            if value is None:
                continue
            normalized.append(
                {
                    "time": item.get("fundingRateTimestamp"),
                    "value": value,
                    "close": value,
                    "fundingRate": value,
                }
            )
        return sorted(normalized, key=lambda row: str(row.get("time") or ""))

    @staticmethod
    def _normalize_bybit_account_ratio_rows(rows: list[Any]) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for item in rows:
            if not isinstance(item, dict):
                continue
            buy_ratio = GatewayService._safe_float(item.get("buyRatio"))
            sell_ratio = GatewayService._safe_float(item.get("sellRatio"))
            if buy_ratio is None or sell_ratio in (None, 0):
                continue
            long_short_ratio = buy_ratio / sell_ratio
            normalized.append(
                {
                    "time": item.get("timestamp"),
                    "buyRatio": buy_ratio,
                    "sellRatio": sell_ratio,
                    "longShortRatio": round(long_short_ratio, 6),
                    "value": round(long_short_ratio, 6),
                    "close": round(long_short_ratio, 6),
                }
            )
        return sorted(normalized, key=lambda row: str(row.get("time") or ""))

    def _build_ohlc_summary(self, rows: Any, value_key_candidates: tuple[str, ...]) -> dict[str, Any] | None:
        normalized_rows = self._normalize_rows(rows)
        if len(normalized_rows) < 1:
            return None

        latest = normalized_rows[-1]
        previous = normalized_rows[-2] if len(normalized_rows) > 1 else None
        latest_value = self._extract_first_numeric(latest, value_key_candidates)
        previous_value = self._extract_first_numeric(previous, value_key_candidates) if previous else None

        return {
            "latest_time": self._extract_time(latest),
            "latest_value": latest_value,
            "previous_value": previous_value,
            "change_pct": self._compute_change_pct(previous_value, latest_value),
            "trend": self._classify_trend(previous_value, latest_value),
        }

    @staticmethod
    def _compute_average_true_range(candles: list[dict[str, Any]]) -> float | None:
        if len(candles) < 2:
            return None
        true_ranges: list[float] = []
        previous_close = candles[0]["close"]
        for candle in candles[1:]:
            high = candle["high"]
            low = candle["low"]
            tr = max(high - low, abs(high - previous_close), abs(low - previous_close))
            true_ranges.append(tr)
            previous_close = candle["close"]
        if not true_ranges:
            return None
        return round(sum(true_ranges) / len(true_ranges), 4)

    @staticmethod
    def _pct_change(start_value: float | None, end_value: float | None) -> float | None:
        if start_value in (None, 0) or end_value is None:
            return None
        return round(((end_value - start_value) / abs(start_value)) * 100, 4)

    @staticmethod
    def _range_pct(low_value: float | None, high_value: float | None, reference_value: float | None) -> float | None:
        if low_value is None or high_value is None or reference_value in (None, 0):
            return None
        return round(((high_value - low_value) / abs(reference_value)) * 100, 4)

    @staticmethod
    def _distance_pct(start_value: float | None, end_value: float | None) -> float | None:
        if start_value in (None, 0) or end_value is None:
            return None
        return round(((end_value - start_value) / abs(start_value)) * 100, 4)

    @staticmethod
    def _pct_of_value(value: float | None, reference_value: float | None) -> float | None:
        if value is None or reference_value in (None, 0):
            return None
        return round((value / abs(reference_value)) * 100, 4)

    @staticmethod
    def _close_position_in_range(low_value: float | None, high_value: float | None, close_value: float | None) -> float | None:
        if low_value is None or high_value is None or close_value is None or high_value == low_value:
            return None
        return round((close_value - low_value) / (high_value - low_value), 4)

    @staticmethod
    def _max_high(candles: list[dict[str, Any]]) -> float | None:
        if not candles:
            return None
        return max(candle["high"] for candle in candles)

    @staticmethod
    def _min_low(candles: list[dict[str, Any]]) -> float | None:
        if not candles:
            return None
        return min(candle["low"] for candle in candles)

    def _build_liquidation_summary(self, rows: Any) -> dict[str, Any] | None:
        normalized_rows = self._normalize_rows(rows)
        if not normalized_rows:
            return None

        recent_rows = normalized_rows[-3:]
        long_candidates = ("longLiquidationUsd", "longUsd", "longsUsd", "buy", "long")
        short_candidates = ("shortLiquidationUsd", "shortUsd", "shortsUsd", "sell", "short")

        long_total = 0.0
        short_total = 0.0
        for row in recent_rows:
            long_total += self._extract_first_numeric(row, long_candidates) or 0.0
            short_total += self._extract_first_numeric(row, short_candidates) or 0.0

        imbalance = short_total - long_total
        if abs(imbalance) < max(long_total, short_total, 1.0) * 0.05:
            bias = "balanced"
        elif imbalance > 0:
            bias = "shorts_dominant_liquidation"
        else:
            bias = "longs_dominant_liquidation"

        return {
            "recent_window_points": len(recent_rows),
            "long_liquidation_usd": round(long_total, 2),
            "short_liquidation_usd": round(short_total, 2),
            "imbalance_usd": round(imbalance, 2),
            "bias": bias,
        }

    def _build_exchange_rank_summary(self, rows: Any, exchange: str) -> dict[str, Any] | None:
        normalized_rows = self._normalize_rows(rows)
        if not normalized_rows:
            return None

        target = None
        for row in normalized_rows:
            row_exchange = str(row.get("exchangeName") or row.get("exchange") or row.get("name") or "").upper()
            if row_exchange == exchange.upper():
                target = row
                break

        top_rows = sorted(
            normalized_rows,
            key=lambda row: self._extract_first_numeric(row, ("openInterestUsd", "open_interest_usd", "openInterest")) or 0.0,
            reverse=True,
        )[:5]

        return {
            "focus_exchange": exchange,
            "focus_exchange_metrics": target,
            "top_open_interest_exchanges": top_rows,
        }

    def _deribit_public_get(
        self,
        method: str,
        *,
        params: dict[str, Any],
        ttl_seconds: int,
    ) -> Any:
        payload = self._http.get_json(
            "deribit",
            f"{DERIBIT_BASE}/public/{method}",
            params=params,
            ttl_seconds=ttl_seconds,
        )
        if not isinstance(payload, dict):
            raise UpstreamServiceError("deribit", f"Deribit returned an unexpected payload for {method}.")
        if payload.get("error"):
            raise UpstreamServiceError("deribit", f"Deribit returned an error for {method}: {payload.get('error')}")
        return payload.get("result")

    def _build_deribit_option_summary(self, rows: list[Any]) -> dict[str, Any]:
        parsed_rows = [self._normalize_deribit_option_row(row) for row in rows if isinstance(row, dict)]
        parsed_rows = [row for row in parsed_rows if row is not None]
        calls = [row for row in parsed_rows if row["option_type"] == "call"]
        puts = [row for row in parsed_rows if row["option_type"] == "put"]

        call_oi = sum(row.get("open_interest", 0.0) for row in calls)
        put_oi = sum(row.get("open_interest", 0.0) for row in puts)
        call_volume_usd = sum(row.get("volume_usd", 0.0) for row in calls)
        put_volume_usd = sum(row.get("volume_usd", 0.0) for row in puts)

        return {
            "contracts": len(parsed_rows),
            "call_open_interest": round(call_oi, 4),
            "put_open_interest": round(put_oi, 4),
            "put_call_open_interest_ratio": self._safe_ratio(put_oi, call_oi),
            "call_volume_usd": round(call_volume_usd, 2),
            "put_volume_usd": round(put_volume_usd, 2),
            "put_call_volume_ratio": self._safe_ratio(put_volume_usd, call_volume_usd),
            "oi_weighted_call_iv": self._weighted_average(calls, "mark_iv", "open_interest"),
            "oi_weighted_put_iv": self._weighted_average(puts, "mark_iv", "open_interest"),
            "volume_weighted_call_iv": self._weighted_average(calls, "mark_iv", "volume_usd"),
            "volume_weighted_put_iv": self._weighted_average(puts, "mark_iv", "volume_usd"),
            "atm_iv_skew": self._build_deribit_atm_iv_skew(parsed_rows),
        }

    def _build_deribit_nearest_expiry_summaries(self, rows: list[Any]) -> list[dict[str, Any]]:
        parsed_rows = [self._normalize_deribit_option_row(row) for row in rows if isinstance(row, dict)]
        parsed_rows = [row for row in parsed_rows if row is not None and row.get("expiry_date") is not None]
        grouped: dict[date, list[dict[str, Any]]] = {}
        for row in parsed_rows:
            grouped.setdefault(row["expiry_date"], []).append(row)

        today = datetime.now(timezone.utc).date()
        summaries: list[dict[str, Any]] = []
        for expiry_date in sorted(grouped):
            expiry_rows = grouped[expiry_date]
            days_to_expiry = (expiry_date - today).days
            if days_to_expiry < 0:
                continue
            calls = [row for row in expiry_rows if row["option_type"] == "call"]
            puts = [row for row in expiry_rows if row["option_type"] == "put"]
            call_oi = sum(row.get("open_interest", 0.0) for row in calls)
            put_oi = sum(row.get("open_interest", 0.0) for row in puts)
            underlying_price = self._first_not_none(*(row.get("underlying_price") for row in expiry_rows))
            atm_call = self._select_deribit_atm_option(calls, underlying_price)
            atm_put = self._select_deribit_atm_option(puts, underlying_price)
            summaries.append(
                {
                    "expiry_date": expiry_date.isoformat(),
                    "days_to_expiry": days_to_expiry,
                    "contracts": len(expiry_rows),
                    "call_open_interest": round(call_oi, 4),
                    "put_open_interest": round(put_oi, 4),
                    "put_call_open_interest_ratio": self._safe_ratio(put_oi, call_oi),
                    "total_volume_usd": round(sum(row.get("volume_usd", 0.0) for row in expiry_rows), 2),
                    "atm_call_iv": atm_call.get("mark_iv") if atm_call else None,
                    "atm_put_iv": atm_put.get("mark_iv") if atm_put else None,
                    "atm_put_call_skew": None
                    if not atm_call or not atm_put or atm_call.get("mark_iv") is None or atm_put.get("mark_iv") is None
                    else round((atm_put["mark_iv"] - atm_call["mark_iv"]), 4),
                    "atm_call_strike": atm_call.get("strike") if atm_call else None,
                    "atm_put_strike": atm_put.get("strike") if atm_put else None,
                    "underlying_price": underlying_price,
                }
            )
        return summaries

    def _build_deribit_futures_summary(self, rows: list[Any]) -> dict[str, Any]:
        normalized_rows = [row for row in rows if isinstance(row, dict)]
        perpetual = next((row for row in normalized_rows if str(row.get("instrument_name")) == "BTC-PERPETUAL"), None)
        dated_futures = [row for row in normalized_rows if str(row.get("instrument_name")) != "BTC-PERPETUAL"]
        nearest_future = None
        if dated_futures:
            nearest_future = min(
                dated_futures,
                key=lambda row: self._parse_deribit_future_expiry(row.get("instrument_name")) or datetime.max.date(),
            )

        perp_mark_price = self._safe_float(perpetual.get("mark_price")) if perpetual else None
        perp_index_price = self._safe_float(perpetual.get("estimated_delivery_price")) if perpetual else None
        nearest_future_mark = self._safe_float(nearest_future.get("mark_price")) if nearest_future else None
        nearest_future_index = self._safe_float(nearest_future.get("estimated_delivery_price")) if nearest_future else None

        return {
            "perpetual": {
                "instrument_name": perpetual.get("instrument_name") if perpetual else None,
                "mark_price": perp_mark_price,
                "index_price": perp_index_price,
                "open_interest": self._safe_float(perpetual.get("open_interest")) if perpetual else None,
                "volume_usd": self._safe_float(perpetual.get("volume_usd")) if perpetual else None,
                "current_funding": self._safe_float(perpetual.get("current_funding")) if perpetual else None,
                "funding_8h": self._safe_float(perpetual.get("funding_8h")) if perpetual else None,
                "basis_pct": self._compute_change_pct(perp_index_price, perp_mark_price),
            },
            "nearest_future": {
                "instrument_name": nearest_future.get("instrument_name") if nearest_future else None,
                "expiry_date": (
                    self._parse_deribit_future_expiry(nearest_future.get("instrument_name")).isoformat()
                    if nearest_future and self._parse_deribit_future_expiry(nearest_future.get("instrument_name"))
                    else None
                ),
                "mark_price": nearest_future_mark,
                "index_price": nearest_future_index,
                "open_interest": self._safe_float(nearest_future.get("open_interest")) if nearest_future else None,
                "volume_usd": self._safe_float(nearest_future.get("volume_usd")) if nearest_future else None,
                "basis_pct": self._compute_change_pct(nearest_future_index, nearest_future_mark),
            },
        }

    def _build_deribit_historical_vol_summary(self, rows: list[Any]) -> dict[str, Any]:
        normalized = []
        for item in rows:
            if not isinstance(item, (list, tuple)) or len(item) < 2:
                continue
            timestamp = self._safe_int(item[0])
            value = self._safe_float(item[1])
            if timestamp is None or value is None:
                continue
            normalized.append({"timestamp": timestamp, "value": value})
        if not normalized:
            return {}
        latest = normalized[-1]
        previous = normalized[-2] if len(normalized) > 1 else None
        return {
            "latest_timestamp": latest["timestamp"],
            "latest_value": latest["value"],
            "previous_value": previous["value"] if previous else None,
            "change_pct": self._compute_change_pct(previous["value"], latest["value"]) if previous else None,
        }

    def _build_deribit_vol_index_summary(self, rows: list[Any]) -> dict[str, Any]:
        normalized = []
        for item in rows:
            if not isinstance(item, (list, tuple)) or len(item) < 5:
                continue
            timestamp = self._safe_int(item[0])
            open_value = self._safe_float(item[1])
            high_value = self._safe_float(item[2])
            low_value = self._safe_float(item[3])
            close_value = self._safe_float(item[4])
            if timestamp is None or close_value is None:
                continue
            normalized.append(
                {
                    "timestamp": timestamp,
                    "open": open_value,
                    "high": high_value,
                    "low": low_value,
                    "close": close_value,
                }
            )
        if not normalized:
            return {}
        latest = normalized[-1]
        first = normalized[0]
        high_24h = max(item["high"] for item in normalized if item["high"] is not None)
        low_24h = min(item["low"] for item in normalized if item["low"] is not None)
        return {
            "latest_timestamp": latest["timestamp"],
            "latest_value": latest["close"],
            "change_24h_pct": self._compute_change_pct(first["open"], latest["close"]),
            "high_24h": high_24h,
            "low_24h": low_24h,
        }

    def _normalize_deribit_option_row(self, row: dict[str, Any]) -> dict[str, Any] | None:
        instrument_name = str(row.get("instrument_name") or "")
        parsed = self._parse_deribit_option_instrument(instrument_name)
        if parsed is None:
            return None
        return {
            "instrument_name": instrument_name,
            "expiry_date": parsed["expiry_date"],
            "strike": parsed["strike"],
            "option_type": parsed["option_type"],
            "open_interest": self._safe_float(row.get("open_interest")) or 0.0,
            "volume_usd": self._safe_float(row.get("volume_usd")) or 0.0,
            "mark_iv": self._safe_float(row.get("mark_iv")),
            "underlying_price": self._safe_float(row.get("underlying_price")),
        }

    def _build_deribit_atm_iv_skew(self, rows: list[dict[str, Any]]) -> dict[str, Any] | None:
        valid_rows = [row for row in rows if row.get("expiry_date") is not None]
        if not valid_rows:
            return None
        today = datetime.now(timezone.utc).date()
        grouped: dict[date, list[dict[str, Any]]] = {}
        for row in valid_rows:
            expiry_date = row.get("expiry_date")
            if not isinstance(expiry_date, date) or expiry_date < today:
                continue
            grouped.setdefault(expiry_date, []).append(row)
        if not grouped:
            return None
        nearest_expiry = min(grouped)
        expiry_rows = grouped[nearest_expiry]
        calls = [row for row in expiry_rows if row["option_type"] == "call"]
        puts = [row for row in expiry_rows if row["option_type"] == "put"]
        underlying_price = self._first_not_none(*(row.get("underlying_price") for row in expiry_rows))
        atm_call = self._select_deribit_atm_option(calls, underlying_price)
        atm_put = self._select_deribit_atm_option(puts, underlying_price)
        if not atm_call or not atm_put:
            return None
        call_iv = atm_call.get("mark_iv")
        put_iv = atm_put.get("mark_iv")
        return {
            "expiry_date": nearest_expiry.isoformat(),
            "underlying_price": underlying_price,
            "atm_call_instrument": atm_call.get("instrument_name"),
            "atm_put_instrument": atm_put.get("instrument_name"),
            "atm_call_iv": call_iv,
            "atm_put_iv": put_iv,
            "atm_put_call_skew": None if call_iv is None or put_iv is None else round(put_iv - call_iv, 4),
        }

    @staticmethod
    def _select_deribit_atm_option(rows: list[dict[str, Any]], underlying_price: float | None) -> dict[str, Any] | None:
        if not rows or underlying_price in (None, 0):
            return None
        eligible = [row for row in rows if row.get("strike") is not None]
        if not eligible:
            return None
        return min(
            eligible,
            key=lambda row: (
                abs((row["strike"] - underlying_price) / underlying_price),
                -(row.get("open_interest", 0.0) or 0.0),
            ),
        )

    @staticmethod
    def _parse_deribit_option_instrument(instrument_name: str) -> dict[str, Any] | None:
        match = re.match(r"^(?P<currency>[A-Z]+)-(?P<expiry>\d{1,2}[A-Z]{3}\d{2})-(?P<strike>\d+)-(?P<option_type>[CP])$", instrument_name)
        if not match:
            return None
        expiry_date = GatewayService._parse_deribit_expiry_code(match.group("expiry"))
        if expiry_date is None:
            return None
        option_type = "call" if match.group("option_type") == "C" else "put"
        return {
            "currency": match.group("currency"),
            "expiry_date": expiry_date,
            "strike": float(match.group("strike")),
            "option_type": option_type,
        }

    @staticmethod
    def _parse_deribit_future_expiry(instrument_name: Any) -> date | None:
        value = str(instrument_name or "")
        match = re.match(r"^[A-Z]+-(?P<expiry>\d{1,2}[A-Z]{3}\d{2})$", value)
        if not match:
            return None
        return GatewayService._parse_deribit_expiry_code(match.group("expiry"))

    @staticmethod
    def _parse_deribit_expiry_code(code: str) -> date | None:
        try:
            return datetime.strptime(code, "%d%b%y").date()
        except ValueError:
            return None

    @staticmethod
    def _weighted_average(rows: list[dict[str, Any]], value_key: str, weight_key: str) -> float | None:
        weighted_sum = 0.0
        total_weight = 0.0
        for row in rows:
            value = GatewayService._safe_float(row.get(value_key))
            weight = GatewayService._safe_float(row.get(weight_key))
            if value is None or weight in (None, 0):
                continue
            weighted_sum += value * weight
            total_weight += weight
        if total_weight == 0:
            return None
        return round(weighted_sum / total_weight, 4)

    @staticmethod
    def _safe_ratio(numerator: float | None, denominator: float | None) -> float | None:
        if numerator is None or denominator in (None, 0):
            return None
        return round(numerator / denominator, 4)

    @staticmethod
    def _first_not_none(*values: Any) -> Any:
        for value in values:
            if value is not None:
                return value
        return None

    @staticmethod
    def _normalize_rows(rows: Any) -> list[dict[str, Any]]:
        if isinstance(rows, list):
            return [item for item in rows if isinstance(item, dict)]
        if isinstance(rows, dict):
            for key in ("list", "data", "items", "history"):
                value = rows.get(key)
                if isinstance(value, list):
                    return [item for item in value if isinstance(item, dict)]
        return []

    @staticmethod
    def _truncate_rows(rows: Any, limit: int = 6) -> list[dict[str, Any]]:
        normalized_rows = GatewayService._normalize_rows(rows)
        return normalized_rows[-limit:]

    def _extract_supported_pairs_for_exchange(self, data: Any, exchange: str) -> list[str]:
        target_exchange = exchange.upper()

        def normalize_exchange_name(value: Any) -> str:
            return str(value or "").upper()

        def extract_pair_strings(value: Any) -> list[str]:
            results: list[str] = []
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, str):
                        results.append(item)
                    elif isinstance(item, dict):
                        for key in ("symbol", "pair", "instrument", "name"):
                            if key in item and isinstance(item[key], str):
                                results.append(item[key])
                                break
            return results

        if isinstance(data, dict):
            direct = data.get(exchange) or data.get(target_exchange) or data.get(exchange.lower())
            direct_pairs = extract_pair_strings(direct)
            if direct_pairs:
                return direct_pairs

            for key, value in data.items():
                if normalize_exchange_name(key) == target_exchange:
                    key_pairs = extract_pair_strings(value)
                    if key_pairs:
                        return key_pairs

            for value in data.values():
                if isinstance(value, dict):
                    exchange_name = normalize_exchange_name(
                        value.get("exchange") or value.get("exchangeName") or value.get("name")
                    )
                    if exchange_name == target_exchange:
                        for key in ("pairs", "symbols", "instruments", "markets", "list", "data"):
                            if key in value:
                                nested_pairs = extract_pair_strings(value[key])
                                if nested_pairs:
                                    return nested_pairs

        if isinstance(data, list):
            for item in data:
                if not isinstance(item, dict):
                    continue
                exchange_name = normalize_exchange_name(
                    item.get("exchange") or item.get("exchangeName") or item.get("name")
                )
                if exchange_name != target_exchange:
                    continue
                for key in ("pairs", "symbols", "instruments", "markets", "list", "data"):
                    if key in item:
                        pairs = extract_pair_strings(item[key])
                        if pairs:
                            return pairs

        return []

    @staticmethod
    def _extract_exchange_history_rows(data: Any, exchange: str) -> list[dict[str, Any]]:
        if not isinstance(data, dict):
            return []

        time_list = data.get("timeList")
        data_map = data.get("dataMap")
        if not isinstance(time_list, list) or not isinstance(data_map, dict):
            return []

        series = data_map.get(exchange) or data_map.get(exchange.upper()) or data_map.get(exchange.title())
        if not isinstance(series, list):
            return []

        rows: list[dict[str, Any]] = []
        for timestamp, value in zip(time_list, series):
            try:
                numeric_value = float(value)
            except (TypeError, ValueError):
                continue
            rows.append(
                {
                    "time": timestamp,
                    "value": numeric_value,
                    "close": numeric_value,
                    "openInterest": numeric_value,
                }
            )
        return rows

    @staticmethod
    def _unique_strings(values: list[str]) -> list[str]:
        seen: set[str] = set()
        items: list[str] = []
        for value in values:
            normalized = str(value).strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            items.append(normalized)
        return items

    @staticmethod
    def _extract_first_numeric(row: dict[str, Any] | None, keys: tuple[str, ...]) -> float | None:
        if row is None:
            return None
        lowered = {str(key).lower(): value for key, value in row.items()}
        for key in keys:
            if key.lower() in lowered:
                value = lowered[key.lower()]
                try:
                    return float(value)
                except (TypeError, ValueError):
                    continue
        return None

    @staticmethod
    def _extract_time(row: dict[str, Any] | None) -> Any:
        if row is None:
            return None
        for key in ("time", "t", "timestamp", "date"):
            if key in row:
                return row[key]
        return None

    @staticmethod
    def _compute_change_pct(previous_value: float | None, latest_value: float | None) -> float | None:
        if previous_value in (None, 0) or latest_value is None:
            return None
        return round(((latest_value - previous_value) / abs(previous_value)) * 100, 4)

    @staticmethod
    def _classify_trend(previous_value: float | None, latest_value: float | None) -> str | None:
        if previous_value is None or latest_value is None:
            return None
        if latest_value > previous_value:
            return "up"
        if latest_value < previous_value:
            return "down"
        return "flat"

    @staticmethod
    def _zip_fields(fields: list[str], row: list[Any]) -> dict[str, Any]:
        return {field: row[index] for index, field in enumerate(fields)}

    @staticmethod
    def _xml_text(parent: ElementTree.Element, name: str) -> str | None:
        node = parent.find(name)
        if node is None or node.text is None:
            return None
        return node.text.strip()

    def _okx_get_rows(
        self,
        path: str,
        *,
        params: dict[str, Any],
        ttl_seconds: int,
    ) -> list[Any]:
        payload = self._http.get_json(
            "okx",
            f"{OKX_BASE}{path}",
            params=params,
            ttl_seconds=ttl_seconds,
        )
        code = str(payload.get("code") or "")
        if code not in {"", "0"}:
            raise UpstreamServiceError("okx", f"OKX returned error code {code}: {payload.get('msg') or 'unknown error'}")
        rows = payload.get("data")
        if not isinstance(rows, list):
            raise UpstreamServiceError("okx", f"OKX returned unexpected payload for {path}.")
        return rows

    def _okx_get_first_row(
        self,
        path: str,
        *,
        params: dict[str, Any],
        ttl_seconds: int,
    ) -> dict[str, Any]:
        rows = self._okx_get_rows(path, params=params, ttl_seconds=ttl_seconds)
        if not rows or not isinstance(rows[0], dict):
            raise UpstreamServiceError("okx", f"OKX returned no data for {path}.")
        return rows[0]

    @staticmethod
    def _to_okx_swap_inst_id(symbol: str) -> str:
        root = GatewayService._extract_root_asset(symbol)
        return f"{root}-USDT-SWAP"

    @staticmethod
    def _to_okx_index_inst_id(symbol: str) -> str:
        root = GatewayService._extract_root_asset(symbol)
        return f"{root}-USDT"

    @staticmethod
    def _to_okx_bar(interval: str) -> str:
        mapping = {
            "15m": "15m",
            "1h": "1H",
            "4h": "4H",
            "8h": "8H",
            "1d": "1D",
            "1w": "1W",
            "1m": "1M",
            "1M": "1M",
        }
        return mapping.get(interval, interval)

    @staticmethod
    def _to_okx_period(period: str) -> str:
        mapping = {
            "5m": "5m",
            "15m": "15m",
            "30m": "30m",
            "1h": "1H",
            "2h": "2H",
            "4h": "4H",
            "6h": "6H",
            "12h": "12H",
            "1d": "1D",
        }
        return mapping.get(period.lower(), "1H")

    @staticmethod
    def _okx_kline_ttl(interval: str) -> int:
        return {
            "15m": 60,
            "1h": 120,
            "4h": 300,
            "8h": 600,
            "1d": 1800,
            "1w": 3600,
            "1M": 7200,
        }.get(interval, 120)
