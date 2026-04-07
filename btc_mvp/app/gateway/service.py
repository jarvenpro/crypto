from __future__ import annotations

from datetime import datetime, timezone
from time import monotonic
from typing import Any
from xml.etree import ElementTree

from app.gateway.config import GatewayConfig
from app.gateway.http import GatewayHttpClient, UpstreamServiceError


BINANCE_SPOT_BASE = "https://api.binance.com"
BINANCE_FUTURES_BASE = "https://fapi.binance.com"
TREASURY_BASE = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od"
BLS_BASE = "https://api.bls.gov/publicAPI/v2/timeseries/data"
FRED_BASE = "https://api.stlouisfed.org/fred"
BEA_BASE = "https://apps.bea.gov/api/data"
SEC_FILES_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions"
CFTC_COT_URL = "https://publicreporting.cftc.gov/resource/6dca-aqww.json"
FED_MONETARY_FEED = "https://www.federalreserve.gov/feeds/press_monetary.xml"
MEMPOOL_FEES_URL = "https://mempool.space/api/v1/fees/recommended"
FEAR_GREED_URL = "https://api.alternative.me/fng/"
COINGECKO_SIMPLE_PRICE_URL = "https://api.coingecko.com/api/v3/simple/price"
COINGLASS_BASE = "https://open-api-v4.coinglass.com"
BYBIT_BASE = "https://api.bybit.com"

COINGECKO_IDS = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "SOL": "solana",
}

DEFAULT_FRED_SERIES = ("FEDFUNDS", "DGS10", "UNRATE")
DEFAULT_BITCOIN_ETF_ENTITIES = ("IBIT", "FBTC", "GBTC")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


class GatewayService:
    def __init__(self, config: GatewayConfig, http_client: GatewayHttpClient) -> None:
        self._config = config
        self._http = http_client
        self._memo_cache: dict[str, tuple[float, Any]] = {}

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
                "mempool": True,
                "fear_greed": True,
                "treasury": True,
            },
        }

    def crypto_overview(self, symbol: str) -> dict[str, Any]:
        symbol = symbol.upper()
        return self._memoize(f"crypto_overview:{symbol}", ttl_seconds=45, builder=lambda: self._build_crypto_overview(symbol))

    def _build_crypto_overview(self, symbol: str) -> dict[str, Any]:
        root_symbol = self._extract_root_asset(symbol)
        coingecko_id = COINGECKO_IDS.get(root_symbol)
        binance_market = self._capture(lambda: self.get_binance_market_overview(symbol))
        spot_price = None
        if binance_market.get("ok"):
            spot_price = binance_market.get("data", {}).get("spot_price")

        payload = {
            "generated_at": utc_now_iso(),
            "symbol": symbol,
            "sources": {
                "binance": binance_market,
                "binance_structure": self._capture(lambda: self.get_binance_multi_timeframe_overview(symbol, spot_price=spot_price)),
                "binance_derivatives": self._capture(lambda: self.get_binance_derivatives_overview(symbol)),
                "bybit": self._capture(lambda: self.get_bybit_market_structure(symbol)),
                "fear_greed": self._capture(self.get_fear_greed_latest),
                "mempool": self._capture(self.get_mempool_recommended_fees) if root_symbol == "BTC" else self._skipped("Mempool data is only mapped for BTC."),
            },
        }

        if coingecko_id:
            payload["sources"]["coingecko"] = self._capture(lambda: self.get_coingecko_simple_price(coingecko_id))
        else:
            payload["sources"]["coingecko"] = self._skipped(f"No CoinGecko asset mapping configured for {root_symbol}.")

        return payload

    def _memoize(self, key: str, ttl_seconds: int, builder) -> Any:
        now = monotonic()
        cached = self._memo_cache.get(key)
        if cached and cached[0] > now:
            return cached[1]
        value = builder()
        self._memo_cache[key] = (now + ttl_seconds, value)
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

    def get_binance_market(self, symbol: str) -> dict[str, Any]:
        symbol = symbol.upper()
        return self._memoize(f"binance_market:{symbol}", ttl_seconds=20, builder=lambda: self._build_binance_market(symbol))

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
        return self._memoize(f"binance_market_overview:{symbol}", ttl_seconds=20, builder=lambda: self._build_binance_market_overview(symbol))

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
            ttl_seconds=60,
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
            ttl_seconds=60,
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
            ttl_seconds=45,
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
            ttl_seconds=45,
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
            ttl_seconds=45,
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
            ttl_seconds=20,
        )

        if asset_id not in payload:
            raise UpstreamServiceError("coingecko", f"CoinGecko did not return data for asset '{asset_id}'.")

        return {
            "asset_id": asset_id,
            "vs_currency": vs_currency,
            "metrics": payload[asset_id],
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
        return self._http.get_json(
            "mempool",
            MEMPOOL_FEES_URL,
            ttl_seconds=30,
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
        return float(value)

    @staticmethod
    def _safe_int(value: Any) -> int | None:
        if value in (None, "", "-"):
            return None
        return int(float(value))

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
