from __future__ import annotations

from datetime import datetime, timezone
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
        root_symbol = self._extract_root_asset(symbol)
        coingecko_id = COINGECKO_IDS.get(root_symbol)

        payload = {
            "generated_at": utc_now_iso(),
            "symbol": symbol,
            "sources": {
                "binance": self._capture(lambda: self.get_binance_market(symbol)),
                "coinglass": self._capture(lambda: self.get_coinglass_market_structure(symbol=symbol, exchange="OKX", interval="1h")),
                "fear_greed": self._capture(self.get_fear_greed_latest),
                "mempool": self._capture(self.get_mempool_recommended_fees) if root_symbol == "BTC" else self._skipped("Mempool data is only mapped for BTC."),
            },
        }

        if coingecko_id:
            payload["sources"]["coingecko"] = self._capture(lambda: self.get_coingecko_simple_price(coingecko_id))
        else:
            payload["sources"]["coingecko"] = self._skipped(f"No CoinGecko asset mapping configured for {root_symbol}.")

        return payload

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
        price = self._http.get_json(
            "binance",
            f"{BINANCE_SPOT_BASE}/api/v3/ticker/price",
            params={"symbol": symbol},
            ttl_seconds=10,
        )
        book = self._http.get_json(
            "binance",
            f"{BINANCE_SPOT_BASE}/api/v3/ticker/bookTicker",
            params={"symbol": symbol},
            ttl_seconds=10,
        )
        open_interest = self._http.get_json(
            "binance",
            f"{BINANCE_FUTURES_BASE}/fapi/v1/openInterest",
            params={"symbol": symbol},
            ttl_seconds=15,
        )
        funding = self._http.get_json(
            "binance",
            f"{BINANCE_FUTURES_BASE}/fapi/v1/fundingRate",
            params={"symbol": symbol, "limit": 1},
            ttl_seconds=15,
        )
        taker_ratio = self._http.get_json(
            "binance",
            f"{BINANCE_FUTURES_BASE}/futures/data/takerlongshortRatio",
            params={"symbol": symbol, "period": "5m", "limit": 1},
            ttl_seconds=15,
        )
        long_short_ratio = self._http.get_json(
            "binance",
            f"{BINANCE_FUTURES_BASE}/futures/data/globalLongShortAccountRatio",
            params={"symbol": symbol, "period": "5m", "limit": 1},
            ttl_seconds=15,
        )
        candles = self._http.get_json(
            "binance",
            f"{BINANCE_SPOT_BASE}/api/v3/klines",
            params={"symbol": symbol, "interval": "5m", "limit": 3},
            ttl_seconds=10,
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
        pair_params = {
            "symbol": normalized_pair_symbol,
            "exchange": normalized_exchange,
            "interval": interval,
        }

        open_interest = self._coinglass_get_data(
            "/api/futures/open-interest/history",
            params=pair_params,
        )
        funding = self._coinglass_get_data(
            "/api/futures/funding-rate/history",
            params=pair_params,
        )
        oi_weighted_funding = self._coinglass_get_data(
            "/api/futures/oi-weight-funding-rate/history",
            params={
                "symbol": normalized_pair_symbol,
                "exchange": normalized_exchange,
                "interval": interval,
            },
        )
        long_short_ratio = self._coinglass_get_data(
            "/api/futures/global-long-short-account-ratio/history",
            params=pair_params,
        )
        liquidation = self._coinglass_get_data(
            "/api/futures/liquidation/history",
            params=pair_params,
        )
        exchange_rank = self._coinglass_get_data(
            "/api/exchange/rank",
            params=None,
        )

        return {
            "symbol": normalized_pair_symbol,
            "asset_symbol": normalized_asset_symbol,
            "exchange": normalized_exchange,
            "interval": interval,
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
            "open": float(item[1]),
            "high": float(item[2]),
            "low": float(item[3]),
            "close": float(item[4]),
            "volume": float(item[5]),
        }

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
