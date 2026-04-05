from __future__ import annotations

from pathlib import Path
import json
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.gateway.config import load_gateway_config


def response_schema() -> dict:
    return {
        "type": "object",
        "properties": {},
        "additionalProperties": True,
    }


def qparam(name: str, description: str, default=None, schema_type: str = "string", required: bool = False, minimum=None, maximum=None) -> dict:
    schema: dict = {"type": schema_type}
    if default is not None:
        schema["default"] = default
    if minimum is not None:
        schema["minimum"] = minimum
    if maximum is not None:
        schema["maximum"] = maximum
    return {
        "name": name,
        "in": "query",
        "required": required,
        "schema": schema,
        "description": description,
    }


def pparam(name: str, description: str, schema_type: str = "string") -> dict:
    return {
        "name": name,
        "in": "path",
        "required": True,
        "schema": {"type": schema_type},
        "description": description,
    }


def op(summary: str, description: str, operation_id: str, parameters: list[dict] | None = None) -> dict:
    return {
        "operationId": operation_id,
        "summary": summary,
        "description": description,
        "parameters": parameters or [],
        "responses": {
            "200": {
                "description": "Successful response",
                "content": {
                    "application/json": {
                        "schema": response_schema()
                    }
                },
            }
        },
        "security": [{"bearerAuth": []}],
    }


def build_schema(base_url: str) -> dict:
    base_url = base_url.rstrip("/")
    return {
        "openapi": "3.1.0",
        "info": {
            "title": "Crypto GPT Aggregator API",
            "version": "1.2.0",
            "description": (
                "Expanded Custom GPT action schema for real-time crypto, macro, regulatory, and market structure data. "
                "Use overview endpoints for synthesis and source endpoints for targeted checks. "
                "Authenticate with Bearer token in GPT action settings."
            ),
        },
        "servers": [
            {
                "url": base_url,
                "description": "Public HTTPS base URL for the deployed aggregator API.",
            }
        ],
        "paths": {
            "/v1/crypto/overview": {
                "get": op(
                    "Get a real-time crypto market overview",
                    "Best default endpoint for crypto direction judgment. Returns a compact snapshot including Binance market data, Binance multi-timeframe structure, Binance derivatives structure, Bybit validation structure, Fear and Greed index, mempool fees for BTC, and CoinGecko price context.",
                    "getCryptoOverview",
                    [
                        qparam("symbol", "Trading symbol, usually BTCUSDT.", default="BTCUSDT"),
                    ],
                )
            },
            "/v1/macro/overview": {
                "get": op(
                    "Get a macro overview",
                    "Best default endpoint for macro context. Returns Federal Reserve monetary updates, Treasury rates, CPI from BLS, selected FRED series when configured, and BEA GDP when configured.",
                    "getMacroOverview",
                    [
                        qparam("fred_series", "Comma-separated FRED series IDs.", default="FEDFUNDS,DGS10,UNRATE"),
                    ],
                )
            },
            "/v1/regulatory/overview": {
                "get": op(
                    "Get a regulatory and ETF filing overview",
                    "Best default endpoint for regulatory context. Returns recent SEC filings for major bitcoin ETF entities and CFTC bitcoin positioning data.",
                    "getRegulatoryOverview",
                    [
                        qparam("entities", "Comma-separated SEC tickers or CIKs.", default="IBIT,FBTC,GBTC"),
                    ],
                )
            },
            "/v1/sources/binance/market": {
                "get": op(
                    "Get Binance spot and futures snapshot",
                    "Returns spot price, book ticker, open interest, funding rate, taker ratio, long-short ratio, and recent candles from Binance.",
                    "getBinanceMarket",
                    [
                        qparam("symbol", "Trading symbol, usually BTCUSDT.", default="BTCUSDT"),
                    ],
                )
            },
            "/v1/sources/binance/derivatives-structure": {
                "get": op(
                    "Get Binance derivatives structure",
                    "Returns Binance derivatives structure for 8h to 12h directional judgment, including mark-index spread, basis, open interest history, top trader long-short ratios, global long-short ratio, and taker buy-sell volume.",
                    "getBinanceDerivativesStructure",
                    [
                        qparam("symbol", "Trading symbol such as BTCUSDT.", default="BTCUSDT"),
                        qparam("period", "History period such as 15m, 30m, 1h, 2h, 4h, or 6h.", default="1h"),
                        qparam("limit", "Number of historical points to return.", default=12, schema_type="integer", minimum=3, maximum=30),
                    ],
                )
            },
            "/v1/sources/binance/multi-timeframe-structure": {
                "get": op(
                    "Get Binance multi-timeframe structure",
                    "Returns Binance multi-timeframe structure for 15m, 1h, 4h, and 8h, including derived range levels, ATR-like volatility context, VWAP approximation, and raw candles for GPT to estimate 4h or 8h high-low zones.",
                    "getBinanceMultiTimeframeStructure",
                    [
                        qparam("symbol", "Trading symbol such as BTCUSDT.", default="BTCUSDT"),
                    ],
                )
            },
            "/v1/sources/bybit/market-structure": {
                "get": op(
                    "Get Bybit derivatives validation snapshot",
                    "Returns Bybit public derivatives structure as a secondary validation source, including ticker context, 1h and 4h price structure, open interest history, funding history, and long-short ratio history.",
                    "getBybitMarketStructure",
                    [
                        qparam("symbol", "Linear perpetual symbol such as BTCUSDT.", default="BTCUSDT"),
                    ],
                )
            },
            "/v1/sources/coingecko/simple-price": {
                "get": op(
                    "Get CoinGecko simple price data",
                    "Returns spot price context, market cap, volume, and 24h change from CoinGecko for a chosen asset ID.",
                    "getCoinGeckoSimplePrice",
                    [
                        qparam("asset_id", "CoinGecko asset ID, such as bitcoin.", default="bitcoin"),
                        qparam("vs_currency", "Quote currency such as usd.", default="usd"),
                    ],
                )
            },
            "/v1/sources/fear-greed/latest": {
                "get": op(
                    "Get latest Fear and Greed index",
                    "Returns the latest crypto Fear and Greed sentiment value and classification.",
                    "getFearGreedLatest",
                )
            },
            "/v1/sources/mempool/fees": {
                "get": op(
                    "Get latest mempool fee recommendations",
                    "Returns recommended Bitcoin network fee rates from mempool.space.",
                    "getMempoolFees",
                )
            },
            "/v1/sources/treasury/latest-avg-rates": {
                "get": op(
                    "Get latest Treasury average rates snapshot",
                    "Returns latest average Treasury rates from Treasury Fiscal Data.",
                    "getTreasuryLatestAvgRates",
                )
            },
            "/v1/sources/bls/series/{series_id}": {
                "get": op(
                    "Get BLS time series observations",
                    "Returns recent observations for a BLS series such as CUUR0000SA0 for CPI.",
                    "getBlsSeries",
                    [
                        pparam("series_id", "BLS series ID, such as CUUR0000SA0."),
                        qparam("limit", "Maximum number of observations to return.", default=12, schema_type="integer", minimum=1, maximum=24),
                    ],
                )
            },
            "/v1/sources/fred/series/{series_id}": {
                "get": op(
                    "Get FRED time series observations",
                    "Returns recent observations for a FRED series such as FEDFUNDS, DGS10, or UNRATE. Requires FRED API key to be configured on the server.",
                    "getFredSeries",
                    [
                        pparam("series_id", "FRED series ID, such as FEDFUNDS."),
                        qparam("limit", "Maximum number of observations to return.", default=12, schema_type="integer", minimum=1, maximum=24),
                    ],
                )
            },
            "/v1/sources/bea/datasets": {
                "get": op(
                    "List BEA datasets",
                    "Returns available BEA datasets. Requires BEA API key to be configured on the server.",
                    "getBeaDatasets",
                )
            },
            "/v1/sources/bea/gdp": {
                "get": op(
                    "Get BEA GDP observations",
                    "Returns recent GDP observations from BEA. Requires BEA API key to be configured on the server.",
                    "getBeaGdp",
                    [
                        qparam("year", "BEA year selector such as LAST5, 2024, or ALL.", default="LAST5"),
                    ],
                )
            },
            "/v1/sources/fed/monetary-feed": {
                "get": op(
                    "Get Federal Reserve monetary updates",
                    "Returns recent Federal Reserve monetary-policy RSS items such as FOMC statements and minutes.",
                    "getFedMonetaryFeed",
                    [
                        qparam("limit", "Number of feed items to return.", default=5, schema_type="integer", minimum=1, maximum=10),
                    ],
                )
            },
            "/v1/sources/sec/company-tickers": {
                "get": op(
                    "Search SEC company tickers",
                    "Search SEC company and ETF entities by ticker or name fragment.",
                    "searchSecCompanyTickers",
                    [
                        qparam("query", "Ticker or name fragment such as IBIT or BlackRock."),
                        qparam("exchange", "Optional exchange filter such as Nasdaq or NYSE."),
                        qparam("limit", "Maximum number of rows to return.", default=25, schema_type="integer", minimum=1, maximum=100),
                    ],
                )
            },
            "/v1/sources/sec/submissions/{entity}": {
                "get": op(
                    "Get SEC submissions by ticker or CIK",
                    "Returns recent SEC filings for a ticker or CIK, useful for ETF issuers and related entities.",
                    "getSecSubmissions",
                    [
                        pparam("entity", "SEC ticker or CIK, such as IBIT."),
                        qparam("forms_limit", "Maximum number of recent filings to return.", default=10, schema_type="integer", minimum=1, maximum=20),
                    ],
                )
            },
            "/v1/sources/cftc/bitcoin-cot": {
                "get": op(
                    "Get CFTC bitcoin commitment of traders data",
                    "Returns recent CFTC bitcoin positioning data, helpful for futures market structure and sentiment analysis.",
                    "getCftcBitcoinCot",
                    [
                        qparam("exchange", "Exchange filter. Use cme or all.", default="cme"),
                        qparam("limit", "Maximum number of records to return.", default=4, schema_type="integer", minimum=1, maximum=10),
                    ],
                )
            },
        },
        "components": {
            "schemas": {},
            "securitySchemes": {
                "bearerAuth": {
                    "type": "http",
                    "scheme": "bearer",
                    "bearerFormat": "API token",
                }
            }
        },
    }


def main() -> int:
    config = load_gateway_config()
    schema = build_schema(config.public_base_url)
    output_path = ROOT / "openapi-gpt-actions-expanded.json"
    output_path.write_text(json.dumps(schema, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
