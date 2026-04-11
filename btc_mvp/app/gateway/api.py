from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Request, Security
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader, HTTPAuthorizationCredentials, HTTPBearer

from app.gateway.config import GatewayConfig, load_gateway_config
from app.gateway.http import GatewayHttpClient, UpstreamServiceError
from app.gateway.service import GatewayService


app = FastAPI(
    title="Crypto GPT Aggregator API",
    version="0.1.0",
    description=(
        "A minimal aggregation layer for Custom GPT Actions. "
        "It wraps selected free or free-tier crypto, macro, regulatory, and on-chain data sources "
        "behind a smaller set of GPT-friendly endpoints."
    ),
)

api_key_scheme = APIKeyHeader(
    name="X-API-Key",
    auto_error=False,
    scheme_name="GatewayApiKey",
    description="Set this to your GATEWAY_API_TOKEN value.",
)
bearer_scheme = HTTPBearer(
    auto_error=False,
    scheme_name="GatewayBearer",
    description="Optional alternative to X-API-Key. Use Bearer <GATEWAY_API_TOKEN>.",
)


def get_config(request: Request) -> GatewayConfig:
    return request.app.state.gateway_config


def get_gateway_service(request: Request) -> GatewayService:
    return request.app.state.gateway_service


def require_api_token(
    api_key: Annotated[str | None, Security(api_key_scheme)] = None,
    bearer: Annotated[HTTPAuthorizationCredentials | None, Security(bearer_scheme)] = None,
    config: GatewayConfig = Depends(get_config),
) -> None:
    if not config.api_token:
        return

    bearer_token = bearer.credentials if bearer else None
    if api_key == config.api_token or bearer_token == config.api_token:
        return

    raise HTTPException(status_code=401, detail="Missing or invalid API token.")


def custom_openapi() -> dict:
    if app.openapi_schema:
        return app.openapi_schema

    config = load_gateway_config()
    schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )
    schema["servers"] = [
        {
            "url": config.public_base_url.rstrip("/"),
            "description": "Public HTTPS base URL used by Custom GPT Actions.",
        }
    ]
    if config.api_token:
        schema["info"]["description"] = (
            f"{app.description}\n\n"
            "Authentication: configure Custom GPT Actions to send `X-API-Key: <your token>`.\n"
            "A Bearer token in the `Authorization` header is also accepted for manual testing."
        )
    else:
        schema["info"]["description"] = (
            f"{app.description}\n\n"
            "Authentication: no API token is currently required."
        )
        for path_item in schema.get("paths", {}).values():
            if not isinstance(path_item, dict):
                continue
            for operation in path_item.values():
                if isinstance(operation, dict):
                    operation.pop("security", None)
        components = schema.get("components")
        if isinstance(components, dict):
            components.pop("securitySchemes", None)
    app.openapi_schema = schema
    return app.openapi_schema


app.openapi = custom_openapi


@app.on_event("startup")
def on_startup() -> None:
    config = load_gateway_config()
    http_client = GatewayHttpClient(config)
    app.state.gateway_config = config
    app.state.gateway_http_client = http_client
    app.state.gateway_service = GatewayService(config, http_client)


@app.on_event("shutdown")
def on_shutdown() -> None:
    http_client: GatewayHttpClient = app.state.gateway_http_client
    http_client.close()


@app.exception_handler(UpstreamServiceError)
def handle_upstream_error(_: Request, exc: UpstreamServiceError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "ok": False,
            "source": exc.source,
            "detail": exc.detail,
        },
    )


protected = APIRouter(dependencies=[Depends(require_api_token)])


@app.get("/health", tags=["System"], summary="Check service health and configured source availability")
def health(service: GatewayService = Depends(get_gateway_service)) -> dict:
    return service.health()


@protected.get(
    "/v1/crypto/overview",
    tags=["Overview"],
    summary="Get a compact crypto market overview for GPT",
    description="Quick triage endpoint for BTC direction. Uses OKX-first market and derivatives context with Bybit as secondary validation. Use it first for a compact market snapshot, but do not treat it as the final 8-12h judgment.",
)
def crypto_overview(
    symbol: str = Query("BTCUSDT", description="Binance symbol such as BTCUSDT."),
    service: GatewayService = Depends(get_gateway_service),
) -> dict:
    return service.crypto_overview(symbol.upper())


@protected.get(
    "/v1/debug/okx/market-overview",
    tags=["Debug"],
    summary="Debug OKX market overview",
    include_in_schema=False,
)
def debug_okx_market_overview(
    symbol: str = Query("BTCUSDT", description="Trading symbol such as BTCUSDT."),
    service: GatewayService = Depends(get_gateway_service),
) -> dict:
    return service.get_okx_market_overview(symbol.upper())


@protected.get(
    "/v1/debug/okx/multi-timeframe-overview",
    tags=["Debug"],
    summary="Debug OKX multi-timeframe overview",
    include_in_schema=False,
)
def debug_okx_multi_timeframe_overview(
    symbol: str = Query("BTCUSDT", description="Trading symbol such as BTCUSDT."),
    service: GatewayService = Depends(get_gateway_service),
) -> dict:
    return service.get_okx_multi_timeframe_overview(symbol.upper())


@protected.get(
    "/v1/debug/okx/derivatives-overview",
    tags=["Debug"],
    summary="Debug OKX derivatives overview",
    include_in_schema=False,
)
def debug_okx_derivatives_overview(
    symbol: str = Query("BTCUSDT", description="Trading symbol such as BTCUSDT."),
    period: str = Query("1h", description="History period such as 15m, 30m, 1h, 2h, 4h, or 6h."),
    limit: int = Query(8, ge=3, le=30, description="Number of historical points to return."),
    service: GatewayService = Depends(get_gateway_service),
) -> dict:
    return service.get_okx_derivatives_overview(symbol.upper(), period=period, limit=limit)


@protected.get("/v1/macro/overview", tags=["Overview"], summary="Get a compact macro overview for GPT")
def macro_overview(
    fred_series: str = Query(
        "FEDFUNDS,DGS10,UNRATE",
        description="Comma-separated FRED series IDs. Ignored if FRED_API_KEY is not configured.",
    ),
    service: GatewayService = Depends(get_gateway_service),
) -> dict:
    series_ids = [item.strip().upper() for item in fred_series.split(",") if item.strip()]
    return service.macro_overview(series_ids)


@protected.get(
    "/v1/macro/event-calendar",
    tags=["Overview"],
    summary="Get the official macro event calendar for the next few days",
    description="Returns upcoming official BLS, BEA, FOMC, and Treasury calendar events for macro risk timing. Use this when an 8-12h BTC judgment may be sensitive to scheduled event risk.",
)
def macro_event_calendar(
    horizon_hours: int = Query(72, ge=8, le=336, description="How many forward hours of official event risk to include."),
    major_only: bool = Query(True, description="When true, filter out lower-importance items such as short-bill Treasury auctions."),
    service: GatewayService = Depends(get_gateway_service),
) -> dict:
    return service.get_macro_event_calendar(horizon_hours=horizon_hours, major_only=major_only)


@protected.get("/v1/regulatory/overview", tags=["Overview"], summary="Get a compact regulatory overview for GPT")
def regulatory_overview(
    entities: str = Query(
        "IBIT,FBTC,GBTC",
        description="Comma-separated SEC tickers or CIKs for recent filing snapshots.",
    ),
    service: GatewayService = Depends(get_gateway_service),
) -> dict:
    entity_list = [item.strip().upper() for item in entities.split(",") if item.strip()]
    return service.regulatory_overview(entity_list)


@protected.get("/v1/sources/binance/market", tags=["Sources"], summary="Get Binance spot and futures snapshot")
def binance_market(
    symbol: str = Query("BTCUSDT", description="Binance symbol such as BTCUSDT."),
    service: GatewayService = Depends(get_gateway_service),
) -> dict:
    return service.get_binance_market(symbol.upper())


@protected.get(
    "/v1/sources/binance/derivatives-structure",
    tags=["Sources"],
    summary="Get Binance derivatives structure",
    description="Required depth endpoint for final 8-12h BTC judgment. Returns mark-index spread, basis, open interest history, top trader ratios, global long-short ratio, and taker buy-sell volume.",
)
def binance_derivatives_structure(
    symbol: str = Query("BTCUSDT", description="Trading symbol such as BTCUSDT."),
    period: str = Query("1h", description="History period such as 15m, 30m, 1h, 2h, 4h, or 6h."),
    limit: int = Query(12, ge=3, le=30, description="Number of historical points to return."),
    service: GatewayService = Depends(get_gateway_service),
) -> dict:
    return service.get_binance_derivatives_structure(symbol.upper(), period=period, limit=limit)


@protected.get(
    "/v1/sources/binance/multi-timeframe-structure",
    tags=["Sources"],
    summary="Get Binance multi-timeframe structure",
    description="Required depth endpoint for final 8-12h BTC judgment. Returns 15m, 1h, 4h, 8h, 1d, 1w, and 1M structure with range levels, support/resistance, Fibonacci, ATR-like volatility, VWAP approximation, and raw candles.",
)
def binance_multi_timeframe_structure(
    symbol: str = Query("BTCUSDT", description="Trading symbol such as BTCUSDT."),
    service: GatewayService = Depends(get_gateway_service),
) -> dict:
    return service.get_binance_multi_timeframe_structure(symbol.upper())


@protected.get(
    "/v1/sources/bybit/market-structure",
    tags=["Sources"],
    summary="Get Bybit derivatives validation snapshot",
    description="Secondary validation endpoint for 8-12h BTC judgment and fallback when Binance is temporarily rate-limited.",
)
def bybit_market_structure(
    symbol: str = Query("BTCUSDT", description="Linear perpetual symbol such as BTCUSDT."),
    service: GatewayService = Depends(get_gateway_service),
) -> dict:
    return service.get_bybit_market_structure(symbol.upper())


@protected.get(
    "/v1/sources/liquidity/summary",
    tags=["Sources"],
    summary="Get a cross-exchange BTC liquidity summary",
    description="Returns a free orderbook-based liquidity map across OKX, Bybit, and optionally Binance. It is the free substitute for a commercial heatmap or liquidation map when doing 8-12h BTC analysis.",
)
def liquidity_summary(
    symbol: str = Query("BTCUSDT", description="Trading symbol such as BTCUSDT."),
    depth_limit: int = Query(100, ge=50, le=500, description="Requested orderbook depth. Each exchange uses the nearest supported depth."),
    include_binance: bool = Query(False, description="When true, include Binance orderbook data. Disabled by default because Binance is more likely to rate-limit."),
    include_okx: bool = Query(True, description="Include OKX orderbook data."),
    include_bybit: bool = Query(True, description="Include Bybit orderbook data."),
    service: GatewayService = Depends(get_gateway_service),
) -> dict:
    return service.get_liquidity_summary(
        symbol=symbol.upper(),
        depth_limit=depth_limit,
        include_binance=include_binance,
        include_okx=include_okx,
        include_bybit=include_bybit,
    )


@protected.get(
    "/v1/sources/liquidity/context",
    tags=["Sources"],
    summary="Get a sampled BTC liquidity and liquidation context",
    description="Returns a higher-accuracy liquidity context using live liquidation sampling plus current orderbook structure across OKX, Bybit, and optionally Binance. Use this for final 8-12h BTC judgment when you want more than a static snapshot.",
)
def liquidity_context(
    symbol: str = Query("BTCUSDT", description="Trading symbol such as BTCUSDT."),
    depth_limit: int = Query(100, ge=50, le=500, description="Requested orderbook depth. Each exchange uses the nearest supported depth."),
    liquidation_sample_seconds: int = Query(4, ge=2, le=8, description="How many seconds to sample live liquidation streams."),
    include_binance: bool = Query(False, description="When true, include Binance orderbook and liquidation sampling. Disabled by default because Binance is more likely to rate-limit."),
    include_okx: bool = Query(True, description="Include OKX orderbook data."),
    include_bybit: bool = Query(True, description="Include Bybit orderbook and liquidation sampling."),
    service: GatewayService = Depends(get_gateway_service),
) -> dict:
    return service.get_liquidity_context(
        symbol=symbol.upper(),
        depth_limit=depth_limit,
        liquidation_sample_seconds=liquidation_sample_seconds,
        include_binance=include_binance,
        include_okx=include_okx,
        include_bybit=include_bybit,
    )


@protected.get("/v1/sources/coingecko/simple-price", tags=["Sources"], summary="Get CoinGecko simple price data")
def coingecko_simple_price(
    asset_id: str = Query("bitcoin", description="CoinGecko asset ID, for example bitcoin."),
    vs_currency: str = Query("usd", description="Quote currency such as usd."),
    service: GatewayService = Depends(get_gateway_service),
) -> dict:
    return service.get_coingecko_simple_price(asset_id=asset_id, vs_currency=vs_currency)


@protected.get(
    "/v1/sources/deribit/options-context",
    tags=["Sources"],
    summary="Get Deribit BTC options and IV context",
    description="Returns Deribit BTC options, futures, historical volatility, and volatility-index context. Use it as the options and implied-volatility layer for final 8-12h BTC judgment.",
)
def deribit_options_context(
    currency: str = Query("BTC", description="Base currency on Deribit, usually BTC."),
    service: GatewayService = Depends(get_gateway_service),
) -> dict:
    return service.get_deribit_options_context(currency=currency.upper())


@protected.get("/v1/sources/coinglass/market-structure", tags=["Sources"], summary="Get CoinGlass derivatives structure snapshot", include_in_schema=False)
def coinglass_market_structure(
    symbol: str = Query("BTCUSDT", description="Trading symbol such as BTCUSDT. The server normalizes this to the coin symbol used by CoinGlass."),
    exchange: str = Query("OKX", description="Exchange to focus on, such as OKX or Binance."),
    interval: str = Query("1h", description="Candle interval, such as 1h or 4h."),
    service: GatewayService = Depends(get_gateway_service),
) -> dict:
    return service.get_coinglass_market_structure(symbol=symbol, exchange=exchange, interval=interval)


@protected.get("/v1/sources/fear-greed/latest", tags=["Sources"], summary="Get latest Fear & Greed index")
def fear_greed_latest(service: GatewayService = Depends(get_gateway_service)) -> dict:
    return service.get_fear_greed_latest()


@protected.get("/v1/sources/mempool/fees", tags=["Sources"], summary="Get mempool.space recommended BTC fees")
def mempool_fees(service: GatewayService = Depends(get_gateway_service)) -> dict:
    return service.get_mempool_recommended_fees()


@protected.get("/v1/sources/treasury/latest-avg-rates", tags=["Sources"], summary="Get latest Treasury average interest rate snapshot")
@protected.get("/v1/sources/treasury/latest-curve", tags=["Sources"], include_in_schema=False)
def treasury_latest_avg_rates(service: GatewayService = Depends(get_gateway_service)) -> dict:
    return service.get_treasury_latest_avg_rates()


@protected.get("/v1/sources/bls/series/{series_id}", tags=["Sources"], summary="Get latest BLS time series observations")
def bls_series(
    series_id: str,
    limit: int = Query(12, ge=1, le=24, description="Maximum number of observations to return."),
    service: GatewayService = Depends(get_gateway_service),
) -> dict:
    return service.get_bls_series(series_id.upper(), limit=limit)


@protected.get("/v1/sources/fred/series/{series_id}", tags=["Sources"], summary="Get latest FRED observations")
def fred_series(
    series_id: str,
    limit: int = Query(12, ge=1, le=24, description="Maximum number of observations to return."),
    service: GatewayService = Depends(get_gateway_service),
) -> dict:
    return service.get_fred_series(series_id.upper(), limit=limit)


@protected.get("/v1/sources/bea/datasets", tags=["Sources"], summary="List BEA datasets")
def bea_datasets(service: GatewayService = Depends(get_gateway_service)) -> dict:
    return service.get_bea_datasets()


@protected.get("/v1/sources/bea/gdp", tags=["Sources"], summary="Get BEA GDP observations")
def bea_gdp(
    year: str = Query("LAST5", description="BEA year selector such as LAST5, 2024, or ALL."),
    service: GatewayService = Depends(get_gateway_service),
) -> dict:
    return service.get_bea_gdp(year=year)


@protected.get("/v1/sources/fed/monetary-feed", tags=["Sources"], summary="Get latest Federal Reserve monetary RSS items")
def fed_monetary_feed(
    limit: int = Query(5, ge=1, le=10, description="Number of RSS items to return."),
    service: GatewayService = Depends(get_gateway_service),
) -> dict:
    return service.get_fed_monetary_feed(limit=limit)


@protected.get("/v1/sources/sec/company-tickers", tags=["Sources"], summary="Search SEC company ticker directory")
def sec_company_tickers(
    query: str | None = Query(None, description="Ticker or name fragment such as IBIT or BlackRock."),
    exchange: str | None = Query(None, description="Optional exchange filter such as Nasdaq or NYSE."),
    limit: int = Query(25, ge=1, le=100, description="Maximum number of rows to return."),
    service: GatewayService = Depends(get_gateway_service),
) -> dict:
    return service.get_sec_company_tickers(query=query, exchange=exchange, limit=limit)


@protected.get("/v1/sources/sec/submissions/{entity}", tags=["Sources"], summary="Get SEC submissions by ticker or CIK")
def sec_submissions(
    entity: str,
    forms_limit: int = Query(10, ge=1, le=20, description="Maximum number of recent filings to return."),
    service: GatewayService = Depends(get_gateway_service),
) -> dict:
    return service.get_sec_submissions(entity=entity, forms_limit=forms_limit)


@protected.get("/v1/sources/cftc/bitcoin-cot", tags=["Sources"], summary="Get latest CFTC bitcoin commitment of traders data")
def cftc_bitcoin_cot(
    exchange: str = Query("cme", description="Exchange filter. Use cme or all."),
    limit: int = Query(4, ge=1, le=10, description="Maximum number of records to return."),
    service: GatewayService = Depends(get_gateway_service),
) -> dict:
    return service.get_cftc_bitcoin_cot(exchange=exchange, limit=limit)


app.include_router(protected)
