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
    schema["info"]["description"] = (
        f"{app.description}\n\n"
        "Authentication: configure Custom GPT Actions to send `X-API-Key: <your token>`.\n"
        "A Bearer token in the `Authorization` header is also accepted for manual testing."
    )
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


@protected.get("/v1/crypto/overview", tags=["Overview"], summary="Get a compact crypto market overview for GPT")
def crypto_overview(
    symbol: str = Query("BTCUSDT", description="Binance symbol such as BTCUSDT."),
    service: GatewayService = Depends(get_gateway_service),
) -> dict:
    return service.crypto_overview(symbol.upper())


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


@protected.get("/v1/sources/coingecko/simple-price", tags=["Sources"], summary="Get CoinGecko simple price data")
def coingecko_simple_price(
    asset_id: str = Query("bitcoin", description="CoinGecko asset ID, for example bitcoin."),
    vs_currency: str = Query("usd", description="Quote currency such as usd."),
    service: GatewayService = Depends(get_gateway_service),
) -> dict:
    return service.get_coingecko_simple_price(asset_id=asset_id, vs_currency=vs_currency)


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
