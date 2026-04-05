from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os

from app.config import _load_dotenv


@dataclass(slots=True)
class GatewayConfig:
    base_dir: Path
    host: str
    port: int
    public_base_url: str
    api_token: str
    user_agent: str
    http_timeout_seconds: float
    http_max_retries: int
    default_symbol: str
    fred_api_key: str
    bls_api_key: str
    bea_api_key: str
    coingecko_demo_api_key: str
    coingecko_pro_api_key: str
    coinglass_api_key: str

    @property
    def fred_enabled(self) -> bool:
        return bool(self.fred_api_key)

    @property
    def bls_enabled(self) -> bool:
        return True

    @property
    def bea_enabled(self) -> bool:
        return bool(self.bea_api_key)

    @property
    def coingecko_auth_enabled(self) -> bool:
        return bool(self.coingecko_demo_api_key or self.coingecko_pro_api_key)

    @property
    def coinglass_enabled(self) -> bool:
        return bool(self.coinglass_api_key)


def load_gateway_config() -> GatewayConfig:
    base_dir = Path(__file__).resolve().parents[2]
    _load_dotenv(base_dir / ".env")

    return GatewayConfig(
        base_dir=base_dir,
        host=os.getenv("GATEWAY_HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", os.getenv("GATEWAY_PORT", "8000"))),
        public_base_url=os.getenv("GATEWAY_PUBLIC_BASE_URL", "https://your-public-domain.example.com"),
        api_token=os.getenv("GATEWAY_API_TOKEN", ""),
        user_agent=os.getenv("GATEWAY_USER_AGENT", "crypto-gpt-aggregator/0.1"),
        http_timeout_seconds=float(os.getenv("GATEWAY_HTTP_TIMEOUT_SECONDS", "20")),
        http_max_retries=int(os.getenv("GATEWAY_HTTP_MAX_RETRIES", "2")),
        default_symbol=os.getenv("SYMBOL", "BTCUSDT"),
        fred_api_key=os.getenv("FRED_API_KEY", ""),
        bls_api_key=os.getenv("BLS_API_KEY", ""),
        bea_api_key=os.getenv("BEA_API_KEY", ""),
        coingecko_demo_api_key=os.getenv("COINGECKO_DEMO_API_KEY", ""),
        coingecko_pro_api_key=os.getenv("COINGECKO_PRO_API_KEY", ""),
        coinglass_api_key=os.getenv("COINGLASS_API_KEY", ""),
    )
