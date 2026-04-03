from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os


def _load_dotenv(dotenv_path: Path) -> None:
    if not dotenv_path.exists():
        return

    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(slots=True)
class AppConfig:
    base_dir: Path
    data_dir: Path
    timezone: str
    symbol: str
    telegram_bot_token: str
    telegram_chat_id: str
    session_start_hour: int
    session_end_hour: int
    enable_europe_session: bool
    europe_session_start_hour: int
    europe_session_end_hour: int
    enable_us_session: bool
    us_session_start_hour: int
    us_session_end_hour: int
    alert_cooldown_minutes: int
    send_neutral_updates: bool
    min_score_to_alert: int
    cryptocompare_api_key: str
    news_cache_seconds: int
    news_max_age_hours: int
    news_limit: int
    news_min_relevance: int
    max_news_items_for_prompt: int
    news_require_explicit_btc: bool
    enable_gdelt_news: bool
    gdelt_cache_seconds: int
    gdelt_max_records: int
    gdelt_timespan_hours: int
    gdelt_query: str
    fmp_api_key: str
    gemini_api_key: str
    gemini_model: str
    strict_session_mode: bool
    allow_outside_session_risk_alerts: bool
    min_confidence_for_trade: int
    min_risk_reward: float
    min_composite_score_for_trade: int
    europe_min_confidence_for_trade: int
    europe_min_risk_reward: float
    europe_min_composite_score_for_trade: int
    us_min_confidence_for_trade: int
    us_min_risk_reward: float
    us_min_composite_score_for_trade: int
    high_impact_news_score: int
    low_impact_news_score: int
    macro_cache_seconds: int
    macro_pre_block_minutes: int
    macro_post_block_minutes: int
    macro_warning_window_minutes: int
    macro_critical_pre_block_minutes: int
    macro_critical_post_block_minutes: int
    macro_critical_warning_window_minutes: int
    macro_high_pre_block_minutes: int
    macro_high_post_block_minutes: int
    macro_high_warning_window_minutes: int
    macro_medium_pre_block_minutes: int
    macro_medium_post_block_minutes: int
    macro_medium_warning_window_minutes: int
    liquidation_sample_seconds: int

    @property
    def telegram_enabled(self) -> bool:
        return bool(self.telegram_bot_token and self.telegram_chat_id)

    @property
    def cryptocompare_enabled(self) -> bool:
        return bool(self.cryptocompare_api_key)

    @property
    def gdelt_enabled(self) -> bool:
        return self.enable_gdelt_news

    @property
    def gemini_enabled(self) -> bool:
        return bool(self.gemini_api_key and self.gemini_model)

    @property
    def fmp_enabled(self) -> bool:
        return bool(self.fmp_api_key)


def load_config() -> AppConfig:
    base_dir = Path(__file__).resolve().parents[1]
    _load_dotenv(base_dir / ".env")

    data_dir = base_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    return AppConfig(
        base_dir=base_dir,
        data_dir=data_dir,
        timezone=os.getenv("TIMEZONE", "Asia/Shanghai"),
        symbol=os.getenv("SYMBOL", "BTCUSDT"),
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", ""),
        session_start_hour=int(os.getenv("SESSION_START_HOUR", "15")),
        session_end_hour=int(os.getenv("SESSION_END_HOUR", "23")),
        enable_europe_session=_env_bool("ENABLE_EUROPE_SESSION", True),
        europe_session_start_hour=int(os.getenv("EUROPE_SESSION_START_HOUR", "15")),
        europe_session_end_hour=int(os.getenv("EUROPE_SESSION_END_HOUR", "20")),
        enable_us_session=_env_bool("ENABLE_US_SESSION", True),
        us_session_start_hour=int(os.getenv("US_SESSION_START_HOUR", "20")),
        us_session_end_hour=int(os.getenv("US_SESSION_END_HOUR", "2")),
        alert_cooldown_minutes=int(os.getenv("ALERT_COOLDOWN_MINUTES", "45")),
        send_neutral_updates=_env_bool("SEND_NEUTRAL_UPDATES", False),
        min_score_to_alert=int(os.getenv("MIN_SCORE_TO_ALERT", "25")),
        cryptocompare_api_key=os.getenv("CRYPTOCOMPARE_API_KEY", ""),
        news_cache_seconds=int(os.getenv("NEWS_CACHE_SECONDS", "120")),
        news_max_age_hours=int(os.getenv("NEWS_MAX_AGE_HOURS", "12")),
        news_limit=int(os.getenv("NEWS_LIMIT", "40")),
        news_min_relevance=int(os.getenv("NEWS_MIN_RELEVANCE", "8")),
        max_news_items_for_prompt=int(os.getenv("MAX_NEWS_ITEMS_FOR_PROMPT", "3")),
        news_require_explicit_btc=_env_bool("NEWS_REQUIRE_EXPLICIT_BTC", True),
        enable_gdelt_news=_env_bool("ENABLE_GDELT_NEWS", True),
        gdelt_cache_seconds=int(os.getenv("GDELT_CACHE_SECONDS", "600")),
        gdelt_max_records=int(os.getenv("GDELT_MAX_RECORDS", "15")),
        gdelt_timespan_hours=int(os.getenv("GDELT_TIMESPAN_HOURS", "12")),
        gdelt_query=os.getenv(
            "GDELT_QUERY",
            "(bitcoin OR btc OR etf OR blackrock OR whale OR miner OR trump OR fed OR fomc OR cpi OR payroll OR powell OR tariff OR war OR dollar)",
        ),
        fmp_api_key=os.getenv("FMP_API_KEY", ""),
        gemini_api_key=os.getenv("GEMINI_API_KEY", ""),
        gemini_model=os.getenv("GEMINI_MODEL", "gemini-2.5-flash"),
        strict_session_mode=_env_bool("STRICT_SESSION_MODE", True),
        allow_outside_session_risk_alerts=_env_bool("ALLOW_OUTSIDE_SESSION_RISK_ALERTS", True),
        min_confidence_for_trade=int(os.getenv("MIN_CONFIDENCE_FOR_TRADE", "62")),
        min_risk_reward=float(os.getenv("MIN_RISK_REWARD", "1.8")),
        min_composite_score_for_trade=int(os.getenv("MIN_COMPOSITE_SCORE_FOR_TRADE", "22")),
        europe_min_confidence_for_trade=int(os.getenv("EUROPE_MIN_CONFIDENCE_FOR_TRADE", "60")),
        europe_min_risk_reward=float(os.getenv("EUROPE_MIN_RISK_REWARD", "1.7")),
        europe_min_composite_score_for_trade=int(os.getenv("EUROPE_MIN_COMPOSITE_SCORE_FOR_TRADE", "20")),
        us_min_confidence_for_trade=int(os.getenv("US_MIN_CONFIDENCE_FOR_TRADE", "68")),
        us_min_risk_reward=float(os.getenv("US_MIN_RISK_REWARD", "2.0")),
        us_min_composite_score_for_trade=int(os.getenv("US_MIN_COMPOSITE_SCORE_FOR_TRADE", "24")),
        high_impact_news_score=int(os.getenv("HIGH_IMPACT_NEWS_SCORE", "12")),
        low_impact_news_score=int(os.getenv("LOW_IMPACT_NEWS_SCORE", "4")),
        macro_cache_seconds=int(os.getenv("MACRO_CACHE_SECONDS", "1800")),
        macro_pre_block_minutes=int(os.getenv("MACRO_PRE_BLOCK_MINUTES", "20")),
        macro_post_block_minutes=int(os.getenv("MACRO_POST_BLOCK_MINUTES", "20")),
        macro_warning_window_minutes=int(os.getenv("MACRO_WARNING_WINDOW_MINUTES", "180")),
        macro_critical_pre_block_minutes=int(os.getenv("MACRO_CRITICAL_PRE_BLOCK_MINUTES", "45")),
        macro_critical_post_block_minutes=int(os.getenv("MACRO_CRITICAL_POST_BLOCK_MINUTES", "45")),
        macro_critical_warning_window_minutes=int(os.getenv("MACRO_CRITICAL_WARNING_WINDOW_MINUTES", "240")),
        macro_high_pre_block_minutes=int(os.getenv("MACRO_HIGH_PRE_BLOCK_MINUTES", "20")),
        macro_high_post_block_minutes=int(os.getenv("MACRO_HIGH_POST_BLOCK_MINUTES", "30")),
        macro_high_warning_window_minutes=int(os.getenv("MACRO_HIGH_WARNING_WINDOW_MINUTES", "180")),
        macro_medium_pre_block_minutes=int(os.getenv("MACRO_MEDIUM_PRE_BLOCK_MINUTES", "10")),
        macro_medium_post_block_minutes=int(os.getenv("MACRO_MEDIUM_POST_BLOCK_MINUTES", "15")),
        macro_medium_warning_window_minutes=int(os.getenv("MACRO_MEDIUM_WARNING_WINDOW_MINUTES", "90")),
        liquidation_sample_seconds=int(os.getenv("LIQUIDATION_SAMPLE_SECONDS", "4")),
    )
