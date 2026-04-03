from __future__ import annotations

from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE_DIR))

from app.clients.gemini import GeminiClient
from app.config import load_config
from app.services.analysis import build_trade_setup
from app.services.news import NewsService, build_ai_prompt, build_ai_schema, parse_ai_summary, render_ai_summary
from app.storage.state import JsonStateStore


def main() -> int:
    config = load_config()
    news_service = NewsService(config, JsonStateStore(config.data_dir / "news_cache.json"))
    news_signal = news_service.fetch_signal(set())

    print("News signal:")
    print(news_signal.to_dict())

    if config.gemini_enabled and news_signal.relevant_count > 0:
        fake_tech = type(
            "FakeTech",
            (),
            {
                "bias": "偏空",
                "score": -8,
                "confidence": 52,
                "session_code": "offhours",
                "session_name": "场外观察 | 非交易时段",
                "session_style": "只观察风险和方向，不主动给正式开仓位。",
                "current_price": 68000.0,
                "session_vwap": 67950.0,
                "higher_tf_bias": "偏空",
                "higher_tf_score": -12,
                "intraday_bias": "中性",
                "intraday_score": -4,
                "entry_bias": "中性",
                "entry_score": 0,
                "price_change_15m_pct": 0.0,
                "price_change_1h_pct": -0.2,
                "open_interest_change_30m_pct": 0.0,
                "taker_ratio": 1.0,
                "funding_rate": 0.0,
                "long_short_ratio": 1.0,
                "session_active": False,
                "support_4h": 67200.0,
                "resistance_4h": 68800.0,
                "support_1h": 67650.0,
                "resistance_1h": 68300.0,
                "support_15m": 67720.0,
                "resistance_15m": 68120.0,
                "support_5m": 67840.0,
                "resistance_5m": 68080.0,
                "breakout_reference_high": 68150.0,
                "breakout_reference_low": 67680.0,
                "breakout_state": "none",
                "atr_15m": 180.0,
                "atr_5m": 90.0,
                "setup_type": "等待确认",
                "liquidation_risk": "中性",
                "horizon": "未来 30-90 分钟",
                "reasons": [],
                "warnings": [],
                "expires_at": "",
                "local_time": "",
                "symbol": "BTCUSDT",
                "spread_bps": 0.0,
                "open_interest": 0.0,
                "ema_fast_15m": 67980.0,
                "ema_slow_15m": 68020.0,
                "ema_fast_5m": 67990.0,
                "ema_slow_5m": 68005.0,
                "ema_fast_1h": 68150.0,
                "ema_slow_1h": 68400.0,
                "ema_fast_4h": 68500.0,
                "ema_slow_4h": 69200.0,
                "rsi_15m": 46.0,
                "rsi_5m": 48.0,
            },
        )()
        fake_setup = build_trade_setup(config, fake_tech, news_signal)
        prompt = build_ai_prompt(fake_tech, news_signal, fake_setup)
        client = GeminiClient(config.gemini_api_key, config.gemini_model)

        print("\nGemini structured output:")
        payload = client.generate_json(prompt, build_ai_schema())
        print(payload)

        print("\nRendered AI summary:")
        print(render_ai_summary(parse_ai_summary(payload)))
    else:
        print("\nGemini skipped. Fill GEMINI_API_KEY and ensure there is at least one relevant news item.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
