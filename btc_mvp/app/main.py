from __future__ import annotations

from datetime import datetime
import traceback

from app.clients.binance import BinanceClient
from app.clients.gemini import GeminiClient
from app.clients.liquidation import BinanceLiquidationClient
from app.clients.telegram import TelegramClient
from app.config import load_config
from app.services.analysis import analyze_market, build_trade_setup, render_message, summarize_liquidations
from app.services.macro import MacroService, MacroWindow
from app.services.news import (
    AiMarketSummary,
    NewsService,
    NewsSignal,
    build_ai_prompt,
    build_ai_schema,
    mark_news_delivered,
    parse_ai_summary,
    render_ai_summary,
    should_send_news_alert,
)
from app.services.quota import ProviderBudget, QuotaManager
from app.storage.state import JsonStateStore
from app.timeutils import resolve_timezone


def run() -> int:
    config = load_config()
    state_store = JsonStateStore(config.data_dir / "state.json")
    quota_store = JsonStateStore(config.data_dir / "quota.json")
    history_store = JsonStateStore(config.data_dir / "history.json")
    news_cache_store = JsonStateStore(config.data_dir / "news_cache.json")
    macro_cache_store = JsonStateStore(config.data_dir / "macro_cache.json")
    quota = QuotaManager(quota_store)
    state = state_store.load()
    binance = BinanceClient()
    news_service = NewsService(config, news_cache_store)
    macro_service = MacroService(config, macro_cache_store)
    liquidation_client = BinanceLiquidationClient()

    try:
        bars_5m = _with_quota(quota, "binance_spot", ProviderBudget(20, 2_000), lambda: binance.get_spot_klines(config.symbol, "5m", 180))
        bars_15m = _with_quota(quota, "binance_spot", ProviderBudget(20, 2_000), lambda: binance.get_spot_klines(config.symbol, "15m", 160))
        bars_1h = _with_quota(quota, "binance_spot", ProviderBudget(20, 2_000), lambda: binance.get_spot_klines(config.symbol, "1h", 140))
        bars_4h = _with_quota(quota, "binance_spot", ProviderBudget(20, 2_000), lambda: binance.get_spot_klines(config.symbol, "4h", 100))
        book_ticker = _with_quota(quota, "binance_spot", ProviderBudget(20, 2_000), lambda: binance.get_book_ticker(config.symbol))
        open_interest_now = _with_quota(quota, "binance_futures", ProviderBudget(20, 1_000), lambda: binance.get_open_interest(config.symbol))
        open_interest_hist = _with_quota(quota, "binance_futures", ProviderBudget(20, 1_000), lambda: binance.get_open_interest_hist(config.symbol))
        taker_ratio_hist = _with_quota(quota, "binance_futures", ProviderBudget(20, 1_000), lambda: binance.get_taker_ratio(config.symbol))
        long_short_hist = _with_quota(quota, "binance_futures", ProviderBudget(20, 1_000), lambda: binance.get_global_long_short_ratio(config.symbol))
        funding_hist = _with_quota(quota, "binance_futures", ProviderBudget(20, 1_000), lambda: binance.get_funding_rate(config.symbol))

        liquidation_snapshot = None
        try:
            liq_events = _with_quota(
                quota,
                "binance_liquidation",
                ProviderBudget(2, 120),
                lambda: liquidation_client.sample_force_orders(config.symbol, config.liquidation_sample_seconds),
            )
            liquidation_snapshot = summarize_liquidations(liq_events, config.liquidation_sample_seconds)
        except Exception as liquidation_error:
            print(f"Liquidation stream skipped: {liquidation_error}")

        result = analyze_market(
            config,
            bars_5m,
            bars_15m,
            bars_1h,
            bars_4h,
            book_ticker,
            open_interest_now,
            open_interest_hist,
            taker_ratio_hist,
            long_short_hist,
            funding_hist,
            liquidation_snapshot,
        )

        delivered_news_ids = set(state.get("delivered_news_ids", []))
        news_signal = NewsSignal(0, 0, 0, False, "消息面未启用。", [], [])
        try:
            if config.cryptocompare_enabled or config.gdelt_enabled:
                news_signal = _with_quota(
                    quota,
                    "cryptocompare_news",
                    ProviderBudget(3, 600),
                    lambda: news_service.fetch_signal(delivered_news_ids),
                )
        except Exception as news_error:
            print(f"News pipeline skipped: {news_error}")
            news_signal = NewsSignal(0, 0, 0, False, f"消息面暂时不可用: {news_error}", [], [])

        macro_window = MacroWindow(False, False, False, "宏观事件过滤未启用。", [])
        try:
            if config.fmp_enabled:
                macro_window = _with_quota(
                    quota,
                    "fmp_macro",
                    ProviderBudget(2, 240),
                    lambda: macro_service.get_window(),
                )
            else:
                macro_window = macro_service.get_window()
        except Exception as macro_error:
            print(f"Macro window skipped: {macro_error}")
            macro_window = MacroWindow(False, False, False, f"宏观事件过滤暂时不可用: {macro_error}", [])

        trade_setup = build_trade_setup(config, result, news_signal, macro_window, liquidation_snapshot)

        ai_summary: AiMarketSummary | None = None
        ai_summary_text: str | None = None
        should_call_ai = (
            config.gemini_enabled
            and (news_signal.relevant_count > 0 or liquidation_snapshot is not None or macro_window.pre_event_warning)
            and (news_signal.breaking or trade_setup.tradeable or abs(news_signal.score) >= config.low_impact_news_score or macro_window.active_block)
        )
        if should_call_ai:
            gemini = GeminiClient(config.gemini_api_key, config.gemini_model)
            try:
                prompt = build_ai_prompt(result, news_signal, trade_setup, macro_window, liquidation_snapshot)
                ai_payload = _with_quota(quota, "gemini", ProviderBudget(2, 120), lambda: gemini.generate_json(prompt, build_ai_schema()))
                ai_summary = parse_ai_summary(ai_payload)
                ai_summary_text = render_ai_summary(ai_summary)
            except Exception as gemini_error:
                print(f"Gemini structured output skipped: {gemini_error}")

        message = render_message(result, trade_setup, news_signal, ai_summary_text, macro_window, liquidation_snapshot)

        history_store.append_jsonl(result.to_dict(), config.data_dir / "signal_history.jsonl")
        history_store.append_jsonl(news_signal.to_dict(), config.data_dir / "news_history.jsonl")
        history_store.append_jsonl(trade_setup.to_dict(), config.data_dir / "trade_setup_history.jsonl")
        history_store.append_jsonl(macro_window.to_dict(), config.data_dir / "macro_window_history.jsonl")
        if liquidation_snapshot is not None:
            history_store.append_jsonl(liquidation_snapshot.to_dict(), config.data_dir / "liquidation_history.jsonl")
        if ai_summary is not None:
            history_store.append_jsonl(ai_summary.to_dict(), config.data_dir / "ai_summary_history.jsonl")

        outside_session_risk_trigger = (
            config.allow_outside_session_risk_alerts
            and not result.session_active
            and (
                (news_signal.breaking and abs(news_signal.score) >= config.high_impact_news_score)
                or (liquidation_snapshot is not None and liquidation_snapshot.squeeze_signal != "中性")
                or macro_window.active_block
            )
        )
        should_send = trade_setup.tradeable or should_send_news_alert(news_signal, config.high_impact_news_score) or macro_window.active_block
        if config.strict_session_mode and not result.session_active:
            should_send = outside_session_risk_trigger

        if should_send:
            if outside_session_risk_trigger:
                message = "*场外风险观察*\n当前不在你的交易时段内，这是一条风险提醒，不是开仓信号。\n\n" + message

            if config.telegram_enabled:
                telegram = TelegramClient(config.telegram_bot_token, config.telegram_chat_id)
                telegram_response = _with_quota(quota, "telegram", ProviderBudget(5, 300), lambda: telegram.send_message(message))
                message_id = telegram_response.get("result", {}).get("message_id", "unknown")
                print(f"Telegram message sent. message_id={message_id}")
            else:
                print("Telegram not configured. Preview only:")
                print(message)

            local_now = datetime.now(resolve_timezone(config.timezone)).replace(second=0, microsecond=0)
            state["last_alert"] = {
                "sent_at": local_now.isoformat(),
                "direction": trade_setup.direction,
                "composite_score": trade_setup.composite_score,
            }
            mark_news_delivered(state, news_signal.delivered_ids)
            state_store.save(state)
        else:
            print("No alert sent. Current setup does not meet confidence / RR / session rules.")
            print(message)

        return 0
    except Exception as exc:
        error_text = f"{type(exc).__name__}: {exc}"
        print(error_text)
        print(traceback.format_exc())
        history_store.append_jsonl(
            {
                "local_time": datetime.now(resolve_timezone(config.timezone)).replace(second=0, microsecond=0).isoformat(),
                "error": error_text,
            },
            config.data_dir / "error_history.jsonl",
        )
        return 1


def _with_quota(quota: QuotaManager, provider_name: str, budget: ProviderBudget, func):
    quota.consume(provider_name, budget)
    return func()
