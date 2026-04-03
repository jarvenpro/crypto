from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from app.clients.binance import Kline
from app.config import AppConfig
from app.services.indicators import atr, ema, rsi, volume_ratio, vwap
from app.timeutils import resolve_timezone
from app.utils import clamp, escape_markdown_text

if TYPE_CHECKING:
    from app.clients.liquidation import LiquidationEvent
    from app.services.macro import MacroWindow
    from app.services.news import NewsSignal


@dataclass(slots=True)
class LiquidationSnapshot:
    sample_seconds: int
    event_count: int
    buy_liq_usd: float
    sell_liq_usd: float
    dominant_side: str
    squeeze_signal: str
    summary: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(slots=True)
class AnalysisResult:
    symbol: str
    local_time: str
    session_name: str
    session_code: str
    session_style: str
    session_active: bool
    higher_tf_bias: str
    higher_tf_score: int
    intraday_bias: str
    intraday_score: int
    entry_bias: str
    entry_score: int
    bias: str
    score: int
    confidence: int
    current_price: float
    session_vwap: float
    support_4h: float
    resistance_4h: float
    support_1h: float
    resistance_1h: float
    support_15m: float
    resistance_15m: float
    support_5m: float
    resistance_5m: float
    breakout_reference_high: float
    breakout_reference_low: float
    breakout_state: str
    atr_15m: float
    atr_5m: float
    ema_fast_15m: float
    ema_slow_15m: float
    ema_fast_5m: float
    ema_slow_5m: float
    ema_fast_1h: float
    ema_slow_1h: float
    ema_fast_4h: float
    ema_slow_4h: float
    rsi_15m: float
    rsi_5m: float
    spread_bps: float
    funding_rate: float
    open_interest: float
    open_interest_change_30m_pct: float
    taker_ratio: float
    long_short_ratio: float
    price_change_15m_pct: float
    price_change_1h_pct: float
    liquidation_risk: str
    setup_type: str
    horizon: str
    reasons: list[str]
    warnings: list[str]
    expires_at: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(slots=True)
class TradeThresholds:
    min_confidence: int
    min_risk_reward: float
    min_composite_score: int
    max_stop_pct: float


@dataclass(slots=True)
class TradeSetup:
    direction: str
    composite_score: int
    confidence: int
    tech_weight: float
    news_weight: float
    tradeable: bool
    entry: float | None
    stop_loss: float | None
    take_profit_1: float | None
    take_profit_2: float | None
    risk_reward_1: float | None
    entry_mode: str
    entry_ready: bool
    entry_zone_low: float | None
    entry_zone_high: float | None
    execution_note: str
    confirmation_items: list[str]
    invalidation: str
    reasons: list[str]
    session_min_confidence: int
    session_min_risk_reward: float
    session_min_composite_score: int

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(slots=True)
class SessionProfile:
    code: str
    label: str
    window: str
    style: str
    active: bool
    start_time: datetime


@dataclass(slots=True)
class EntryPlan:
    mode: str
    ready: bool
    entry_reference: float | None
    zone_low: float | None
    zone_high: float | None
    note: str
    confirmations: list[str]


def analyze_market(
    config: AppConfig,
    bars_5m: list[Kline],
    bars_15m: list[Kline],
    bars_1h: list[Kline],
    bars_4h: list[Kline],
    book_ticker: dict[str, float],
    open_interest_now: float,
    open_interest_hist: list[dict],
    taker_ratio_hist: list[dict],
    long_short_hist: list[dict],
    funding_hist: list[dict],
    liquidation_snapshot: LiquidationSnapshot | None = None,
) -> AnalysisResult:
    timezone = resolve_timezone(config.timezone)
    local_now = datetime.now(timezone)
    session = _resolve_session_profile(config, local_now)

    session_bars_5m = [bar for bar in bars_5m if bar.open_time.astimezone(timezone) >= session.start_time]
    if not session_bars_5m:
        session_bars_5m = bars_5m[-72:]

    current_price = bars_5m[-1].close_price
    session_vwap = vwap(session_bars_5m)

    ema_fast_5m = ema([bar.close_price for bar in bars_5m[-40:]], 9)
    ema_slow_5m = ema([bar.close_price for bar in bars_5m[-50:]], 21)
    ema_fast_15m = ema([bar.close_price for bar in bars_15m[-40:]], 9)
    ema_slow_15m = ema([bar.close_price for bar in bars_15m[-50:]], 21)
    ema_fast_1h = ema([bar.close_price for bar in bars_1h[-60:]], 20)
    ema_slow_1h = ema([bar.close_price for bar in bars_1h[-80:]], 50)
    ema_fast_4h = ema([bar.close_price for bar in bars_4h[-60:]], 20)
    ema_slow_4h = ema([bar.close_price for bar in bars_4h[-80:]], 50)

    rsi_5m = rsi([bar.close_price for bar in bars_5m[-60:]], 14)
    rsi_15m = rsi([bar.close_price for bar in bars_15m[-60:]], 14)
    atr_5m = atr(bars_5m[-50:], 14)
    atr_15m = atr(bars_15m[-50:], 14)
    vol_ratio_15m = volume_ratio(bars_15m, 8)

    support_5m, resistance_5m = _nearest_levels(current_price, bars_5m, 12)
    support_15m, resistance_15m = _nearest_levels(current_price, bars_15m, 12)
    support_1h, resistance_1h = _nearest_levels(current_price, bars_1h, 10)
    support_4h, resistance_4h = _nearest_levels(current_price, bars_4h, 10)

    price_change_15m_pct = _pct_change(bars_15m[-2].close_price, bars_15m[-1].close_price) if len(bars_15m) >= 2 else 0.0
    price_change_1h_pct = _pct_change(bars_1h[-2].close_price, bars_1h[-1].close_price) if len(bars_1h) >= 2 else 0.0
    oi_change_30m_pct = (
        _pct_change(
            _float_from_hist(open_interest_hist[-7], "sumOpenInterestValue", "sumOpenInterest"),
            _float_from_hist(open_interest_hist[-1], "sumOpenInterestValue", "sumOpenInterest"),
        )
        if len(open_interest_hist) >= 7
        else 0.0
    )
    taker_ratio = float(taker_ratio_hist[-1]["buySellRatio"]) if taker_ratio_hist else 1.0
    long_short_ratio = float(long_short_hist[-1]["longShortRatio"]) if long_short_hist else 1.0
    funding_rate = float(funding_hist[-1]["fundingRate"]) if funding_hist else 0.0

    bid = book_ticker["bid_price"]
    ask = book_ticker["ask_price"]
    mid = (bid + ask) / 2 if bid and ask else current_price
    spread_bps = ((ask - bid) / mid) * 10_000 if mid else 0.0

    reasons: list[str] = []
    warnings: list[str] = []

    higher_tf_score = 0
    if bars_4h[-1].close_price > ema_fast_4h > ema_slow_4h:
        higher_tf_score += 14
        reasons.append("4H 维持多头趋势，价格站在 4H 均线结构上方。")
    elif bars_4h[-1].close_price < ema_fast_4h < ema_slow_4h:
        higher_tf_score -= 14
        reasons.append("4H 维持空头趋势，价格压在 4H 均线结构下方。")

    if bars_1h[-1].close_price > ema_fast_1h > ema_slow_1h:
        higher_tf_score += 10
        reasons.append("1H 方向与 4H 同步偏多。")
    elif bars_1h[-1].close_price < ema_fast_1h < ema_slow_1h:
        higher_tf_score -= 10
        reasons.append("1H 方向与 4H 同步偏空。")

    if _near_level(current_price, support_4h, max(atr_15m * 0.8, current_price * 0.003)):
        higher_tf_score += 4
        warnings.append("当前位置已经贴近 4H 支撑，追空要防反抽。")
    if _near_level(current_price, resistance_4h, max(atr_15m * 0.8, current_price * 0.003)):
        higher_tf_score -= 4
        warnings.append("当前位置已经贴近 4H 阻力，追多要防冲高回落。")

    recent_15m_high = max(bar.high_price for bar in bars_15m[-9:-1])
    recent_15m_low = min(bar.low_price for bar in bars_15m[-9:-1])
    breakout_state = "none"

    intraday_score = 0
    if bars_15m[-1].close_price > session_vwap:
        intraday_score += 8
        reasons.append("15m 价格在本时段 VWAP 上方，盘内节奏偏多。")
    else:
        intraday_score -= 8
        reasons.append("15m 价格在本时段 VWAP 下方，盘内节奏偏空。")

    if ema_fast_15m > ema_slow_15m:
        intraday_score += 10
        reasons.append("15m EMA 维持多头排列。")
    else:
        intraday_score -= 10
        reasons.append("15m EMA 维持空头排列。")

    if bars_15m[-1].close_price > recent_15m_high and vol_ratio_15m > 1.10:
        intraday_score += 9
        breakout_state = "up"
        reasons.append("15m 刚放量上破盘内小区间。")
    elif bars_15m[-1].close_price < recent_15m_low and vol_ratio_15m > 1.10:
        intraday_score -= 9
        breakout_state = "down"
        reasons.append("15m 刚放量下破盘内小区间。")

    if _near_level(current_price, support_15m, max(atr_15m * 0.55, current_price * 0.0018)):
        if higher_tf_score >= 0:
            intraday_score += 7
            reasons.append("价格靠近 15m 支撑，适合等低风险多头位置。")
        else:
            warnings.append("当前位置已经靠近 15m 支撑，做空别在这里追。")
    if _near_level(current_price, resistance_15m, max(atr_15m * 0.55, current_price * 0.0018)):
        if higher_tf_score <= 0:
            intraday_score -= 7
            reasons.append("价格靠近 15m 阻力，适合等低风险空头位置。")
        else:
            warnings.append("当前位置已经靠近 15m 阻力，做多别在这里追。")

    if higher_tf_score >= 12 and intraday_score > 0:
        intraday_score += 4
    elif higher_tf_score <= -12 and intraday_score < 0:
        intraday_score -= 4
    elif higher_tf_score >= 12 and intraday_score < 0:
        warnings.append("15m 日内节奏与 1H/4H 大环境相反，逆势单需要更挑位置。")
    elif higher_tf_score <= -12 and intraday_score > 0:
        warnings.append("15m 日内节奏与 1H/4H 大环境相反，逆势单需要更挑位置。")

    entry_score = 0
    latest_5m = bars_5m[-1]
    recent_5m_high = max(bar.high_price for bar in bars_5m[-7:-1])
    recent_5m_low = min(bar.low_price for bar in bars_5m[-7:-1])
    sweep = _detect_liquidity_sweep(latest_5m, recent_5m_high, recent_5m_low)

    if ema_fast_5m > ema_slow_5m:
        entry_score += 6
        reasons.append("5m EMA 已转多，适合作为多头确认。")
    else:
        entry_score -= 6
        reasons.append("5m EMA 仍偏空，短线抢多要保守。")

    if taker_ratio > 1.05:
        entry_score += 5
        reasons.append("主动买盘略强，5m 入场确认偏多。")
    elif taker_ratio < 0.95:
        entry_score -= 5
        reasons.append("主动卖盘略强，5m 入场确认偏空。")

    if sweep == "lower_sweep":
        entry_score += 6
        reasons.append("5m 刚扫低回收，更像短线多头确认。")
    elif sweep == "upper_sweep":
        entry_score -= 6
        reasons.append("5m 刚扫高回落，更像短线空头确认。")

    if liquidation_snapshot is not None and liquidation_snapshot.event_count > 0:
        if liquidation_snapshot.squeeze_signal == "空头回补":
            entry_score += 4
        elif liquidation_snapshot.squeeze_signal == "多头踩踏":
            entry_score -= 4

    if rsi_5m > 72 and entry_score > 0:
        entry_score -= 3
        warnings.append("5m RSI 偏热，当前更适合等回踩确认而不是直接追多。")
    elif rsi_5m < 28 and entry_score < 0:
        entry_score += 3
        warnings.append("5m RSI 偏冷，当前更适合等反抽确认而不是直接追空。")

    liquidation_risk = _liquidation_proxy(long_short_ratio, funding_rate, oi_change_30m_pct)
    if liquidation_risk == "多头拥挤":
        warnings.append("多头拥挤，冲高失败时容易出现快速回落。")
    elif liquidation_risk == "空头拥挤":
        warnings.append("空头拥挤，上破时容易出现急拉。")

    if spread_bps > 3.0:
        warnings.append("盘口点差偏大，实际成交价可能比理论入场更差。")

    if not session.active:
        warnings.insert(0, "当前不在你的交易时段内，只建议观察，不建议主动开新仓。")

    alignment = 0
    if _sign(higher_tf_score) == _sign(intraday_score) and _sign(intraday_score) != 0:
        alignment += 4
    if _sign(intraday_score) == _sign(entry_score) and _sign(entry_score) != 0:
        alignment += 3
    if _sign(higher_tf_score) != 0 and _sign(intraday_score) != 0 and _sign(higher_tf_score) != _sign(intraday_score):
        alignment -= 4

    score = int(round(higher_tf_score * 0.30 + intraday_score * 0.50 + entry_score * 0.20 + alignment))
    confidence = int(
        clamp(
            36 + abs(higher_tf_score) * 0.45 + abs(intraday_score) * 0.70 + abs(entry_score) * 0.45 + max(alignment, 0) * 2,
            20,
            92 if session.active else 55,
        )
    )

    higher_tf_bias = _bias_from_score(higher_tf_score, 8)
    intraday_bias = _bias_from_score(intraday_score, 7)
    entry_bias = _bias_from_score(entry_score, 6)
    bias = _bias_from_score(score, 18)

    if session.code == "europe":
        horizon = "未来 30-120 分钟"
        expires_minutes = 90
    elif session.code == "us":
        horizon = "未来 30-180 分钟"
        expires_minutes = 120
    else:
        horizon = "未来 30-90 分钟"
        expires_minutes = 75

    setup_type = _setup_type_from_biases(intraday_bias, entry_bias, current_price, support_15m, resistance_15m)
    expires_at = (local_now + timedelta(minutes=expires_minutes)).replace(second=0, microsecond=0).isoformat()

    return AnalysisResult(
        symbol=config.symbol,
        local_time=local_now.replace(second=0, microsecond=0).isoformat(),
        session_name=session.window,
        session_code=session.code,
        session_style=session.style,
        session_active=session.active,
        higher_tf_bias=higher_tf_bias,
        higher_tf_score=higher_tf_score,
        intraday_bias=intraday_bias,
        intraday_score=intraday_score,
        entry_bias=entry_bias,
        entry_score=entry_score,
        bias=bias,
        score=score,
        confidence=confidence,
        current_price=current_price,
        session_vwap=session_vwap,
        support_4h=support_4h,
        resistance_4h=resistance_4h,
        support_1h=support_1h,
        resistance_1h=resistance_1h,
        support_15m=support_15m,
        resistance_15m=resistance_15m,
        support_5m=support_5m,
        resistance_5m=resistance_5m,
        breakout_reference_high=recent_15m_high,
        breakout_reference_low=recent_15m_low,
        breakout_state=breakout_state,
        atr_15m=atr_15m,
        atr_5m=atr_5m,
        ema_fast_15m=ema_fast_15m,
        ema_slow_15m=ema_slow_15m,
        ema_fast_5m=ema_fast_5m,
        ema_slow_5m=ema_slow_5m,
        ema_fast_1h=ema_fast_1h,
        ema_slow_1h=ema_slow_1h,
        ema_fast_4h=ema_fast_4h,
        ema_slow_4h=ema_slow_4h,
        rsi_15m=rsi_15m,
        rsi_5m=rsi_5m,
        spread_bps=spread_bps,
        funding_rate=funding_rate,
        open_interest=open_interest_now,
        open_interest_change_30m_pct=oi_change_30m_pct,
        taker_ratio=taker_ratio,
        long_short_ratio=long_short_ratio,
        price_change_15m_pct=price_change_15m_pct,
        price_change_1h_pct=price_change_1h_pct,
        liquidation_risk=liquidation_risk,
        setup_type=setup_type,
        horizon=horizon,
        reasons=reasons[:8],
        warnings=warnings[:8],
        expires_at=expires_at,
    )


def summarize_liquidations(events: list[LiquidationEvent], sample_seconds: int) -> LiquidationSnapshot:
    buy_liq = sum(item.notional_usd for item in events if item.side.upper() == "BUY")
    sell_liq = sum(item.notional_usd for item in events if item.side.upper() == "SELL")
    if buy_liq > sell_liq * 1.3 and buy_liq > 250_000:
        dominant_side = "BUY"
        squeeze_signal = "空头回补"
        summary = f"最近 {sample_seconds} 秒监测到空头回补型强平偏多，规模约 ${buy_liq:,.0f}。"
    elif sell_liq > buy_liq * 1.3 and sell_liq > 250_000:
        dominant_side = "SELL"
        squeeze_signal = "多头踩踏"
        summary = f"最近 {sample_seconds} 秒监测到多头踩踏型强平偏空，规模约 ${sell_liq:,.0f}。"
    else:
        dominant_side = "BALANCED"
        squeeze_signal = "中性"
        summary = f"最近 {sample_seconds} 秒强平流相对均衡，未见特别明显的挤压方向。"

    return LiquidationSnapshot(
        sample_seconds=sample_seconds,
        event_count=len(events),
        buy_liq_usd=buy_liq,
        sell_liq_usd=sell_liq,
        dominant_side=dominant_side,
        squeeze_signal=squeeze_signal,
        summary=summary,
    )


def build_trade_setup(
    config: AppConfig,
    result: AnalysisResult,
    news_signal: NewsSignal | None,
    macro_window: MacroWindow | None = None,
    liquidation_snapshot: LiquidationSnapshot | None = None,
) -> TradeSetup:
    thresholds = _resolve_trade_thresholds(config, result.session_code)
    news_score = news_signal.score if news_signal else 0
    breaking_news = bool(news_signal and news_signal.breaking)
    tech_weight, news_weight = _resolve_weights(config, result.session_code, news_score, breaking_news)

    technical_for_direction = int(round(result.higher_tf_score * 0.25 + result.intraday_score * 0.50 + result.entry_score * 0.25))
    composite_score = int(round(technical_for_direction * tech_weight + news_score * news_weight))
    confidence = int(
        clamp(
            result.confidence + abs(news_score) * news_weight * 0.8 + (3 if result.entry_bias == result.intraday_bias and result.entry_bias != "中性" else 0),
            20,
            95,
        )
    )

    reasons = [
        "先看 4H/1H 大环境，再用 15m 定盘内方向，最后用 5m 做入场确认。",
        f"当前时段模板: `{result.session_style}`。",
        f"本时段门槛: 置信度>={thresholds.min_confidence}% / RR>={thresholds.min_risk_reward:.2f} / 综合分>={thresholds.min_composite_score}。",
        f"技术/消息权重: {tech_weight:.0%} / {news_weight:.0%}。",
    ]
    if breaking_news:
        reasons.append("存在突发消息，盘内方向判断会更依赖消息面。")
    if result.higher_tf_bias != "中性":
        reasons.append(f"4H/1H 大环境当前 `{result.higher_tf_bias}`。")
    if result.intraday_bias != "中性":
        reasons.append(f"15m 盘内节奏当前 `{result.intraday_bias}`。")
    if result.entry_bias != "中性":
        reasons.append(f"5m 入场确认当前 `{result.entry_bias}`。")
    if liquidation_snapshot is not None and liquidation_snapshot.event_count > 0:
        reasons.append(liquidation_snapshot.summary)

    if composite_score >= thresholds.min_composite_score:
        direction = "做多"
    elif composite_score <= -thresholds.min_composite_score:
        direction = "做空"
    else:
        direction = "观望"

    entry_plan = _build_entry_plan(result, direction)
    invalidation = (
        "若 15m 跌回关键支撑下方且 5m 无法收回，多头思路失效。"
        if direction == "做多"
        else "若 15m 站回关键阻力上方且 5m 无法重新转弱，空头思路失效。"
    )

    if direction == "观望":
        return TradeSetup(
            direction=direction,
            composite_score=composite_score,
            confidence=confidence,
            tech_weight=tech_weight,
            news_weight=news_weight,
            tradeable=False,
            entry=None,
            stop_loss=None,
            take_profit_1=None,
            take_profit_2=None,
            risk_reward_1=None,
            entry_mode=entry_plan.mode,
            entry_ready=False,
            entry_zone_low=entry_plan.zone_low,
            entry_zone_high=entry_plan.zone_high,
            execution_note="15m 方向还不够清晰，继续等待更干净的盘内结构。",
            confirmation_items=entry_plan.confirmations,
            invalidation="15m 方向还不够清晰，继续等待更干净的盘内结构。",
            reasons=reasons,
            session_min_confidence=thresholds.min_confidence,
            session_min_risk_reward=thresholds.min_risk_reward,
            session_min_composite_score=thresholds.min_composite_score,
        )

    entry = entry_plan.entry_reference if entry_plan.entry_reference is not None else result.current_price
    stop_loss, risk = _build_intraday_stop(entry, result, direction, thresholds.max_stop_pct)
    if risk <= 0:
        return TradeSetup(
            direction="观望",
            composite_score=composite_score,
            confidence=confidence,
            tech_weight=tech_weight,
            news_weight=news_weight,
            tradeable=False,
            entry=None,
            stop_loss=None,
            take_profit_1=None,
            take_profit_2=None,
            risk_reward_1=None,
            entry_mode=entry_plan.mode,
            entry_ready=False,
            entry_zone_low=entry_plan.zone_low,
            entry_zone_high=entry_plan.zone_high,
            execution_note="当前 5m/15m 结构给不出合理的日内止损，先观望。",
            confirmation_items=entry_plan.confirmations,
            invalidation="当前 5m/15m 结构给不出合理的日内止损，先观望。",
            reasons=reasons,
            session_min_confidence=thresholds.min_confidence,
            session_min_risk_reward=thresholds.min_risk_reward,
            session_min_composite_score=thresholds.min_composite_score,
        )

    take_profit_1, take_profit_2 = _build_intraday_targets(entry, risk, result, direction, thresholds.min_risk_reward)
    reward_1 = abs(take_profit_1 - entry)
    risk_reward_1 = reward_1 / risk if risk else 0.0
    tradeable = True

    if confidence < thresholds.min_confidence:
        tradeable = False
        reasons.append(f"当前综合置信度 {confidence}% 还没到本时段门槛。")
    if risk_reward_1 + 1e-6 < thresholds.min_risk_reward:
        tradeable = False
        reasons.append(f"当前第一目标盈亏比只有 {risk_reward_1:.2f}，不够符合你的日内打法。")
    if not entry_plan.ready:
        tradeable = False
        reasons.append("5m 入场条件还没完全到位，先等价格和确认同步。")
    if direction == "做多" and result.higher_tf_score <= -16:
        confidence = max(confidence - 8, 20)
        reasons.append("这单在逆 4H/1H 大环境做多，只能当短线反抽。")
    if direction == "做空" and result.higher_tf_score >= 16:
        confidence = max(confidence - 8, 20)
        reasons.append("这单在逆 4H/1H 大环境做空，只能当短线回落。")
    if macro_window and macro_window.active_block:
        tradeable = False
        confidence = max(confidence - 12, 20)
        reasons.append(f"宏观硬风控生效: {macro_window.recommended_action}")
    elif macro_window and macro_window.pre_event_warning:
        confidence = max(confidence - 5, 20)
        reasons.append(f"宏观观察窗口: {macro_window.recommended_action}")
        if risk_reward_1 + 1e-6 < thresholds.min_risk_reward + 0.35:
            tradeable = False
            reasons.append("临近宏观事件时，这笔单的盈亏比还不够高。")
    if config.strict_session_mode and not result.session_active:
        tradeable = False
        reasons.append("当前不在交易时段内，方向单自动降级为观望。")

    return TradeSetup(
        direction=direction,
        composite_score=composite_score,
        confidence=confidence,
        tech_weight=tech_weight,
        news_weight=news_weight,
        tradeable=tradeable,
        entry=entry,
        stop_loss=stop_loss,
        take_profit_1=take_profit_1,
        take_profit_2=take_profit_2,
        risk_reward_1=risk_reward_1,
        entry_mode=entry_plan.mode,
        entry_ready=entry_plan.ready,
        entry_zone_low=entry_plan.zone_low,
        entry_zone_high=entry_plan.zone_high,
        execution_note=entry_plan.note,
        confirmation_items=entry_plan.confirmations,
        invalidation=invalidation,
        reasons=reasons[:8],
        session_min_confidence=thresholds.min_confidence,
        session_min_risk_reward=thresholds.min_risk_reward,
        session_min_composite_score=thresholds.min_composite_score,
    )


def render_message(
    result: AnalysisResult,
    trade_setup: TradeSetup,
    news_signal: NewsSignal | None = None,
    ai_summary: str | None = None,
    macro_window: MacroWindow | None = None,
    liquidation_snapshot: LiquidationSnapshot | None = None,
) -> str:
    reasons_block = "\n".join(f"- {escape_markdown_text(item)}" for item in result.reasons[:6]) or "- 暂无明显技术优势。"
    warning_block = "\n".join(f"- {escape_markdown_text(item)}" for item in result.warnings[:6]) or "- 暂无额外风险提示。"

    sections = [
        "*BTC 日内分析助手*",
        f"时间: `{result.local_time}`",
        f"交易时段: `{result.session_name}`",
        f"时段模板: `{result.session_style}`",
        f"总技术判断: *{result.bias}* | 技术分: *{result.score}* | 技术置信度: *{result.confidence}%*",
        f"综合判断: *{trade_setup.direction}* | 综合分: *{trade_setup.composite_score}* | 综合置信度: *{trade_setup.confidence}%*",
        f"观察窗口: `{result.horizon}`",
        f"当前价格: `${result.current_price:,.2f}` | Session VWAP: `${result.session_vwap:,.2f}`",
        "",
        "*大环境 4H / 1H*",
        f"方向: *{result.higher_tf_bias}* | 分数: *{result.higher_tf_score}*",
        f"4H EMA(20/50): `{result.ema_fast_4h:,.2f} / {result.ema_slow_4h:,.2f}`",
        f"1H EMA(20/50): `{result.ema_fast_1h:,.2f} / {result.ema_slow_1h:,.2f}`",
        f"4H 支撑/阻力: `${result.support_4h:,.2f} / {result.resistance_4h:,.2f}`",
        f"1H 支撑/阻力: `${result.support_1h:,.2f} / {result.resistance_1h:,.2f}`",
        "",
        "*日内判断 15m*",
        f"方向: *{result.intraday_bias}* | 分数: *{result.intraday_score}*",
        f"15m EMA(9/21): `{result.ema_fast_15m:,.2f} / {result.ema_slow_15m:,.2f}` | RSI: `{result.rsi_15m:.1f}`",
        f"15m 支撑/阻力: `${result.support_15m:,.2f} / {result.resistance_15m:,.2f}` | ATR: `{result.atr_15m:,.2f}`",
        f"15m 突破参考高/低: `${result.breakout_reference_high:,.2f} / {result.breakout_reference_low:,.2f}` | 结构: `{result.breakout_state}`",
        f"15m 价格变化: `{result.price_change_15m_pct:+.2f}%` | 30m OI 变化: `{result.open_interest_change_30m_pct:+.2f}%`",
        "",
        "*入场确认 5m*",
        f"方向: *{result.entry_bias}* | 分数: *{result.entry_score}*",
        f"5m EMA(9/21): `{result.ema_fast_5m:,.2f} / {result.ema_slow_5m:,.2f}` | RSI: `{result.rsi_5m:.1f}`",
        f"5m 支撑/阻力: `${result.support_5m:,.2f} / {result.resistance_5m:,.2f}` | ATR: `{result.atr_5m:,.2f}`",
        f"Taker Ratio: `{result.taker_ratio:.2f}` | Long/Short: `{result.long_short_ratio:.2f}` | Funding: `{result.funding_rate:+.4%}`",
        "",
        "*主要依据*",
        reasons_block,
    ]

    if liquidation_snapshot is not None:
        sections.extend(
            [
                "",
                "*清算流*",
                f"采样: `{liquidation_snapshot.sample_seconds}s` | 事件数: `{liquidation_snapshot.event_count}`",
                f"空头回补: `${liquidation_snapshot.buy_liq_usd:,.0f}` | 多头踩踏: `${liquidation_snapshot.sell_liq_usd:,.0f}`",
                escape_markdown_text(liquidation_snapshot.summary),
            ]
        )

    if macro_window is not None:
        sections.extend(["", "*宏观窗口*", escape_markdown_text(macro_window.summary)])
        if macro_window.recommended_action:
            sections.append(f"处理建议: {escape_markdown_text(macro_window.recommended_action)}")
        for item in macro_window.upcoming[:2]:
            mode_label = "禁开仓" if item.block_mode == "hard_block" else "只观察"
            sections.append(
                f"- {escape_markdown_text(item.event_name)} | `{item.country}` | "
                f"`{item.minutes_to_event}m` | `{item.event_type_label}` | `{item.severity_label}` | `{mode_label}`"
            )

    if news_signal is not None:
        sections.extend(
            [
                "",
                "*消息面*",
                f"消息分: *{news_signal.score:+d}* | 高相关: *{news_signal.relevant_count}* | 新消息: *{news_signal.fresh_count}*",
                f"技术/消息权重: `{trade_setup.tech_weight:.0%} / {trade_setup.news_weight:.0%}`",
                escape_markdown_text(news_signal.summary_line),
            ]
        )
        if news_signal.headlines:
            sections.extend(f"- {escape_markdown_text(_format_headline(item))}" for item in news_signal.headlines[:3])

    sections.extend(
        [
            "",
            "*交易计划*",
            f"是否值得出手: *{'是' if trade_setup.tradeable else '否'}*",
            f"方向: *{trade_setup.direction}*",
            f"入场模式: *{trade_setup.entry_mode}* | 当前状态: *{'可执行' if trade_setup.entry_ready else '继续等待'}*",
            f"当前时段门槛: `置信度>={trade_setup.session_min_confidence}% / RR>={trade_setup.session_min_risk_reward:.2f} / 综合分>={trade_setup.session_min_composite_score}`",
        ]
    )

    if trade_setup.entry_zone_low is not None and trade_setup.entry_zone_high is not None:
        sections.append(f"关注区间: `${trade_setup.entry_zone_low:,.2f} - {trade_setup.entry_zone_high:,.2f}`")
    sections.append(escape_markdown_text(trade_setup.execution_note))

    if trade_setup.confirmation_items:
        sections.append("5m 确认条件:")
        sections.extend(f"- {escape_markdown_text(item)}" for item in trade_setup.confirmation_items)

    if trade_setup.entry is not None:
        sections.extend(
            [
                f"计划入场参考: `${trade_setup.entry:,.2f}`",
                f"止损: `${trade_setup.stop_loss:,.2f}` | 止盈 1: `${trade_setup.take_profit_1:,.2f}` | 止盈 2: `${trade_setup.take_profit_2:,.2f}`",
                f"第一目标盈亏比: `{trade_setup.risk_reward_1:.2f}`",
            ]
        )

    sections.extend(f"- {escape_markdown_text(item)}" for item in trade_setup.reasons)
    sections.append(f"失效条件: {escape_markdown_text(trade_setup.invalidation)}")

    if ai_summary:
        sections.extend(["", "*AI 快读*", escape_markdown_text(ai_summary.strip())])

    sections.extend(["", "*风险提醒*", warning_block, "", f"失效时间: `{result.expires_at}`"])
    return "\n".join(sections)


def _resolve_weights(config: AppConfig, session_code: str, news_score: int, breaking_news: bool) -> tuple[float, float]:
    if breaking_news or abs(news_score) >= config.high_impact_news_score:
        return 0.40, 0.60
    if session_code == "europe":
        return (0.78, 0.22) if abs(news_score) <= config.low_impact_news_score else (0.62, 0.38)
    if session_code == "us":
        return (0.70, 0.30) if abs(news_score) <= config.low_impact_news_score else (0.55, 0.45)
    return (0.75, 0.25) if abs(news_score) <= config.low_impact_news_score else (0.60, 0.40)


def _resolve_trade_thresholds(config: AppConfig, session_code: str) -> TradeThresholds:
    if session_code == "europe":
        return TradeThresholds(
            min_confidence=config.europe_min_confidence_for_trade,
            min_risk_reward=config.europe_min_risk_reward,
            min_composite_score=config.europe_min_composite_score_for_trade,
            max_stop_pct=0.0048,
        )
    if session_code == "us":
        return TradeThresholds(
            min_confidence=config.us_min_confidence_for_trade,
            min_risk_reward=config.us_min_risk_reward,
            min_composite_score=config.us_min_composite_score_for_trade,
            max_stop_pct=0.0065,
        )
    return TradeThresholds(
        min_confidence=config.min_confidence_for_trade,
        min_risk_reward=config.min_risk_reward,
        min_composite_score=config.min_composite_score_for_trade,
        max_stop_pct=0.0055,
    )


def _build_intraday_stop(entry: float, result: AnalysisResult, direction: str, max_stop_pct: float) -> tuple[float, float]:
    buffer = max(result.atr_5m * 0.18, result.atr_15m * 0.10, entry * 0.0007)
    min_risk = max(result.atr_5m * 0.35, entry * 0.0009)
    max_risk = max(entry * max_stop_pct, result.atr_15m * 0.70)

    if direction == "做多":
        raw_stop = min(result.support_5m, result.support_15m) - buffer
        raw_risk = entry - raw_stop
        risk = clamp(raw_risk, min_risk, max_risk)
        stop_loss = entry - risk
    else:
        raw_stop = max(result.resistance_5m, result.resistance_15m) + buffer
        raw_risk = raw_stop - entry
        risk = clamp(raw_risk, min_risk, max_risk)
        stop_loss = entry + risk

    return stop_loss, risk


def _build_intraday_targets(
    entry: float,
    risk: float,
    result: AnalysisResult,
    direction: str,
    min_risk_reward: float,
) -> tuple[float, float]:
    if direction == "做多":
        candidates = sorted({level for level in (result.resistance_5m, result.resistance_15m, result.resistance_1h) if level > entry})
        natural_1 = candidates[0] if candidates else entry + risk * min_risk_reward
        natural_2 = candidates[1] if len(candidates) > 1 else natural_1 + risk * 0.8
        target_1 = max(natural_1, entry + risk * min_risk_reward)
        target_2 = max(natural_2, entry + risk * (min_risk_reward + 0.8), target_1 + risk * 0.6)
    else:
        candidates = sorted({level for level in (result.support_5m, result.support_15m, result.support_1h) if level < entry}, reverse=True)
        natural_1 = candidates[0] if candidates else entry - risk * min_risk_reward
        natural_2 = candidates[1] if len(candidates) > 1 else natural_1 - risk * 0.8
        target_1 = min(natural_1, entry - risk * min_risk_reward)
        target_2 = min(natural_2, entry - risk * (min_risk_reward + 0.8), target_1 - risk * 0.6)

    return target_1, target_2


def _build_entry_plan(result: AnalysisResult, direction: str) -> EntryPlan:
    pullback_pad = max(result.atr_15m * 0.25, result.current_price * 0.0010)
    breakout_pad = max(result.atr_5m * 0.35, result.current_price * 0.0008)

    if direction == "做多":
        if result.breakout_state == "up" and result.intraday_bias == "偏多":
            zone_low = result.breakout_reference_high - breakout_pad
            zone_high = result.breakout_reference_high + breakout_pad
            ready = zone_low <= result.current_price <= zone_high and result.ema_fast_5m >= result.ema_slow_5m and result.taker_ratio >= 1.0
            note = "15m 已经先突破，优先等回踩突破位后的 5m 再次转强。"
            confirmations = [
                "价格回踩后仍守住 15m 突破位",
                "5m EMA9 >= EMA21",
                "Taker Ratio >= 1.00",
                "5m 收盘重新站回突破位上方",
            ]
            entry_reference = result.current_price if ready else (zone_low + zone_high) / 2
            return EntryPlan("突破回踩入场", ready, entry_reference, zone_low, zone_high, note, confirmations)

        zone_low = result.support_15m - pullback_pad
        zone_high = result.support_15m + pullback_pad
        ready = zone_low <= result.current_price <= zone_high and result.ema_fast_5m >= result.ema_slow_5m and result.taker_ratio >= 1.0 and result.entry_score >= 0
        note = "优先等价格回踩 15m 支撑带，再看 5m 能不能守住并重新转强。"
        confirmations = [
            "价格进入 15m 支撑带附近",
            "5m EMA9 >= EMA21",
            "5m 不再创新低，最好出现扫低回收",
            "Taker Ratio >= 1.00",
        ]
        entry_reference = result.current_price if ready else (zone_low + zone_high) / 2
        return EntryPlan("回踩入场", ready, entry_reference, zone_low, zone_high, note, confirmations)

    if direction == "做空":
        if result.breakout_state == "down" and result.intraday_bias == "偏空":
            zone_low = result.breakout_reference_low - breakout_pad
            zone_high = result.breakout_reference_low + breakout_pad
            ready = zone_low <= result.current_price <= zone_high and result.ema_fast_5m <= result.ema_slow_5m and result.taker_ratio <= 1.0
            note = "15m 已经先破位，优先等回抽破位点后的 5m 再次转弱。"
            confirmations = [
                "价格回抽后仍压在 15m 破位点下方",
                "5m EMA9 <= EMA21",
                "Taker Ratio <= 1.00",
                "5m 收盘重新跌回破位点下方",
            ]
            entry_reference = result.current_price if ready else (zone_low + zone_high) / 2
            return EntryPlan("突破回踩入场", ready, entry_reference, zone_low, zone_high, note, confirmations)

        zone_low = result.resistance_15m - pullback_pad
        zone_high = result.resistance_15m + pullback_pad
        ready = zone_low <= result.current_price <= zone_high and result.ema_fast_5m <= result.ema_slow_5m and result.taker_ratio <= 1.0 and result.entry_score <= 0
        note = "优先等价格回抽 15m 阻力带，再看 5m 能不能重新转弱。"
        confirmations = [
            "价格进入 15m 阻力带附近",
            "5m EMA9 <= EMA21",
            "5m 不再创新高，最好出现扫高回落",
            "Taker Ratio <= 1.00",
        ]
        entry_reference = result.current_price if ready else (zone_low + zone_high) / 2
        return EntryPlan("回踩入场", ready, entry_reference, zone_low, zone_high, note, confirmations)

    return EntryPlan("等待方向", False, None, None, None, "先等 15m 给出更清晰的日内方向。", [])


def _nearest_levels(price: float, bars: list[Kline], lookback: int) -> tuple[float, float]:
    recent_bars = bars[-max(lookback * 4, lookback + 5):]
    lows = [bar.low_price for bar in recent_bars[-lookback:]]
    highs = [bar.high_price for bar in recent_bars[-lookback:]]
    supports, resistances = _collect_levels(recent_bars, min(lows), max(highs))
    support = _nearest_below(price, supports, fallback=min(lows))
    resistance = _nearest_above(price, resistances, fallback=max(highs))
    return support, resistance


def _resolve_session_profile(config: AppConfig, now_local: datetime) -> SessionProfile:
    if config.enable_europe_session:
        active, start_time = _is_in_window(now_local, config.europe_session_start_hour, config.europe_session_end_hour)
        if active:
            return SessionProfile(
                code="europe",
                label="欧盘",
                window=f"欧盘 | {config.europe_session_start_hour:02d}:00-{config.europe_session_end_hour:02d}:00",
                style="先看回踩位置，再看 5m 确认，不追远离支撑阻力的单。",
                active=True,
                start_time=start_time,
            )

    if config.enable_us_session:
        active, start_time = _is_in_window(now_local, config.us_session_start_hour, config.us_session_end_hour)
        if active:
            return SessionProfile(
                code="us",
                label="美盘",
                window=f"美盘 | {config.us_session_start_hour:02d}:00-{config.us_session_end_hour:02d}:00",
                style="更接受突破和扩张，但止损止盈仍按日内结构计算。",
                active=True,
                start_time=start_time,
            )

    return SessionProfile(
        code="offhours",
        label="场外观察",
        window="场外观察 | 非交易时段",
        style="只观察风险和方向，不主动给正式开仓位。",
        active=False,
        start_time=now_local - timedelta(hours=6),
    )


def _collect_levels(bars: list[Kline], fallback_low: float, fallback_high: float) -> tuple[list[float], list[float]]:
    supports = [fallback_low]
    resistances = [fallback_high]
    if len(bars) < 5:
        return supports, resistances

    for index in range(2, len(bars) - 2):
        low = bars[index].low_price
        high = bars[index].high_price
        if low <= min(bars[index - 2].low_price, bars[index - 1].low_price, bars[index + 1].low_price, bars[index + 2].low_price):
            supports.append(low)
        if high >= max(bars[index - 2].high_price, bars[index - 1].high_price, bars[index + 1].high_price, bars[index + 2].high_price):
            resistances.append(high)

    return sorted(set(round(level, 2) for level in supports)), sorted(set(round(level, 2) for level in resistances))


def _nearest_below(price: float, levels: list[float], fallback: float) -> float:
    below = [level for level in levels if level < price]
    return max(below) if below else fallback


def _nearest_above(price: float, levels: list[float], fallback: float) -> float:
    above = [level for level in levels if level > price]
    return min(above) if above else fallback


def _detect_liquidity_sweep(latest: Kline, recent_high: float, recent_low: float) -> str:
    if latest.high_price > recent_high and latest.close_price < recent_high:
        return "upper_sweep"
    if latest.low_price < recent_low and latest.close_price > recent_low:
        return "lower_sweep"
    return "none"


def _liquidation_proxy(long_short_ratio: float, funding_rate: float, oi_change_30m_pct: float) -> str:
    if long_short_ratio > 1.8 and funding_rate > 0 and oi_change_30m_pct > 0.5:
        return "多头拥挤"
    if long_short_ratio < 0.7 and funding_rate < 0 and oi_change_30m_pct > 0.5:
        return "空头拥挤"
    return "中性"


def _is_in_window(now_local: datetime, start_hour: int, end_hour: int) -> tuple[bool, datetime]:
    if start_hour < end_hour:
        start = now_local.replace(hour=start_hour, minute=0, second=0, microsecond=0)
        end = now_local.replace(hour=end_hour, minute=0, second=0, microsecond=0)
        return start <= now_local < end, start

    start = now_local.replace(hour=start_hour, minute=0, second=0, microsecond=0)
    if now_local.hour < end_hour:
        start = start - timedelta(days=1)
    end = start + timedelta(days=1)
    end = end.replace(hour=end_hour, minute=0, second=0, microsecond=0)
    return start <= now_local < end, start


def _near_level(price: float, level: float, tolerance: float) -> bool:
    return abs(price - level) <= tolerance


def _sign(value: float) -> int:
    if value > 0:
        return 1
    if value < 0:
        return -1
    return 0


def _bias_from_score(score: int, threshold: int) -> str:
    if score >= threshold:
        return "偏多"
    if score <= -threshold:
        return "偏空"
    return "中性"


def _setup_type_from_biases(intraday_bias: str, entry_bias: str, price: float, support_15m: float, resistance_15m: float) -> str:
    if intraday_bias == "偏多" and entry_bias == "偏多":
        return "顺势做多"
    if intraday_bias == "偏空" and entry_bias == "偏空":
        return "顺势做空"
    if abs(price - support_15m) < abs(price - resistance_15m):
        return "支撑位反应"
    if abs(price - resistance_15m) < abs(price - support_15m):
        return "阻力位反应"
    return "等待确认"


def _pct_change(old_value: float, new_value: float) -> float:
    if old_value == 0:
        return 0.0
    return ((new_value / old_value) - 1) * 100


def _float_from_hist(item: dict, *keys: str) -> float:
    for key in keys:
        if key in item:
            return float(item[key])
    raise KeyError(f"None of the expected keys were found: {keys}")


def _format_headline(item) -> str:
    return f"[{item.age_minutes}m] {item.source}: {item.title}"
