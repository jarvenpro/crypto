from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from app.clients.cryptocompare import CryptoCompareClient, RawNewsItem
from app.clients.gdelt import GdeltClient
from app.config import AppConfig
from app.storage.state import JsonStateStore
from app.utils import clamp


BTC_TITLE_TERMS = {"bitcoin", "btc", "etf", "blackrock", "grayscale", "miner", "mining", "hashrate", "mempool", "whale"}
MACRO_TITLE_TERMS = {
    "trump",
    "fed",
    "fomc",
    "cpi",
    "pce",
    "nfp",
    "payroll",
    "powell",
    "rates",
    "tariff",
    "war",
    "ceasefire",
    "sec",
}

MARKET_CONTEXT_TERMS = {
    "market",
    "markets",
    "usd",
    "dollar",
    "yields",
    "yield",
    "treasury",
    "stock",
    "stocks",
    "equity",
    "equities",
    "currency",
    "currencies",
    "risk",
    "asset",
    "assets",
    "liquidity",
    "investor",
    "investors",
    "oil",
    "gold",
}

BTC_KEYWORDS = {
    "bitcoin": 8,
    "btc": 8,
    "etf": 6,
    "blackrock": 5,
    "grayscale": 4,
    "fed": 4,
    "fomc": 4,
    "cpi": 4,
    "pce": 4,
    "nfp": 4,
    "payroll": 4,
    "powell": 4,
    "tariff": 4,
    "war": 5,
    "trump": 5,
    "whale": 5,
    "miner": 5,
    "mining": 5,
    "hashrate": 5,
    "mempool": 5,
    "liquidation": 4,
    "inflow": 4,
    "outflow": 4,
    "treasury": 3,
    "strategy": 3,
    "microstrategy": 4,
}

POSITIVE_KEYWORDS = {
    "approval": 5,
    "adoption": 4,
    "buy": 2,
    "accumulation": 4,
    "inflow": 3,
    "launch": 2,
    "reserve": 3,
    "bullish": 3,
    "partnership": 2,
    "surge": 2,
    "ceasefire": 3,
    "cut": 2,
}

NEGATIVE_KEYWORDS = {
    "hack": -5,
    "exploit": -5,
    "lawsuit": -4,
    "ban": -4,
    "outflow": -3,
    "sell": -2,
    "dump": -4,
    "liquidation": -3,
    "bearish": -3,
    "tariff": -4,
    "war": -5,
    "recession": -4,
    "plunge": -3,
    "falls below": -2,
    "miss": -2,
    "hotter-than-expected": -3,
}

EVENT_TYPE_LABELS = {
    "btc_direct": "BTC 直接事件",
    "macro": "宏观",
    "etf": "ETF",
    "miner": "矿工",
    "whale": "巨鲸",
    "geopolitics": "地缘",
    "exchange": "交易所",
    "security": "安全事件",
    "policy": "政策监管",
    "other": "其他",
    "none": "无明显主题",
}

DIRECTIONAL_BIAS_LABELS = {
    "bullish": "利多",
    "bearish": "利空",
    "neutral": "中性",
    "two_way": "双向波动",
}


@dataclass(slots=True)
class NewsItem:
    item_id: str
    title: str
    url: str
    source: str
    published_at: str
    age_minutes: int
    relevance_score: int
    impact_score: int
    tags: list[str]

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(slots=True)
class NewsSignal:
    score: int
    relevant_count: int
    fresh_count: int
    breaking: bool
    summary_line: str
    headlines: list[NewsItem]
    delivered_ids: list[str]

    def to_dict(self) -> dict:
        return {
            "score": self.score,
            "relevant_count": self.relevant_count,
            "fresh_count": self.fresh_count,
            "breaking": self.breaking,
            "summary_line": self.summary_line,
            "headlines": [item.to_dict() for item in self.headlines],
            "delivered_ids": self.delivered_ids,
        }


@dataclass(slots=True)
class AiMarketSummary:
    position_conclusion: str
    news_impact: str
    risk_warning: str
    counter_case: str
    event_type: str
    directional_bias: str
    severity: int
    half_life_minutes: int
    no_trade_risk: bool

    def to_dict(self) -> dict:
        return asdict(self)


class NewsService:
    def __init__(self, config: AppConfig, cache_store: JsonStateStore) -> None:
        self.config = config
        self.cache_store = cache_store
        self.client = CryptoCompareClient(config.cryptocompare_api_key)
        self.gdelt_client = GdeltClient()

    def fetch_signal(self, delivered_ids: set[str]) -> NewsSignal:
        if not self.config.cryptocompare_enabled and not self.config.gdelt_enabled:
            return NewsSignal(0, 0, 0, False, "消息面未启用。", [], [])

        raw_items = self._load_or_fetch_items()
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(hours=self.config.news_max_age_hours)

        relevant_items: list[NewsItem] = []
        for raw_item in raw_items:
            if raw_item.published_at < cutoff:
                continue
            scored = _score_news_item(raw_item, now, self.config)
            if scored is not None:
                relevant_items.append(scored)

        relevant_items.sort(key=lambda item: (abs(item.impact_score), item.relevance_score, -item.age_minutes), reverse=True)
        top_items = relevant_items[: self.config.max_news_items_for_prompt]
        fresh_items = [item for item in top_items if item.item_id not in delivered_ids]

        raw_score = sum(item.impact_score for item in top_items)
        news_score = int(clamp(raw_score, -30, 30))
        breaking = any(item.age_minutes <= 90 and abs(item.impact_score) >= self.config.high_impact_news_score for item in fresh_items)

        distinct_sources = len({item.source.lower() for item in top_items})
        if not top_items:
            summary = f"最近 {self.config.news_max_age_hours} 小时没有抓到足够高优先级的消息。"
        elif news_score >= 10:
            summary = f"消息面偏多，最近 {self.config.news_max_age_hours} 小时抓到 {len(top_items)} 条高影响消息，来自 {distinct_sources} 个源。"
        elif news_score <= -10:
            summary = f"消息面偏空，最近 {self.config.news_max_age_hours} 小时抓到 {len(top_items)} 条高影响消息，来自 {distinct_sources} 个源。"
        else:
            summary = f"消息面偏中性，最近 {self.config.news_max_age_hours} 小时抓到 {len(top_items)} 条可跟踪消息，来自 {distinct_sources} 个源。"

        return NewsSignal(
            score=news_score,
            relevant_count=len(top_items),
            fresh_count=len(fresh_items),
            breaking=breaking,
            summary_line=summary,
            headlines=top_items,
            delivered_ids=[item.item_id for item in fresh_items],
        )

    def _load_or_fetch_items(self) -> list[RawNewsItem]:
        cache = self.cache_store.load()
        items: list[RawNewsItem] = []

        cryptocompare_items = self._safe_load_source_items(
            cache=cache,
            source_name="cryptocompare",
            cache_seconds=self.config.news_cache_seconds,
            fetcher=lambda: self.client.get_latest_news(limit=self.config.news_limit),
            enabled=self.config.cryptocompare_enabled,
        )
        items.extend(cryptocompare_items)

        gdelt_items = self._safe_load_source_items(
            cache=cache,
            source_name="gdelt",
            cache_seconds=self.config.gdelt_cache_seconds,
            fetcher=lambda: self.gdelt_client.get_latest_news(
                query=self.config.gdelt_query,
                timespan_hours=self.config.gdelt_timespan_hours,
                max_records=self.config.gdelt_max_records,
            ),
            enabled=self.config.gdelt_enabled,
        )
        items.extend(gdelt_items)

        deduped = _dedupe_items(items)
        return deduped

    def _safe_load_source_items(
        self,
        cache: dict[str, Any],
        source_name: str,
        cache_seconds: int,
        fetcher,
        enabled: bool,
    ) -> list[RawNewsItem]:
        try:
            return self._load_or_fetch_source_items(
                cache=cache,
                source_name=source_name,
                cache_seconds=cache_seconds,
                fetcher=fetcher,
                enabled=enabled,
            )
        except Exception:
            return []

    def _load_or_fetch_source_items(
        self,
        cache: dict[str, Any],
        source_name: str,
        cache_seconds: int,
        fetcher,
        enabled: bool,
    ) -> list[RawNewsItem]:
        if not enabled:
            return []

        sources_cache = cache.setdefault("sources", {})
        source_cache = sources_cache.get(source_name, {})
        fetched_at_text = source_cache.get("fetched_at")
        items_payload = source_cache.get("items", [])

        if fetched_at_text and items_payload:
            fetched_at = datetime.fromisoformat(fetched_at_text)
            age_seconds = (datetime.now(timezone.utc) - fetched_at).total_seconds()
            if age_seconds < cache_seconds:
                return [_raw_item_from_dict(item) for item in items_payload]

        try:
            items = fetcher()
        except Exception:
            if items_payload:
                return [_raw_item_from_dict(item) for item in items_payload]
            raise

        sources_cache[source_name] = {
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "items": [_raw_item_to_dict(item) for item in items],
        }
        cache["sources"] = sources_cache
        self.cache_store.save(cache)
        return items


def build_ai_prompt(
    technical_result,
    news_signal: NewsSignal,
    trade_setup,
    macro_window=None,
    liquidation_snapshot=None,
) -> str:
    headline_lines = "\n".join(
        f"- {item.title} | source={item.source} | age={item.age_minutes}m | impact={item.impact_score:+d}"
        for item in news_signal.headlines[:3]
    ) or "- 暂无新增高相关消息"

    macro_line = macro_window.summary if macro_window is not None else "宏观窗口未启用"
    liquidation_line = liquidation_snapshot.summary if liquidation_snapshot is not None else "当前没有清算流样本"

    return (
        "你是 BTC 日内交易助手里的消息解释层，只允许讨论未来 30 分钟到 6 小时，绝不允许给中长期判断。\n"
        "你的任务不是替代规则引擎下结论，而是把消息面对当前日内交易位置的影响压缩成结构化结论。\n"
        "请优先识别宏观、战争、特朗普、ETF、矿工、巨鲸、交易所、安全事件，即使标题没直接写 BTC 也要判断是否会显著影响 BTC。\n"
        "如果消息本身会让短线方向失真、滑点变大、或者盈亏比变差，要把 no_trade_risk 设为 true。\n"
        "position_conclusion 要聚焦“现在是不是好位置”，不要复述原始新闻。\n"
        "news_impact 只描述消息面对未来 30m-6h 的影响。\n"
        "risk_warning 要说最需要防的风险。\n"
        "counter_case 要给出最可能打脸当前判断的条件。\n"
        "只输出 JSON，不要输出 Markdown，不要补充解释。\n\n"
        f"技术面方向={technical_result.bias}\n"
        f"技术分数={technical_result.score}\n"
        f"综合方向={trade_setup.direction}\n"
        f"综合分数={trade_setup.composite_score}\n"
        f"综合置信度={trade_setup.confidence}\n"
        f"第一目标盈亏比={trade_setup.risk_reward_1}\n"
        f"是否值得出手={trade_setup.tradeable}\n"
        f"消息分数={news_signal.score}\n"
        f"消息结论={news_signal.summary_line}\n"
        f"宏观窗口={macro_line}\n"
        f"清算流={liquidation_line}\n"
        "消息头条:\n"
        f"{headline_lines}"
    )


def build_ai_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "position_conclusion": {
                "type": "string",
                "description": "一句中文短句，少于 26 个字，聚焦当前是不是好位置。",
            },
            "news_impact": {
                "type": "string",
                "description": "一句中文短句，少于 32 个字，描述未来 30m-6h 的消息影响。",
            },
            "risk_warning": {
                "type": "string",
                "description": "一句中文短句，少于 32 个字，描述最重要风险。",
            },
            "counter_case": {
                "type": "string",
                "description": "一句中文短句，少于 32 个字，描述最可能打脸当前判断的条件。",
            },
            "event_type": {
                "type": "string",
                "enum": ["btc_direct", "macro", "etf", "miner", "whale", "geopolitics", "exchange", "security", "policy", "other", "none"],
            },
            "directional_bias": {
                "type": "string",
                "enum": ["bullish", "bearish", "neutral", "two_way"],
            },
            "severity": {
                "type": "integer",
                "minimum": 0,
                "maximum": 5,
            },
            "half_life_minutes": {
                "type": "integer",
                "minimum": 15,
                "maximum": 360,
            },
            "no_trade_risk": {
                "type": "boolean",
            },
        },
        "required": [
            "position_conclusion",
            "news_impact",
            "risk_warning",
            "counter_case",
            "event_type",
            "directional_bias",
            "severity",
            "half_life_minutes",
            "no_trade_risk",
        ],
        "additionalProperties": False,
    }


def parse_ai_summary(payload: dict[str, Any]) -> AiMarketSummary:
    return AiMarketSummary(
        position_conclusion=_limit_text(str(payload.get("position_conclusion") or "消息面暂无额外加分"), 26),
        news_impact=_limit_text(str(payload.get("news_impact") or "短线消息影响有限"), 32),
        risk_warning=_limit_text(str(payload.get("risk_warning") or "警惕突发消息改变短线结构"), 32),
        counter_case=_limit_text(str(payload.get("counter_case") or "若价格与消息方向背离要快速收手"), 32),
        event_type=_normalize_event_type(str(payload.get("event_type") or "none")),
        directional_bias=_normalize_directional_bias(str(payload.get("directional_bias") or "neutral")),
        severity=_clamp_int(payload.get("severity"), 0, 5, 2),
        half_life_minutes=_clamp_int(payload.get("half_life_minutes"), 15, 360, 90),
        no_trade_risk=bool(payload.get("no_trade_risk", False)),
    )


def render_ai_summary(summary: AiMarketSummary) -> str:
    event_label = EVENT_TYPE_LABELS.get(summary.event_type, "其他")
    bias_label = DIRECTIONAL_BIAS_LABELS.get(summary.directional_bias, "中性")
    no_trade_label = "是" if summary.no_trade_risk else "否"
    return "\n".join(
        [
            f"位置结论: {summary.position_conclusion}",
            f"消息影响: {summary.news_impact}",
            f"风险提醒: {summary.risk_warning}",
            f"反方条件: {summary.counter_case}",
            f"事件类型: {event_label} | 方向偏向: {bias_label} | 强度: {summary.severity}/5 | 半衰期: {summary.half_life_minutes}m | 不交易风险: {no_trade_label}",
        ]
    )


def should_send_news_alert(news_signal: NewsSignal, high_impact_threshold: int) -> bool:
    return news_signal.fresh_count > 0 and (news_signal.breaking or abs(news_signal.score) >= high_impact_threshold)


def mark_news_delivered(state: dict, delivered_ids: list[str]) -> None:
    if not delivered_ids:
        return

    delivered = state.setdefault("delivered_news_ids", [])
    merged = delivered + [item for item in delivered_ids if item not in delivered]
    state["delivered_news_ids"] = merged[-300:]


def _score_news_item(raw_item: RawNewsItem, now: datetime, config: AppConfig) -> NewsItem | None:
    normalized_title = _normalize_title(raw_item.title, raw_item.source)
    normalized_body = raw_item.body.strip()
    categories = raw_item.categories.strip()

    title_text = normalized_title.lower()
    body_text = normalized_body.lower()
    category_text = categories.lower()
    title_and_categories = f"{title_text} {category_text}"
    full_text = f"{title_text} {body_text} {category_text}"

    title_has_btc = any(term in title_text for term in BTC_TITLE_TERMS)
    title_has_macro = any(term in title_text for term in MACRO_TITLE_TERMS)
    title_has_market_context = any(term in title_text for term in MARKET_CONTEXT_TERMS)

    if config.news_require_explicit_btc:
        if not title_has_btc and not title_has_macro:
            return None
    elif not title_has_btc and not title_has_macro:
        return None

    if title_has_macro and not title_has_btc and not title_has_market_context:
        return None

    relevance = 0
    impact = 0
    tags: list[str] = []

    for keyword, weight in BTC_KEYWORDS.items():
        if keyword in title_and_categories:
            relevance += weight
            tags.append(keyword)

    for keyword, weight in POSITIVE_KEYWORDS.items():
        if keyword in full_text:
            impact += weight

    for keyword, weight in NEGATIVE_KEYWORDS.items():
        if keyword in full_text:
            impact += weight

    if title_has_btc:
        relevance += 5
    if title_has_macro:
        relevance += 5

    age_minutes = max(int((now - raw_item.published_at).total_seconds() // 60), 0)
    age_multiplier = 1.0 if age_minutes <= 120 else 0.75 if age_minutes <= 360 else 0.5
    relevance = int(relevance * age_multiplier)
    impact = int(impact * age_multiplier)

    if relevance < config.news_min_relevance:
        return None

    return NewsItem(
        item_id=raw_item.item_id,
        title=normalized_title,
        url=raw_item.url,
        source=raw_item.source,
        published_at=raw_item.published_at.astimezone(timezone.utc).isoformat(),
        age_minutes=age_minutes,
        relevance_score=relevance,
        impact_score=impact,
        tags=sorted(set(tags))[:6],
    )


def _normalize_title(title: str, source: str) -> str:
    cleaned = " ".join(title.split()).strip()
    if ":" not in cleaned:
        return cleaned

    prefix, remainder = cleaned.split(":", 1)
    prefix_norm = prefix.strip().lower()
    source_norm = source.strip().lower()
    if prefix_norm and (prefix_norm == source_norm or prefix_norm in source_norm or source_norm in prefix_norm):
        return remainder.strip()
    return cleaned


def _raw_item_to_dict(item: RawNewsItem) -> dict:
    return {
        "item_id": item.item_id,
        "title": item.title,
        "body": item.body,
        "url": item.url,
        "source": item.source,
        "published_at": item.published_at.astimezone(timezone.utc).isoformat(),
        "categories": item.categories,
    }


def _raw_item_from_dict(payload: dict) -> RawNewsItem:
    return RawNewsItem(
        item_id=str(payload["item_id"]),
        title=str(payload["title"]),
        body=str(payload["body"]),
        url=str(payload["url"]),
        source=str(payload["source"]),
        published_at=datetime.fromisoformat(str(payload["published_at"])),
        categories=str(payload["categories"]),
    )


def _dedupe_items(items: list[RawNewsItem]) -> list[RawNewsItem]:
    seen_keys: set[str] = set()
    deduped: list[RawNewsItem] = []
    sorted_items = sorted(items, key=lambda item: item.published_at, reverse=True)
    for item in sorted_items:
        key = _dedupe_key(item)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        deduped.append(item)
    return deduped


def _dedupe_key(item: RawNewsItem) -> str:
    if item.url:
        return item.url.rstrip("/").lower()
    return " ".join(item.title.lower().split())


def _limit_text(text: str, limit: int) -> str:
    compact = " ".join(text.split()).strip()
    if len(compact) <= limit:
        return compact
    return compact[: max(limit - 1, 1)].rstrip() + "…"


def _normalize_event_type(value: str) -> str:
    normalized = value.strip().lower()
    if normalized in EVENT_TYPE_LABELS:
        return normalized
    return "other"


def _normalize_directional_bias(value: str) -> str:
    normalized = value.strip().lower()
    if normalized in DIRECTIONAL_BIAS_LABELS:
        return normalized
    return "neutral"


def _clamp_int(value: Any, minimum: int, maximum: int, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(minimum, min(maximum, parsed))
