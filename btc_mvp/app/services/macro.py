from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone

from app.clients.fmp import FmpClient
from app.config import AppConfig
from app.storage.state import JsonStateStore


US_TERMS = {
    "",
    "us",
    "usa",
    "united states",
    "usd",
    "america",
}

MAJOR_MACRO_TERMS = US_TERMS | {
    "eur",
    "eurozone",
    "european union",
    "eu",
    "ecb",
    "gbp",
    "uk",
    "united kingdom",
    "boe",
    "jpy",
    "japan",
    "boj",
    "cny",
    "cnh",
    "china",
}


@dataclass(slots=True)
class MacroEvent:
    event_name: str
    country: str
    importance: str
    event_time: str
    minutes_to_event: int
    event_type: str = "other"
    event_type_label: str = "其他"
    severity: str = "medium"
    severity_label: str = "中"
    severity_score: int = 3
    block_mode: str = "warning_only"
    pre_block_minutes: int = 20
    post_block_minutes: int = 20
    warning_window_minutes: int = 180
    actual: str | None = None
    forecast: str | None = None
    previous: str | None = None

    @property
    def block_active(self) -> bool:
        return self.block_mode == "hard_block" and -self.post_block_minutes <= self.minutes_to_event <= self.pre_block_minutes

    @property
    def warning_active(self) -> bool:
        return -self.post_block_minutes <= self.minutes_to_event <= self.warning_window_minutes

    def to_dict(self) -> dict:
        payload = asdict(self)
        payload["block_active"] = self.block_active
        payload["warning_active"] = self.warning_active
        return payload


@dataclass(slots=True)
class MacroWindow:
    enabled: bool
    active_block: bool
    pre_event_warning: bool
    summary: str
    upcoming: list[MacroEvent]
    block_mode: str = "disabled"
    recommended_action: str = "宏观过滤未启用。"
    current_event_type: str = ""
    current_severity: str = ""

    def to_dict(self) -> dict:
        return {
            "enabled": self.enabled,
            "active_block": self.active_block,
            "pre_event_warning": self.pre_event_warning,
            "summary": self.summary,
            "block_mode": self.block_mode,
            "recommended_action": self.recommended_action,
            "current_event_type": self.current_event_type,
            "current_severity": self.current_severity,
            "upcoming": [item.to_dict() for item in self.upcoming],
        }


@dataclass(slots=True)
class _EventProfile:
    event_type: str
    event_type_label: str
    severity: str
    severity_label: str
    severity_score: int
    block_mode: str
    pre_block_minutes: int
    post_block_minutes: int
    warning_window_minutes: int


@dataclass(slots=True)
class _WindowState:
    mode: str
    summary: str
    recommended_action: str
    current_event_type: str
    current_severity: str


class MacroService:
    def __init__(self, config: AppConfig, cache_store: JsonStateStore) -> None:
        self.config = config
        self.cache_store = cache_store
        self.client = FmpClient(config.fmp_api_key)

    def get_window(self) -> MacroWindow:
        if not self.config.fmp_enabled:
            return MacroWindow(False, False, False, "宏观事件过滤未启用。", [])

        now = datetime.now(timezone.utc)
        raw_events = self._load_or_fetch(now.date())

        selected: list[MacroEvent] = []
        for item in raw_events:
            event = _parse_macro_event(item, now, self.config)
            if event is None:
                continue
            if abs(event.minutes_to_event) <= event.warning_window_minutes:
                selected.append(event)

        if not selected:
            return MacroWindow(
                True,
                False,
                False,
                "未来几小时内没有命中的高影响宏观窗口。",
                [],
                block_mode="clear",
                recommended_action="宏观层暂时不额外干扰，继续按技术面和消息面筛选位置。",
            )

        selected.sort(key=_event_sort_key)
        active_blocks = [item for item in selected if item.block_active]
        warning_events = [item for item in selected if item.warning_active]

        state = _build_window_state(active_blocks, warning_events)
        return MacroWindow(
            True,
            bool(active_blocks),
            bool(warning_events),
            state.summary,
            selected[:3],
            block_mode=state.mode,
            recommended_action=state.recommended_action,
            current_event_type=state.current_event_type,
            current_severity=state.current_severity,
        )

    def _load_or_fetch(self, current_date) -> list[dict]:
        cache = self.cache_store.load()
        fetched_at_text = cache.get("fetched_at")
        items_payload = cache.get("items", [])

        if fetched_at_text:
            fetched_at = datetime.fromisoformat(fetched_at_text)
            age_seconds = (datetime.now(timezone.utc) - fetched_at).total_seconds()
            if age_seconds < self.config.macro_cache_seconds and items_payload:
                return items_payload

        date_from = current_date.isoformat()
        date_to = (current_date + timedelta(days=1)).isoformat()
        items = self.client.get_economic_calendar(date_from, date_to)
        self.cache_store.save(
            {
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "items": items,
            }
        )
        return items


def _parse_macro_event(item: dict, now: datetime, config: AppConfig) -> MacroEvent | None:
    name = str(item.get("event") or item.get("name") or item.get("title") or "").strip()
    country = str(item.get("country") or item.get("currency") or "").strip()
    importance = str(item.get("impact") or item.get("importance") or item.get("importanceLevel") or "").strip()
    date_text = str(item.get("date") or item.get("datetime") or item.get("releaseDate") or "").strip()

    if not name or not date_text:
        return None

    parsed_time = _parse_event_time(date_text)
    if parsed_time is None:
        return None

    profile = _classify_event(name, country, importance, config)
    if profile is None:
        return None

    minutes_to_event = int((parsed_time - now).total_seconds() // 60)
    return MacroEvent(
        event_name=name,
        country=country or "N/A",
        importance=importance or "Unknown",
        event_time=parsed_time.isoformat(),
        minutes_to_event=minutes_to_event,
        event_type=profile.event_type,
        event_type_label=profile.event_type_label,
        severity=profile.severity,
        severity_label=profile.severity_label,
        severity_score=profile.severity_score,
        block_mode=profile.block_mode,
        pre_block_minutes=profile.pre_block_minutes,
        post_block_minutes=profile.post_block_minutes,
        warning_window_minutes=profile.warning_window_minutes,
        actual=_clean_value(item.get("actual") or item.get("actualValue")),
        forecast=_clean_value(item.get("estimate") or item.get("forecast") or item.get("consensus")),
        previous=_clean_value(item.get("previous") or item.get("previousValue")),
    )


def _classify_event(name: str, country: str, importance: str, config: AppConfig) -> _EventProfile | None:
    text = f"{name} {importance}".lower()
    country_text = country.lower()

    if _contains_any(text, ("powell", "jerome powell", "fed chair")):
        return _profile(config, "powell", "Powell 讲话", "critical", "hard_block")

    if _contains_any(text, ("fomc", "federal funds", "fed rate", "interest rate decision", "rate decision")):
        if _is_major_country(country_text) or "fomc" in text or "federal funds" in text:
            return _profile(config, "rate_decision", "利率决议 / FOMC", "critical", "hard_block")

    if _contains_any(text, ("cpi", "consumer price index", "pce", "core pce", "ppi", "inflation")):
        if _is_us_or_unspecified(country_text):
            return _profile(config, "inflation", "通胀数据", "critical", "hard_block")

    if _contains_any(text, ("non farm", "non-farm", "nonfarm", "payroll", "employment situation", "adp employment", "unemployment rate")):
        if _is_us_or_unspecified(country_text):
            return _profile(config, "labor_major", "就业数据", "critical", "hard_block")

    if _contains_any(text, ("jobless claims", "initial claims", "continuing claims")):
        if _is_us_or_unspecified(country_text):
            return _profile(config, "claims", "失业金数据", "medium", "warning_only")

    if _contains_any(text, ("gdp", "gross domestic product", "retail sales", "ism", "consumer confidence", "durable goods", "pmi")):
        if _is_major_country(country_text):
            return _profile(config, "growth", "增长景气数据", "high", "warning_only")

    if _contains_any(text, ("treasury", "bond auction", "10-year note", "30-year bond", "2-year note")):
        if _is_us_or_unspecified(country_text):
            return _profile(config, "treasury", "美债相关事件", "medium", "warning_only")

    if "high" in text and _is_major_country(country_text):
        return _profile(config, "high_impact_other", "高影响宏观事件", "high", "warning_only")

    return None


def _profile(config: AppConfig, event_type: str, label: str, severity: str, block_mode: str) -> _EventProfile:
    if severity == "critical":
        return _EventProfile(
            event_type=event_type,
            event_type_label=label,
            severity=severity,
            severity_label="极高",
            severity_score=5,
            block_mode=block_mode,
            pre_block_minutes=config.macro_critical_pre_block_minutes,
            post_block_minutes=config.macro_critical_post_block_minutes,
            warning_window_minutes=config.macro_critical_warning_window_minutes,
        )
    if severity == "high":
        return _EventProfile(
            event_type=event_type,
            event_type_label=label,
            severity=severity,
            severity_label="高",
            severity_score=4,
            block_mode=block_mode,
            pre_block_minutes=config.macro_high_pre_block_minutes,
            post_block_minutes=config.macro_high_post_block_minutes,
            warning_window_minutes=config.macro_high_warning_window_minutes,
        )
    return _EventProfile(
        event_type=event_type,
        event_type_label=label,
        severity=severity,
        severity_label="中",
        severity_score=3,
        block_mode=block_mode,
        pre_block_minutes=config.macro_medium_pre_block_minutes,
        post_block_minutes=config.macro_medium_post_block_minutes,
        warning_window_minutes=config.macro_medium_warning_window_minutes,
    )


def _build_window_state(active_blocks: list[MacroEvent], warning_events: list[MacroEvent]) -> _WindowState:
    if active_blocks:
        anchor = active_blocks[0]
        return _WindowState(
            mode="hard_block",
            summary=(
                f"宏观硬风控生效: {anchor.event_type_label} | "
                f"{anchor.event_name} | {anchor.severity_label}影响 | {_time_phrase(anchor.minutes_to_event)}。"
            ),
            recommended_action="当前禁止开新仓，等首轮数据波动和扫单结束后再重新评估。",
            current_event_type=anchor.event_type,
            current_severity=anchor.severity,
        )

    if warning_events:
        anchor = warning_events[0]
        return _WindowState(
            mode="warning_only",
            summary=(
                f"临近宏观观察窗口: {anchor.event_type_label} | "
                f"{anchor.event_name} | {anchor.severity_label}影响 | {_time_phrase(anchor.minutes_to_event)}。"
            ),
            recommended_action="只看更高盈亏比的位置，轻仓快进快出，不在数据前后追价。",
            current_event_type=anchor.event_type,
            current_severity=anchor.severity,
        )

    return _WindowState(
        mode="clear",
        summary="未来几小时内没有命中的高影响宏观窗口。",
        recommended_action="宏观层暂时不额外干扰，继续按技术面和消息面筛选位置。",
        current_event_type="",
        current_severity="",
    )


def _event_sort_key(event: MacroEvent) -> tuple[int, int, int]:
    priority = 0 if event.block_active else 1 if event.warning_active else 2
    return (priority, abs(event.minutes_to_event), -event.severity_score)


def _contains_any(text: str, keywords: tuple[str, ...]) -> bool:
    return any(keyword in text for keyword in keywords)


def _is_us_or_unspecified(country_text: str) -> bool:
    return not country_text or country_text in US_TERMS


def _is_major_country(country_text: str) -> bool:
    return not country_text or country_text in MAJOR_MACRO_TERMS


def _time_phrase(minutes_to_event: int) -> str:
    if minutes_to_event > 0:
        return f"{minutes_to_event} 分钟后公布"
    if minutes_to_event < 0:
        return f"公布后 {abs(minutes_to_event)} 分钟"
    return "正在公布"


def _clean_value(value) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _parse_event_time(date_text: str) -> datetime | None:
    normalized = date_text.replace("Z", "+00:00")
    for candidate in (normalized, normalized.replace(" ", "T")):
        try:
            parsed = datetime.fromisoformat(candidate)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            continue
    return None
