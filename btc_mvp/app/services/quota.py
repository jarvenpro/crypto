from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from app.storage.state import JsonStateStore


@dataclass(slots=True)
class ProviderBudget:
    minute_limit: int
    day_limit: int


class QuotaManager:
    def __init__(self, store: JsonStateStore) -> None:
        self.store = store

    def consume(self, provider_name: str, budget: ProviderBudget, cost: int = 1) -> None:
        state = self.store.load()
        quota = state.setdefault("quota", {})
        provider = quota.setdefault(provider_name, {})

        now = datetime.now(timezone.utc)
        current_day = now.strftime("%Y-%m-%d")
        current_minute = now.strftime("%Y-%m-%dT%H:%M")

        if provider.get("day") != current_day:
            provider["day"] = current_day
            provider["day_count"] = 0

        if provider.get("minute") != current_minute:
            provider["minute"] = current_minute
            provider["minute_count"] = 0

        if provider["day_count"] + cost > budget.day_limit:
            raise RuntimeError(f"{provider_name} exceeded self-imposed daily limit")
        if provider["minute_count"] + cost > budget.minute_limit:
            raise RuntimeError(f"{provider_name} exceeded self-imposed per-minute limit")

        provider["day_count"] += cost
        provider["minute_count"] += cost
        self.store.save(state)
