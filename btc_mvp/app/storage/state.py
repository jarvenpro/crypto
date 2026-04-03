from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any


class JsonStateStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {}
        try:
            return json.loads(self.path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            broken_path = self.path.with_suffix(".broken.json")
            self.path.replace(broken_path)
            return {}

    def save(self, data: dict[str, Any]) -> None:
        self.path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def append_jsonl(self, payload: dict[str, Any], target: Path) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(payload, ensure_ascii=False)
        with target.open("a", encoding="utf-8") as handle:
            handle.write(f"{line}\n")


def iso_utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
