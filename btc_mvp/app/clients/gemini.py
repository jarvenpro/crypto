from __future__ import annotations

import base64
import json
from typing import Any

from app.telegram_tools import run_powershell_json
from app.utils import http_post_json


class GeminiClient:
    def __init__(self, api_key: str, model: str) -> None:
        self.api_key = api_key
        self.model = model

    def generate_text(
        self,
        prompt: str,
        temperature: float = 0.2,
        max_output_tokens: int = 280,
    ) -> str:
        if not self.api_key:
            raise RuntimeError("GEMINI_API_KEY is missing.")

        url = f"https://generativelanguage.googleapis.com/v1beta/models/{self.model}:generateContent?key={self.api_key}"
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": temperature,
                "maxOutputTokens": max_output_tokens,
                "thinkingConfig": {
                    "thinkingBudget": 0,
                },
            },
        }

        try:
            response = http_post_json(url, payload, timeout=30)
        except Exception:
            response = self._generate_via_powershell(url, payload)

        return self._extract_text(response)

    def generate_json(
        self,
        prompt: str,
        schema: dict[str, Any],
        temperature: float = 0.1,
        max_output_tokens: int = 400,
    ) -> dict[str, Any]:
        if not self.api_key:
            raise RuntimeError("GEMINI_API_KEY is missing.")

        url = f"https://generativelanguage.googleapis.com/v1beta/models/{self.model}:generateContent?key={self.api_key}"
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": temperature,
                "maxOutputTokens": max_output_tokens,
                "responseMimeType": "application/json",
                "responseJsonSchema": schema,
                "thinkingConfig": {
                    "thinkingBudget": 0,
                },
            },
        }

        try:
            response = http_post_json(url, payload, timeout=30)
        except Exception:
            response = self._generate_via_powershell(url, payload)

        text = self._extract_text(response)
        return self._parse_json_text(text)

    @staticmethod
    def _extract_text(response: dict[str, Any]) -> str:
        candidates = response.get("candidates", [])
        if not candidates:
            raise RuntimeError(f"Gemini returned no candidates: {response}")

        parts = candidates[0].get("content", {}).get("parts", [])
        text = "".join(str(part.get("text", "")) for part in parts).strip()
        if not text:
            raise RuntimeError(f"Gemini returned an empty response: {response}")
        return text

    @staticmethod
    def _parse_json_text(text: str) -> dict[str, Any]:
        candidate = text.strip()
        if candidate.startswith("```"):
            lines = [line for line in candidate.splitlines() if not line.strip().startswith("```")]
            candidate = "\n".join(lines).strip()

        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Gemini returned invalid JSON: {text}") from exc

        if not isinstance(parsed, dict):
            raise RuntimeError(f"Gemini JSON output must be an object: {parsed}")
        return parsed

    @staticmethod
    def _generate_via_powershell(url: str, payload: dict) -> dict:
        body_json = json.dumps(payload, ensure_ascii=False)
        body_base64 = base64.b64encode(body_json.encode("utf-8")).decode("ascii")
        script = (
            "[Console]::InputEncoding = [System.Text.Encoding]::UTF8; "
            "[Console]::OutputEncoding = [System.Text.Encoding]::UTF8; "
            f"$payload = [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String('{body_base64}')); "
            f"$resp = Invoke-RestMethod -Method Post -Uri '{url}' -ContentType 'application/json; charset=utf-8' -Body $payload; "
            "$resp | ConvertTo-Json -Depth 20 -Compress"
        )
        return run_powershell_json(script, timeout=40)
