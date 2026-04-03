from __future__ import annotations

from app.telegram_tools import telegram_post_via_powershell
from app.utils import http_post_json


class TelegramClient:
    def __init__(self, bot_token: str, chat_id: str) -> None:
        self.bot_token = bot_token
        self.chat_id = chat_id

    def send_message(self, text: str) -> dict:
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        }
        try:
            return http_post_json(url, payload)
        except Exception as exc:
            return self._send_via_powershell(url, payload, exc)

    @staticmethod
    def _send_via_powershell(url: str, payload: dict, original_error: Exception) -> dict:
        try:
            return telegram_post_via_powershell(url, payload)
        except Exception as fallback_error:
            raise RuntimeError(
                "Telegram send failed in both Python HTTPS and PowerShell fallback. "
                f"Python error: {original_error}; PowerShell error: {fallback_error}"
            ) from original_error
