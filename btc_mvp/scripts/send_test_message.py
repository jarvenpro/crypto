from __future__ import annotations

from pathlib import Path
import os
import sys

BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE_DIR))

from app.clients.telegram import TelegramClient
from app.config import load_config


def main() -> int:
    config = load_config()
    if not config.telegram_enabled:
        print("Telegram is not configured yet. Please fill TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env.")
        return 1

    custom_text = os.getenv("TEST_MESSAGE", "").strip()
    text = custom_text or (
        "*BTC MVP 测试消息*\n"
        "如果你收到了这条消息，说明 Telegram 推送链路已经打通。\n"
        "下一步我们就可以继续接入新闻面和 Gemini。"
    )

    client = TelegramClient(config.telegram_bot_token, config.telegram_chat_id)
    response = client.send_message(text)
    print("Telegram test message sent successfully.")
    print(response)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
