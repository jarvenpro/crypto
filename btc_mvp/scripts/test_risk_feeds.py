from __future__ import annotations

from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE_DIR))

from app.clients.liquidation import BinanceLiquidationClient
from app.config import load_config
from app.services.analysis import summarize_liquidations
from app.services.macro import MacroService
from app.storage.state import JsonStateStore


def main() -> int:
    config = load_config()

    print("Liquidation sample:")
    liq_client = BinanceLiquidationClient()
    events = liq_client.sample_force_orders(config.symbol, config.liquidation_sample_seconds)
    snapshot = summarize_liquidations(events, config.liquidation_sample_seconds)
    print(snapshot.to_dict())

    print("\nMacro window:")
    macro_service = MacroService(config, JsonStateStore(config.data_dir / "macro_cache.json"))
    print(macro_service.get_window().to_dict())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
