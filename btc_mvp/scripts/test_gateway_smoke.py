from __future__ import annotations

from pathlib import Path
import sys

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.gateway.api import app


def main() -> int:
    with TestClient(app) as client:
        for path in ("/health", "/openapi.json"):
            response = client.get(path)
            print(path, response.status_code)
            if response.status_code != 200:
                print(response.text)
                return 1

    print("Gateway smoke test passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
