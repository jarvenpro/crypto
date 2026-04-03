from __future__ import annotations

import uvicorn

from app.gateway.config import load_gateway_config


def main() -> None:
    config = load_gateway_config()
    uvicorn.run(
        "app.gateway.api:app",
        host=config.host,
        port=config.port,
        reload=False,
    )


if __name__ == "__main__":
    main()
