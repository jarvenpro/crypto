from __future__ import annotations

import asyncio
from collections import defaultdict
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timezone
from math import floor
from typing import Any
import json

import websockets

from app.gateway.http import GatewayHttpClient, UpstreamServiceError


BINANCE_FUTURES_BASE = "https://fapi.binance.com"
BYBIT_BASE = "https://api.bybit.com"
OKX_BASE = "https://www.okx.com"
BINANCE_FORCE_ORDER_WS = "wss://fstream.binance.com/ws/{stream}"
BYBIT_PUBLIC_LINEAR_WS = "wss://stream.bybit.com/v5/public/linear"


@dataclass(slots=True)
class LiquidationEvent:
    venue: str
    liquidated_side: str
    order_side: str
    price: float
    quantity: float
    notional_usd: float
    event_time: str


class LiquidityContextBuilder:
    def __init__(self, http_client: GatewayHttpClient) -> None:
        self._http = http_client

    async def build_context(
        self,
        *,
        symbol: str = "BTCUSDT",
        depth_limit: int = 100,
        liquidation_sample_seconds: int = 4,
        include_binance: bool = True,
        include_bybit: bool = True,
        include_okx: bool = True,
    ) -> dict[str, Any]:
        symbol = symbol.upper()
        requested_depth_limit = max(50, min(depth_limit, 500))
        binance_depth_limit = self._normalize_binance_depth_limit(requested_depth_limit)
        bybit_depth_limit = min(requested_depth_limit, 200)
        okx_depth_limit = min(requested_depth_limit, 200)
        liquidation_sample_seconds = max(2, min(liquidation_sample_seconds, 8))

        venues: dict[str, Any] = {}
        source_status: dict[str, Any] = {}

        binance_book = None
        bybit_book = None
        okx_book = None

        if include_binance:
            try:
                binance_book = self._get_binance_orderbook(symbol, depth_limit=binance_depth_limit)
                source_status["binance_orderbook"] = {"ok": True}
            except UpstreamServiceError as exc:
                source_status["binance_orderbook"] = {"ok": False, "reason": exc.detail}

        if include_bybit:
            try:
                bybit_book = self._get_bybit_orderbook(symbol, depth_limit=bybit_depth_limit)
                source_status["bybit_orderbook"] = {"ok": True}
            except UpstreamServiceError as exc:
                source_status["bybit_orderbook"] = {"ok": False, "reason": exc.detail}

        if include_okx:
            try:
                okx_book = self._get_okx_orderbook(symbol, depth_limit=okx_depth_limit)
                source_status["okx_orderbook"] = {"ok": True}
            except UpstreamServiceError as exc:
                source_status["okx_orderbook"] = {"ok": False, "reason": exc.detail}

        reference_price = (
            (binance_book or {}).get("mid_price")
            or (bybit_book or {}).get("mid_price")
            or (okx_book or {}).get("mid_price")
        )

        tasks: list[asyncio.Future] = []
        task_labels: list[str] = []
        if include_binance:
            tasks.append(asyncio.create_task(self._sample_binance_liquidations(symbol, liquidation_sample_seconds)))
            task_labels.append("binance_liquidations")
        if include_bybit:
            tasks.append(asyncio.create_task(self._sample_bybit_liquidations(symbol, liquidation_sample_seconds)))
            task_labels.append("bybit_liquidations")

        results = await asyncio.gather(*tasks, return_exceptions=True) if tasks else []
        liquidations_by_label: dict[str, list[LiquidationEvent]] = {}
        for label, result in zip(task_labels, results):
            if isinstance(result, list):
                liquidations_by_label[label] = result
                source_status[label] = {"ok": True, "count": len(result)}
            else:
                liquidations_by_label[label] = []
                source_status[label] = {"ok": False, "reason": str(result)}

        payload = {
            "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "symbol": symbol,
            "reference_price": reference_price,
            "depth_limits": {
                "requested": requested_depth_limit,
                "binance": binance_depth_limit if include_binance else None,
                "bybit": bybit_depth_limit if include_bybit else None,
                "okx": okx_depth_limit if include_okx else None,
            },
            "venues": venues,
            "source_status": source_status,
            "combined": {},
        }

        if binance_book:
            payload["venues"]["binance"] = {
                "orderbook": self._build_orderbook_summary("BINANCE", binance_book, reference_price),
                "liquidations": self._build_liquidation_summary(
                    "BINANCE",
                    liquidations_by_label.get("binance_liquidations", []),
                    reference_price,
                    liquidation_sample_seconds,
                ),
            }

        if bybit_book:
            payload["venues"]["bybit"] = {
                "orderbook": self._build_orderbook_summary("BYBIT", bybit_book, reference_price),
                "liquidations": self._build_liquidation_summary(
                    "BYBIT",
                    liquidations_by_label.get("bybit_liquidations", []),
                    reference_price,
                    liquidation_sample_seconds,
                ),
            }

        if okx_book:
            payload["venues"]["okx"] = {
                "orderbook": self._build_orderbook_summary("OKX", okx_book, reference_price),
            }

        if not payload["venues"]:
            raise UpstreamServiceError("liquidity", "No exchange data was available for liquidity context.")

        payload["combined"] = self._build_combined_liquidity_view(payload["venues"], reference_price)
        return payload

    @staticmethod
    def _normalize_binance_depth_limit(requested_depth_limit: int) -> int:
        allowed_limits = (5, 10, 20, 50, 100, 500, 1000)
        eligible_limits = [limit for limit in allowed_limits if limit <= requested_depth_limit]
        if eligible_limits:
            return eligible_limits[-1]
        return 5

    def _get_binance_orderbook(self, symbol: str, *, depth_limit: int) -> dict[str, Any]:
        rows = self._http.get_json(
            "binance",
            f"{BINANCE_FUTURES_BASE}/fapi/v1/depth",
            params={"symbol": symbol, "limit": depth_limit},
            ttl_seconds=10,
        )
        return self._normalize_orderbook_payload(
            bids=rows.get("bids", []),
            asks=rows.get("asks", []),
            venue="BINANCE",
        )

    def _get_bybit_orderbook(self, symbol: str, *, depth_limit: int) -> dict[str, Any]:
        payload = self._http.get_json(
            "bybit",
            f"{BYBIT_BASE}/v5/market/orderbook",
            params={"category": "linear", "symbol": symbol, "limit": depth_limit},
            ttl_seconds=10,
        )
        result = payload.get("result", {})
        return self._normalize_orderbook_payload(
            bids=result.get("b", []),
            asks=result.get("a", []),
            venue="BYBIT",
        )

    def _get_okx_orderbook(self, symbol: str, *, depth_limit: int) -> dict[str, Any]:
        inst_id = self._to_okx_swap_inst_id(symbol)
        payload = self._http.get_json(
            "okx",
            f"{OKX_BASE}/api/v5/market/books",
            params={"instId": inst_id, "sz": depth_limit},
            ttl_seconds=10,
        )
        rows = payload.get("data", [])
        row = rows[0] if rows else {}
        return self._normalize_orderbook_payload(
            bids=row.get("bids", []),
            asks=row.get("asks", []),
            venue="OKX",
        )

    async def _sample_binance_liquidations(self, symbol: str, sample_seconds: int) -> list[LiquidationEvent]:
        url = BINANCE_FORCE_ORDER_WS.format(stream=f"{symbol.lower()}@forceOrder")
        end_time = asyncio.get_running_loop().time() + sample_seconds
        events: list[LiquidationEvent] = []

        async with websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=5) as websocket:
            while asyncio.get_running_loop().time() < end_time:
                timeout = max(0.2, end_time - asyncio.get_running_loop().time())
                try:
                    raw = await asyncio.wait_for(websocket.recv(), timeout=timeout)
                except TimeoutError:
                    continue
                data = json.loads(raw)
                order = data.get("o")
                if not isinstance(order, dict):
                    continue
                order_side = str(order.get("S") or "").upper()
                fill_price = self._safe_float(order.get("ap")) or self._safe_float(order.get("p"))
                quantity = self._safe_float(order.get("z")) or self._safe_float(order.get("q"))
                if fill_price is None or quantity is None:
                    continue
                liquidated_side = "long" if order_side == "SELL" else "short"
                event_time = self._ms_to_iso(order.get("T"))
                events.append(
                    LiquidationEvent(
                        venue="BINANCE",
                        liquidated_side=liquidated_side,
                        order_side=order_side,
                        price=fill_price,
                        quantity=quantity,
                        notional_usd=round(fill_price * quantity, 2),
                        event_time=event_time,
                    )
                )
        return events

    async def _sample_bybit_liquidations(self, symbol: str, sample_seconds: int) -> list[LiquidationEvent]:
        end_time = asyncio.get_running_loop().time() + sample_seconds
        events: list[LiquidationEvent] = []

        async with websockets.connect(BYBIT_PUBLIC_LINEAR_WS, ping_interval=20, ping_timeout=20, close_timeout=5) as websocket:
            await websocket.send(json.dumps({"op": "subscribe", "args": [f"allLiquidation.{symbol}"]}))

            while asyncio.get_running_loop().time() < end_time:
                timeout = max(0.2, end_time - asyncio.get_running_loop().time())
                try:
                    raw = await asyncio.wait_for(websocket.recv(), timeout=timeout)
                except TimeoutError:
                    continue

                payload = json.loads(raw)
                rows = payload.get("data")
                if not isinstance(rows, list):
                    continue

                for item in rows:
                    if not isinstance(item, dict):
                        continue
                    order_side = str(item.get("S") or "").upper()
                    price = self._safe_float(item.get("p"))
                    quantity = self._safe_float(item.get("v"))
                    if price is None or quantity is None:
                        continue
                    # Bybit docs: Buy means long position liquidation, Sell means short liquidation.
                    liquidated_side = "long" if order_side == "BUY" else "short"
                    events.append(
                        LiquidationEvent(
                            venue="BYBIT",
                            liquidated_side=liquidated_side,
                            order_side=order_side,
                            price=price,
                            quantity=quantity,
                            notional_usd=round(price * quantity, 2),
                            event_time=self._ms_to_iso(item.get("T")),
                        )
                    )
        return events

    def _build_orderbook_summary(self, venue: str, orderbook: dict[str, Any], reference_price: float | None) -> dict[str, Any]:
        bids = orderbook.get("bids", [])
        asks = orderbook.get("asks", [])
        mid_price = orderbook.get("mid_price") or reference_price
        return {
            "venue": venue,
            "best_bid": orderbook.get("best_bid"),
            "best_ask": orderbook.get("best_ask"),
            "mid_price": mid_price,
            "spread_bps": orderbook.get("spread_bps"),
            "top_bid_walls": self._summarize_top_walls(bids, mid_price, side="bid"),
            "top_ask_walls": self._summarize_top_walls(asks, mid_price, side="ask"),
            "band_depth": self._summarize_depth_bands(bids, asks, mid_price),
            "heatmap_bands": self._build_depth_heatmap_bands(bids, asks, mid_price),
        }

    def _build_liquidation_summary(
        self,
        venue: str,
        events: list[LiquidationEvent],
        reference_price: float | None,
        sample_seconds: int,
    ) -> dict[str, Any]:
        long_total = round(sum(event.notional_usd for event in events if event.liquidated_side == "long"), 2)
        short_total = round(sum(event.notional_usd for event in events if event.liquidated_side == "short"), 2)
        if abs(long_total - short_total) < max(long_total, short_total, 1.0) * 0.05:
            bias = "balanced"
        elif long_total > short_total:
            bias = "longs_dominant_liquidation"
        else:
            bias = "shorts_dominant_liquidation"

        return {
            "venue": venue,
            "sample_seconds": sample_seconds,
            "event_count": len(events),
            "long_liquidation_usd": long_total,
            "short_liquidation_usd": short_total,
            "bias": bias,
            "distribution_bands": self._build_liquidation_distribution(events, reference_price),
            "recent_events": [
                {
                    "liquidated_side": event.liquidated_side,
                    "price": event.price,
                    "quantity": event.quantity,
                    "notional_usd": event.notional_usd,
                    "event_time": event.event_time,
                }
                for event in events[-8:]
            ],
        }

    def _build_combined_liquidity_view(self, venues: dict[str, Any], reference_price: float | None) -> dict[str, Any]:
        orderbook_venues = [
            venue_data.get("orderbook")
            for venue_data in venues.values()
            if isinstance(venue_data, dict) and isinstance(venue_data.get("orderbook"), dict)
        ]
        liquidation_venues = [
            venue_data.get("liquidations")
            for venue_data in venues.values()
            if isinstance(venue_data, dict) and isinstance(venue_data.get("liquidations"), dict)
        ]

        upper_wall_strength = 0.0
        lower_wall_strength = 0.0
        for venue in orderbook_venues:
            for wall in venue.get("top_ask_walls", [])[:3]:
                upper_wall_strength += wall.get("notional", 0.0)
            for wall in venue.get("top_bid_walls", [])[:3]:
                lower_wall_strength += wall.get("notional", 0.0)

        long_liq_total = sum(item.get("long_liquidation_usd", 0.0) for item in liquidation_venues)
        short_liq_total = sum(item.get("short_liquidation_usd", 0.0) for item in liquidation_venues)

        if upper_wall_strength > lower_wall_strength * 1.15:
            wall_bias = "upside_heavier"
        elif lower_wall_strength > upper_wall_strength * 1.15:
            wall_bias = "downside_heavier"
        else:
            wall_bias = "balanced"

        if short_liq_total > long_liq_total * 1.15:
            liquidation_bias = "short_liquidations_dominant"
        elif long_liq_total > short_liq_total * 1.15:
            liquidation_bias = "long_liquidations_dominant"
        else:
            liquidation_bias = "balanced"

        narratives: list[str] = []
        if wall_bias == "upside_heavier":
            narratives.append("Near-price ask-side liquidity looks thicker than bid-side liquidity.")
        elif wall_bias == "downside_heavier":
            narratives.append("Near-price bid-side liquidity looks thicker than ask-side liquidity.")
        else:
            narratives.append("Near-price book walls look relatively balanced across sides.")

        if liquidation_bias == "short_liquidations_dominant":
            narratives.append("Recent sampled liquidations are skewed toward shorts being forced out.")
        elif liquidation_bias == "long_liquidations_dominant":
            narratives.append("Recent sampled liquidations are skewed toward longs being forced out.")
        else:
            narratives.append("Recent sampled liquidations look fairly balanced.")

        if wall_bias == "upside_heavier" and liquidation_bias == "short_liquidations_dominant":
            sweep_risk = "upside_sweep_risk_high"
        elif wall_bias == "downside_heavier" and liquidation_bias == "long_liquidations_dominant":
            sweep_risk = "downside_sweep_risk_high"
        else:
            sweep_risk = "two_sided_or_mixed"

        return {
            "reference_price": reference_price,
            "wall_bias": wall_bias,
            "liquidation_bias": liquidation_bias,
            "upside_wall_strength_usd": round(upper_wall_strength, 2),
            "downside_wall_strength_usd": round(lower_wall_strength, 2),
            "short_liquidation_total_usd": round(short_liq_total, 2),
            "long_liquidation_total_usd": round(long_liq_total, 2),
            "liquidity_sweep_risk": sweep_risk,
            "narratives": narratives,
        }

    @staticmethod
    def _normalize_orderbook_payload(*, bids: list[Any], asks: list[Any], venue: str) -> dict[str, Any]:
        normalized_bids = LiquidityContextBuilder._normalize_levels(bids)
        normalized_asks = LiquidityContextBuilder._normalize_levels(asks)
        best_bid = normalized_bids[0]["price"] if normalized_bids else None
        best_ask = normalized_asks[0]["price"] if normalized_asks else None
        mid_price = round((best_bid + best_ask) / 2, 4) if best_bid and best_ask else best_bid or best_ask
        spread_bps = None
        if best_bid and best_ask and mid_price:
            spread_bps = round(((best_ask - best_bid) / mid_price) * 10000, 4)

        return {
            "venue": venue,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "mid_price": mid_price,
            "spread_bps": spread_bps,
            "bids": normalized_bids,
            "asks": normalized_asks,
        }

    @staticmethod
    def _normalize_levels(rows: list[Any]) -> list[dict[str, float]]:
        levels: list[dict[str, float]] = []
        for item in rows:
            if not isinstance(item, (list, tuple)) or len(item) < 2:
                continue
            try:
                price = float(item[0])
                size = float(item[1])
            except (TypeError, ValueError):
                continue
            if price <= 0 or size <= 0:
                continue
            levels.append(
                {
                    "price": price,
                    "size": size,
                    "notional": round(price * size, 2),
                }
            )
        return levels

    @staticmethod
    def _summarize_top_walls(levels: list[dict[str, float]], reference_price: float | None, *, side: str) -> list[dict[str, Any]]:
        if not reference_price:
            return levels[:5]
        eligible = [
            {
                **level,
                "distance_pct": round(((level["price"] - reference_price) / reference_price) * 100, 4),
            }
            for level in levels
            if abs((level["price"] - reference_price) / reference_price) <= 0.02
        ]
        if side == "bid":
            eligible = [item for item in eligible if item["price"] <= reference_price]
        else:
            eligible = [item for item in eligible if item["price"] >= reference_price]
        return sorted(eligible, key=lambda item: item["notional"], reverse=True)[:5]

    @staticmethod
    def _summarize_depth_bands(
        bids: list[dict[str, float]],
        asks: list[dict[str, float]],
        reference_price: float | None,
    ) -> dict[str, Any]:
        if not reference_price:
            return {}
        band_pcts = (0.1, 0.25, 0.5, 1.0)
        summary: dict[str, Any] = {}
        for pct in band_pcts:
            bid_total = sum(level["notional"] for level in bids if 0 <= ((reference_price - level["price"]) / reference_price) * 100 <= pct)
            ask_total = sum(level["notional"] for level in asks if 0 <= ((level["price"] - reference_price) / reference_price) * 100 <= pct)
            if abs(bid_total - ask_total) < max(bid_total, ask_total, 1.0) * 0.05:
                imbalance = "balanced"
            elif bid_total > ask_total:
                imbalance = "bid_heavier"
            else:
                imbalance = "ask_heavier"
            summary[f"{pct:.2f}%"] = {
                "bid_notional": round(bid_total, 2),
                "ask_notional": round(ask_total, 2),
                "imbalance": imbalance,
            }
        return summary

    @staticmethod
    def _build_depth_heatmap_bands(
        bids: list[dict[str, float]],
        asks: list[dict[str, float]],
        reference_price: float | None,
        *,
        bucket_pct: float = 0.1,
        max_distance_pct: float = 2.0,
    ) -> dict[str, list[dict[str, Any]]]:
        if not reference_price:
            return {"bids": [], "asks": []}

        def bucket(levels: list[dict[str, float]], side: str) -> list[dict[str, Any]]:
            bands: dict[int, float] = defaultdict(float)
            for level in levels:
                distance_pct = ((level["price"] - reference_price) / reference_price) * 100
                if side == "bid" and distance_pct > 0:
                    continue
                if side == "ask" and distance_pct < 0:
                    continue
                if abs(distance_pct) > max_distance_pct:
                    continue
                idx = int(floor(abs(distance_pct) / bucket_pct))
                bands[idx] += level["notional"]
            results: list[dict[str, Any]] = []
            for idx, notional in bands.items():
                lower = round(idx * bucket_pct, 4)
                upper = round((idx + 1) * bucket_pct, 4)
                results.append(
                    {
                        "band_pct": f"{lower:.2f}-{upper:.2f}",
                        "total_notional": round(notional, 2),
                    }
                )
            return sorted(results, key=lambda item: item["total_notional"], reverse=True)[:8]

        return {
            "bids": bucket(bids, "bid"),
            "asks": bucket(asks, "ask"),
        }

    @staticmethod
    def _build_liquidation_distribution(
        events: list[LiquidationEvent],
        reference_price: float | None,
        *,
        bucket_pct: float = 0.25,
        max_distance_pct: float = 3.0,
    ) -> list[dict[str, Any]]:
        if not reference_price:
            return []
        bands: dict[tuple[str, int], float] = defaultdict(float)
        for event in events:
            distance_pct = ((event.price - reference_price) / reference_price) * 100
            if abs(distance_pct) > max_distance_pct:
                continue
            idx = int(floor(abs(distance_pct) / bucket_pct))
            side = "above_price" if distance_pct >= 0 else "below_price"
            bands[(side, idx)] += event.notional_usd

        output: list[dict[str, Any]] = []
        for (side, idx), total in bands.items():
            lower = round(idx * bucket_pct, 4)
            upper = round((idx + 1) * bucket_pct, 4)
            output.append(
                {
                    "side": side,
                    "band_pct": f"{lower:.2f}-{upper:.2f}",
                    "total_notional_usd": round(total, 2),
                }
            )
        return sorted(output, key=lambda item: item["total_notional_usd"], reverse=True)[:10]

    @staticmethod
    def _to_okx_swap_inst_id(symbol: str) -> str:
        root = symbol.upper()
        if root.endswith("USDT"):
            return f"{root[:-4]}-USDT-SWAP"
        return f"{root}-SWAP"

    @staticmethod
    def _safe_float(value: Any) -> float | None:
        if value in (None, "", "-"):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _ms_to_iso(value: Any) -> str:
        if value in (None, "", "-"):
            return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        try:
            return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc).replace(microsecond=0).isoformat()
        except (TypeError, ValueError, OSError):
            return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
