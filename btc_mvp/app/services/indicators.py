from __future__ import annotations

from math import isclose

from app.clients.binance import Kline


def ema(values: list[float], period: int) -> float:
    if not values:
        raise ValueError("ema requires at least one value")
    if period <= 0:
        raise ValueError("period must be positive")

    multiplier = 2 / (period + 1)
    result = values[0]
    for value in values[1:]:
        result = (value - result) * multiplier + result
    return result


def rsi(values: list[float], period: int = 14) -> float:
    if len(values) <= period:
        return 50.0

    gains: list[float] = []
    losses: list[float] = []
    for prev, current in zip(values[:-1], values[1:]):
        delta = current - prev
        gains.append(max(delta, 0.0))
        losses.append(abs(min(delta, 0.0)))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for gain, loss in zip(gains[period:], losses[period:]):
        avg_gain = ((avg_gain * (period - 1)) + gain) / period
        avg_loss = ((avg_loss * (period - 1)) + loss) / period

    if isclose(avg_loss, 0.0):
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def volume_ratio(bars: list[Kline], lookback: int = 12) -> float:
    if len(bars) < lookback + 1:
        return 1.0
    baseline = sum(bar.volume for bar in bars[-(lookback + 1):-1]) / lookback
    if baseline == 0:
        return 1.0
    return bars[-1].volume / baseline


def vwap(bars: list[Kline]) -> float:
    cumulative_volume = 0.0
    cumulative_value = 0.0
    for bar in bars:
        typical_price = (bar.high_price + bar.low_price + bar.close_price) / 3
        cumulative_value += typical_price * bar.volume
        cumulative_volume += bar.volume
    if cumulative_volume == 0:
        return bars[-1].close_price
    return cumulative_value / cumulative_volume


def atr(bars: list[Kline], period: int = 14) -> float:
    if len(bars) < 2:
        return 0.0

    true_ranges: list[float] = []
    previous_close = bars[0].close_price
    for bar in bars[1:]:
        true_range = max(
            bar.high_price - bar.low_price,
            abs(bar.high_price - previous_close),
            abs(bar.low_price - previous_close),
        )
        true_ranges.append(true_range)
        previous_close = bar.close_price

    if not true_ranges:
        return 0.0
    if len(true_ranges) < period:
        return sum(true_ranges) / len(true_ranges)

    initial = sum(true_ranges[:period]) / period
    result = initial
    for value in true_ranges[period:]:
        result = ((result * (period - 1)) + value) / period
    return result
