"""Technical indicators for Fibonacci Bollinger Instant Breakout."""

from __future__ import annotations

import numpy as np


def hlc3(high: np.ndarray, low: np.ndarray, close: np.ndarray) -> np.ndarray:
    return (high + low + close) / 3.0


def vwma(src: np.ndarray, volume: np.ndarray, length: int) -> float:
    """Volume-weighted moving average of the last `length` bars."""
    s = src[-length:]
    v = volume[-length:]
    total_vol = float(np.sum(v))
    if total_vol <= 0:
        return float(np.mean(s))
    return float(np.sum(s * v) / total_vol)


def stdev(src: np.ndarray, length: int) -> float:
    """Population/sample stdev matching TradingView ta.stdev (sample, ddof=1)."""
    s = src[-length:]
    if len(s) < 2:
        return 0.0
    return float(np.std(s, ddof=1))


def ema_series(closes: np.ndarray, period: int) -> np.ndarray:
    """Full EMA series for `closes`."""
    n = len(closes)
    out = np.empty(n, dtype=float)
    if n == 0:
        return out
    alpha = 2.0 / (period + 1)
    out[0] = closes[0]
    for i in range(1, n):
        out[i] = alpha * closes[i] + (1.0 - alpha) * out[i - 1]
    return out


def ema_last(closes: np.ndarray, period: int) -> float:
    return float(ema_series(closes, period)[-1])


def fibonacci_bollinger(
    ohlcv: list[list[float]],
    length: int = 200,
    mult: float = 3.0,
) -> tuple[float, float, float] | None:
    """
    Fibonacci Bollinger Bands (TradingView / Rashad style).

    basis = VWMA(hlc3, length)           # purple middle line
    dev   = mult * stdev(hlc3, length)
    upper_0.236 = basis + 0.236 * dev    # first grey above purple
    upper_1.000 = basis + 1.000 * dev    # top red line

    Returns (upper_0236, upper_1000, basis) or None if not enough data.
    """
    if len(ohlcv) < length:
        return None

    arr = np.asarray(ohlcv, dtype=float)
    high = arr[:, 2]
    low = arr[:, 3]
    close = arr[:, 4]
    volume = arr[:, 5]
    src = hlc3(high, low, close)

    basis = vwma(src, volume, length)
    dev = mult * stdev(src, length)
    upper_0236 = basis + 0.236 * dev
    upper_1000 = basis + 1.0 * dev
    return upper_0236, upper_1000, basis
