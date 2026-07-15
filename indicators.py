"""Technical indicators: Fibonacci Bollinger Bands + EMA."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class FibonacciBollingerLevels:
    """Upper FBB levels used by the breakout scanner."""

    basis: float
    upper_0764: float  # 5th grey line (0.764)
    upper_1000: float  # top red line (1.000)


def _hlc3(high: np.ndarray, low: np.ndarray, close: np.ndarray) -> np.ndarray:
    return (high + low + close) / 3.0


def _vwma(source: np.ndarray, volume: np.ndarray, length: int) -> float | None:
    if len(source) < length or length <= 0:
        return None
    src = source[-length:]
    vol = volume[-length:]
    vol_sum = float(np.sum(vol))
    if vol_sum <= 0:
        return None
    return float(np.sum(src * vol) / vol_sum)


def _stdev(source: np.ndarray, length: int) -> float | None:
    if len(source) < length or length <= 1:
        return None
    return float(np.std(source[-length:], ddof=0))


def fibonacci_bollinger_bands(
    ohlcv: list[list[float]],
    length: int = 200,
    mult: float = 3.0,
) -> FibonacciBollingerLevels | None:
    """
    TradingView-style Fibonacci Bollinger Bands (Rashad).

    basis = VWMA(hlc3, length)
    dev   = mult * STDEV(hlc3, length)
    upper_0764 = basis + 0.764 * dev   # grey
    upper_1000 = basis + 1.000 * dev   # red
    """
    if len(ohlcv) < length:
        return None

    arr = np.asarray(ohlcv, dtype=float)
    high = arr[:, 2]
    low = arr[:, 3]
    close = arr[:, 4]
    volume = arr[:, 5]
    src = _hlc3(high, low, close)

    basis = _vwma(src, volume, length)
    stdev = _stdev(src, length)
    if basis is None or stdev is None:
        return None

    dev = mult * stdev
    return FibonacciBollingerLevels(
        basis=basis,
        upper_0764=basis + 0.764 * dev,
        upper_1000=basis + 1.0 * dev,
    )


def ema(values: np.ndarray | list[float], period: int) -> float | None:
    """Exponential moving average of the last value."""
    if period <= 0 or len(values) < period:
        return None

    data = np.asarray(values, dtype=float)
    alpha = 2.0 / (period + 1.0)
    seed = float(np.mean(data[:period]))
    value = seed
    for price in data[period:]:
        value = alpha * float(price) + (1.0 - alpha) * value
    return float(value)


def candle_open_ms(timestamp_ms: int, timeframe_sec: int = 300) -> int:
    """Floor timestamp to the start of its candle."""
    tf_ms = timeframe_sec * 1000
    return (int(timestamp_ms) // tf_ms) * tf_ms
