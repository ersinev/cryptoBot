"""Unit tests for Fibonacci Bollinger Bands and EMA helpers."""

from __future__ import annotations

import unittest

import numpy as np

from indicators import ema, fibonacci_bollinger_bands


class IndicatorTests(unittest.TestCase):
    def test_fbb_levels_match_manual_formula(self) -> None:
        # Build a synthetic series long enough for length=20
        length = 20
        ohlcv = []
        for i in range(length):
            price = 100 + i
            high = price + 1
            low = price - 1
            close = price
            volume = 10 + i
            ohlcv.append([i * 60_000, price, high, low, close, volume])

        levels = fibonacci_bollinger_bands(ohlcv, length=length, mult=3.0)
        self.assertIsNotNone(levels)
        assert levels is not None

        highs = np.array([c[2] for c in ohlcv], dtype=float)
        lows = np.array([c[3] for c in ohlcv], dtype=float)
        closes = np.array([c[4] for c in ohlcv], dtype=float)
        volumes = np.array([c[5] for c in ohlcv], dtype=float)
        src = (highs + lows + closes) / 3.0

        basis = float(np.sum(src * volumes) / np.sum(volumes))
        stdev = float(np.std(src, ddof=0))
        dev = 3.0 * stdev

        self.assertAlmostEqual(levels.basis, basis, places=10)
        self.assertAlmostEqual(levels.upper_0764, basis + 0.764 * dev, places=10)
        self.assertAlmostEqual(levels.upper_1000, basis + 1.0 * dev, places=10)
        self.assertGreater(levels.upper_1000, levels.upper_0764)

    def test_fbb_requires_enough_candles(self) -> None:
        ohlcv = [[0, 1, 1, 1, 1, 1] for _ in range(10)]
        self.assertIsNone(fibonacci_bollinger_bands(ohlcv, length=20))

    def test_ema_basic(self) -> None:
        values = [float(i) for i in range(1, 20)]
        value = ema(values, 9)
        self.assertIsNotNone(value)
        assert value is not None
        self.assertGreater(value, 0)
        # EMA should be near the recent prices for an uptrend
        self.assertGreater(value, float(np.mean(values[:9])))


if __name__ == "__main__":
    unittest.main()
