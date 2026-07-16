"""
Shared FBB pump strategy — single source of truth for bot.py and backtest.py.
"""

from __future__ import annotations

import os
from pathlib import Path

import numpy as np
from dotenv import load_dotenv

from indicators import ema_series

load_dotenv(Path(__file__).resolve().parent / ".env")

TIMEFRAME = "1m"
TIMEFRAME_MS = 60 * 1000
FBB_LENGTH = 200
FBB_MULT = 3.0
EXIT_TIMEFRAME = "5m"
EXIT_TF_MS = 5 * 60 * 1000
EMA_PERIOD = 9
FEE_RATE = 0.0004

ORDER_USDT = float(os.getenv("ORDER_USDT", "100"))
MIN_CANDLE_QUOTE_VOL = float(os.getenv("MIN_CANDLE_QUOTE_VOL", "10000"))
VOL_LOOKBACK = int(os.getenv("VOL_LOOKBACK", "20"))
ENTRY_VOL_LIMIT = VOL_LOOKBACK + 2
VOL_MULT = float(os.getenv("VOL_MULT", "3.0"))
MIN_CANDLE_PCT = float(os.getenv("MIN_CANDLE_PCT", "4.0"))
TRAIL_ACTIVATE_PCT = float(os.getenv("TRAIL_ACTIVATE_PCT", "5.0"))
TRAIL_PCT = float(os.getenv("TRAIL_PCT", "5.0"))

OHLCV_LIMIT = FBB_LENGTH + 30
OHLCV_5M_LIMIT = EMA_PERIOD + 30


def aggregate_ohlcv(ohlcv: list[list[float]], bucket_ms: int) -> list[list[float]]:
    """Roll lower-TF candles into `bucket_ms` OHLCV bars."""
    if not ohlcv:
        return []
    out: list[list[float]] = []
    bucket_ts = (int(ohlcv[0][0]) // bucket_ms) * bucket_ms
    o = h = l = c = v = 0.0

    for row in ohlcv:
        ts = int(row[0])
        b = (ts // bucket_ms) * bucket_ms
        ro, rh, rl, rc, rv = map(float, row[1:6])
        if b != bucket_ts:
            out.append([float(bucket_ts), o, h, l, c, v])
            bucket_ts = b
            o, h, l, c, v = ro, rh, rl, rc, rv
        else:
            if v == 0:
                o, h, l, c, v = ro, rh, rl, rc, rv
            else:
                h = max(h, rh)
                l = min(l, rl)
                c = rc
                v += rv

    if v > 0:
        out.append([float(bucket_ts), o, h, l, c, v])
    return out


def five_m_just_closed(ohlcv: list[list[float]], idx: int) -> bool:
    ts = int(ohlcv[idx][0])
    if idx + 1 >= len(ohlcv):
        return True
    next_ts = int(ohlcv[idx + 1][0])
    return (ts // EXIT_TF_MS) != (next_ts // EXIT_TF_MS)


def update_armed(armed: bool, o: float, l: float, grey: float) -> bool:
    if o < grey or l < grey:
        return True
    return armed


def broke_red_line(h: float, red: float) -> bool:
    return h > red


def candle_up_pct(o: float, h: float) -> float:
    if o <= 0:
        return 0.0
    return (h - o) / o * 100.0


def entry_fill_price(o: float, h: float, l: float, red: float) -> float:
    fill = o if o > red else red
    return min(max(fill, l), h)


def volume_ok(
    base_vol: float,
    close_price: float,
    closed_base_volumes: list[float],
) -> tuple[bool, float, float]:
    """Match backtest: quote_vol = base_vol * close."""
    quote_vol = base_vol * max(close_price, 0.0)
    if quote_vol < MIN_CANDLE_QUOTE_VOL:
        return False, quote_vol, 0.0
    if len(closed_base_volumes) < VOL_LOOKBACK:
        return False, quote_vol, 0.0
    avg = sum(closed_base_volumes[-VOL_LOOKBACK:]) / VOL_LOOKBACK
    if avg <= 0:
        return False, quote_vol, 0.0
    rel = base_vol / avg
    return rel >= VOL_MULT, quote_vol, rel


def entry_rules_met(armed: bool, high: float, open_: float, red: float) -> bool:
    if not armed:
        return False
    if high <= red:
        return False
    if open_ <= 0:
        return False
    return candle_up_pct(open_, high) >= MIN_CANDLE_PCT


def price_entry_ready(
    armed: bool,
    tried_this_candle: bool,
    high: float,
    open_: float,
    red: float,
) -> bool:
    """Price rules only — one evaluation per 1m candle (like backtest bar)."""
    if not armed or tried_this_candle:
        return False
    if high <= red:
        return False
    if open_ <= 0:
        return False
    return candle_up_pct(open_, high) >= MIN_CANDLE_PCT


def entry_candle_stop_hit(entry_candle_low: float, price: float) -> tuple[bool, float]:
    """Stop when price breaks below the entry 1m candle low (structure stop)."""
    if entry_candle_low <= 0:
        return False, 0.0
    if price > entry_candle_low:
        return False, 0.0
    return True, max(entry_candle_low, price)


def trail_should_arm(entry: float, close: float) -> bool:
    """Arm trail only on a 1m CLOSE >= +TRAIL_ACTIVATE_PCT% (ignore wick ticks)."""
    if entry <= 0 or close <= 0:
        return False
    return close >= entry * (1.0 + TRAIL_ACTIVATE_PCT / 100.0)


def trail_stop_hit(
    entry: float, high_water: float, price: float, *, armed: bool
) -> tuple[bool, float]:
    """Trailing stop — only after close-armed; floor at entry (no trail loss)."""
    if not armed or entry <= 0 or high_water <= 0:
        return False, 0.0
    stop_px = max(entry, high_water * (1.0 - TRAIL_PCT / 100.0))
    if price > stop_px:
        return False, 0.0
    return True, max(stop_px, price)


def ema_exit_signal(
    ohlcv_1m: list[list[float]],
) -> tuple[bool, float, float, str]:
    bars_5m = aggregate_ohlcv(ohlcv_1m, EXIT_TF_MS)
    if len(bars_5m) < EMA_PERIOD:
        return False, 0.0, 0.0, ""
    closes = np.asarray([float(b[4]) for b in bars_5m], dtype=float)
    ema = ema_series(closes, EMA_PERIOD)
    bar_close = float(bars_5m[-1][4])
    ema_val = float(ema[-1])
    if bar_close >= ema_val:
        return False, bar_close, ema_val, ""
    reason = f"EMA{EMA_PERIOD} 5m close {bar_close:.8f} < {ema_val:.8f}"
    return True, bar_close, ema_val, reason


def active_1m_bar(
    candle_ts: int,
    o: float,
    h: float,
    l: float,
    c: float,
    v: float,
) -> list[float]:
    low = l if l != float("inf") else o
    return [float(candle_ts), o, h, low, c, v]


def ohlcv_with_active(
    closed: list[list[float]],
    candle_ts: int,
    o: float,
    h: float,
    l: float,
    c: float,
    v: float,
) -> list[list[float]]:
    return closed + [active_1m_bar(candle_ts, o, h, l, c, v)]
