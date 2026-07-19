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
EMA_PERIOD = 9
FEE_RATE = 0.001  # spot taker ~0.1%

ORDER_USDT = float(os.getenv("ORDER_USDT", "100"))
# Optional absolute chart BASE floor (0 = off). Prefer VOL_SPIKE_* on current candle.
MIN_CANDLE_BASE_VOL = float(os.getenv("MIN_CANDLE_BASE_VOL", "0"))
# Legacy USDT quote floor (used only when MIN_CANDLE_BASE_VOL <= 0 and spike off)
MIN_CANDLE_QUOTE_VOL = float(os.getenv("MIN_CANDLE_QUOTE_VOL", "10000"))
# Current 1m candle BASE vol >= VOL_SPIKE_MULT * mean(prev VOL_SPIKE_LOOKBACK closed)
VOL_SPIKE_LOOKBACK = int(os.getenv("VOL_SPIKE_LOOKBACK", "5"))
VOL_SPIKE_MULT = float(os.getenv("VOL_SPIKE_MULT", "2.0"))
USE_VOL_SPIKE = os.getenv("USE_VOL_SPIKE", "1").strip().lower() in (
    "1",
    "true",
    "yes",
)
VOL_LOOKBACK = int(os.getenv("VOL_LOOKBACK", "20"))
ENTRY_VOL_LIMIT = VOL_LOOKBACK + 2
VOL_MULT = float(os.getenv("VOL_MULT", "3.0"))
MIN_CANDLE_PCT = float(os.getenv("MIN_CANDLE_PCT", "2.0"))
TRAIL_ACTIVATE_PCT = float(os.getenv("TRAIL_ACTIVATE_PCT", "5.0"))
TRAIL_PCT = float(os.getenv("TRAIL_PCT", "2.0"))
# Legacy single-step (unused if PARTIAL_LADDER set)
PARTIAL_TP_PCT = float(os.getenv("PARTIAL_TP_PCT", "3.0"))
PARTIAL_TP_FRAC = float(os.getenv("PARTIAL_TP_FRAC", "0.3"))
USE_TRAIL = os.getenv("USE_TRAIL", "1").strip().lower() in ("1", "true", "yes")
# Hard stop below entry (0 = off). Replaces/alongside entry-candle-low.
HARD_STOP_PCT = float(os.getenv("HARD_STOP_PCT", "1.5"))
USE_ENTRY_CANDLE_STOP = os.getenv("USE_ENTRY_CANDLE_STOP", "0").strip().lower() in (
    "1",
    "true",
    "yes",
)
# After first ladder partial: stop at entry * (1 + BREAKEVEN_PCT/100)
USE_BREAKEVEN = os.getenv("USE_BREAKEVEN", "1").strip().lower() in (
    "1",
    "true",
    "yes",
)
BREAKEVEN_PCT = float(os.getenv("BREAKEVEN_PCT", "0.3"))
USE_EMA_EXIT = os.getenv("USE_EMA_EXIT", "0").strip().lower() in (
    "1",
    "true",
    "yes",
)
# Runner EMA timeframe: "1m", "2m", "3m", or "5m"
_EMA_TF_MS = {
    "1m": 60_000,
    "2m": 2 * 60_000,
    "3m": 3 * 60_000,
    "5m": 5 * 60_000,
}
EMA_EXIT_TF = os.getenv("EMA_EXIT_TF", "5m").strip().lower()
if EMA_EXIT_TF not in _EMA_TF_MS:
    EMA_EXIT_TF = "5m"
EMA_EXIT_TF_MS = _EMA_TF_MS[EMA_EXIT_TF]
EXIT_TIMEFRAME = EMA_EXIT_TF
EXIT_TF_MS = EMA_EXIT_TF_MS
EMA_PROGRESSIVE = os.getenv("EMA_PROGRESSIVE", "0").strip().lower() in (
    "1",
    "true",
    "yes",
)
# progressive: "1m2m" | "1m3m" | "1m3m5m"
EMA_PROG_MODE = os.getenv("EMA_PROG_MODE", "1m2m").strip().lower()
if EMA_PROG_MODE not in ("1m3m5m", "1m3m", "1m2m"):
    EMA_PROG_MODE = "1m2m"


def _parse_partial_ladder(raw: str) -> list[tuple[float, float]]:
    """Parse '3:0.30,5:0.30' → [(3.0, 0.30), (5.0, 0.30)] (pct, frac of initial)."""
    out: list[tuple[float, float]] = []
    for part in raw.split(","):
        part = part.strip()
        if not part or ":" not in part:
            continue
        a, b = part.split(":", 1)
        pct, frac = float(a.strip()), float(b.strip())
        if pct > 0 and frac > 0:
            out.append((pct, frac))
    out.sort(key=lambda x: x[0])
    return out


PARTIAL_LADDER = _parse_partial_ladder(
    os.getenv("PARTIAL_LADDER", "3:0.70")
)
if not PARTIAL_LADDER and PARTIAL_TP_PCT > 0 and PARTIAL_TP_FRAC > 0:
    PARTIAL_LADDER = [(PARTIAL_TP_PCT, PARTIAL_TP_FRAC)]


def ladder_label() -> str:
    if not PARTIAL_LADDER:
        return ""
    bits = [f"{int(f*100)}%@+{p:g}%" for p, f in PARTIAL_LADDER]
    return " then ".join(bits)

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


def ema_tf_ms(tf: str) -> int:
    return _EMA_TF_MS.get(tf, EMA_EXIT_TF_MS)


def runner_ema_tf(entry: float, high_water: float) -> str:
    """
    Progressive runner EMA (EMA_PROGRESSIVE=1):
      1m2m:   MFE < +3% → 1m | >= +3% → 2m
      1m3m:   MFE < +3% → 1m | >= +3% → 3m
      1m3m5m: MFE < +3% → 1m | +3%..+5% → 3m | >= +5% → 5m
    Thresholds follow PARTIAL_LADDER levels when present.
    Fixed mode (EMA_PROGRESSIVE=0) always returns EMA_EXIT_TF.
    """
    if not EMA_PROGRESSIVE:
        return EMA_EXIT_TF
    if entry <= 0 or high_water <= 0:
        return "1m"
    mfe_pct = (high_water / entry - 1.0) * 100.0
    t_lo = PARTIAL_LADDER[0][0] if PARTIAL_LADDER else 3.0
    t_hi = PARTIAL_LADDER[1][0] if len(PARTIAL_LADDER) > 1 else 5.0
    if EMA_PROG_MODE == "1m2m":
        return "2m" if mfe_pct >= t_lo else "1m"
    if EMA_PROG_MODE == "1m3m":
        return "3m" if mfe_pct >= t_lo else "1m"
    if mfe_pct >= t_hi:
        return "5m"
    if mfe_pct >= t_lo:
        return "3m"
    return "1m"


def tf_just_closed(ohlcv: list[list[float]], idx: int, tf: str) -> bool:
    """True when the current 1m bar is the last bar of `tf` bucket."""
    if tf == "1m":
        return True
    bucket = ema_tf_ms(tf)
    ts = int(ohlcv[idx][0])
    if idx + 1 >= len(ohlcv):
        return True
    next_ts = int(ohlcv[idx + 1][0])
    return (ts // bucket) != (next_ts // bucket)


def ema_tf_just_closed(ohlcv: list[list[float]], idx: int) -> bool:
    """True when the current 1m bar is the last bar of an EMA_EXIT_TF bucket."""
    return tf_just_closed(ohlcv, idx, EMA_EXIT_TF)


def five_m_just_closed(ohlcv: list[list[float]], idx: int) -> bool:
    """Alias — bucket size follows EMA_EXIT_TF (1m/3m/5m)."""
    return ema_tf_just_closed(ohlcv, idx)


def update_armed(armed: bool, o: float, l: float, grey: float) -> bool:
    if o < grey or l < grey:
        return True
    return armed


def broke_entry_line(h: float, entry_line: float) -> bool:
    """Break of FBB upper 0.786 (wick / high touch)."""
    return h > entry_line


def broke_red_line(h: float, red: float) -> bool:
    """Deprecated alias — prefer broke_entry_line (0.786)."""
    return broke_entry_line(h, red)


def candle_up_pct(o: float, h: float) -> float:
    if o <= 0:
        return 0.0
    return (h - o) / o * 100.0


def entry_fill_price(o: float, h: float, l: float, entry_line: float) -> float:
    fill = o if o > entry_line else entry_line
    return min(max(fill, l), h)


def volume_ok(
    base_vol: float,
    close_price: float,
    closed_base_volumes: list[float],
) -> tuple[bool, float, float]:
    """Legacy: abs + rel. Prefer prev_candle_quote_ok for live entry."""
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


def prev_candle_quote_ok(base_vol: float, close_price: float) -> tuple[bool, float]:
    """Previous closed 1m quote vol only (>= MIN_CANDLE_QUOTE_VOL). No rel filter."""
    quote_vol = base_vol * max(close_price, 0.0)
    return quote_vol >= MIN_CANDLE_QUOTE_VOL, quote_vol


def prev_candle_base_ok(base_vol: float) -> tuple[bool, float]:
    """Previous closed 1m BASE vol (chart Volume) >= MIN_CANDLE_BASE_VOL."""
    if MIN_CANDLE_BASE_VOL <= 0:
        return True, base_vol
    return base_vol >= MIN_CANDLE_BASE_VOL, base_vol


def current_vol_spike_ok(
    current_base_vol: float,
    prev_closed_base_vols: list[float],
    *,
    lookback: int | None = None,
    mult: float | None = None,
) -> tuple[bool, float, float]:
    """
    Current candle chart BASE vol vs mean of previous N closed candles.
    Returns (ok, ratio, avg_prev).
    """
    lb = VOL_SPIKE_LOOKBACK if lookback is None else lookback
    m = VOL_SPIKE_MULT if mult is None else mult
    if lb <= 0 or m <= 0:
        return True, 0.0, 0.0
    if len(prev_closed_base_vols) < lb:
        return False, 0.0, 0.0
    prev = prev_closed_base_vols[-lb:]
    avg = sum(prev) / lb
    if avg <= 0:
        return False, 0.0, 0.0
    ratio = current_base_vol / avg
    return ratio >= m, ratio, avg


def entry_vol_ok(
    current_base_vol: float,
    prev_closed_base_vols: list[float],
    *,
    prev_close_price: float = 0.0,
) -> tuple[bool, str, float]:
    """
    Entry volume gate.
    Default: current candle BASE >= VOL_SPIKE_MULT x avg(prev VOL_SPIKE_LOOKBACK).
    Fallback (USE_VOL_SPIKE=0): prev closed absolute base/quote floors.
    Returns (ok, log_label, measured).
    """
    if USE_VOL_SPIKE:
        ok, ratio, avg = current_vol_spike_ok(
            current_base_vol, prev_closed_base_vols
        )
        return (
            ok,
            f"cur_base={current_base_vol:.0f} avg{VOL_SPIKE_LOOKBACK}={avg:.0f} "
            f"x{ratio:.2f} (need >={VOL_SPIKE_MULT:g}x)",
            ratio,
        )
    if not prev_closed_base_vols:
        return False, "no prev candle", 0.0
    prev_base = float(prev_closed_base_vols[-1])
    if MIN_CANDLE_BASE_VOL > 0:
        ok, meas = prev_candle_base_ok(prev_base)
        return ok, f"prev_1m_base={meas:.0f}", meas
    ok, meas = prev_candle_quote_ok(prev_base, prev_close_price)
    return ok, f"prev_1m_quote={meas:.0f}", meas


def prev_candle_vol_ok(base_vol: float, close_price: float) -> tuple[bool, float]:
    """Legacy helper — prefer entry_vol_ok / current_vol_spike_ok."""
    if MIN_CANDLE_BASE_VOL > 0:
        return prev_candle_base_ok(base_vol)
    return prev_candle_quote_ok(base_vol, close_price)


def entry_rules_met(armed: bool, high: float, open_: float, entry_line: float) -> bool:
    if not armed:
        return False
    if high <= entry_line:
        return False
    if open_ <= 0:
        return False
    return candle_up_pct(open_, high) >= MIN_CANDLE_PCT


def price_entry_ready(
    armed: bool,
    tried_this_candle: bool,
    high: float,
    open_: float,
    entry_line: float,
) -> bool:
    """Price rules only — one evaluation per 1m candle (like backtest bar)."""
    if not armed or tried_this_candle:
        return False
    if high <= entry_line:
        return False
    if open_ <= 0:
        return False
    return candle_up_pct(open_, high) >= MIN_CANDLE_PCT


def entry_candle_stop_hit(entry_candle_low: float, price: float) -> tuple[bool, float]:
    """Stop when price breaks below the entry 1m candle low (structure stop)."""
    if not USE_ENTRY_CANDLE_STOP:
        return False, 0.0
    if entry_candle_low <= 0:
        return False, 0.0
    if price > entry_candle_low:
        return False, 0.0
    return True, max(entry_candle_low, price)


def hard_stop_hit(entry: float, price: float) -> tuple[bool, float]:
    """Fixed %% stop below entry (HARD_STOP_PCT)."""
    if HARD_STOP_PCT <= 0 or entry <= 0:
        return False, 0.0
    stop_px = entry * (1.0 - HARD_STOP_PCT / 100.0)
    if price > stop_px:
        return False, 0.0
    return True, max(stop_px, price)


def breakeven_stop_hit(
    entry: float, price: float, *, armed: bool
) -> tuple[bool, float]:
    """After partial: exit if price <= entry * (1 + BREAKEVEN_PCT/100)."""
    if not armed or entry <= 0:
        return False, 0.0
    stop_px = entry * (1.0 + BREAKEVEN_PCT / 100.0)
    if price > stop_px:
        return False, 0.0
    return True, max(stop_px, price)


def trail_should_arm(entry: float, high_water: float) -> bool:
    """Arm trail when price has traded +TRAIL_ACTIVATE_PCT% above entry (intrabar OK)."""
    if not USE_TRAIL:
        return False
    if entry <= 0 or high_water <= 0:
        return False
    return high_water >= entry * (1.0 + TRAIL_ACTIVATE_PCT / 100.0)


def next_ladder_partial(
    entry: float,
    high_water: float,
    done: list[bool],
) -> tuple[bool, float, float, int]:
    """
    Next unmet ladder step if high touched its %.
    Returns (hit, fill_px, frac_of_initial, step_index).
    """
    if entry <= 0 or high_water <= 0 or not PARTIAL_LADDER:
        return False, 0.0, 0.0, -1
    if len(done) < len(PARTIAL_LADDER):
        done = done + [False] * (len(PARTIAL_LADDER) - len(done))
    for i, (pct, frac) in enumerate(PARTIAL_LADDER):
        if done[i]:
            continue
        tp = entry * (1.0 + pct / 100.0)
        if high_water >= tp:
            return True, tp, frac, i
    return False, 0.0, 0.0, -1


def partial_tp_hit(
    entry: float, high_water: float, *, taken: bool
) -> tuple[bool, float]:
    """Legacy single-step helper."""
    if taken or PARTIAL_TP_PCT <= 0 or PARTIAL_TP_FRAC <= 0 or entry <= 0:
        return False, 0.0
    tp = entry * (1.0 + PARTIAL_TP_PCT / 100.0)
    if high_water < tp:
        return False, 0.0
    return True, tp


def trail_stop_hit(
    entry: float, high_water: float, price: float, *, armed: bool
) -> tuple[bool, float]:
    """Trailing stop — after arm; floor at entry (no trail loss)."""
    if not armed or entry <= 0 or high_water <= 0:
        return False, 0.0
    stop_px = max(entry, high_water * (1.0 - TRAIL_PCT / 100.0))
    if price > stop_px:
        return False, 0.0
    return True, max(stop_px, price)


def ema_exit_signal(
    ohlcv_1m: list[list[float]],
    tf: str | None = None,
) -> tuple[bool, float, float, str]:
    """Exit when last closed bar of `tf` (or EMA_EXIT_TF) closes below EMA9."""
    use_tf = tf or EMA_EXIT_TF
    if use_tf not in _EMA_TF_MS:
        use_tf = EMA_EXIT_TF
    if use_tf == "1m":
        if len(ohlcv_1m) < EMA_PERIOD:
            return False, 0.0, 0.0, ""
        closes = np.asarray([float(b[4]) for b in ohlcv_1m], dtype=float)
        ema = ema_series(closes, EMA_PERIOD)
        bar_close = float(closes[-1])
        ema_val = float(ema[-1])
    else:
        bars = aggregate_ohlcv(ohlcv_1m, ema_tf_ms(use_tf))
        if len(bars) < EMA_PERIOD:
            return False, 0.0, 0.0, ""
        closes = np.asarray([float(b[4]) for b in bars], dtype=float)
        ema = ema_series(closes, EMA_PERIOD)
        bar_close = float(bars[-1][4])
        ema_val = float(ema[-1])
    if bar_close >= ema_val:
        return False, bar_close, ema_val, ""
    reason = f"EMA{EMA_PERIOD} {use_tf} close {bar_close:.8f} < {ema_val:.8f}"
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
