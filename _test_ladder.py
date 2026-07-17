"""Test ladder exit: 30%@+3, 30%@+5, trail +10/-3, EMA5m."""
from __future__ import annotations

import asyncio
import os

for k, v in [
    ("MIN_CANDLE_PCT", "2.5"),
    ("MIN_CANDLE_QUOTE_VOL", "10000"),
    ("PARTIAL_LADDER", "3:0.30,5:0.30"),
    ("USE_TRAIL", "1"),
    ("EMA_EXIT_TF", "5m"),
    ("TRAIL_ACTIVATE_PCT", "10.0"),
    ("TRAIL_PCT", "3.0"),
]:
    os.environ[k] = v

import importlib

import strategy as st

importlib.reload(st)
import backtest as bt

importlib.reload(bt)
import ccxt.async_support as ccxt


async def main() -> None:
    print(
        "LADDER",
        st.PARTIAL_LADDER,
        "trail",
        st.TRAIL_ACTIVATE_PCT,
        st.TRAIL_PCT,
        "ema",
        st.EMA_EXIT_TF,
        "candle",
        st.MIN_CANDLE_PCT,
    )
    fut = ccxt.binanceusdm({"enableRateLimit": True})
    await fut.load_markets()
    now = int(fut.milliseconds())
    trade_start = now - 5 * 86_400_000
    since = trade_start - st.FBB_LENGTH * 60_000
    all_t = []
    for sym in ["AKE/USDT:USDT", "OBOL/USDT:USDT"]:
        ohlcv = await bt.fetch_ohlcv_range(fut, sym, since, quiet=True)
        print(f"\n### {sym}")
        trades, _ = bt.run_backtest(sym, ohlcv, trade_start, verbose=True)
        bt.print_symbol_summary(sym, trades, ohlcv, "last 5d ladder")
        all_t.extend(trades)
    await fut.close()
    total = sum(t.pnl_usdt for t in all_t)
    print(f"\nCOMBINED TOTAL {total:+.4f} USDT | trades={len(all_t)}")


if __name__ == "__main__":
    asyncio.run(main())
