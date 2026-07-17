"""Compare ladder exit with EMA9 on 1m / 3m / 5m (same data, same entry)."""
from __future__ import annotations

import asyncio
import importlib
import os

import ccxt.async_support as ccxt

BASE_ENV = [
    ("MIN_CANDLE_PCT", "2.5"),
    ("MIN_CANDLE_QUOTE_VOL", "10000"),
    ("PARTIAL_LADDER", "3:0.30,5:0.30"),
    ("USE_TRAIL", "1"),
    ("TRAIL_ACTIVATE_PCT", "10.0"),
    ("TRAIL_PCT", "3.0"),
    ("ORDER_USDT", "100"),
]
# Screenshot trades + prior AKE/OBOL set
BASES = [
    "JASMY",
    "BLUR",
    "SPELL",
    "AMP",
    "DODO",
    "BTTC",
    "DGB",
    "VANRY",
    "FORM",
    "AKE",
    "OBOL",
]
EMA_TFS = ("1m", "3m", "5m")


def _reload(ema_tf: str):
    for k, v in BASE_ENV:
        os.environ[k] = v
    os.environ["EMA_EXIT_TF"] = ema_tf
    import strategy as st
    import backtest as bt

    importlib.reload(st)
    importlib.reload(bt)
    return st, bt


def _resolve(markets: dict, base: str) -> str | None:
    for cand in (f"{base}/USDT", f"{base}/USDT:USDT"):
        if cand in markets:
            return cand
    # BTTC sometimes listed as BTT
    if base == "BTTC":
        for cand in ("BTTC/USDT", "BTT/USDT", "BTTC/USDT:USDT", "BTT/USDT:USDT"):
            if cand in markets:
                return cand
    return None


async def main() -> None:
    spot = ccxt.binance({"enableRateLimit": True, "options": {"defaultType": "spot"}})
    fut = ccxt.binanceusdm({"enableRateLimit": True})
    await spot.load_markets()
    await fut.load_markets()

    resolved: list[tuple[str, str, object]] = []
    for base in BASES:
        sym = _resolve(spot.markets, base)
        if sym:
            resolved.append((base, sym, spot))
            continue
        sym = _resolve(fut.markets, base)
        if sym:
            resolved.append((base, sym, fut))
            print(f"NOTE {base}: spot yok → futures {sym}")
        else:
            print(f"SKIP {base}: neither spot nor futures")

    st0, bt0 = _reload("1m")
    now = int(spot.milliseconds())
    trade_start = now - 5 * 86_400_000
    since = trade_start - st0.FBB_LENGTH * 60_000

    cache: dict[str, list] = {}
    for base, sym, ex in resolved:
        try:
            ohlcv = await bt0.fetch_ohlcv_range(ex, sym, since, quiet=True)
        except Exception as e:
            print(f"FAIL fetch {sym}: {e}")
            continue
        if len(ohlcv) < st0.FBB_LENGTH + 50:
            print(f"SKIP {sym}: only {len(ohlcv)} bars")
            continue
        cache[sym] = ohlcv
        print(f"cached {sym}: {len(ohlcv)} bars")

    await spot.close()
    await fut.close()

    if not cache:
        print("No data — abort")
        return

    totals: dict[str, float] = {}
    counts: dict[str, int] = {}
    per_sym: dict[str, dict[str, float]] = {tf: {} for tf in EMA_TFS}

    for ema_tf in EMA_TFS:
        st, bt = _reload(ema_tf)
        print("\n" + "#" * 72)
        print(
            f"# EMA{st.EMA_PERIOD} {ema_tf} | ladder {st.PARTIAL_LADDER} | "
            f"trail +{st.TRAIL_ACTIVATE_PCT:g}/-{st.TRAIL_PCT:g}"
        )
        print("#" * 72)
        all_t = []
        for sym, ohlcv in cache.items():
            print(f"\n### {sym} | EMA {ema_tf}")
            trades, _ = bt.run_backtest(sym, ohlcv, trade_start, verbose=True)
            bt.print_symbol_summary(
                sym, trades, ohlcv, f"last 5d | EMA{st.EMA_PERIOD} {ema_tf}"
            )
            pnl = sum(t.pnl_usdt for t in trades)
            per_sym[ema_tf][sym] = pnl
            all_t.extend(trades)
        total = sum(t.pnl_usdt for t in all_t)
        totals[ema_tf] = total
        counts[ema_tf] = len(all_t)
        print(f"\n>>> EMA {ema_tf} COMBINED {total:+.4f} USDT | trades={len(all_t)}")

    print("\n" + "=" * 72)
    print("PER-SYMBOL PnL (5d)")
    print("=" * 72)
    hdr = f"{'symbol':22} | {'EMA1m':>10} | {'EMA3m':>10} | {'EMA5m':>10}"
    print(hdr)
    print("-" * len(hdr))
    for sym in cache:
        print(
            f"{sym:22} | "
            f"{per_sym['1m'].get(sym, 0):+10.4f} | "
            f"{per_sym['3m'].get(sym, 0):+10.4f} | "
            f"{per_sym['5m'].get(sym, 0):+10.4f}"
        )
    print("-" * len(hdr))
    print(
        f"{'TOTAL':22} | "
        f"{totals['1m']:+10.4f} | "
        f"{totals['3m']:+10.4f} | "
        f"{totals['5m']:+10.4f}"
    )
    print("=" * 72)
    print("COMPARE SUMMARY (same ladder, same 5d data)")
    for ema_tf in EMA_TFS:
        print(
            f"  EMA9 {ema_tf:>2} : {totals[ema_tf]:+.4f} USDT | "
            f"trades={counts[ema_tf]}"
        )
    best = max(EMA_TFS, key=lambda t: totals[t])
    print(f"BEST: EMA9 {best} ({totals[best]:+.4f} USDT)")
    print("=" * 72)


if __name__ == "__main__":
    asyncio.run(main())
