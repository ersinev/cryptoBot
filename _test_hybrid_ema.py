"""Compare fixed5m vs progressive 1m3m5m vs progressive 1m3m."""
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
    ("EMA_EXIT_TF", "5m"),
]
BASES = [
    "KAITO",
    "POWR",
    "ATM",
    "FORM",
    "SYN",
    "G",
    "STRAX",
    "BLUR",
    "ACT",
    "VANRY",
    "DGB",
    "AMP",
    "DODO",
    "JASMY",
]
# name, EMA_PROGRESSIVE, EMA_PROG_MODE
MODES = (
    ("fixed5m", "0", "1m3m5m"),
    ("hybrid_1m3m5m", "1", "1m3m5m"),
    ("hybrid_1m3m", "1", "1m3m"),
)


def _reload(progressive: str, mode: str):
    for k, v in BASE_ENV:
        os.environ[k] = v
    os.environ["EMA_PROGRESSIVE"] = progressive
    os.environ["EMA_PROG_MODE"] = mode
    import strategy as st
    import backtest as bt

    importlib.reload(st)
    importlib.reload(bt)
    return st, bt


def _resolve(markets: dict, base: str) -> str | None:
    for cand in (f"{base}/USDT", f"{base}/USDT:USDT"):
        if cand in markets:
            return cand
    return None


async def main() -> None:
    spot = ccxt.binance({"enableRateLimit": True, "options": {"defaultType": "spot"}})
    await spot.load_markets()

    resolved: list[str] = []
    for base in BASES:
        sym = _resolve(spot.markets, base)
        if not sym:
            print(f"SKIP {base}")
            continue
        resolved.append(sym)

    st0, bt0 = _reload("0", "1m3m5m")
    now = int(spot.milliseconds())
    trade_start = now - 5 * 86_400_000
    since = trade_start - st0.FBB_LENGTH * 60_000

    cache: dict[str, list] = {}
    for sym in resolved:
        ohlcv = await bt0.fetch_ohlcv_range(spot, sym, since, quiet=True)
        if len(ohlcv) < st0.FBB_LENGTH + 50:
            print(f"SKIP {sym}: {len(ohlcv)} bars")
            continue
        cache[sym] = ohlcv
        print(f"cached {sym}: {len(ohlcv)} bars")
    await spot.close()

    results: dict[str, dict] = {}

    for mode_name, prog, prog_mode in MODES:
        st, bt = _reload(prog, prog_mode)
        print("\n" + "#" * 72)
        print(
            f"# {mode_name} | progressive={st.EMA_PROGRESSIVE} | "
            f"mode={st.EMA_PROG_MODE}"
        )
        print("#" * 72)
        all_t = []
        per: dict[str, float] = {}
        for sym, ohlcv in cache.items():
            trades, _ = bt.run_backtest(sym, ohlcv, trade_start, verbose=False)
            pnl = sum(t.pnl_usdt for t in trades)
            per[sym] = pnl
            all_t.extend(trades)
            print(f"\n{sym} | {mode_name} | trades={len(trades)} | PnL {pnl:+.4f}")
            for i, t in enumerate(trades, 1):
                print(
                    f"  {i:02d} BUY {bt.ms_to_str(t.entry_time)} @ {t.entry_price:.8g} "
                    f"-> SELL {bt.ms_to_str(t.exit_time)} @ {t.exit_price:.8g} | "
                    f"{t.exit_reason} | {t.pnl_usdt:+.4f}"
                )
        total = sum(t.pnl_usdt for t in all_t)
        results[mode_name] = {
            "total": total,
            "trades": len(all_t),
            "per": per,
            "wins": sum(1 for t in all_t if t.pnl_usdt > 0),
            "losses": sum(1 for t in all_t if t.pnl_usdt <= 0),
        }
        print(f"\n>>> {mode_name} COMBINED {total:+.4f} | trades={len(all_t)}")

    names = [m[0] for m in MODES]
    print("\n" + "=" * 88)
    print("PER-SYMBOL COMPARE (5d)")
    print("=" * 88)
    hdr = f"{'symbol':16}" + "".join(f" | {n:>12}" for n in names)
    print(hdr)
    print("-" * len(hdr))
    for sym in cache:
        row = f"{sym:16}"
        for n in names:
            row += f" | {results[n]['per'].get(sym, 0.0):+12.4f}"
        print(row)
    print("-" * len(hdr))
    tot = f"{'TOTAL':16}"
    for n in names:
        tot += f" | {results[n]['total']:+12.4f}"
    print(tot)
    print("=" * 88)
    for n in names:
        r = results[n]
        print(
            f"{n}: PnL={r['total']:+.4f} | trades={r['trades']} | "
            f"W/L={r['wins']}/{r['losses']}"
        )
    best = max(names, key=lambda n: results[n]["total"])
    print(f"BEST: {best} ({results[best]['total']:+.4f})")
    print("=" * 88)


if __name__ == "__main__":
    asyncio.run(main())
