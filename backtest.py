"""
FBB Instant Breakout backtest (1m) — Binance Spot.

Entry: armed persist (grey dip sticks) + red break + 10K vol + 3x + 4% candle
Exit: entry-candle-low | EMA9 5m until 1m CLOSE +trail activate | then trail
"""

from __future__ import annotations

import asyncio
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import ccxt.async_support as ccxt
from dotenv import load_dotenv

from indicators import fibonacci_bollinger
from markets import list_spot_usdt_symbols
from strategy import (
    EMA_EXIT_TF,
    EMA_PERIOD,
    EXIT_TF_MS,
    FBB_LENGTH,
    FBB_MULT,
    FEE_RATE,
    MIN_CANDLE_PCT,
    MIN_CANDLE_QUOTE_VOL,
    ORDER_USDT,
    PARTIAL_LADDER,
    TIMEFRAME,
    TRAIL_ACTIVATE_PCT,
    TRAIL_PCT,
    USE_TRAIL,
    aggregate_ohlcv,
    broke_entry_line,
    candle_up_pct,
    ema_exit_signal,
    entry_candle_stop_hit,
    entry_fill_price,
    five_m_just_closed,
    ladder_label,
    next_ladder_partial,
    prev_candle_quote_ok,
    trail_should_arm,
    trail_stop_hit,
    update_armed,
)

load_dotenv(Path(__file__).resolve().parent / ".env")

API_KEY = os.getenv("BINANCE_API_KEY", "").strip()
API_SECRET = os.getenv("BINANCE_SECRET", "").strip()

LOOKBACK_DAYS = 5

# Manual test symbols (empty → gainer list or full universe)
SYMBOLS: list[str] = [
    "STG/USDT",
    "H/USDT",
    "BEAT/USDT",
    "GRASS/USDT",
    "M/USDT",
    "OBOL/USDT",
    "AKE/USDT",
]
GAINER_TICKERS: list[str] = []
UNIVERSE_LIMIT = 100

MAX_ENTRIES_PER_DAY = 999

@dataclass
class Trade:
    symbol: str
    entry_time: int
    entry_price: float
    exit_time: int
    exit_price: float
    qty: float
    pnl_usdt: float
    pnl_pct: float
    fees: float
    exit_reason: str
    vol_ratio: float
    candle_pct: float


@dataclass
class SymbolResult:
    symbol: str
    trades: list[Trade] = field(default_factory=list)
    skipped_vol: int = 0
    candles: int = 0
    error: str = ""

    @property
    def pnl(self) -> float:
        return sum(t.pnl_usdt for t in self.trades)


def ms_to_str(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M UTC"
    )


def make_exchange() -> ccxt.binance:
    """Binance Spot Demo — same feed style as bot.py."""
    cfg: dict = {
        "enableRateLimit": True,
        "options": {"defaultType": "spot"},
    }
    if API_KEY and API_SECRET:
        cfg["apiKey"] = API_KEY
        cfg["secret"] = API_SECRET
    exchange = ccxt.binance(cfg)
    if API_KEY and API_SECRET:
        exchange.enable_demo_trading(True)
    return exchange


async def load_universe(exchange: ccxt.Exchange) -> list[str]:
    """All active spot USDT pairs — same as bot.py."""
    return list_spot_usdt_symbols(exchange.markets)


def resolve_gainer_symbols(exchange: ccxt.Exchange) -> tuple[list[str], list[str]]:
    """Map CMC gainer tickers to exact Binance spot symbols."""
    found: list[str] = []
    missing: list[str] = []
    for t in GAINER_TICKERS:
        sym = f"{t}/USDT"
        if sym in exchange.markets:
            found.append(sym)
        else:
            missing.append(t)
    return found, missing


async def fetch_ohlcv_range(
    exchange: ccxt.Exchange,
    symbol: str,
    since: int,
    *,
    quiet: bool = False,
) -> list[list[float]]:
    all_candles: list[list[float]] = []
    limit = 1000
    tf_ms = exchange.parse_timeframe(TIMEFRAME) * 1000
    cursor = since

    while True:
        batch = await exchange.fetch_ohlcv(symbol, TIMEFRAME, since=cursor, limit=limit)
        if not batch:
            break
        if all_candles and batch[0][0] == all_candles[-1][0]:
            batch = batch[1:]
        if not batch:
            break
        all_candles.extend(batch)
        last_ts = int(batch[-1][0])
        if not quiet:
            print(f"  ... {len(all_candles)} candles | last {ms_to_str(last_ts)}")
        cursor = last_ts + tf_ms
        now = int(exchange.milliseconds())
        if last_ts >= now - tf_ms:
            break
        if len(batch) < limit:
            break
        await asyncio.sleep(max(exchange.rateLimit / 1000, 0.15))

    if all_candles:
        now = int(exchange.milliseconds())
        if now - int(all_candles[-1][0]) < tf_ms:
            all_candles = all_candles[:-1]

    return all_candles


def run_backtest(
    symbol: str,
    ohlcv: list[list[float]],
    trade_start_ms: int,
    *,
    verbose: bool = True,
) -> tuple[list[Trade], int]:
    trades: list[Trade] = []
    in_pos = False
    entry_price = 0.0
    entry_time = 0
    qty = 0.0
    rem_qty = 0.0
    entry_vol_ratio = 0.0
    entry_candle_pct = 0.0
    entry_candle_low = 0.0
    high_water = 0.0
    trail_armed = False
    ladder_done = [False] * len(PARTIAL_LADDER)
    realized_pnl = 0.0
    realized_fees = 0.0
    armed = False
    skipped_vol = 0
    n = len(ohlcv)
    # Funnel counters (why setups die)
    cnt_armed_break = 0
    cnt_fail_vol_abs = 0
    cnt_fail_pct = 0
    cnt_pass = 0

    def _sell_qty(sell_q: float, exit_px: float) -> tuple[float, float]:
        buy_fee = sell_q * entry_price * FEE_RATE
        sell_fee = sell_q * exit_px * FEE_RATE
        pnl = sell_q * (exit_px - entry_price) - buy_fee - sell_fee
        return pnl, buy_fee + sell_fee

    def _close_pos(exit_ts: int, exit_px: float, reason: str) -> None:
        nonlocal in_pos, armed, rem_qty, realized_pnl, realized_fees
        if rem_qty > 0:
            pnl, fees = _sell_qty(rem_qty, exit_px)
            realized_pnl += pnl
            realized_fees += fees
            rem_qty = 0.0
        pnl_pct = (exit_px / entry_price - 1.0) * 100.0 if entry_price else 0.0
        trades.append(
            Trade(
                symbol=symbol,
                entry_time=entry_time,
                entry_price=entry_price,
                exit_time=exit_ts,
                exit_price=exit_px,
                qty=qty,
                pnl_usdt=realized_pnl,
                pnl_pct=(realized_pnl / ORDER_USDT) * 100.0 if ORDER_USDT else pnl_pct,
                fees=realized_fees,
                exit_reason=reason,
                vol_ratio=entry_vol_ratio,
                candle_pct=entry_candle_pct,
            )
        )
        if verbose:
            sign = "+" if realized_pnl >= 0 else ""
            print(
                f"SELL #{len(trades):03d} | {ms_to_str(exit_ts)} | "
                f"price={exit_px:.8f} | {reason} | "
                f"PnL={sign}{realized_pnl:.4f} USDT ({sign}{pnl_pct:.2f}%)"
            )
        in_pos = False
        armed = False
        realized_pnl = 0.0
        realized_fees = 0.0

    if verbose:
        print(f"\nRunning backtest on {n} closed 1m candles...\n")
        print(
            f"Entry: armed persist + FBB 0.786 wick break | "
            f"prev 1m vol >= {MIN_CANDLE_QUOTE_VOL:.0f} | candle >= {MIN_CANDLE_PCT}%\n"
        )
        partial_txt = (ladder_label() + " + ") if PARTIAL_LADDER else ""
        trail_txt = (
            f"then +{TRAIL_ACTIVATE_PCT:.0f}% trail -{TRAIL_PCT:.0f}%"
            if USE_TRAIL
            else "no trail"
        )
        print(
            f"Exit: entry-candle-low | {partial_txt}EMA{EMA_PERIOD} {EMA_EXIT_TF} "
            f"until trail ({trail_txt})\n"
        )
        print("-" * 72)

    for i in range(FBB_LENGTH, n):
        closed = ohlcv[:i]
        candle = ohlcv[i]
        ts = int(candle[0])
        o = float(candle[1])
        h = float(candle[2])
        l = float(candle[3])
        c = float(candle[4])
        v = float(candle[5])

        if in_pos:
            high_water = max(high_water, h)

            # Ladder partials (can fire multiple steps on same bar)
            while rem_qty > 0:
                hit_tp, tp_px, frac, step_i = next_ladder_partial(
                    entry_price, high_water, ladder_done
                )
                if not hit_tp:
                    break
                sell_q = min(qty * frac, rem_qty)
                pnl, fees = _sell_qty(sell_q, tp_px)
                realized_pnl += pnl
                realized_fees += fees
                rem_qty -= sell_q
                ladder_done[step_i] = True
                pct_lvl = PARTIAL_LADDER[step_i][0]
                if verbose:
                    print(
                        f"PART #{len(trades)+1:03d} | {ms_to_str(ts)} | "
                        f"price={tp_px:.8f} | ladder +{pct_lvl:g}% | "
                        f"sold {frac*100:.0f}% init | leg={pnl:+.4f} USDT"
                    )

            if USE_TRAIL and trail_should_arm(entry_price, high_water):
                trail_armed = True

            if USE_TRAIL:
                hit, fill = trail_stop_hit(
                    entry_price, high_water, l, armed=trail_armed
                )
                if hit:
                    _close_pos(ts, fill, f"trail -{TRAIL_PCT}%")
                    continue

            hit, fill = entry_candle_stop_hit(entry_candle_low, l)
            if hit:
                any_part = any(ladder_done)
                reason = (
                    "entry candle low (runner)" if any_part else "entry candle low"
                )
                _close_pos(ts, fill, reason)
                continue

            check_ema = EMA_EXIT_TF == "1m" or five_m_just_closed(ohlcv, i)
            if not trail_armed and check_ema:
                should_exit, bar_close, ema_val, reason = ema_exit_signal(
                    ohlcv[: i + 1]
                )
                if should_exit:
                    if any(ladder_done):
                        reason = f"EMA runner | {reason}"
                    _close_pos(ts, bar_close, reason)
                    continue

        fbb = fibonacci_bollinger(closed, length=FBB_LENGTH, mult=FBB_MULT)
        if fbb is None:
            continue
        upper_0236, upper_0786, upper_1000, _ = fbb

        if in_pos:
            continue

        if ts < trade_start_ms:
            armed = update_armed(armed, o, l, upper_0236)
            continue

        armed = update_armed(armed, o, l, upper_0236)

        if not (armed and broke_entry_line(h, upper_0786)):
            continue
        cnt_armed_break += 1

        if not closed:
            skipped_vol += 1
            cnt_fail_vol_abs += 1
            continue
        prev = closed[-1]
        vol_pass, quote_vol = prev_candle_quote_ok(float(prev[5]), float(prev[4]))
        if not vol_pass:
            skipped_vol += 1
            cnt_fail_vol_abs += 1
            continue

        candle_pct = candle_up_pct(o, h)
        if candle_pct < MIN_CANDLE_PCT:
            cnt_fail_pct += 1
            continue

        cnt_pass += 1
        fill = entry_fill_price(o, h, l, upper_0786)
        qty = ORDER_USDT / fill
        rem_qty = qty
        entry_price = fill
        entry_time = ts
        entry_candle_low = l
        high_water = fill
        trail_armed = False
        ladder_done = [False] * len(PARTIAL_LADDER)
        realized_pnl = 0.0
        realized_fees = 0.0
        entry_vol_ratio = quote_vol
        entry_candle_pct = candle_pct
        in_pos = True
        armed = False
        if verbose:
            print(
                f"BUY  #{len(trades)+1:03d} | {ms_to_str(ts)} | "
                f"price={fill:.8f} | grey={upper_0236:.8f} | "
                f"entry0786={upper_0786:.8f} | red={upper_1000:.8f} | "
                f"prev_1m_vol={quote_vol/1e3:.0f}K | "
                f"up={candle_pct:.2f}% | low={l:.8f} open={o:.8f}"
            )

    if verbose and in_pos:
        last = ohlcv[-1]
        print(
            f"\nOPEN POSITION still held | entry={entry_price:.8f} "
            f"@ {ms_to_str(entry_time)} | last_close={float(last[4]):.8f}"
        )
    if verbose:
        print("\nFilter funnel:")
        print(f"  FBB armed persist + 0.786 break : {cnt_armed_break}")
        print(
            f"  fail prev 1m quote vol: {cnt_fail_vol_abs}  "
            f"(need >= {MIN_CANDLE_QUOTE_VOL:.0f})"
        )
        print(f"  fail candle pct     : {cnt_fail_pct}  (need >= {MIN_CANDLE_PCT}%)")
        print(f"  passed all (entries): {cnt_pass}")

    return trades, skipped_vol


def print_symbol_summary(
    symbol: str,
    trades: list[Trade],
    ohlcv: list[list[float]],
    window_label: str,
) -> float:
    """AKE-style summary: dates first, then numbered BUY/SELL list."""
    print("\n" + "=" * 72)
    print(f"RESULT - {symbol} | {window_label} | 1m FBB 0.786 break + EMA9/trail")
    print("=" * 72)
    if not ohlcv:
        print("No data.")
        return 0.0

    print(
        f"Data range : {ms_to_str(int(ohlcv[0][0]))} -> {ms_to_str(int(ohlcv[-1][0]))}"
    )
    print(f"Candles    : {len(ohlcv)}")
    print(f"Trades     : {len(trades)}")
    print(f"Notional   : {ORDER_USDT} USDT / trade")
    print(f"1m vol min : prev closed >= {MIN_CANDLE_QUOTE_VOL:.0f} USDT (no rel)")
    print(f"Candle up  : >= {MIN_CANDLE_PCT}% (high-open)/open")
    print("Armed       : grey dip persists until FBB 0.786 break")
    print(
        f"Exit        : entry-candle-low | "
        f"{(ladder_label() + ' + ') if PARTIAL_LADDER else ''}"
        f"EMA{EMA_PERIOD} {EMA_EXIT_TF}"
        f"{'' if not USE_TRAIL else f' / +{TRAIL_ACTIVATE_PCT:.0f}% trail -{TRAIL_PCT:.0f}%'}"
    )

    if not trades:
        print("\nNo trades triggered.")
        print("=" * 72)
        print("OVERALL PnL   : +0.0000 USDT")
        print("=" * 72)
        return 0.0

    total_pnl = sum(t.pnl_usdt for t in trades)
    total_fees = sum(t.fees for t in trades)
    wins = [t for t in trades if t.pnl_usdt > 0]
    losses = [t for t in trades if t.pnl_usdt <= 0]
    avg = total_pnl / len(trades)
    best = max(trades, key=lambda t: t.pnl_usdt)
    worst = min(trades, key=lambda t: t.pnl_usdt)

    print("-" * 72)
    for i, t in enumerate(trades, 1):
        sign = "+" if t.pnl_usdt >= 0 else ""
        print(
            f"{i:03d} | BUY  {ms_to_str(t.entry_time)} @ {t.entry_price:.8f} | "
            f"1m_vol {t.vol_ratio/1e3:.0f}K | up {t.candle_pct:.1f}%\n"
            f"    | SELL {ms_to_str(t.exit_time)} @ {t.exit_price:.8f} | "
            f"{t.exit_reason} | "
            f"PnL {sign}{t.pnl_usdt:.4f} USDT ({sign}{t.pnl_pct:.2f}%)"
        )

    print("-" * 72)
    print(f"Wins / Losses : {len(wins)} / {len(losses)}")
    print(f"Win rate      : {len(wins)/len(trades)*100:.1f}%")
    print(f"Total fees    : {total_fees:.4f} USDT")
    print(f"Avg PnL/trade : {avg:+.4f} USDT")
    print(f"Best trade    : {best.pnl_usdt:+.4f} USDT")
    print(f"Worst trade   : {worst.pnl_usdt:+.4f} USDT")
    print("=" * 72)
    sign = "+" if total_pnl >= 0 else ""
    print(f"OVERALL PnL   : {sign}{total_pnl:.4f} USDT")
    print("=" * 72)
    return total_pnl


async def run_symbol(
    exchange: ccxt.Exchange,
    symbol: str,
    trade_start: int,
    since: int,
    *,
    idx: int = 0,
    total: int = 0,
    verbose: bool = True,
) -> SymbolResult:
    result = SymbolResult(symbol=symbol)
    try:
        if verbose:
            print("\n" + "#" * 72)
            print(f"# {symbol}")
            print("#" * 72)
            print(f"Fetching {symbol} {TIMEFRAME} from {ms_to_str(since)}...")

        ohlcv = await fetch_ohlcv_range(exchange, symbol, since, quiet=not verbose)
        result.candles = len(ohlcv)
        if len(ohlcv) < FBB_LENGTH + 5:
            result.error = f"not enough candles ({len(ohlcv)})"
            if verbose:
                print(result.error)
            else:
                print(f"[{idx:3d}/{total}] {symbol:22s} ERR {result.error}")
            return result

        if verbose:
            print(
                f"Data range : {ms_to_str(int(ohlcv[0][0]))} -> "
                f"{ms_to_str(int(ohlcv[-1][0]))} | candles={len(ohlcv)}"
            )

        trades, skipped = run_backtest(
            symbol, ohlcv, trade_start, verbose=verbose
        )
        result.trades = trades
        result.skipped_vol = skipped
        if verbose:
            print_symbol_summary(
                symbol, trades, ohlcv, f"last {LOOKBACK_DAYS}d"
            )
        else:
            sign = "+" if result.pnl >= 0 else ""
            w = sum(1 for t in trades if t.pnl_usdt > 0)
            if trades:
                print(
                    f"[{idx:3d}/{total}] {symbol:22s} "
                    f"trades={len(trades):2d} W/L={w}/{len(trades)-w} "
                    f"PnL={sign}{result.pnl:.4f} USDT"
                )
            else:
                print(f"[{idx:3d}/{total}] {symbol:22s} trades=0")
    except Exception as exc:  # noqa: BLE001
        result.error = str(exc)
        print(f"ERR {symbol}: {exc}")
    return result


def utc_day_key(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def pump_score(t: Trade) -> float:
    """Higher = more 'explosion-like' entry (quote_vol in millions * pct)."""
    return (t.vol_ratio / 1_000_000.0) * t.candle_pct


def select_pump_trades(
    all_trades: list[Trade],
    max_per_day: int = MAX_ENTRIES_PER_DAY,
) -> list[Trade]:
    """
    Live-bot style selection across the whole market:
    - 1 position at a time
    - max `max_per_day` new entries per UTC day
    - when several signals share the same entry bar, keep the highest score
    """
    if not all_trades:
        return []

    # Same-bar multi-symbol race → keep highest pump score
    best_at_ts: dict[int, Trade] = {}
    for t in all_trades:
        prev = best_at_ts.get(t.entry_time)
        if prev is None or pump_score(t) > pump_score(prev):
            best_at_ts[t.entry_time] = t

    ordered = sorted(
        best_at_ts.values(),
        key=lambda t: (t.entry_time, -pump_score(t)),
    )

    selected: list[Trade] = []
    day_counts: dict[str, int] = {}
    busy_until = 0

    for t in ordered:
        if t.entry_time < busy_until:
            continue
        day = utc_day_key(t.entry_time)
        if day_counts.get(day, 0) >= max_per_day:
            continue
        selected.append(t)
        day_counts[day] = day_counts.get(day, 0) + 1
        busy_until = t.exit_time + 1

    return selected


def print_trade_block(title: str, trades: list[Trade], days: int) -> float:
    print("\n" + "#" * 72)
    print(f"# {title}")
    print("#" * 72)
    if not trades:
        print("No trades.")
        return 0.0

    wins = [t for t in trades if t.pnl_usdt > 0]
    losses = [t for t in trades if t.pnl_usdt <= 0]
    best = max(trades, key=lambda t: t.pnl_usdt)
    worst = min(trades, key=lambda t: t.pnl_usdt)
    grand = sum(t.pnl_usdt for t in trades)
    per_day = len(trades) / max(days, 1)

    print(f"Trades          : {len(trades)}")
    print(f"Avg trades/day  : {per_day:.2f}")
    print(f"Wins / Losses   : {len(wins)} / {len(losses)}")
    print(f"Win rate        : {len(wins)/len(trades)*100:.1f}%")
    print(f"Best trade      : {best.symbol} {best.pnl_usdt:+.4f} USDT ({best.pnl_pct:+.1f}%)")
    print(f"Worst trade     : {worst.symbol} {worst.pnl_usdt:+.4f} USDT")

    print("\n--- Trades ---")
    for i, t in enumerate(sorted(trades, key=lambda x: x.entry_time), 1):
        sign = "+" if t.pnl_usdt >= 0 else ""
        print(
            f"{i:03d} | {t.symbol:20s} | "
            f"BUY {ms_to_str(t.entry_time)} @ {t.entry_price:.8f} | "
            f"1m_vol {t.vol_ratio/1e3:.0f}K USDT up {t.candle_pct:.1f}% "
            f"score={pump_score(t):.0f}\n"
            f"    | SELL {ms_to_str(t.exit_time)} @ {t.exit_price:.8f} | "
            f"PnL {sign}{t.pnl_usdt:.4f} USDT ({sign}{t.pnl_pct:.2f}%)"
        )

    print("-" * 72)
    sign = "+" if grand >= 0 else ""
    print(f"TOTAL PnL : {sign}{grand:.4f} USDT")
    print("#" * 72)
    return grand


async def main() -> None:
    exchange = make_exchange()
    try:
        await exchange.load_markets()
        print("Data source: Binance Spot Demo (enable_demo_trading)")
        if SYMBOLS:
            symbols = [s for s in SYMBOLS if s in exchange.markets]
            missing = [s for s in SYMBOLS if s not in exchange.markets]
            for m in missing:
                print(f"WARN: symbol not found: {m}")
            print(f"Universe: {len(symbols)} symbols (manual list)")
        elif GAINER_TICKERS:
            symbols, missing_tickers = resolve_gainer_symbols(exchange)
            print(
                f"Gainer list: {len(GAINER_TICKERS)} tickers | "
                f"{len(symbols)} on Binance Spot | "
                f"{len(missing_tickers)} not listed"
            )
            if missing_tickers:
                print("Not on Binance Spot:", ", ".join(missing_tickers))
            print(f"Universe: {symbols}")
        else:
            symbols = await load_universe(exchange)
            total_uni = len(symbols)
            if UNIVERSE_LIMIT and UNIVERSE_LIMIT > 0:
                symbols = symbols[:UNIVERSE_LIMIT]
            print(
                f"Universe: {len(symbols)} / {total_uni} spot USDT pairs "
                f"(first {UNIVERSE_LIMIT or 'all'}, sorted A-Z, no 24h filter)"
            )
        partial_bit = (ladder_label() + " + ") if PARTIAL_LADDER else ""
        trail_bit = (
            f" / +{TRAIL_ACTIVATE_PCT:.0f}% trail -{TRAIL_PCT:.0f}%"
            if USE_TRAIL
            else " / no trail"
        )
        print(
            f"BACKTEST | spot | last {LOOKBACK_DAYS}d | 1m FBB 0.786 break | "
            f"prev 1m vol>={MIN_CANDLE_QUOTE_VOL:.0f} USDT | "
            f"candle>={MIN_CANDLE_PCT}% | "
            f"exit entry-candle-low / {partial_bit}EMA{EMA_PERIOD} {EMA_EXIT_TF}{trail_bit} | "
            f"notional {ORDER_USDT} USDT"
        )
        print("-" * 72)

        now = int(exchange.milliseconds())
        day_ms = 24 * 60 * 60 * 1000
        tf_ms = exchange.parse_timeframe(TIMEFRAME) * 1000
        trade_start = now - LOOKBACK_DAYS * day_ms
        since_base = trade_start - FBB_LENGTH * tf_ms

        results: list[SymbolResult] = []
        verbose = len(symbols) <= 10

        for idx, symbol in enumerate(symbols, 1):
            market = exchange.market(symbol)
            created = market.get("created")
            since = since_base
            if created and int(created) > since:
                since = int(created)

            res = await run_symbol(
                exchange,
                symbol,
                trade_start,
                since,
                idx=idx,
                total=len(symbols),
                verbose=verbose,
            )
            results.append(res)

        # Combined AKE-style list
        all_trades = [t for r in results for t in r.trades]
        pump_trades = select_pump_trades(all_trades, MAX_ENTRIES_PER_DAY)

        print("\n" + "#" * 72)
        print("# COMBINED — all symbols")
        print("#" * 72)
        grand = 0.0
        for r in results:
            sign = "+" if r.pnl >= 0 else ""
            print(
                f"{r.symbol:22s}  trades={len(r.trades):2d}  "
                f"PnL={sign}{r.pnl:.4f} USDT"
            )
            grand += r.pnl
        sign = "+" if grand >= 0 else ""
        print("-" * 72)
        print(f"{'SUM (independent)':22s}  PnL={sign}{grand:.4f} USDT")

        if len(symbols) > 1:
            print_trade_block(
                f"PUMP HUNTER (1 position + max {MAX_ENTRIES_PER_DAY}/day)",
                pump_trades,
                LOOKBACK_DAYS,
            )
    finally:
        await exchange.close()


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    asyncio.run(main())
