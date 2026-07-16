"""
Quick live health check for FBB bot.

Order test modes:
  --order-test     100 USDT BTC buy + sell (round-trip)
  --buy-only       100 USDT BTC buy only (open position stays — easiest to see in UI)
  --order-only     Skip health checks, run --order-test
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from pathlib import Path

import ccxt.pro as ccxtpro
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

from strategy import (  # noqa: E402
    FBB_LENGTH,
    MIN_CANDLE_PCT,
    MIN_CANDLE_QUOTE_VOL,
    OHLCV_LIMIT,
    ORDER_USDT,
    TIMEFRAME,
    VOL_LOOKBACK,
    VOL_MULT,
)
from bot import API_KEY, API_SECRET  # noqa: E402

TEST_USDT = 100.0
TEST_SYMBOL = "BTC/USDT:USDT"


async def make_exchange():
    exchange = ccxtpro.binanceusdm(
        {
            "apiKey": API_KEY,
            "secret": API_SECRET,
            "enableRateLimit": True,
            "options": {"defaultType": "future", "adjustForTimeDifference": True},
        }
    )
    exchange.enable_demo_trading(True)
    return exchange


def _key_hint() -> str:
    if len(API_KEY) < 8:
        return "(invalid key)"
    return f"{API_KEY[:4]}...{API_KEY[-4:]}"


async def _calc_buy_amount(exchange, sym: str, target_usdt: float) -> tuple[float, float]:
    ticker = await exchange.fetch_ticker(sym)
    px = float(ticker["last"])
    market = exchange.market(sym)
    min_amt = float((market.get("limits") or {}).get("amount", {}).get("min") or 0.0001)
    amount = float(exchange.amount_to_precision(sym, target_usdt / px))
    for _ in range(30):
        if amount * px >= target_usdt:
            break
        amount = float(exchange.amount_to_precision(sym, amount + min_amt))
    return amount, px


async def _confirm_order(exchange, sym: str, order_id: str) -> dict:
    """Re-fetch order from Binance — create_order response can be incomplete."""
    for attempt in range(5):
        try:
            order = await exchange.fetch_order(order_id, sym)
            if float(order.get("filled") or 0) > 0 or order.get("status") == "closed":
                return order
        except Exception as exc:  # noqa: BLE001
            if attempt == 4:
                raise
            print(f"  fetch_order retry {attempt + 1}: {exc}")
        await asyncio.sleep(0.5)
    raise RuntimeError(f"Order {order_id} not confirmed on demo API")


async def _print_api_proof(exchange, sym: str, order_ids: list[str]) -> None:
    await asyncio.sleep(1.0)
    print("API PROOF (demo-fapi.binance.com):")
    print(f"  endpoint: {exchange.urls['api'].get('fapiPrivate')}")
    print(f"  api key : {_key_hint()}  <- bunu Demo API Management ile eslestir")

    for oid in order_ids:
        order = await exchange.fetch_order(oid, sym)
        print(
            f"  CONFIRMED | {order.get('datetime')} | {order.get('side').upper()} | "
            f"id={order.get('id')} | status={order.get('status')} | "
            f"filled={order.get('filled')} | avg={order.get('average')}"
        )

    trades = await exchange.fetch_my_trades(sym, limit=10)
    ours = [t for t in trades if str(t.get("order")) in order_ids]
    if ours:
        print("  TRADE HISTORY (UI -> Trade History sekmesi):")
        for t in ours[-4:]:
            print(
                f"    {t.get('datetime')} | {t.get('side').upper()} | "
                f"order={t.get('order')} | qty={t.get('amount')} | price={t.get('price')}"
            )


def _print_ui_help() -> None:
    print("-" * 60)
    print("BINANCE UI — emirleri gormek icin:")
    print("  1) binance.com'a gir")
    print("  2) Ustte WALLET / hesap menusunden DEMO TRADING moduna gec")
    print("     (canli futures DEGIL — demo modu acik olmali)")
    print("  3) Futures -> Positions (buy-only) veya Order History / Trade History")
    print("  4) 'Hide Other Pairs' kapali olsun, filtre: 1 Day")
    print("  5) API key eslesmesi: yukaridaki key hint = Demo API Management key")
    print("-" * 60)


async def order_buy_only(exchange) -> None:
    sym = TEST_SYMBOL
    if sym not in exchange.markets:
        print("FAIL: BTC/USDT not in markets")
        return

    bal = await exchange.fetch_balance()
    usdt = float((bal.get("USDT") or {}).get("free") or 0)
    print(f"BUY-ONLY TEST | demo USDT free: {usdt:.2f}")
    if usdt < TEST_USDT:
        print(f"FAIL: need >= {TEST_USDT:.0f} USDT on demo account")
        return

    amount, px = await _calc_buy_amount(exchange, sym, TEST_USDT)
    notional = amount * px
    print("-" * 60)
    print(f"BUY ONLY | {sym} | target={TEST_USDT:.0f} USDT | qty={amount} (~{notional:.2f})")
    raw = await exchange.create_order(sym, "market", "buy", amount)
    buy = await _confirm_order(exchange, sym, str(raw["id"]))
    buy_id = str(buy["id"])
    filled = float(buy["filled"])
    avg = float(buy["average"] or buy.get("price") or px)
    print(
        f"  OK | orderId={buy_id} | filled={filled} | avg={avg:.2f} | "
        f"~{filled * avg:.2f} USDT"
    )

    positions = await exchange.fetch_positions([sym])
    for p in positions:
        contracts = float(p.get("contracts") or 0)
        if contracts > 0:
            print(
                f"  POSITION OPEN | {sym} | contracts={contracts} | "
                f"entry={p.get('entryPrice')} | pnl={p.get('unrealizedPnl')}"
            )

    await _print_api_proof(exchange, sym, [buy_id])
    _print_ui_help()
    print("SATMAK ICIN: Futures panelden manuel kapat veya:")
    print("  python test_bot.py --sell-only")


async def order_sell_only(exchange) -> None:
    sym = TEST_SYMBOL
    positions = await exchange.fetch_positions([sym])
    amount = 0.0
    for p in positions:
        contracts = abs(float(p.get("contracts") or 0))
        side = (p.get("side") or "").lower()
        if contracts > 0 and side in ("long", "both", ""):
            amount = contracts
            break
    if amount <= 0:
        print("SELL-ONLY: acik BTC long pozisyon yok")
        return
    amount = float(exchange.amount_to_precision(sym, amount))
    print(f"SELL ONLY | {sym} | qty={amount}")
    raw = await exchange.create_order(sym, "market", "sell", amount, params={"reduceOnly": True})
    sell = await _confirm_order(exchange, sym, str(raw["id"]))
    print(f"  OK | orderId={sell['id']} | filled={sell['filled']}")
    await _print_api_proof(exchange, sym, [str(sell["id"])])


async def order_smoke_test(exchange, round_trip: bool = True) -> None:
    sym = TEST_SYMBOL
    if sym not in exchange.markets:
        print("ORDER TEST skipped: BTC/USDT not found")
        return

    bal = await exchange.fetch_balance()
    usdt = float((bal.get("USDT") or {}).get("free") or 0)
    print(f"ORDER TEST | demo USDT free: {usdt:.2f}")
    if usdt < TEST_USDT:
        print(f"ORDER TEST skipped: need >= {TEST_USDT:.0f} USDT on demo account")
        return

    amount, px = await _calc_buy_amount(exchange, sym, TEST_USDT)
    notional = amount * px
    print("-" * 60)
    print(f"BUY  | {sym} | target={TEST_USDT:.0f} USDT | qty={amount} (~{notional:.2f})")
    raw_buy = await exchange.create_order(sym, "market", "buy", amount)
    buy = await _confirm_order(exchange, sym, str(raw_buy["id"]))
    buy_id = str(buy["id"])
    filled = float(buy["filled"])
    buy_avg = float(buy["average"] or buy.get("price") or px)
    print(
        f"  OK | orderId={buy_id} | filled={filled} | avg={buy_avg:.2f} | "
        f"~{filled * buy_avg:.2f} USDT"
    )

    order_ids = [buy_id]
    if round_trip:
        filled = float(exchange.amount_to_precision(sym, filled))
        print(f"SELL | {sym} | qty={filled} | reduceOnly")
        raw_sell = await exchange.create_order(
            sym, "market", "sell", filled, params={"reduceOnly": True}
        )
        sell = await _confirm_order(exchange, sym, str(raw_sell["id"]))
        sell_id = str(sell["id"])
        order_ids.append(sell_id)
        print(f"  OK | orderId={sell_id} | filled={sell['filled']} | round-trip OK")

    print(f"Order IDs: {', '.join(order_ids)}")
    await _print_api_proof(exchange, sym, order_ids)
    _print_ui_help()


async def count_universe(exchange) -> list[str]:
    selected: list[str] = []
    for symbol, market in exchange.markets.items():
        if not market.get("active", True):
            continue
        if market.get("quote") != "USDT":
            continue
        if not market.get("swap", False) and not market.get("linear", False):
            continue
        if market.get("settle") not in (None, "USDT"):
            continue
        selected.append(symbol)
    selected.sort()
    return selected


async def warmup_sample(exchange, symbols: list[str], n: int = 8) -> dict:
    sample = symbols[:n]
    ready = 0
    for sym in sample:
        try:
            candles = await exchange.fetch_ohlcv(sym, TIMEFRAME, limit=OHLCV_LIMIT)
            if len(candles) >= FBB_LENGTH + 1:
                ready += 1
        except Exception as exc:  # noqa: BLE001
            print(f"  OHLCV fail {sym}: {exc}")
    return {"sample": len(sample), "ready": ready}


async def ticker_probe(exchange, states: set[str], seconds: float = 15.0) -> dict:
    seen: set[str] = set()
    ticks = 0
    t0 = time.time()
    try:
        while time.time() - t0 < seconds:
            batch = await asyncio.wait_for(exchange.watch_tickers(), timeout=10.0)
            for sym in batch:
                if sym in states:
                    seen.add(sym)
                    ticks += 1
    except asyncio.TimeoutError:
        pass
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc), "seen": len(seen), "ticks": ticks}
    return {
        "seconds": round(time.time() - t0, 1),
        "symbols_seen": len(seen),
        "tick_updates": ticks,
    }


async def main(
    order_test: bool,
    order_only: bool,
    buy_only: bool,
    sell_only: bool,
) -> None:
    if not API_KEY or not API_SECRET:
        print("FAIL: .env missing BINANCE_API_KEY / BINANCE_SECRET")
        sys.exit(1)

    exchange = await make_exchange()
    try:
        await exchange.load_markets()

        if sell_only:
            await order_sell_only(exchange)
            return
        if buy_only:
            print(f"DEMO API key: {_key_hint()}")
            await order_buy_only(exchange)
            return
        if order_only:
            print(f"DEMO API key: {_key_hint()}")
            await order_smoke_test(exchange)
            return

        print("=" * 60)
        print("FBB BOT - LIVE HEALTH CHECK (Binance DEMO)")
        print("=" * 60)
        print(f"Filters: 1m vol>={MIN_CANDLE_QUOTE_VOL:.0f} | rel>={VOL_MULT}x")
        print(f"         candle>={MIN_CANDLE_PCT}% | notional={ORDER_USDT} USDT")
        print("         exit: hard stop disaster / EMA9 5m close")
        print(f"DEMO API key: {_key_hint()}")
        print("-" * 60)
        print("OK  Markets loaded")

        try:
            bal = await exchange.fetch_balance()
            usdt = float((bal.get("USDT") or {}).get("free") or 0)
            print(f"OK  API auth | demo USDT free: {usdt:.2f}")
        except Exception as exc:  # noqa: BLE001
            print(f"FAIL API auth: {exc}")
            print("TIP: API key must be from Demo Trading (binance.com -> Demo mode)")
            return

        symbols = await count_universe(exchange)
        print(f"OK  Universe: {len(symbols)} USDT-M perpetual pairs (all active)")

        wh = await warmup_sample(exchange, symbols, n=8)
        print(f"OK  OHLCV warmup sample: {wh['ready']}/{wh['sample']} ready for FBB")

        print("... WebSocket ticker probe (15s) ...")
        probe = await ticker_probe(exchange, set(symbols), seconds=15.0)
        if probe.get("error"):
            print(f"WARN Ticker stream: {probe['error']}")
        else:
            print(
                f"OK  Ticker stream {probe['seconds']}s | "
                f"{probe['symbols_seen']} symbols live | "
                f"{probe['tick_updates']} tick updates"
            )

        print("-" * 60)
        if order_test:
            print("Running order smoke test (100 USDT BTC buy + sell)...")
            await order_smoke_test(exchange)
        else:
            print("Order tests:")
            print("  python test_bot.py --buy-only     # sadece al, UI'da pozisyon gor")
            print("  python test_bot.py --order-test   # al + sat round-trip")
            print("  python test_bot.py --sell-only    # acik pozisyonu kapat")

        print("=" * 60)
        print("HEALTH CHECK DONE")
        print("=" * 60)
    finally:
        await exchange.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--order-test", action="store_true", help="100 USDT BTC buy+sell")
    parser.add_argument("--order-only", action="store_true", help="Skip checks, run buy+sell test")
    parser.add_argument(
        "--buy-only",
        action="store_true",
        help="100 USDT BTC buy only — open position visible in UI",
    )
    parser.add_argument("--sell-only", action="store_true", help="Close open BTC demo position")
    args = parser.parse_args()
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(
        main(
            order_test=args.order_test,
            order_only=args.order_only,
            buy_only=args.buy_only,
            sell_only=args.sell_only,
        )
    )
