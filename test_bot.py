"""
Quick live health check for FBB spot bot.

Order test modes:
  --order-test     ~100 USDT BTC buy + sell (round-trip)
  --buy-only       ~100 USDT BTC buy only
  --order-only     Skip health checks, run --order-test
  --sell-only      Sell free BTC balance
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

from markets import list_spot_usdt_symbols  # noqa: E402
from strategy import (  # noqa: E402
    FBB_LENGTH,
    MIN_CANDLE_PCT,
    MIN_CANDLE_QUOTE_VOL,
    OHLCV_LIMIT,
    ORDER_USDT,
    TIMEFRAME,
    VOL_MULT,
)
from bot import API_KEY, API_SECRET  # noqa: E402

TEST_USDT = 100.0
TEST_SYMBOL = "BTC/USDT"


async def make_exchange():
    exchange = ccxtpro.binance(
        {
            "apiKey": API_KEY,
            "secret": API_SECRET,
            "enableRateLimit": True,
            "options": {
                "defaultType": "spot",
                "adjustForTimeDifference": True,
                "createMarketBuyOrderRequiresPrice": False,
            },
        }
    )
    exchange.enable_demo_trading(True)
    return exchange


def _key_hint() -> str:
    if len(API_KEY) < 8:
        return "(invalid key)"
    return f"{API_KEY[:4]}...{API_KEY[-4:]}"


async def _confirm_order(exchange, sym: str, order_id: str) -> dict:
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
    raise RuntimeError(f"Order {order_id} not confirmed")


async def _print_api_proof(exchange, sym: str, order_ids: list[str]) -> None:
    await asyncio.sleep(1.0)
    print("API PROOF (api.binance.com spot):")
    print(f"  api key : {_key_hint()}")

    for oid in order_ids:
        order = await exchange.fetch_order(oid, sym)
        print(
            f"  CONFIRMED | {order.get('datetime')} | {order.get('side').upper()} | "
            f"id={order.get('id')} | status={order.get('status')} | "
            f"filled={order.get('filled')} | avg={order.get('average')}"
        )


def _print_ui_help() -> None:
    print("-" * 60)
    print("BINANCE UI — emirleri gormek icin:")
    print("  1) binance.com -> DEMO TRADING moduna gec")
    print("  2) Spot -> Orders / Trade History / Wallet Spot")
    print("  3) API key: Demo Trading API Management")
    print("-" * 60)


async def order_buy_only(exchange) -> None:
    sym = TEST_SYMBOL
    if sym not in exchange.markets:
        print("FAIL: BTC/USDT not in markets")
        return

    bal = await exchange.fetch_balance()
    usdt = float((bal.get("USDT") or {}).get("free") or 0)
    print(f"BUY-ONLY TEST | demo spot USDT free: {usdt:.2f}")
    if usdt < TEST_USDT:
        print(f"FAIL: need >= {TEST_USDT:.0f} USDT on demo spot wallet")
        return

    print("-" * 60)
    print(f"BUY ONLY | {sym} | cost={TEST_USDT:.0f} USDT")
    if hasattr(exchange, "create_market_buy_order_with_cost"):
        raw = await exchange.create_market_buy_order_with_cost(sym, TEST_USDT)
    else:
        raw = await exchange.create_order(
            sym, "market", "buy", TEST_USDT, params={"quoteOrderQty": TEST_USDT}
        )
    buy = await _confirm_order(exchange, sym, str(raw["id"]))
    buy_id = str(buy["id"])
    filled = float(buy["filled"])
    avg = float(buy["average"] or buy.get("price") or 0)
    print(
        f"  OK | orderId={buy_id} | filled={filled} | avg={avg:.2f} | "
        f"~{filled * avg:.2f} USDT"
    )

    bal2 = await exchange.fetch_balance()
    btc = float((bal2.get("BTC") or {}).get("free") or 0)
    print(f"  BTC free balance: {btc}")

    await _print_api_proof(exchange, sym, [buy_id])
    _print_ui_help()
    print("SATMAK ICIN: Spot'tan manuel sat veya:")
    print("  python test_bot.py --sell-only")


async def order_sell_only(exchange) -> None:
    sym = TEST_SYMBOL
    bal = await exchange.fetch_balance()
    amount = float((bal.get("BTC") or {}).get("free") or 0)
    if amount <= 0:
        print("SELL-ONLY: free BTC yok")
        return
    amount = float(exchange.amount_to_precision(sym, amount))
    print(f"SELL ONLY | {sym} | qty={amount}")
    raw = await exchange.create_order(sym, "market", "sell", amount)
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
    print(f"ORDER TEST | spot USDT free: {usdt:.2f}")
    if usdt < TEST_USDT:
        print(f"ORDER TEST skipped: need >= {TEST_USDT:.0f} USDT on spot wallet")
        return

    print("-" * 60)
    print(f"BUY  | {sym} | cost={TEST_USDT:.0f} USDT")
    if hasattr(exchange, "create_market_buy_order_with_cost"):
        raw_buy = await exchange.create_market_buy_order_with_cost(sym, TEST_USDT)
    else:
        raw_buy = await exchange.create_order(
            sym, "market", "buy", TEST_USDT, params={"quoteOrderQty": TEST_USDT}
        )
    buy = await _confirm_order(exchange, sym, str(raw_buy["id"]))
    buy_id = str(buy["id"])
    filled = float(buy["filled"])
    buy_avg = float(buy["average"] or buy.get("price") or 0)
    print(
        f"  OK | orderId={buy_id} | filled={filled} | avg={buy_avg:.2f} | "
        f"~{filled * buy_avg:.2f} USDT"
    )

    order_ids = [buy_id]
    if round_trip:
        await asyncio.sleep(1.0)
        bal2 = await exchange.fetch_balance()
        free_btc = float((bal2.get("BTC") or {}).get("free") or 0)
        sell_amt = float(exchange.amount_to_precision(sym, min(filled, free_btc)))
        if sell_amt <= 0:
            print(f"SELL skipped: free BTC={free_btc} filled={filled}")
        else:
            print(f"SELL | {sym} | qty={sell_amt} (free={free_btc})")
            raw_sell = await exchange.create_order(sym, "market", "sell", sell_amt)
            sell = await _confirm_order(exchange, sym, str(raw_sell["id"]))
            sell_id = str(sell["id"])
            order_ids.append(sell_id)
            print(f"  OK | orderId={sell_id} | filled={sell['filled']} | round-trip OK")

    print(f"Order IDs: {', '.join(order_ids)}")
    await _print_api_proof(exchange, sym, order_ids)
    _print_ui_help()


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
            print(f"Demo Spot API key: {_key_hint()}")
            await order_buy_only(exchange)
            return
        if order_only:
            print(f"Demo Spot API key: {_key_hint()}")
            await order_smoke_test(exchange)
            return

        print("=" * 60)
        print("FBB BOT - LIVE HEALTH CHECK (Binance SPOT DEMO)")
        print("=" * 60)
        print(f"Filters: 1m vol>={MIN_CANDLE_QUOTE_VOL:.0f} | rel>={VOL_MULT}x")
        print(f"         candle>={MIN_CANDLE_PCT}% | notional={ORDER_USDT} USDT")
        print("         exit: entry-candle-low / EMA9 5m / trail")
        print(f"Demo Spot API key: {_key_hint()}")
        print("-" * 60)
        print("OK  Markets loaded")

        try:
            bal = await exchange.fetch_balance()
            usdt = float((bal.get("USDT") or {}).get("free") or 0)
            print(f"OK  API auth | demo spot USDT free: {usdt:.2f}")
        except Exception as exc:  # noqa: BLE001
            print(f"FAIL API auth: {exc}")
            print("TIP: API key must be from Demo Trading (binance.com → Demo mode)")
            return

        symbols = list_spot_usdt_symbols(exchange.markets)
        print(f"OK  Universe: {len(symbols)} spot USDT pairs")

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
            print("Running order smoke test (~100 USDT BTC buy + sell)...")
            await order_smoke_test(exchange)
        else:
            print("Order tests:")
            print("  python test_bot.py --buy-only     # sadece al")
            print("  python test_bot.py --order-test   # al + sat round-trip")
            print("  python test_bot.py --sell-only    # BTC sat")

        print("=" * 60)
        print("HEALTH CHECK DONE")
        print("=" * 60)
    finally:
        await exchange.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--order-test", action="store_true", help="~100 USDT BTC buy+sell")
    parser.add_argument("--order-only", action="store_true", help="Skip checks, run buy+sell test")
    parser.add_argument(
        "--buy-only",
        action="store_true",
        help="~100 USDT BTC buy only",
    )
    parser.add_argument("--sell-only", action="store_true", help="Sell free BTC")
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
