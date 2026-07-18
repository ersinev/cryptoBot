"""
Fibonacci Bollinger Instant Breakout — Binance Spot

Entry (1m, instant on FBB 0.786 break — one band below red):
  1) Grey dip arms (persists across candles until entry)
  2) Price breaks FBB upper 0.786 (wick/high)
  3) Previous closed 1m quote vol >= MIN_CANDLE_QUOTE_VOL (no rel filter)
  4) Candle upside >= MIN_CANDLE_PCT → market buy (quote USDT)

Exit:
  - Stop: entry 1m candle low (structure)
  - Ladder: PARTIAL_LADDER e.g. 40%@+3% then 30%@+5%
  - Until +TRAIL_ACTIVATE_PCT%: EMA9 progressive (<3% 1m, >=3% 3m) or fixed EMA_EXIT_TF
  - After +TRAIL_ACTIVATE%: trail TRAIL_PCT% from high (default +10% / -3%)
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass, field
from typing import Any

import ccxt.pro as ccxtpro
from pathlib import Path

from dotenv import load_dotenv

from indicators import fibonacci_bollinger
from markets import list_spot_usdt_symbols
from notify import notify_buy, notify_sell, telegram_enabled
from strategy import (
    EMA_EXIT_TF,
    EMA_PERIOD,
    EMA_PROG_MODE,
    EMA_PROGRESSIVE,
    FBB_LENGTH,
    FBB_MULT,
    MIN_CANDLE_PCT,
    MIN_CANDLE_QUOTE_VOL,
    OHLCV_LIMIT,
    ORDER_USDT,
    PARTIAL_LADDER,
    TIMEFRAME,
    TIMEFRAME_MS,
    TRAIL_ACTIVATE_PCT,
    TRAIL_PCT,
    USE_TRAIL,
    candle_up_pct,
    ema_exit_signal,
    entry_candle_stop_hit,
    entry_rules_met,
    ladder_label,
    next_ladder_partial,
    ohlcv_with_active,
    runner_ema_tf,
    tf_just_closed,
    trail_should_arm,
    trail_stop_hit,
    prev_candle_quote_ok,
    price_entry_ready,
    update_armed,
)

# Always load .env next to this file (not depending on shell cwd)
load_dotenv(Path(__file__).resolve().parent / ".env")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
API_KEY = os.getenv("BINANCE_API_KEY", "").strip()
API_SECRET = os.getenv("BINANCE_SECRET", "").strip()
MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "20"))
SYMBOL_REFRESH_SEC = 300
FBB_REFRESH_SEC = int(os.getenv("FBB_REFRESH_SEC", "90"))
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("fbb-bot")


@dataclass
class SymbolState:
    symbol: str
    ohlcv: list[list[float]] = field(default_factory=list)  # closed candles only
    candle_ts: int = 0
    open: float = 0.0
    high: float = 0.0
    low: float = float("inf")
    close: float = 0.0
    volume: float = 0.0  # active candle volume (from last OHLCV sync)
    upper_0236: float = 0.0  # grey — arm
    upper_0786: float = 0.0  # entry break (one below red)
    upper_1000: float = 0.0  # red
    ready: bool = False
    entry_armed: bool = False  # grey dip — persists until entry
    broke_red: bool = False  # one entry attempt per 1m candle (backtest parity)


@dataclass
class Position:
    symbol: str
    amount: float
    entry_price: float
    entry_time: float
    entry_candle_low: float = 0.0
    high_since_entry: float = 0.0
    trail_armed: bool = False
    entry_candle_ts: int = 0
    close_pending: bool = False
    initial_amount: float = 0.0
    ladder_done: list[bool] = field(default_factory=list)


class FBBInstantBreakoutBot:
    def __init__(self) -> None:
        if not API_KEY or not API_SECRET:
            raise SystemExit(
                "BINANCE_API_KEY and BINANCE_SECRET must be set in .env "
                "(see .env.example). Use Binance Demo Trading API keys."
            )

        self.exchange = ccxtpro.binance(
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

        self.symbols: list[str] = []
        self.states: dict[str, SymbolState] = {}
        self.positions: dict[str, Position] = {}
        # Same symbol cannot be bought again until it is fully sold
        self.held_symbols: set[str] = set()
        self._trade_lock = asyncio.Lock()
        self._sem = asyncio.Semaphore(MAX_CONCURRENCY)
        self._running = True
        self._tick_count = 0

    async def _heartbeat_loop(self) -> None:
        """Periodic alive log so terminal does not look frozen."""
        while self._running:
            await asyncio.sleep(60)
            open_n = len(self.positions)
            log.info(
                "SCANNING | ticks=%d | open_positions=%d | watching %d coins",
                self._tick_count,
                open_n,
                len(self.symbols),
            )

    async def close(self) -> None:
        self._running = False
        try:
            await self.exchange.close()
        except Exception as exc:  # noqa: BLE001
            log.warning("Exchange close error: %s", exc)

    async def run(self) -> None:
        log.info("Starting FBB Instant Breakout bot (Binance SPOT DEMO)")
        self.exchange.enable_demo_trading(True)
        await self.exchange.load_markets()
        await self.refresh_symbols()
        await self.warmup_all()
        trail_txt = (
            f"then +{TRAIL_ACTIVATE_PCT:.0f}% trail -{TRAIL_PCT:.0f}%"
            if USE_TRAIL
            else "no trail"
        )
        partial_txt = (ladder_label() + " + ") if PARTIAL_LADDER else ""
        if EMA_PROGRESSIVE:
            ema_cfg = (
                "prog 1m->3m" if EMA_PROG_MODE == "1m3m" else "prog 1m->3m->5m"
            )
        else:
            ema_cfg = EMA_EXIT_TF
        log.info(
            "Config: %s USDT/trade | spot DEMO | 1m break FBB 0.786 | "
            "exit entry-candle-low / %sEMA%d %s (%s) | telegram=%s",
            ORDER_USDT,
            partial_txt,
            EMA_PERIOD,
            ema_cfg,
            trail_txt,
            "ON" if telegram_enabled() else "OFF",
        )

        tasks = [
            asyncio.create_task(self._ticker_loop(), name="tickers"),
            asyncio.create_task(self._symbol_refresh_loop(), name="symbols"),
            asyncio.create_task(self._ohlcv_refresh_loop(), name="ohlcv"),
            asyncio.create_task(self._candle_boundary_loop(), name="candles"),
            asyncio.create_task(self._heartbeat_loop(), name="heartbeat"),
        ]
        log.info(
            "Live scan ON | %d spot coins websocket | armed persist | FBB refresh %ds | "
            "prev-candle vol (local, no REST on signal)",
            len(self.symbols),
            FBB_REFRESH_SEC,
        )
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self.close()

    # ------------------------------------------------------------------
    # Universe
    # ------------------------------------------------------------------
    async def refresh_symbols(self) -> None:
        """All active spot USDT pairs (no leveraged tokens)."""
        selected = list_spot_usdt_symbols(self.exchange.markets)
        added = sorted(set(selected) - set(self.symbols))
        removed = sorted(set(self.symbols) - set(selected))
        self.symbols = selected

        for s in removed:
            self.states.pop(s, None)
        for s in added:
            self.states.setdefault(s, SymbolState(symbol=s))

        log.info(
            "Universe: %d spot USDT pairs | +%d / -%d | "
            "entry: prev 1m vol>=%.0f USDT & up>=%.1f%%",
            len(self.symbols),
            len(added),
            len(removed),
            MIN_CANDLE_QUOTE_VOL,
            MIN_CANDLE_PCT,
        )

    async def _symbol_refresh_loop(self) -> None:
        while self._running:
            await asyncio.sleep(SYMBOL_REFRESH_SEC)
            await self.refresh_symbols()
            new_syms = [s for s in self.symbols if not self.states[s].ready]
            if new_syms:
                await self._warmup_symbols(new_syms)

    # ------------------------------------------------------------------
    # OHLCV
    # ------------------------------------------------------------------
    async def warmup_all(self) -> None:
        log.info("Warming up OHLCV for %d symbols...", len(self.symbols))
        await self._warmup_symbols(self.symbols)
        ready = sum(1 for s in self.symbols if self.states[s].ready)
        log.info("Warmup done: %d / %d ready", ready, len(self.symbols))

    async def _warmup_symbols(self, symbols: list[str]) -> None:
        async def one(sym: str) -> None:
            async with self._sem:
                await self._fetch_and_apply_ohlcv(sym)

        results = await asyncio.gather(
            *(one(s) for s in symbols), return_exceptions=True
        )
        for sym, res in zip(symbols, results):
            if isinstance(res, Exception):
                log.debug("Warmup error %s: %s", sym, res)

    async def _fetch_and_apply_ohlcv(self, symbol: str) -> None:
        try:
            candles = await self.exchange.fetch_ohlcv(
                symbol, TIMEFRAME, limit=OHLCV_LIMIT
            )
        except Exception as exc:  # noqa: BLE001
            log.debug("OHLCV fetch failed %s: %s", symbol, exc)
            return

        if not candles or len(candles) < FBB_LENGTH + 1:
            return

        now_ms = int(self.exchange.milliseconds())
        last_ts = int(candles[-1][0])
        if now_ms - last_ts < TIMEFRAME_MS:
            closed = candles[:-1]
            active = candles[-1]
        else:
            closed = candles
            active = None

        state = self.states.setdefault(symbol, SymbolState(symbol=symbol))
        prev_ts = state.candle_ts

        state.ohlcv = [list(map(float, c)) for c in closed]
        self._recompute_indicators(state)

        if active is not None:
            new_ts = int(active[0])
            self._apply_active(
                state,
                ts=new_ts,
                o=float(active[1]),
                h=float(active[2]),
                l=float(active[3]),
                c=float(active[4]),
                v=float(active[5]),
                reset_flags=(new_ts != prev_ts),
            )
        elif state.ohlcv:
            last = state.ohlcv[-1]
            border = int(last[0]) + TIMEFRAME_MS
            px = float(last[4])
            self._apply_active(
                state, ts=border, o=px, h=px, l=px, c=px, v=0.0, reset_flags=True
            )

        state.ready = state.upper_0786 > 0

    def _recompute_indicators(self, state: SymbolState) -> None:
        fbb = fibonacci_bollinger(state.ohlcv, length=FBB_LENGTH, mult=FBB_MULT)
        if fbb is None:
            state.ready = False
            return
        state.upper_0236, state.upper_0786, state.upper_1000, _ = fbb

    def _apply_active(
        self,
        state: SymbolState,
        ts: int,
        o: float,
        h: float,
        l: float,
        c: float,
        reset_flags: bool,
        v: float = 0.0,
    ) -> None:
        state.candle_ts = ts
        state.open = o
        state.high = h
        state.low = l
        state.close = c
        if v > 0:
            state.volume = v
        elif reset_flags:
            state.volume = 0.0
        if reset_flags:
            state.broke_red = False
        self._update_entry_arm(state)

    def _price_entry_ready(self, state: SymbolState) -> bool:
        return price_entry_ready(
            state.entry_armed,
            state.broke_red,
            state.high,
            state.open,
            state.upper_0786,
        )

    def _update_entry_arm(self, state: SymbolState) -> None:
        if not state.ready:
            return
        state.entry_armed = update_armed(
            state.entry_armed,
            state.open,
            state.low,
            state.upper_0236,
        )

    def _roll_local_candle(self, state: SymbolState, new_ts: int, price: float) -> None:
        """Local roll at 1m boundary; append closed bar and recompute FBB."""
        if state.candle_ts and new_ts > state.candle_ts:
            state.ohlcv.append(
                [
                    float(state.candle_ts),
                    state.open,
                    state.high,
                    state.low if state.low != float("inf") else state.open,
                    state.close,
                    float(state.volume),
                ]
            )
            if len(state.ohlcv) > OHLCV_LIMIT:
                state.ohlcv = state.ohlcv[-OHLCV_LIMIT:]
            self._recompute_indicators(state)

        self._apply_active(
            state,
            ts=new_ts,
            o=price,
            h=price,
            l=price,
            c=price,
            v=0.0,
            reset_flags=True,
        )

    async def _ohlcv_refresh_loop(self) -> None:
        """Slow FBB refresh for all coins. Volume fetched on-demand per candidate only."""
        while self._running:
            await asyncio.sleep(FBB_REFRESH_SEC)
            if not self.symbols:
                continue
            chunk = 40
            for i in range(0, len(self.symbols), chunk):
                if not self._running:
                    break
                batch = self.symbols[i : i + chunk]
                await self._warmup_symbols(batch)
                await asyncio.sleep(1.0)

    async def _candle_boundary_loop(self) -> None:
        while self._running:
            now_ms = int(time.time() * 1000)
            next_boundary = ((now_ms // TIMEFRAME_MS) + 1) * TIMEFRAME_MS
            await asyncio.sleep(max(0.5, (next_boundary - now_ms) / 1000.0 + 0.35))

            border_ts = (int(time.time() * 1000) // TIMEFRAME_MS) * TIMEFRAME_MS
            prev_border = border_ts - TIMEFRAME_MS
            for sym in list(self.positions.keys()):
                await self._fetch_and_apply_ohlcv(sym)

            # EMA runner: each 1m close; per-position TF decides if bucket closed
            if self.positions:
                for sym in list(self.positions.keys()):
                    await self._maybe_ema_exit(sym)

            for sym in list(self.symbols):
                state = self.states.get(sym)
                if not state or not state.ready or state.candle_ts == 0:
                    continue
                if state.candle_ts < border_ts:
                    closed_close = state.close or state.open
                    self._roll_local_candle(state, border_ts, closed_close)

    # ------------------------------------------------------------------
    # Tickers → stops + entry
    # ------------------------------------------------------------------
    async def _ticker_loop(self) -> None:
        backoff = 1.0
        while self._running:
            try:
                # All-market ticker stream; filter locally to our universe
                tickers = await self.exchange.watch_tickers()
                backoff = 1.0
                self._tick_count += len(tickers)
                for symbol, ticker in tickers.items():
                    if symbol not in self.states:
                        continue
                    last = ticker.get("last")
                    if last is None:
                        continue
                    await self._on_price(symbol, float(last), ticker)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "watch_tickers error: %s — reconnect in %.1fs", exc, backoff
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    async def _on_price(
        self, symbol: str, price: float, ticker: dict[str, Any]
    ) -> None:
        state = self.states.get(symbol)
        if not state or not state.ready:
            return

        now_ms = int(ticker.get("timestamp") or self.exchange.milliseconds())
        candle_ts = (now_ms // TIMEFRAME_MS) * TIMEFRAME_MS

        if state.candle_ts == 0:
            self._apply_active(
                state,
                ts=candle_ts,
                o=price,
                h=price,
                l=price,
                c=price,
                v=0.0,
                reset_flags=True,
            )
        elif candle_ts > state.candle_ts:
            self._roll_local_candle(state, candle_ts, price)
        else:
            state.high = max(state.high, price)
            state.low = min(state.low, price)
            state.close = price
            self._update_entry_arm(state)

        if symbol in self.positions:
            await self._check_stops(symbol, price)

        if symbol in self.held_symbols:
            return

        if not self._price_entry_ready(state):
            return

        await self._maybe_enter(state)

    async def _maybe_enter(self, state: SymbolState) -> None:
        if state.symbol in self.held_symbols or state.broke_red:
            return
        if not self._price_entry_ready(state):
            return

        candle_pct = candle_up_pct(state.open, state.high)

        if not state.ohlcv:
            return
        prev = state.ohlcv[-1]
        vol_ok, quote_vol = prev_candle_quote_ok(float(prev[5]), float(prev[4]))
        if not vol_ok:
            log.debug(
                "WAIT PREV VOL %s | prev 1m quote %.0f (min %.0f)",
                state.symbol,
                quote_vol,
                MIN_CANDLE_QUOTE_VOL,
            )
            return

        async with self._trade_lock:
            if state.symbol in self.held_symbols or state.broke_red:
                return
            if not entry_rules_met(
                state.entry_armed,
                state.high,
                state.open,
                state.upper_0786,
            ):
                return
            state.broke_red = True
            log.info(
                "SIGNAL BUY %s | high=%.6f > entry0786=%.6f | red=%.6f | grey=%.6f | "
                "prev_1m_vol=%.0f USDT | up=%.2f%% | low=%.6f open=%.6f",
                state.symbol,
                state.high,
                state.upper_0786,
                state.upper_1000,
                state.upper_0236,
                quote_vol,
                candle_pct,
                state.low,
                state.open,
            )
            candle_low = state.low if state.low != float("inf") else state.open
            ok = await self._market_buy(
                state.symbol, state.high, state.candle_ts, candle_low
            )
            if ok:
                state.entry_armed = False
            else:
                state.broke_red = False

    async def _maybe_ema_exit(self, symbol: str) -> None:
        if symbol not in self.positions:
            return
        state = self.states.get(symbol)
        if state is None or not state.ohlcv:
            return

        pos = self.positions.get(symbol)
        if pos is not None and pos.trail_armed:
            return

        ema_tf = (
            runner_ema_tf(pos.entry_price, pos.high_since_entry)
            if pos is not None
            else EMA_EXIT_TF
        )
        series = state.ohlcv
        if ema_tf in ("3m", "5m"):
            series = ohlcv_with_active(
                state.ohlcv,
                state.candle_ts,
                state.open,
                state.high,
                state.low,
                state.close,
                state.volume,
            )
            if len(series) < 2 or not tf_just_closed(
                series, len(series) - 1, ema_tf
            ):
                return
        elif len(series) < EMA_PERIOD:
            return

        should_exit, bar_close, ema_val, reason = ema_exit_signal(
            series, tf=ema_tf
        )
        if not should_exit:
            return

        async with self._trade_lock:
            if symbol not in self.positions:
                return
            log.info(
                "SIGNAL SELL %s | %s",
                symbol,
                reason,
            )
            await self._market_close(symbol, reason=reason)

    def _maybe_arm_trail(self, symbol: str, high_water: float) -> None:
        if not USE_TRAIL:
            return
        pos = self.positions.get(symbol)
        if pos is None or pos.trail_armed:
            return
        if trail_should_arm(pos.entry_price, high_water):
            pos.trail_armed = True
            log.info(
                "TRAIL ARMED %s | high=%.6f >= +%.0f%% of entry=%.6f (intrabar)",
                symbol,
                high_water,
                TRAIL_ACTIVATE_PCT,
                pos.entry_price,
            )

    async def _check_stops(self, symbol: str, price: float) -> None:
        pos = self.positions.get(symbol)
        if pos is None or pos.entry_price <= 0 or pos.close_pending:
            return

        pos.high_since_entry = max(pos.high_since_entry, price)
        if len(pos.ladder_done) < len(PARTIAL_LADDER):
            pos.ladder_done = pos.ladder_done + [False] * (
                len(PARTIAL_LADDER) - len(pos.ladder_done)
            )

        # Ladder scale-outs (one step per call; next tick can fire next step)
        hit_tp, tp_px, frac, step_i = next_ladder_partial(
            pos.entry_price, pos.high_since_entry, pos.ladder_done
        )
        if hit_tp and step_i >= 0:
            pos.close_pending = True
            async with self._trade_lock:
                if symbol not in self.positions:
                    return
                cur = self.positions[symbol]
                if cur.ladder_done[step_i]:
                    cur.close_pending = False
                    return
                pct_lvl = PARTIAL_LADDER[step_i][0]
                log.info(
                    "LADDER TP %s | +%g%% touch | sell %.0f%% init @~%.6f | entry=%.6f",
                    symbol,
                    pct_lvl,
                    frac * 100,
                    tp_px,
                    cur.entry_price,
                )
                ok = await self._market_sell_partial(
                    symbol,
                    frac,
                    reason=f"ladder +{pct_lvl:g}%",
                    step_i=step_i,
                )
                if not ok:
                    cur = self.positions.get(symbol)
                    if cur is not None:
                        cur.close_pending = False
                return

        self._maybe_arm_trail(symbol, pos.high_since_entry)

        hit, stop_fill = False, 0.0
        reason = ""
        if USE_TRAIL:
            hit, stop_fill = trail_stop_hit(
                pos.entry_price,
                pos.high_since_entry,
                price,
                armed=pos.trail_armed,
            )
            reason = f"trail -{TRAIL_PCT}%"
        if not hit:
            hit, stop_fill = entry_candle_stop_hit(pos.entry_candle_low, price)
            reason = "entry candle low"

        if not hit:
            return

        pnl_pct = (stop_fill - pos.entry_price) / pos.entry_price * 100.0
        pos.close_pending = True

        async with self._trade_lock:
            if symbol not in self.positions:
                return
            log.info(
                "STOP %s | %s | price=%.6f <= stop=%.6f | entry=%.6f | "
                "high=%.6f | pnl=%.2f%%",
                symbol,
                reason,
                price,
                stop_fill,
                pos.entry_price,
                pos.high_since_entry,
                pnl_pct,
            )
            await self._market_close(symbol, reason=reason)

    # ------------------------------------------------------------------
    # Orders
    # ------------------------------------------------------------------
    async def _market_buy(
        self,
        symbol: str,
        ref_price: float,
        entry_candle_ts: int,
        entry_candle_low: float,
    ) -> bool:
        try:
            market = self.exchange.market(symbol)
            # Spot: spend ORDER_USDT quote
            if hasattr(self.exchange, "create_market_buy_order_with_cost"):
                order = await self.exchange.create_market_buy_order_with_cost(
                    symbol, ORDER_USDT
                )
            else:
                order = await self.exchange.create_order(
                    symbol,
                    "market",
                    "buy",
                    ORDER_USDT,
                    params={"quoteOrderQty": ORDER_USDT},
                )
            fill = float(order.get("average") or order.get("price") or ref_price)
            filled = float(order.get("filled") or 0.0)
            if filled <= 0 and fill > 0:
                filled = ORDER_USDT / fill
            filled = float(self.exchange.amount_to_precision(symbol, filled))
            min_amt = (market.get("limits") or {}).get("amount", {}).get("min")
            if min_amt and filled < float(min_amt):
                log.error(
                    "Filled %.8f below min %s for %s — increase ORDER_USDT",
                    filled,
                    min_amt,
                    symbol,
                )
                return False
            notional = filled * fill if fill > 0 else ORDER_USDT
            self.positions[symbol] = Position(
                symbol=symbol,
                amount=filled,
                entry_price=fill,
                entry_time=time.time(),
                entry_candle_low=entry_candle_low,
                high_since_entry=fill,
                trail_armed=False,
                entry_candle_ts=entry_candle_ts,
                initial_amount=filled,
                ladder_done=[False] * len(PARTIAL_LADDER),
            )
            self.held_symbols.add(symbol)
            log.info(
                "ORDER BUY | %s | orderId=%s | qty=%s | avg=%.6f | stop_low=%.6f | "
                "~%.2f USDT | open_positions=%d",
                symbol,
                order.get("id"),
                filled,
                fill,
                entry_candle_low,
                notional,
                len(self.positions),
            )
            await notify_buy(
                symbol,
                price=fill,
                qty=filled,
                notional=notional,
                stop_low=entry_candle_low,
                order_id=str(order.get("id") or ""),
            )
            return True
        except Exception as exc:  # noqa: BLE001
            log.error("Market buy failed %s: %s", symbol, exc)
            return False

    def _release_symbol(self, symbol: str, *, keep_broke_red: bool = False) -> None:
        """Allow this symbol to be bought again only after a full sell."""
        self.held_symbols.discard(symbol)
        self.positions.pop(symbol, None)
        state = self.states.get(symbol)
        if state is not None:
            state.entry_armed = False
            if not keep_broke_red:
                state.broke_red = False
        log.info(
            "UNLOCK %s | open_positions=%d | can buy again on new setup",
            symbol,
            len(self.positions),
        )

    async def _market_sell_partial(
        self,
        symbol: str,
        frac: float,
        reason: str,
        *,
        step_i: int = -1,
    ) -> bool:
        """Sell frac of initial size; keep runner for EMA/trail."""
        pos = self.positions.get(symbol)
        if pos is None or frac <= 0 or frac >= 1:
            if pos is not None:
                pos.close_pending = False
            return False
        try:
            base_amt = pos.initial_amount if pos.initial_amount > 0 else pos.amount
            sell_amt = base_amt * frac
            try:
                market = self.exchange.market(pos.symbol)
                base = market.get("base") or pos.symbol.split("/")[0]
                await asyncio.sleep(0.2)
                bal = await self.exchange.fetch_balance()
                free = float(
                    (bal.get(base) or {}).get("free")
                    or (bal.get("free") or {}).get(base)
                    or 0
                )
                if free > 0:
                    sell_amt = min(sell_amt, free * 0.99, pos.amount)
            except Exception as exc:  # noqa: BLE001
                log.debug("partial balance: %s", exc)

            sell_amt = float(self.exchange.amount_to_precision(pos.symbol, sell_amt))
            if sell_amt <= 0:
                log.warning("Partial sell qty=0 on %s", symbol)
                pos.close_pending = False
                return False

            entry = pos.entry_price
            order = await self.exchange.create_order(
                pos.symbol, "market", "sell", sell_amt
            )
            exit_px = float(order.get("average") or order.get("price") or entry)
            pos.amount = max(0.0, pos.amount - sell_amt)
            if 0 <= step_i < len(pos.ladder_done):
                pos.ladder_done[step_i] = True
            pos.close_pending = False
            log.info(
                "ORDER LADDER SELL | %s | reason=%s | qty=%s | left=%s | avg=%.6f",
                symbol,
                reason,
                sell_amt,
                pos.amount,
                exit_px,
            )
            await notify_sell(
                symbol,
                reason=reason,
                entry=entry,
                exit_price=exit_px,
                qty=sell_amt,
                order_id=str(order.get("id") or ""),
            )
            min_amt = (
                (self.exchange.market(symbol).get("limits") or {})
                .get("amount", {})
                .get("min")
            )
            if min_amt and pos.amount < float(min_amt):
                await self._market_close(symbol, reason="dust after partial")
            return True
        except Exception as exc:  # noqa: BLE001
            log.error("Partial sell failed %s: %s", symbol, exc)
            if pos := self.positions.get(symbol):
                pos.close_pending = False
            return False

    async def _market_close(self, symbol: str, reason: str = "") -> None:
        pos = self.positions.get(symbol)
        if pos is None:
            return
        entry_candle_ts = pos.entry_candle_ts
        state = self.states.get(symbol)
        same_candle = (
            state is not None
            and entry_candle_ts > 0
            and state.candle_ts == entry_candle_ts
        )
        try:
            amount = pos.amount
            try:
                market = self.exchange.market(pos.symbol)
                base = market.get("base") or pos.symbol.split("/")[0]
                await asyncio.sleep(0.3)
                bal = await self.exchange.fetch_balance()
                free = float(
                    (bal.get(base) or {}).get("free")
                    or (bal.get("free") or {}).get(base)
                    or 0
                )
                if free > 0:
                    # Spot fee may leave slightly less than filled qty
                    amount = min(amount, free)
            except Exception as exc:  # noqa: BLE001
                log.debug("fetch_balance fallback: %s", exc)

            amount = float(self.exchange.amount_to_precision(pos.symbol, amount))
            if amount <= 0:
                log.warning("Nothing to close on %s", pos.symbol)
                self._release_symbol(pos.symbol, keep_broke_red=same_candle)
                return

            entry = pos.entry_price
            order = await self.exchange.create_order(
                pos.symbol,
                "market",
                "sell",
                amount,
            )
            exit_px = float(
                order.get("average") or order.get("price") or entry
            )
            log.info(
                "ORDER SELL | %s | reason=%s | orderId=%s | qty=%s | "
                "open_positions=%d",
                pos.symbol,
                reason or "close",
                order.get("id"),
                amount,
                len(self.positions) - 1,
            )
            await notify_sell(
                pos.symbol,
                reason=reason or "close",
                entry=entry,
                exit_price=exit_px,
                qty=amount,
                order_id=str(order.get("id") or ""),
            )
            self._release_symbol(pos.symbol, keep_broke_red=same_candle)
        except Exception as exc:  # noqa: BLE001
            log.error("Market close failed %s: %s", pos.symbol, exc)
            if pos := self.positions.get(symbol):
                pos.close_pending = False


async def _main() -> None:
    bot = FBBInstantBreakoutBot()
    loop = asyncio.get_running_loop()

    def _stop() -> None:
        log.info("Shutdown requested...")
        bot._running = False
        for task in asyncio.all_tasks(loop):
            if task is not asyncio.current_task():
                task.cancel()

    if sys.platform != "win32":
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _stop)

    try:
        await bot.run()
    except KeyboardInterrupt:
        log.info("Interrupted")
    finally:
        await bot.close()


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(_main())
