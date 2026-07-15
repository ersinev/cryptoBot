"""
Fibonacci Bollinger Instant Breakout Bot
Binance USD-M Futures · ccxt.pro · asyncio

Entry (5m, forming candle — no close wait):
  1. Universe = active USDT linear swaps with 24h quote volume >= filter
  2. FBB(200, mult=3): grey 0.764 + red 1.000
  3. Active candle open OR low is below grey 0.764
  4. Last price breaks ABOVE red 1.000 → market BUY immediately

Exit:
  New 5m candle closes below EMA(9) → market CLOSE
"""

from __future__ import annotations

import asyncio
import logging
import math
import signal
import time
from dataclasses import dataclass, field
from typing import Any

import ccxt
import ccxt.pro as ccxtpro

from config import Config, load_config
from exchange_client import create_exchange
from indicators import (
    candle_open_ms,
    ema,
    fibonacci_bollinger_bands,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("fbb-bot")


@dataclass
class SymbolState:
    symbol: str
    upper_0764: float = 0.0
    upper_1000: float = 0.0
    candle_ts: int = 0
    open: float = 0.0
    low: float = 0.0
    last: float = 0.0
    touched_below_0764: bool = False
    entry_armed: bool = False
    signaled_candle_ts: int = 0
    updated_at: float = 0.0


@dataclass
class Position:
    symbol: str
    amount: float
    entry_price: float
    entry_time: float = field(default_factory=time.time)


class FibonacciBreakoutBot:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.exchange: ccxtpro.binanceusdm = create_exchange(config)
        self.symbols: list[str] = []
        self.states: dict[str, SymbolState] = {}
        self.positions: dict[str, Position] = {}
        self._entry_lock = asyncio.Lock()
        self._stop = asyncio.Event()
        self._markets_ready = asyncio.Event()

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #
    async def run(self) -> None:
        logger.info("Starting Fibonacci Bollinger Instant Breakout bot")
        try:
            await self.exchange.load_markets()
            await self._sync_open_positions()
            await self.refresh_universe()
            self._markets_ready.set()

            tasks = [
                asyncio.create_task(self._universe_loop(), name="universe"),
                asyncio.create_task(self._indicator_loop(), name="indicators"),
                asyncio.create_task(self._ticker_loop(), name="tickers"),
                asyncio.create_task(self._exit_loop(), name="exits"),
            ]

            stop_waiter = asyncio.create_task(self._stop.wait(), name="stop")
            done, pending = await asyncio.wait(
                [*tasks, stop_waiter],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

            for task in done:
                if task is stop_waiter:
                    continue
                exc = task.exception()
                if exc:
                    logger.error("Task %s crashed: %s", task.get_name(), exc)
        finally:
            await self._shutdown()

    def request_stop(self) -> None:
        self._stop.set()

    async def _shutdown(self) -> None:
        logger.info("Shutting down…")
        try:
            await self.exchange.close()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Exchange close error: %s", exc)

    # ------------------------------------------------------------------ #
    # Universe
    # ------------------------------------------------------------------ #
    async def refresh_universe(self) -> None:
        """Load active USDT linear swaps filtered by 24h quote volume."""
        try:
            await self.exchange.load_markets(reload=True)
            tickers = await self.exchange.fetch_tickers()
        except Exception as exc:  # noqa: BLE001
            logger.error("Universe refresh failed: %s", exc)
            return

        selected: list[str] = []
        for symbol, market in self.exchange.markets.items():
            if not market.get("active", True):
                continue
            if market.get("quote") != "USDT":
                continue
            if not market.get("swap") and market.get("type") not in {"swap", "future"}:
                continue
            # Prefer linear USDT-margined contracts only
            if market.get("linear") is False:
                continue

            ticker = tickers.get(symbol) or {}
            quote_volume = ticker.get("quoteVolume")
            if quote_volume is None:
                info = ticker.get("info") or {}
                quote_volume = info.get("quoteVolume") or info.get("quote_volume")
            try:
                qv = float(quote_volume or 0.0)
            except (TypeError, ValueError):
                qv = 0.0

            if qv < self.config.min_quote_volume_usdt:
                continue
            selected.append(symbol)

        selected.sort()
        previous = set(self.symbols)
        self.symbols = selected

        for symbol in selected:
            if symbol not in self.states:
                self.states[symbol] = SymbolState(symbol=symbol)

        for symbol in list(self.states):
            if symbol not in selected and symbol not in self.positions:
                del self.states[symbol]

        added = set(selected) - previous
        removed = previous - set(selected)
        logger.info(
            "Universe: %d symbols (volume >= %.0f USDT) | +%d / -%d",
            len(selected),
            self.config.min_quote_volume_usdt,
            len(added),
            len(removed),
        )

    async def _universe_loop(self) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(self.config.universe_refresh_sec)
            await self.refresh_universe()

    # ------------------------------------------------------------------ #
    # Indicators / candle state
    # ------------------------------------------------------------------ #
    async def _indicator_loop(self) -> None:
        await self._markets_ready.wait()
        sem = asyncio.Semaphore(self.config.fetch_concurrency)

        while not self._stop.is_set():
            symbols = list(self.symbols)
            if not symbols:
                await asyncio.sleep(2)
                continue

            async def _one(sym: str) -> None:
                async with sem:
                    await self._refresh_symbol_indicators(sym)

            await asyncio.gather(*[_one(s) for s in symbols], return_exceptions=True)
            await asyncio.sleep(self.config.indicator_refresh_sec)

    async def _refresh_symbol_indicators(self, symbol: str) -> None:
        try:
            ohlcv = await self.exchange.fetch_ohlcv(
                symbol,
                timeframe=self.config.timeframe,
                limit=self.config.ohlcv_limit,
            )
        except ccxt.NetworkError as exc:
            logger.debug("OHLCV network error %s: %s", symbol, exc)
            return
        except ccxt.ExchangeError as exc:
            logger.debug("OHLCV exchange error %s: %s", symbol, exc)
            return
        except Exception as exc:  # noqa: BLE001
            logger.debug("OHLCV error %s: %s", symbol, exc)
            return

        if not ohlcv or len(ohlcv) < self.config.fbb_length:
            return

        levels = fibonacci_bollinger_bands(
            ohlcv,
            length=self.config.fbb_length,
            mult=self.config.fbb_mult,
        )
        if levels is None:
            return

        candle = ohlcv[-1]
        ts = int(candle[0])
        open_ = float(candle[1])
        low = float(candle[3])
        last = float(candle[4])

        state = self.states.setdefault(symbol, SymbolState(symbol=symbol))
        if state.candle_ts and ts > state.candle_ts:
            # New candle — reset path tracking
            state.touched_below_0764 = False
            state.entry_armed = False

        state.upper_0764 = levels.upper_0764
        state.upper_1000 = levels.upper_1000
        state.candle_ts = ts
        state.open = open_
        state.low = min(low, state.low) if state.low and state.candle_ts == ts else low
        state.last = last
        state.updated_at = time.time()
        self._update_path_flags(state)

    def _update_path_flags(self, state: SymbolState) -> None:
        if state.upper_0764 <= 0 or state.upper_1000 <= 0:
            return
        if state.open <= state.upper_0764 or state.low <= state.upper_0764:
            state.touched_below_0764 = True
            state.entry_armed = True

    # ------------------------------------------------------------------ #
    # Live tickers → instant breakout entries
    # ------------------------------------------------------------------ #
    async def _ticker_loop(self) -> None:
        await self._markets_ready.wait()

        while not self._stop.is_set():
            symbols = list(self.symbols)
            if not symbols:
                await asyncio.sleep(1)
                continue

            # Binance watch_tickers works well with batches
            batches = [
                symbols[i : i + self.config.ticker_batch_size]
                for i in range(0, len(symbols), self.config.ticker_batch_size)
            ]

            async def _watch_batch(batch: list[str]) -> None:
                while not self._stop.is_set():
                    # Refresh batch membership periodically via outer restart
                    current = [s for s in batch if s in self.states]
                    if not current:
                        await asyncio.sleep(1)
                        return
                    try:
                        tickers = await self.exchange.watch_tickers(current)
                        now_ms = int(time.time() * 1000)
                        for symbol, ticker in tickers.items():
                            await self._on_ticker(symbol, ticker, now_ms)
                    except asyncio.CancelledError:
                        raise
                    except ccxt.NetworkError as exc:
                        logger.warning("Ticker WS network error: %s", exc)
                        await asyncio.sleep(1)
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("Ticker WS error: %s", exc)
                        await asyncio.sleep(1)

            tasks = [asyncio.create_task(_watch_batch(b)) for b in batches]
            # Restart batch watchers when universe refresh interval elapses
            try:
                await asyncio.wait(
                    [asyncio.create_task(self._stop.wait()), *tasks],
                    timeout=self.config.universe_refresh_sec,
                    return_when=asyncio.FIRST_COMPLETED,
                )
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

    async def _on_ticker(self, symbol: str, ticker: dict[str, Any], now_ms: int) -> None:
        state = self.states.get(symbol)
        if state is None or state.upper_1000 <= 0:
            return

        last = ticker.get("last")
        if last is None:
            last = ticker.get("close")
        try:
            price = float(last)
        except (TypeError, ValueError):
            return
        if price <= 0:
            return

        candle_ts = candle_open_ms(now_ms, timeframe_sec=300)
        if state.candle_ts and candle_ts > state.candle_ts:
            state.candle_ts = candle_ts
            state.open = price
            state.low = price
            state.touched_below_0764 = False
            state.entry_armed = False
        else:
            if not state.candle_ts:
                state.candle_ts = candle_ts
                state.open = price
                state.low = price
            else:
                state.low = min(state.low, price) if state.low else price

        state.last = price
        self._update_path_flags(state)

        if not state.entry_armed or not state.touched_below_0764:
            return
        if state.signaled_candle_ts == state.candle_ts:
            return
        if symbol in self.positions:
            return
        if len(self.positions) >= self.config.max_positions:
            return

        # Instant breakout: last price crosses ABOVE red 1.000
        if price > state.upper_1000:
            # Claim this candle immediately to avoid duplicate concurrent entries
            state.signaled_candle_ts = state.candle_ts
            logger.info(
                "BREAKOUT %s | price=%.8f > red(1.000)=%.8f | grey(0.764)=%.8f | "
                "open=%.8f low=%.8f",
                symbol,
                price,
                state.upper_1000,
                state.upper_0764,
                state.open,
                state.low,
            )
            opened = await self._open_long(symbol, price)
            if not opened:
                # Allow a retry later in the same candle if the order failed
                state.signaled_candle_ts = 0

    # ------------------------------------------------------------------ #
    # Orders
    # ------------------------------------------------------------------ #
    async def _open_long(self, symbol: str, ref_price: float) -> bool:
        async with self._entry_lock:
            if symbol in self.positions:
                return False
            if len(self.positions) >= self.config.max_positions:
                return False

            try:
                await self._ensure_leverage(symbol)
                amount = await self._amount_for_notional(symbol, ref_price)
                if amount <= 0:
                    logger.warning("Skip entry %s: computed amount is 0", symbol)
                    return False

                order = await self.exchange.create_order(
                    symbol=symbol,
                    type="market",
                    side="buy",
                    amount=amount,
                    params={"reduceOnly": False},
                )
                filled = float(order.get("filled") or amount)
                avg = float(order.get("average") or ref_price)
                self.positions[symbol] = Position(
                    symbol=symbol,
                    amount=filled,
                    entry_price=avg,
                )
                logger.info(
                    "ENTER LONG %s | amount=%s | avg=%.8f | order=%s",
                    symbol,
                    filled,
                    avg,
                    order.get("id"),
                )
                return True
            except Exception as exc:  # noqa: BLE001
                logger.error("Entry failed %s: %s", symbol, exc)
                return False

    async def _close_long(self, symbol: str, reason: str) -> None:
        position = self.positions.get(symbol)
        if position is None:
            return

        try:
            amount = await self._position_contracts(symbol)
            if amount <= 0:
                amount = position.amount
            if amount <= 0:
                logger.warning("Close skipped %s: no size", symbol)
                self.positions.pop(symbol, None)
                return

            order = await self.exchange.create_order(
                symbol=symbol,
                type="market",
                side="sell",
                amount=amount,
                params={"reduceOnly": True},
            )
            avg = float(order.get("average") or 0.0)
            logger.info(
                "EXIT LONG %s | reason=%s | amount=%s | avg=%.8f | order=%s",
                symbol,
                reason,
                amount,
                avg,
                order.get("id"),
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("Exit failed %s: %s", symbol, exc)
            return

        self.positions.pop(symbol, None)

    async def _ensure_leverage(self, symbol: str) -> None:
        try:
            await self.exchange.set_leverage(self.config.leverage, symbol)
        except Exception as exc:  # noqa: BLE001
            # Many testnet accounts already have leverage set; non-fatal
            logger.debug("set_leverage %s: %s", symbol, exc)

    async def _amount_for_notional(self, symbol: str, price: float) -> float:
        market = self.exchange.market(symbol)
        notional = self.config.order_size_usdt
        amount = notional / price if price > 0 else 0.0
        amount = float(self.exchange.amount_to_precision(symbol, amount))

        min_amount = None
        limits = market.get("limits") or {}
        amount_limits = limits.get("amount") or {}
        min_amount = amount_limits.get("min")
        if min_amount is not None and amount < float(min_amount):
            amount = float(min_amount)
            amount = float(self.exchange.amount_to_precision(symbol, amount))

        cost_min = (limits.get("cost") or {}).get("min")
        if cost_min is not None and amount * price < float(cost_min):
            amount = math.ceil((float(cost_min) / price) * 1e8) / 1e8
            amount = float(self.exchange.amount_to_precision(symbol, amount))

        return amount

    async def _position_contracts(self, symbol: str) -> float:
        try:
            positions = await self.exchange.fetch_positions([symbol])
        except Exception:  # noqa: BLE001
            try:
                positions = await self.exchange.fetch_positions()
            except Exception as exc:  # noqa: BLE001
                logger.debug("fetch_positions failed: %s", exc)
                return 0.0

        for pos in positions:
            if pos.get("symbol") != symbol:
                continue
            contracts = pos.get("contracts")
            if contracts is None:
                continue
            try:
                value = abs(float(contracts))
            except (TypeError, ValueError):
                continue
            side = (pos.get("side") or "").lower()
            if side == "short":
                continue
            if value > 0:
                return float(self.exchange.amount_to_precision(symbol, value))
        return 0.0

    async def _sync_open_positions(self) -> None:
        """Adopt any already-open long positions from the account."""
        try:
            positions = await self.exchange.fetch_positions()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not sync positions: %s", exc)
            return

        for pos in positions:
            symbol = pos.get("symbol")
            if not symbol:
                continue
            try:
                contracts = abs(float(pos.get("contracts") or 0.0))
            except (TypeError, ValueError):
                continue
            side = (pos.get("side") or "").lower()
            if contracts <= 0 or side == "short":
                continue
            entry = float(pos.get("entryPrice") or 0.0)
            self.positions[symbol] = Position(
                symbol=symbol,
                amount=contracts,
                entry_price=entry,
            )
            logger.info("Synced open position: %s amount=%s", symbol, contracts)

    # ------------------------------------------------------------------ #
    # Exit watcher — candle close below EMA9
    # ------------------------------------------------------------------ #
    async def _exit_loop(self) -> None:
        await self._markets_ready.wait()
        watchers: dict[str, asyncio.Task] = {}

        while not self._stop.is_set():
            active = set(self.positions.keys())

            for symbol in list(watchers):
                if symbol not in active:
                    watchers[symbol].cancel()
                    watchers.pop(symbol, None)

            for symbol in active:
                if symbol not in watchers or watchers[symbol].done():
                    watchers[symbol] = asyncio.create_task(
                        self._watch_exit_symbol(symbol),
                        name=f"exit:{symbol}",
                    )

            await asyncio.sleep(0.5)

        for task in watchers.values():
            task.cancel()
        await asyncio.gather(*watchers.values(), return_exceptions=True)

    async def _watch_exit_symbol(self, symbol: str) -> None:
        last_closed_ts: int | None = None

        while not self._stop.is_set() and symbol in self.positions:
            try:
                ohlcv = await self.exchange.watch_ohlcv(
                    symbol,
                    timeframe=self.config.timeframe,
                    limit=max(self.config.ema_period + 5, 50),
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.warning("Exit OHLCV watch error %s: %s", symbol, exc)
                await asyncio.sleep(1)
                continue

            if len(ohlcv) < self.config.ema_period + 2:
                continue

            # Forming candle is last; previous candle is the most recent close
            closed = ohlcv[-2]
            closed_ts = int(closed[0])

            if last_closed_ts is None:
                # Seed with current closed candle; wait for the next boundary
                last_closed_ts = closed_ts
                continue

            if closed_ts <= last_closed_ts:
                continue

            last_closed_ts = closed_ts
            closes = [float(c[4]) for c in ohlcv[:-1]]
            ema9 = ema(closes, self.config.ema_period)
            if ema9 is None:
                continue

            closed_price = float(closed[4])
            if closed_price < ema9:
                logger.info(
                    "EXIT SIGNAL %s | closed=%.8f < EMA9=%.8f (candle %s)",
                    symbol,
                    closed_price,
                    ema9,
                    closed_ts,
                )
                await self._close_long(symbol, reason="5m close < EMA9")
                return


async def _amain() -> None:
    config = load_config()
    bot = FibonacciBreakoutBot(config)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, bot.request_stop)
        except NotImplementedError:
            pass

    await bot.run()


def main() -> None:
    try:
        asyncio.run(_amain())
    except KeyboardInterrupt:
        logger.info("Interrupted")
    except ValueError as exc:
        logger.error("%s", exc)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
