# Fibonacci Bollinger Instant Breakout Bot

Async Binance USDT-M Futures scanner using `ccxt.pro` + `asyncio`.

## Strategy

**Timeframe:** 5m  
**Indicator:** Fibonacci Bollinger Bands (VWMA basis, length 200, mult 3.0)

| Level | Role |
|-------|------|
| Upper 0.236 | Grey line just above purple basis — Open or Low must be below this |
| Upper 1.000 | Red line — instant market buy when last price breaks above |
| 9 EMA | Exit when a new 5m candle **closes** below it |

Universe: all active USDT-M swaps. Entry also needs breakout 5m quote volume ≥ 5,000,000 USDT and candle upside ≥ 5%.

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
```

Edit `.env`:

```env
BINANCE_API_KEY=...
BINANCE_SECRET=...
ORDER_USDT=50
MIN_CANDLE_QUOTE_VOL=5000000
MIN_CANDLE_PCT=5.0
MAX_ENTRIES_PER_DAY=2
```

## Run (Futures Testnet)

```bash
python bot.py
```

`bot.py` enables `set_sandbox_mode(True)` for [Binance Futures Testnet](https://testnet.binancefuture.com/).

If you use **Binance Demo Trading** (main site demo), in `bot.py` replace sandbox mode with:

```python
await self.exchange.enable_demo_trading(True)
```

and remove `self.exchange.set_sandbox_mode(True)`.

## Notes

- Only one long position at a time.
- Entry does **not** wait for candle close; exit does.
- Never commit real API secrets to git (`.env` is gitignored).
