# Fibonacci Bollinger Instant Breakout Bot (Binance Spot Demo)

Async Binance **Spot Demo** scanner using `ccxt.pro` + `asyncio`.

## Strategy

**Timeframe:** 1m entry / 5m EMA exit  
**Indicator:** Fibonacci Bollinger Bands (length 200, mult 3.0)

| Level | Role |
|-------|------|
| Upper 0.236 | Grey — Open or Low arms the setup |
| Upper 1.000 | Red — instant market buy when high breaks |
| Entry candle low | Structure stop |
| 9 EMA (5m) | Exit until trail arms |
| Trail | After 1m close +TRAIL_ACTIVATE_PCT%, trail TRAIL_PCT% from high (floor=entry) |

Universe: all active spot USDT pairs (leveraged tokens excluded).

## Setup

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env        # Windows: copy .env.example .env
```

Edit `.env` with **Demo Trading** API keys:

1. binance.com → **Demo Trading** mode  
2. API Management → create key (Spot)  
3. Paste into `.env`

```env
BINANCE_API_KEY=...
BINANCE_SECRET=...
ORDER_USDT=50
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
```

## Run

```bash
python bot.py
python test_bot.py
python backtest.py
```

`bot.py` calls `enable_demo_trading(True)` — fake USDT, no real money.

## Live Spot later

Remove / disable `enable_demo_trading(True)` and use a live Spot API key.

## Notes

- Long-only spot: buys spend USDT, sells the base coin.
- Never commit `.env` (gitignored).
