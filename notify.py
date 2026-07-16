"""Telegram notifications for live bot fills."""

from __future__ import annotations

import asyncio
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

log = logging.getLogger("fbb-bot")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()


def telegram_enabled() -> bool:
    return bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)


def _send_sync(text: str) -> None:
    if not telegram_enabled():
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = urllib.parse.urlencode(
        {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "disable_web_page_preview": "true",
        }
    ).encode()
    req = urllib.request.Request(url, data=data, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        log.warning("Telegram HTTP %s: %s", exc.code, body[:200])
    except Exception as exc:  # noqa: BLE001
        log.warning("Telegram send failed: %s", exc)


async def notify(text: str) -> None:
    if not telegram_enabled():
        return
    await asyncio.to_thread(_send_sync, text)


async def notify_buy(
    symbol: str,
    *,
    price: float,
    qty: float,
    notional: float,
    stop_low: float,
    order_id: str | None = None,
) -> None:
    msg = (
        f"BUY {symbol}\n"
        f"price={price:.8f}\n"
        f"qty={qty}\n"
        f"~{notional:.2f} USDT\n"
        f"stop_low={stop_low:.8f}"
    )
    if order_id:
        msg += f"\norderId={order_id}"
    await notify(msg)


async def notify_sell(
    symbol: str,
    *,
    reason: str,
    entry: float,
    exit_price: float,
    qty: float,
    order_id: str | None = None,
) -> None:
    pnl_pct = ((exit_price / entry) - 1.0) * 100.0 if entry > 0 else 0.0
    pnl_usdt = qty * (exit_price - entry) if entry > 0 else 0.0
    sign = "+" if pnl_pct >= 0 else ""
    msg = (
        f"SELL {symbol}\n"
        f"reason={reason or 'close'}\n"
        f"entry={entry:.8f} → exit={exit_price:.8f}\n"
        f"PnL={sign}{pnl_pct:.2f}% ({sign}{pnl_usdt:.2f} USDT)"
    )
    if order_id:
        msg += f"\norderId={order_id}"
    await notify(msg)
