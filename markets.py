"""Binance Spot helpers shared by bot / backtest / tests."""

from __future__ import annotations

from typing import Any


def is_tradable_spot_usdt(market: dict[str, Any]) -> bool:
    """Active spot USDT pairs; skip leveraged tokens (BTCUP, ETHDOWN, …)."""
    if not market.get("active", True):
        return False
    if market.get("quote") != "USDT":
        return False
    mtype = (market.get("type") or "").lower()
    if mtype and mtype != "spot":
        return False
    if market.get("spot") is False:
        return False
    base = str(market.get("base") or "")
    upper = base.upper()
    if upper.endswith("UP") or upper.endswith("DOWN"):
        return False
    if "BULL" in upper or "BEAR" in upper:
        return False
    return True


def list_spot_usdt_symbols(markets: dict[str, Any]) -> list[str]:
    selected = [s for s, m in markets.items() if is_tradable_spot_usdt(m)]
    selected.sort()
    return selected
