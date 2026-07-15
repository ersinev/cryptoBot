"""Configuration loaded from environment / .env."""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


def _bool(value: str | None, default: bool = True) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Config:
    api_key: str
    api_secret: str
    use_testnet: bool = True
    min_quote_volume_usdt: float = 5_000_000.0
    order_size_usdt: float = 50.0
    max_positions: int = 1
    leverage: int = 5
    timeframe: str = "5m"
    fbb_length: int = 200
    fbb_mult: float = 3.0
    ema_period: int = 9
    universe_refresh_sec: int = 300
    indicator_refresh_sec: int = 20
    ohlcv_limit: int = 250
    fetch_concurrency: int = 8
    ticker_batch_size: int = 50


def load_config() -> Config:
    api_key = os.getenv("BINANCE_API_KEY", "").strip()
    api_secret = os.getenv("BINANCE_API_SECRET", "").strip()

    if not api_key:
        raise ValueError("BINANCE_API_KEY is required (set it in .env).")
    if not api_secret:
        raise ValueError(
            "BINANCE_API_SECRET is required. "
            "Add your Binance Futures Testnet secret to .env "
            "(https://testnet.binancefuture.com/)."
        )

    return Config(
        api_key=api_key,
        api_secret=api_secret,
        use_testnet=_bool(os.getenv("USE_TESTNET"), True),
        min_quote_volume_usdt=float(os.getenv("MIN_QUOTE_VOLUME_USDT", "5000000")),
        order_size_usdt=float(os.getenv("ORDER_SIZE_USDT", "50")),
        max_positions=int(os.getenv("MAX_POSITIONS", "1")),
        leverage=int(os.getenv("LEVERAGE", "5")),
    )
