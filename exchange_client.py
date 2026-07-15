"""Async Binance USD-M Futures exchange factory (ccxt.pro)."""

from __future__ import annotations

import logging

import ccxt.pro as ccxtpro

from config import Config

logger = logging.getLogger(__name__)


def create_exchange(config: Config) -> ccxtpro.binanceusdm:
    exchange = ccxtpro.binanceusdm(
        {
            "apiKey": config.api_key,
            "secret": config.api_secret,
            "enableRateLimit": True,
            "options": {
                "defaultType": "swap",
                "adjustForTimeDifference": True,
            },
        }
    )

    if config.use_testnet:
        exchange.set_sandbox_mode(True)
        logger.info("Exchange mode: Binance USD-M Futures TESTNET")
    else:
        logger.warning("Exchange mode: Binance USD-M Futures LIVE")

    return exchange
