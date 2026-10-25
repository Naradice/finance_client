"""Standalone download function for Alpha Vantage — no account/risk manager overhead."""

import pandas as pd

from .. import frames as Frame
from . import target as Target
from .client import VantageClient


def download_ohlc(
    api_key: str,
    symbols,
    length: int = None,
    frame: int = Frame.D1,
    finance_target=Target.FX,
) -> pd.DataFrame:
    """Download OHLC data from Alpha Vantage without trading machinery.

    Args:
        api_key (str): Alpha Vantage API key.
        symbols (str | list[str]): Symbol(s) to download (e.g. "USDJPY" or ["USDJPY", "EURUSD"]).
        length (int | None): Number of bars to return. None returns all available data.
        frame (int): Timeframe constant from finance_client.frames. Defaults to Frame.D1.
        finance_target: Market type — Target.FX, Target.STOCK, or Target.CRYPTO_CURRENCY. Defaults to Target.FX.

    Returns:
        pd.DataFrame: OHLC data sorted oldest-first.
    """
    if isinstance(symbols, str):
        symbols = [symbols]

    client = VantageClient(
        api_key=api_key,
        symbols=symbols,
        frame=frame,
        finance_target=finance_target,
        data_only=True,
    )
    return client.download(symbols, length=length, frame=frame)
