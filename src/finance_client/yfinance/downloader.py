"""Standalone download function for Yahoo Finance — no account/risk manager overhead."""

import pandas as pd

from .. import frames as Frame
from .client import YahooClient


def download_ohlc(
    symbols,
    length: int = None,
    frame: int = Frame.D1,
    adjust_close: bool = False,
) -> pd.DataFrame:
    """Download OHLC data from Yahoo Finance without trading machinery.

    Args:
        symbols (str | list[str]): Symbol(s) to download (e.g. "AAPL" or ["AAPL", "MSFT"]).
        length (int | None): Number of bars to return. None returns all available data.
        frame (int): Timeframe constant from finance_client.frames. Defaults to Frame.D1.
        adjust_close (bool): Use adjusted close prices. Defaults to False.

    Returns:
        pd.DataFrame: OHLC data sorted oldest-first.
    """
    if isinstance(symbols, str):
        symbols = [symbols]

    client = YahooClient(
        symbols=symbols,
        frame=frame,
        adjust_close=adjust_close,
        data_only=True,
    )
    return client.download(symbols, length=length, frame=frame)
