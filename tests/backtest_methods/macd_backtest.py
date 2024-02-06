import os
import sys

import dotenv
import pandas as pd

try:
    dotenv.load_dotenv("tests/.env")
except Exception:
    pass
module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
print(module_path)
sys.path.append(module_path)

from finance_client import fprocess, logger
from finance_client.csv.client import CSVClient

datetime_column = "Time"
ohlc_columns = ["Open", "High", "Low", "Close"]
additional_column = ["Adj Close", "Volume"]
csv_file = os.path.abspath("L:/data/csv/USDJPY_forex_min30.csv")


def __get_macd_trend(data: pd.DataFrame, target_column, signal_column_name, macd_column_name):
    # 1:long, -1:short
    if type(data) != type(None) and signal_column_name in data:
        signal = data[signal_column_name].iloc[-1]
        macd = data[macd_column_name].iloc[-1]
        if macd >= signal:
            return 1, data[target_column].iloc[-1]
        else:
            return -1, data[target_column].iloc[-1]
    return 0, None


def macd_cross(position, previouse_trend, data, target_column="Close", signal_column_name="Signal", macd_column_name="MACD"):
    tick_trend, price = __get_macd_trend(data, target_column, signal_column_name, macd_column_name)
    signal = None
    if tick_trend != 0:
        long_short = position
        if previouse_trend == 1 and tick_trend == -1:
            previouse_trend = tick_trend
            if long_short == 1:
                signal = "close_sell"
            else:
                signal = "sell"
        elif previouse_trend == -1 and tick_trend == 1:
            previouse_trend = tick_trend
            if long_short == -1:
                signal = "close_buy"
            else:
                signal = "buy"
    return signal, tick_trend


def MACD_backtest():
    macd_p = fprocess.MACDProcess(short_window=12, long_window=26, signal_window=9, target_column="Close")
    client = CSVClient(
        files=csv_file,
        date_column=datetime_column,
        symbols=["forex"],
        idc_process=[macd_p],
        start_index=100,
        enable_trade_log=True,
        logger=logger,
    )
    df = client.get_ohlc(30)
    position = 0
    trend = 0
    while len(df) == 30:
        signal, trend = macd_cross(
            position, trend, df, target_column="Close", signal_column_name=macd_p.KEY_SIGNAL, macd_column_name=macd_p.KEY_MACD
        )
        if signal is not None:
            if "close" in signal:
                if "buy" in signal:
                    client.close_position(position=pos)
                    suc, pos = client.open_trade(True, amount=1, order_type="Market", symbol="forex")
                    position = 1
                else:
                    client.close_position(position=pos)
                    suc, pos = client.open_trade(False, amount=1, order_type="Market", symbol="forex")
                    position = -1
            elif signal == "buy":
                suc, pos = client.open_trade(True, amount=1, order_type="Market", symbol="forex")
                position = 1
            elif signal == "sell":
                suc, pos = client.open_trade(False, amount=1, order_type="Market", symbol="forex")
                position = -1

        df = client.get_ohlc(30)


if __name__ == "__main__":
    MACD_backtest()
