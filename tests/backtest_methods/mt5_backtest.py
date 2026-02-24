import os
import sys
import time

import dotenv
import pandas as pd

base_path = os.path.abspath(f"{os.path.dirname(__file__)}/..")
dotenv.load_dotenv(f"{base_path}/.env")
module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(module_path)

from finance_client import Frame, fprocess
from finance_client.mt5.client import MT5Client
from finance_client.position import ORDER_TYPE


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


def macd_cross(position, previouse_trend, data, target_column="close", signal_column_name="Signal", macd_column_name="MACD"):
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


def MACD_backtest(symbol="USDJPY"):
    macd_p = fprocess.MACDProcess(short_window=12, long_window=26, signal_window=9, target_column="close")
    client = MT5Client(
        id=int(os.environ["mt5_id"]),
        password=os.environ["mt5_password"],
        server=os.environ["mt5_server"],
        frame=Frame.D1,
        simulation=False,
        back_test=True,
        auto_index=True,
        idc_process=[macd_p],
    )
    df = client.get_ohlc(symbols=symbol, length=30)
    position = 0
    trend = 0
    while len(df) >= 30:
        signal, trend = macd_cross(position, trend, df, target_column="close", signal_column_name=macd_p.KEY_SIGNAL, macd_column_name=macd_p.KEY_MACD)
        if signal is not None:
            if "close" in signal:
                if "buy" in signal:
                    client.close_position(id=id)
                    suc, id = client.open_trade(True, volume=1, order_type=ORDER_TYPE.market, symbol=symbol)
                    position = 1
                else:
                    client.close_position(id=id)
                    suc, id = client.open_trade(False, volume=1, order_type=ORDER_TYPE.market, symbol=symbol)
                    position = -1
            elif signal == "buy":
                suc, id = client.open_trade(True, volume=1, order_type=ORDER_TYPE.market, symbol=symbol)
                position = 1
            elif signal == "sell":
                suc, id = client.open_trade(False, volume=1, order_type=ORDER_TYPE.market, symbol=symbol)
                position = -1
        time.sleep(2)
        df = client.get_ohlc(symbols=symbol, length=30)


if __name__ == "__main__":
    MACD_backtest()
