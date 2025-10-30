import asyncio
import datetime
import logging
import uuid

import pandas as pd

from . import frames as Frame
from .client_base import ClientBase
from .fprocess import idcprocess
from .fprocess.fprocess.indicaters import technical
from .position import POSITION_TYPE

logger = logging.getLogger(__name__)

S_EMA_KEY = "EMA10"
M_EMA_KEY = "EMA50"
L_EMA_KEY = "EMA200"
MACD_KEY = "MACD"
MACD_SIG_KEY = "MACD_Signal"


class AgentTool:

    def __init__(self, client: ClientBase, max_volume=None):
        self.client = client
        self.max_volume = max_volume
        # for simulation, step index is used to simulate time
        self._step_index = self.client._step_index if hasattr(self.client, "_step_index") else 0
        self._EMA10 = idcprocess.EMAProcess(window=10, key=S_EMA_KEY, column="close")
        self._EMA50 = idcprocess.EMAProcess(window=50, key=M_EMA_KEY, column="close")
        self._EMA200 = idcprocess.EMAProcess(window=200, key=L_EMA_KEY, column="close")
        self._MACD = idcprocess.MACDProcess(key=MACD_KEY, target_column="close", short_window=12, long_window=26, signal_window=9)
        self._RSI = idcprocess.RSIProcess(window=14, key="RSI", ohlc_column_name=("open", "high", "low", "close"))
        self._Bollinger = idcprocess.BBANDProcess(window=20, key="Bollinger", target_column="close", alpha=2)
        self._ATR = idcprocess.ATRProcess(window=14, key="ATR", ohlc_column_name=("open", "high", "low", "close"))
        self._CCI = idcprocess.CCIProcess(window=20, key="CCI", ohlc_column=("open", "high", "low", "close"))

    def order(self, is_buy: bool, price: float, volume: float, symbol: str, order_type: int, tp: float, sl: float):
        """order to open a position
        Args:
            is_buy (bool): buy order or not
            price (float): order price for limit or stop order. Specify 0 if you order with market price
            volume (float): amount of trade volume. point * volume will be ordered.
            symbol (str): symbol of currency, stock etc. ex USDJPY.
            order_type (int): 0: Market, 1: Limit, 2: Stop
            tp (float): specify take profit price. if less than 0 is specified, order without tp
            sl (float): specify stop loss price. if less than 0 is specified, order without sl

        Returns:
            price (float): price returned from client. 0 if order is failed.
            id (str): id of position or order. error message is returned if order is failed.
        """
        logger.debug(f"tool:open_trade with {is_buy}, {price}, {volume}, {symbol}, {order_type}, {tp}, {sl}")
        if tp is None or tp <= 0:
            tp = None
        else:
            tp = float(tp)
        if sl is None or sl <= 0:
            sl = None
        else:
            sl = float(sl)
        if order_type == 0:
            price = None
        if price is not None:
            price = float(price)
        if volume > 0 and volume < 1:
            volume = volume * 100
        if self.max_volume is not None and volume > self.max_volume:
            volume = self.max_volume
        # sometimes AI Agent order limit order as stop order. So if price is invalid, it will be treated as a stop order.
        if order_type == 1:
            if is_buy:
                ask_price = self.get_ask_rate(symbol)
                if price >= float(ask_price):
                    order_type = 2
                    print("Changed order type to Stop")
            else:
                bid_price = self.get_bid_rate(symbol)
                if price <= float(bid_price):
                    order_type = 2
                    print("Changed order type to Stop")

        suc, position = self.client.open_trade(is_buy=is_buy, price=price, symbol=symbol, order_type=order_type, amount=volume, tp=tp, sl=sl)
        if suc and position is not None:
            result = {"price": str(position.price), "id": str(position.id)}
            self._step_index = 0
        else:
            if isinstance(position, str):
                result = {"price": 0, "msg": position}
            else:
                result = {"price": 0, "msg": "unknown error"}
        logger.debug(f"tool:open_trade result: {result}")
        return result

    def get_orders(self):
        """get all orders
        Returns:
            {
                $id: {
                    price (float): price of order,
                    volume (float): volume of order,
                    symbol (str): symbol of order,
                    is_buy (bool): True if long position,
                    tp (float): take profit price of order,
                    sl (float): stop loss price of order,
                    order_type (str): "limit", "stop",
                    mins_from_created (int): minutes from order creation
                }
            }
        """
        # logger.debug("tool:get_orders")
        return_orders_dict = {}
        if self.client.back_test:
            symbols = set()
            for order in orders:
                symbols.add(order.symbol)
            self.__advance_step(list(symbols))
            orders = self.client.get_orders()
        else:
            orders = self.client.get_orders()
        for order in orders:
            return_orders_dict[str(order.id)] = {
                "price": str(order.price),
                "volume": str(order.amount),
                "symbol": order.symbol,
                "order_type": order.order_type.name,
                "is_buy": True if order.position_type == POSITION_TYPE.long else False,
                "tp": "0" if order.tp is None else str(order.tp),
                "sl": "0" if order.sl is None else str(order.sl),
                "mins_from_created": str((datetime.datetime.now(tz=datetime.timezone.utc) - order.created).total_seconds() // 60),
            }

        # logger.debug(f"get_orders result: {len(return_orders_dict)}")
        return return_orders_dict

    def close_position(self, id: str, volume: float, symbol: str):
        """closed a position based on id. id should be specified which is returned when order it.
        Args:
            id (str): id if position
            volume (float): volume of position to close. if 0 is specified, close all volume
            symbol (str): symbol of currency, stock etc. ex USDJPY.
        Returns:
            closed_price (float): closed price. 0 if order is failed.
            profit(float): profit of your trade result. 0 if order is failed.
            msg (str): error message if order is failed.
        """
        logger.debug(f"close_position with {id}, {volume}, {symbol}")
        closed_result = self.client.close_position(price=None, id=id, amount=volume, symbol=symbol)
        if closed_result.error:
            result = {"closed_price": str(closed_result.price), "profit": str(closed_result.profit), "msg": closed_result.msg}
        else:
            result = {"closed_price": str(closed_result.price), "profit": str(closed_result.profit)}
        logger.debug(f"close_position result: {result}")
        return result

    def close_all_positions(self):
        """
        Returns:
            {
                $id:{
                    closed_price (float): price,
                    profit(float): profit by your order
                }
            }
        """
        logger.debug("tool:close_all_positions")
        results = self.client.close_all_positions()
        # convert result to dict for agent
        result_dict = {}
        for result in results:
            if result is not None:
                id = result.id
                suc = result.error is False
                if suc:
                    result_dict[id] = {"closed_price": str(result.price), "profit": str(result.profit)}
                else:
                    result_dict[id] = {"closed_price": str(result.price), "profit": str(result.profit), "msg": result.msg}
        logger.debug(f"close_all_positions result {len(result_dict)}")
        return result_dict

    def get_positions(self):
        """
        Returns:
            {
                $id: {
                    price (float): price of position,
                    volume (float): volume of position,
                    symbol (str): symbol of position,
                    is_buy (bool): True if long position,
                    tp (float): take profit price of position,
                    sl (float): stop loss price of position,
                }
            }
        """
        # logger.debug("tool: get_positions")
        try:
            self.__advance_step(None)
        except Exception as e:
            logger.error(f"Error in advancing step for positions: {e}")
        positions = self.client.get_positions()
        return_positions_dict = {}
        for position in positions:
            return_positions_dict[str(position.id)] = {
                "price": str(position.price),
                "volume": str(position.amount),
                "symbol": position.symbol,
                "is_buy": True if position.position_type == POSITION_TYPE.long else False,
                "tp": "0" if position.tp is None else str(position.tp),
                "sl": "0" if position.sl is None else str(position.sl),
            }
        # logger.debug(f"get_positions result: {len(return_positions_dict)}")
        return return_positions_dict

    def cancel_order(self, id: str):
        """
        Args:
            id(str): id of order
        Returns:
            {
                result(bool): True if Success
            }
        """
        logger.debug(f"tool: cancel_order for {id}")
        suc = self.client.cancel_order(id)
        message = "cancel_order success" if suc else "already canceled"
        logger.debug(f"cancel_order result {suc}")
        return {"result": suc, "message": message}

    def get_ask_rate(self, symbol: str):
        """return current ask rate of specified symbol

        Args:
            symbol (str): symbol of currency, stock etc. ex USDJPY.]
        Returns:
            rate (float)
        """
        logger.debug(f"tool: get_ask_rate for {symbol}")
        self.__advance_step(symbol)
        rates = self.client.get_current_ask(symbol)
        logger.debug(f"get_ask_rate result: {rates}")
        return rates

    def get_bid_rate(self, symbol: str):
        """return current bid rate of specified symbol

        Args:
            symbol (str): symbol of currency, stock etc. ex USDJPY.]
        Returns:
            rate (float)
        """
        logger.debug(f"tool: get_bid_rate for {symbol}")
        self.__advance_step(symbol)
        rates = self.client.get_current_bid(symbol)
        logger.debug(f"get_bid_rate result: {rates}")
        return rates

    def get_current_spread(self, symbol: str):
        """return current spread of specified symbol

        Args:
            symbol (str): symbol of currency, stock etc. ex USDJPY.]
        Returns:
            rate (float)
        """
        logger.debug(f"tool: get_current_spread for {symbol}")
        if hasattr(self.client, "get_current_spread"):
            spread = self.client.get_current_spread(symbol)
        else:
            ask_rates = self.client.get_current_ask(symbol)
            bid_rates = self.client.get_current_bid(symbol)
            spread = ask_rates - bid_rates
        logger.debug(f"get_current_spread result: {spread}")
        return spread

    def __get_ohlc(self, symbol: str, length: int, frame: str):
        """Internal method to get OHLC data from the client.
        Args:
            symbol (str): symbol of currency, stock etc. ex USDJPY.
            length (int): specify data length > 1. If less than 0 is specified, return all date.
            frame (str): specify frame to get time series data. any of Xmin (e.g. 1min), Xh(e.g. 1h), XD(e.g. 1D), WX(e.g. W1), MOX(e.g. MO1)
        Returns:
            pd.DataFrame: DataFrame containing OHLC data
        """
        self._step_index += 1
        ohlc_df = self.client.get_ohlc(symbol, length, frame)
        if ohlc_df is None:
            return {"open": {}, "high": {}, "low": {}, "close": {}}
        if ohlc_df.empty:
            return {"open": {}, "high": {}, "low": {}, "close": {}}
        # format
        ohlc_columns = self.client.get_ohlc_columns(symbol)
        ordered_columns = []
        fixed_columns = []
        for column_key in ["Open", "High", "Low", "Close", "Volume"]:
            if column_key in ohlc_columns:
                df_column_key = ohlc_columns[column_key]
                ordered_columns.append(df_column_key)
                fixed_columns.append(column_key.lower())
        ohlc_df = ohlc_df[ordered_columns]
        ohlc_df.columns = fixed_columns
        return ohlc_df

    def get_ohlc(self, symbol: str, length: int, frame: str):
        """
        Args:
            symbol (str): symbol of currency, stock etc. ex USDJPY.
            length (int): specify data length > 1. If less than 0 is specified, return all date.
            frame (str): specify frame to get time series data. any of Xmin (e.g. 1min), Xh(e.g. 1h), XD(e.g. 1D), WX(e.g. W1), MOX(e.g. MO1)

        Returns:
            {
                $index: {
                    "open": float,
                    "high": float,
                    "low": float,
                    "close": float
                },
                ...
            }
        """
        logger.debug(f"tool: get_ohlc for {symbol}, {length}, {frame}")
        ohlc_df = self.__get_ohlc(symbol, length, frame)
        if isinstance(ohlc_df, dict):
            return ohlc_df
        if isinstance(ohlc_df.index, pd.DatetimeIndex):
            ohlc_df.index = ohlc_df.index.strftime("%Y-%m-%dT%H:%M:%S%z")
        logger.debug(f"get_ohlc result: {ohlc_df.shape}")
        return ohlc_df.T.to_dict()

    def get_ohlc_with_indicators(self, symbol: str, length: int, frame: str):
        """
        Args:
            symbol (str): symbol of currency, stock etc. ex USDJPY.
            length (int): specify data length > 1. If less than 0 is specified, return all date.
            frame (str): specify frame to get time series data. any of Xmin (e.g. 1min), Xh(e.g. 1h), XD(e.g. 1D), WX(e.g. W1), MOX(e.g. MO1)

        Returns:
            {
                $index: {
                    "open": float,
                    "high": float,
                    "low": float,
                    "close": float,
                    "EMA10": float,
                    "EMA50": float,
                    "EMA200": float,
                    "MACD": float,
                    "MACD_Signal": float,
                    "RSI": float,
                    "RSI_Gain": float,
                    "RSI_Loss": float,
                    "Bollinger_UV": float,
                    "Bollinger_LV": float,
                    "Bollinger_Width": float,
                    "Bollinger_Std": float,
                    "ATR": float,
                    "CCI": float
                },
                ...
            }
        """
        ohlc_df = self.__get_ohlc(symbol, length + 210, frame)
        if isinstance(ohlc_df, dict):
            return {
                "open": {},
                "high": {},
                "low": {},
                "close": {},
                "EMA10": {},
                "EMA50": {},
                "EMA200": {},
                "MACD": {},
                "RSI": {},
                "Bollinger": {},
                "ATR": {},
                "CCI": {},
            }

        macd_df = self._MACD.run(ohlc_df)
        macd_df = macd_df[[self._MACD.KEY_MACD, self._MACD.KEY_SIGNAL]]
        ohlc_df = pd.concat([ohlc_df, macd_df], axis=1)
        ohlc_df = self._RSI.run(ohlc_df)
        bb_df = self._Bollinger.run(ohlc_df)
        bb_df = bb_df[
            [self._Bollinger.KEY_UPPER_VALUE, self._Bollinger.KEY_LOWER_VALUE, self._Bollinger.KEY_WIDTH_VALUE, self._Bollinger.KEY_STD_VALUE]
        ]
        ohlc_df = pd.concat([ohlc_df, bb_df], axis=1)
        ohlc_df = self._ATR.run(ohlc_df)
        ohlc_df = self._CCI.run(ohlc_df)
        try:
            ohlc_df = self._EMA10.run(ohlc_df)
            ohlc_df = self._EMA50.run(ohlc_df)
            ohlc_df = self._EMA200.run(ohlc_df)
        except Exception as e:
            logger.exception("Error occurred while calculating EMA indicators")

        ohlc_df = ohlc_df.iloc[-length:]  # Get the last 'length' rows
        ohlc_df = ohlc_df.map(lambda x: f"{x:.5f}" if isinstance(x, float) else str(x))
        if isinstance(ohlc_df.index, pd.DatetimeIndex):
            ohlc_df.index = ohlc_df.index.strftime("%Y-%m-%dT%H:%M:%S%z")

        return ohlc_df.T.to_dict()

    def get_current_datetime(self):
        date = self.client.get_current_datetime()
        if isinstance(date, datetime.datetime):
            return date.isoformat()
        return date

    def __advance_step(self, symbol):
        """advance step index for simulation"""
        if self.client.back_test and hasattr(self.client, "_step_index"):
            if self._step_index == self._step_index:
                # advance step index for back test in client. Order Completion would be checked in get_ohlc as well
                self.client.get_ohlc(symbol, 1, None)
                self._step_index = self.client._step_index
                logger.debug(f"tool: advance_step to {self._step_index}")
        return self._step_index


class PriceMonitor:

    def __init__(self, client_tool, event_queue):
        self.client_tool = client_tool
        self.event_queue = event_queue
        self._worker_params = {}

    def add_border_alert(self, symbol: str, time_frame: int, column: str, target_value: float, when: str, once: bool):
        """Add a worker to monitor price. If the price reaches the target value, an alert will be triggered.

        Args:
            symbol (str): 監視対象の通貨ペア
            time_frame (int): 監視する時間足（分単位）
            column (str): 監視するカラム名
            target_value (float): 目標値
            when (str, optional): "over" or "under". 目標値を超えたときに発火するか、下回ったときに発火するか。
            once (bool, optional): 一度だけ発火させるかどうか。

        Returns:
            bool: 成功した場合はTrue、失敗した場合はFalse
        """
        if time_frame > 60:
            if time_frame % 60 != 0:
                print(time_frame, "is not supported")
                return False
        elif time_frame <= 0:
            print(time_frame, "is not supported")
            return False
        key = (symbol, time_frame)
        if key not in self._worker_params:
            self._worker_params[key] = {}
        id = uuid.uuid4().hex
        self._worker_params[key][id] = {"type": "border", "column": column, "target_value": target_value, "when": when, "once": once}
        return True

    def _check_engulfing(self, df):
        signal_df = technical.bearish_engulfing(df, "open", "close")
        is_bear = bool(signal_df.iloc[-1])
        if is_bear:
            return "bear"
        signal_df = technical.bullish_engulfing(df, "open", "close")
        is_bull = bool(signal_df.iloc[-1])
        if is_bull:
            return "bull"
        return None

    def _check_pinbar(self, df):
        signal_df = technical.bearish_pinbar(df, ("open", "high", "low", "close"))
        is_bear = bool(signal_df.iloc[-1])
        if is_bear:
            return "bear"
        signal_df = technical.bullish_pinbar(df, ("open", "high", "low", "close"))
        is_bull = bool(signal_df.iloc[-1])
        if is_bull:
            return "bull"
        return None

    def _check_ma_cross(self, df):
        short_ema = df[S_EMA_KEY]
        long_ema = df[M_EMA_KEY]
        if (short_ema.iloc[-1] > long_ema.iloc[-1]) and (short_ema.iloc[-2] <= long_ema.iloc[-2]):
            return "bull"
        if (short_ema.iloc[-1] < long_ema.iloc[-1]) and (short_ema.iloc[-2] >= long_ema.iloc[-2]):
            return "bear"
        return None

    def _check_macd_cross(self, df):
        macd = df[MACD_KEY]
        macd_signal = df[MACD_SIG_KEY]
        if (macd.iloc[-1] > macd_signal.iloc[-1]) and (macd.iloc[-2] <= macd_signal.iloc[-2]):
            return "bull"
        if (macd.iloc[-1] < macd_signal.iloc[-1]) and (macd.iloc[-2] >= macd_signal.iloc[-2]):
            return "bear"
        return None

    def add_signal_alert(self, symbol: str, time_frame: int, indicator: str, once: bool):
        """Add a signal (bull/bear) alert for a specific indicator.

        Args:
            symbol (str): The trading pair symbol.
            time_frame (int): The time frame in minutes.
            indicator (str): The technical indicator to monitor. One of engulfing, pinbar, ema, macd.
            once (bool): Whether to trigger the alert only once.
        """
        if time_frame > 60:
            if time_frame % 60 != 0:
                print(time_frame, "is not supported")
                return False
        elif time_frame <= 0:
            print(time_frame, "is not supported")
            return False
        key = (symbol, time_frame)
        if key not in self._worker_params:
            self._worker_params[key] = {}
        id = uuid.uuid4().hex
        self._worker_params[key][id] = {"type": "signal", "indicator": indicator, "once": once}
        return True

    def _check_border(self, ohlc_dict, target_column, target_value, when):
        if target_column in ohlc_dict:
            if when == "over":
                if ohlc_dict[target_column][-1] >= target_value:
                    return True
            else:
                if ohlc_dict[target_column][-1] <= target_value:
                    return True
        else:
            print(f"{target_column} not found in OHLC data")
        return False

    def _check(self):
        params = self._worker_params.copy()
        for symbol, time_frame in params.keys():
            now = datetime.datetime.now(tz=datetime.timezone.utc)
            if time_frame < 60:
                match = now.minute % time_frame
            else:
                match = now.hour % (time_frame // 60)
            if match == 0:
                frame = Frame.to_freq_str(time_frame)
                ohlc_dict = self.client_tool.get_ohlc_with_indicators(symbol, 10, frame)
                for items in params[(symbol, time_frame)]:
                    for id, item in items.items():
                        try:
                            check_type = str(item["type"]).lower()
                        except Exception as e:
                            print(f"Error processing item {item}: {e}")
                            self._worker_params[(symbol, time_frame)].pop(id, None)
                            continue
                        if check_type == "signal":
                            indicator = item["indicator"]
                            if indicator == "macd":
                                signal = self._check_macd_cross(ohlc_dict)
                                if signal:
                                    self.event_queue.put(("price_technical", symbol, time_frame, "macd", signal))
                            elif indicator == "engulfing":
                                signal = self._check_engulfing(ohlc_dict)
                                if signal:
                                    self.event_queue.put(("price_technical", symbol, time_frame, "engulfing", signal))
                            elif indicator == "pinbar":
                                signal = self._check_pinbar(ohlc_dict)
                                if signal:
                                    self.event_queue.put(("price_technical", symbol, time_frame, "pinbar", signal))
                            elif indicator == "ema":
                                signal = self._check_ema(ohlc_dict)
                                if signal:
                                    self.event_queue.put(("price_technical", symbol, time_frame, "ema", signal))
                            else:
                                print(f"Unknown indicator: {indicator}")
                                self._worker_params[(symbol, time_frame)].pop(id, None)
                        elif check_type == "border":
                            target_value = item["target_value"]
                            when = item["when"]
                            column = item["column"]
                            remove = item["once"]
                            if self._check_border(ohlc_dict, column, target_value, when):
                                self.event_queue.put(("price_technical", symbol, time_frame, column, target_value))
                                if remove:
                                    self._worker_params[(symbol, time_frame)].pop(id, None)
                        else:
                            print(f"Unknown check type: {check_type}")

    async def start(self, max_count=100):
        count = 0
        while count < max_count:
            await self._check()
            await asyncio.sleep(60)
            count += 1
