import asyncio
import datetime
import logging
import math
import uuid

import pandas as pd

from . import frames as Frame
from .client_base import ClientBase
from .fprocess import idcprocess
from .fprocess.fprocess.indicaters import technical
from .risk_manager.risk_options.percent_equity import PercentEquityRisk
from .position import POSITION_SIDE

logger = logging.getLogger(__name__)

S_EMA_KEY = "EMA10"
M_EMA_KEY = "EMA50"
L_EMA_KEY = "EMA200"
MACD_KEY = "MACD"
MACD_SIG_KEY = "MACD_Signal"
SMA20_KEY = "SMA20"


class AgentTool:

    def __init__(self, client: ClientBase, max_volume=None, max_length=100, risk_option=None):
        self.client = client
        self.max_volume = max_volume
        self.risk_option = risk_option or PercentEquityRisk(percent=1.0)
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
        self._SMA20 = idcprocess.MAProcess(window=20, key=SMA20_KEY, column="close")
        self.max_length = max_length

    def order(self, is_buy: bool, price: float, volume: float, symbol: str, order_type: int, tp: float, sl: float):
        """Place an order to open a new position.

        Args:
            is_buy (bool): True for a buy (long) order, False for a sell (short) order.
            price (float): order price for limit or stop orders. Specify 0 for market orders.
            volume (float): trade size in lots. The actual notional = base_unit * volume.
            symbol (str): currency pair or instrument symbol, e.g. "USDJPY".
            order_type (int): 0 = Market order (executes immediately at current price),
                              1 = Limit order (buy below / sell above the specified price),
                              2 = Stop order (buy above / sell below the specified price).
            tp (float): take-profit price. Specify 0 or a negative value to place the order without a take-profit.
            sl (float): stop-loss price. Specify 0 or a negative value to place the order without a stop-loss.

        Returns:
            On success: {"price": str, "id": str}
                price — actual fill price returned by the broker.
                id    — position or pending-order ID assigned by the broker.
            On failure: {"price": "0", "msg": str}
                msg — error description from the broker.
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
        if order_type is None:
            logger.info("order_type is None, set to Market order")
            order_type = 0
        if order_type == 0:
            price = None
            logger.info("Market order, price is set to None")
        if price is not None:
            price = float(price)
        if self.max_volume is not None and volume > self.max_volume:
            volume = self.max_volume
        # sometimes AI Agent order limit order as stop order. So if price is invalid, it will be treated as a stop order.
        if order_type == 1:
            if is_buy:
                ask_price = self.get_ask_rate(symbol)
                if price >= float(ask_price):
                    order_type = 2
                    logger.debug("Changed order type to Stop")
            else:
                bid_price = self.get_bid_rate(symbol)
                if price <= float(bid_price):
                    order_type = 2
                    logger.debug("Changed order type to Stop")

        suc, position = self.client.open_trade(is_buy=is_buy, price=price, symbol=symbol, order_type=order_type, volume=volume, tp=tp, sl=sl)
        if suc and position is not None:
            result = {"price": str(position.price), "id": str(position.id)}
            self._step_index = 0
        else:
            if isinstance(position, str):
                result = {"price": "0", "msg": position}
            else:
                result = {"price": "0", "msg": "unknown error"}
        logger.debug(f"tool:open_trade result: {result}")
        return result

    def smart_order(self, is_buy: bool, price: float, symbol: str, order_type: int, tp: float, sl: float):
        """Place an order to open a new position. volume is calculated by risk management module based on stop loss level and risk percentage.

        Args:
            is_buy (bool): True for a buy (long) order, False for a sell (short) order.
            price (float): order price for limit or stop orders. Specify 0 for market orders.
            symbol (str): currency pair or instrument symbol, e.g. "USDJPY".
            order_type (int): 0 = Market order (executes immediately at current price),
                              1 = Limit order (buy below / sell above the specified price),
                              2 = Stop order (buy above / sell below the specified price).
            tp (float): take-profit price.
            sl (float): stop-loss price.

        Returns:
            On success: {"price": str, "id": str}
                price — actual fill price returned by the broker.
                id    — position or pending-order ID assigned by the broker.
            On failure: {"price": "0", "msg": str}
                msg — error description from the broker.
        """
        logger.debug(f"tool:smart_order with {is_buy}, {price}, {symbol}, {order_type}, {tp}, {sl}")
        if tp is None or tp <= 0:
            tp = None
        else:
            tp = float(tp)
        if sl is None or sl <= 0:
            sl = None
        else:
            sl = float(sl)
        if order_type is None:
            logger.info("order_type is None, set to Market order")
            order_type = 0
        if order_type == 0:
            price = None
            logger.info("Market order, price is set to None")
        if price is not None:
            price = float(price)
        # sometimes AI Agent order limit order as stop order. So if price is invalid, it will be treated as a stop order.
        if order_type == 1:
            if is_buy:
                ask_price = self.get_ask_rate(symbol)
                if price >= float(ask_price):
                    order_type = 2
                    logger.debug("Changed order type to Stop")
            else:
                bid_price = self.get_bid_rate(symbol)
                if price <= float(bid_price):
                    order_type = 2
                    logger.debug("Changed order type to Stop")

        if self.client.risk_option:
            suc, position = self.client.smart_order(is_buy=is_buy, entry_price=price, symbol=symbol, order_type=order_type, tp=tp, sl=sl)
        else:
            suc, position = self.client.smart_order(
                is_buy=is_buy, entry_price=price, symbol=symbol, risk_option=self.risk_option, order_type=order_type, tp=tp, sl=sl
            )
        if suc and position is not None:
            result = {"price": str(position.price), "id": str(position.id)}
            self._step_index = 0
        else:
            if isinstance(position, str):
                result = {"price": "0", "msg": position}
            else:
                result = {"price": "0", "msg": "unknown error"}
        logger.debug(f"tool:smart_order result: {result}")
        return result

    def get_orders(self):
        """Return all currently pending (unfilled) orders.

        An empty dict {} is the normal response when no pending orders exist —
        it does NOT indicate an error.

        Returns:
            dict keyed by order ID string. Each value contains:
                price (str): trigger price of the pending order.
                volume (str): order size in lots.
                symbol (str): instrument symbol, e.g. "USDJPY".
                is_buy (bool): True for a buy order, False for a sell order.
                tp (str): take-profit price, or "0" if not set.
                sl (str): stop-loss price, or "0" if not set.
                order_type (str): "limit" or "stop".
                mins_from_created (str): minutes elapsed since the order was placed.
            Example with one order: {"123": {"price": "155.00", "volume": "0.1", ...}}
            Example with no orders: {}
        """
        # logger.debug("tool:get_orders")
        return_orders_dict = {}
        if self.client.back_test:
            symbols = set()
            self.__advance_step(list(symbols))
            orders = self.client.get_orders()
            for order in orders:
                symbols.add(order.symbol)
        else:
            orders = self.client.get_orders()
        for order in orders:
            return_orders_dict[str(order.id)] = {
                "price": str(order.price),
                "volume": str(order.volume),
                "symbol": order.symbol,
                "order_type": order.order_type.name,
                "is_buy": True if order.position_side == POSITION_SIDE.long else False,
                "tp": "0" if order.tp is None else str(order.tp),
                "sl": "0" if order.sl is None else str(order.sl),
                "mins_from_created": str((datetime.datetime.now(tz=datetime.timezone.utc) - order.created).total_seconds() // 60),
            }

        # logger.debug(f"get_orders result: {len(return_orders_dict)}")
        return return_orders_dict

    def close_position(self, id: str, volume: float):
        """Close an open position by its ID.

        Args:
            id (str): position ID as returned by order() when the position was opened.
            volume (float): volume to close in lots. Specify 0 to close the entire position.

        Returns:
            On success: {"closed_price": str, "profit": str}
                closed_price — actual execution price at which the position was closed.
                profit       — realised profit/loss for this trade in account currency.
            On failure: {"closed_price": "0", "profit": "0", "msg": str}
                msg — error description (e.g. position ID not found).
        """
        logger.debug(f"close_position with {id}, {volume}")
        try:
            closed_result = self.client.close_position(id=id, volume=volume)
        except Exception as e:
            logger.exception(f"Error in close_position")
            return {"closed_price": "0", "profit": "0", "msg": str(e)}
        if closed_result.error:
            result = {"closed_price": str(closed_result.price), "profit": str(closed_result.profit), "msg": closed_result.msg}
        else:
            result = {"closed_price": str(closed_result.price), "profit": str(closed_result.profit)}
        logger.debug(f"close_position result: {result}")
        return result

    def close_all_positions(self):
        """Close every open position at the current market price.

        An empty dict {} is the normal response when there were no open positions to close —
        it does NOT indicate an error.

        Returns:
            dict keyed by position ID string. Each value contains:
                closed_price (str): execution price at which the position was closed.
                profit (str): realised profit/loss for that position.
            On partial failure an additional "msg" key is included for that position.
            Example: {"456": {"closed_price": "155.20", "profit": "320.00"}}
            Example with nothing to close: {}
            On total failure: {"msg": "failed to close all positions"}
        """
        logger.debug("tool:close_all_positions")
        try:
            results = self.client.close_all_positions()
        except Exception:
            logger.exception(f"Error in close_all_positions")
            return {"msg": "failed to close all positions"}
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
        """Return all currently open positions.

        IMPORTANT: An empty dict {} is the completely normal response when no positions
        are held (e.g. after a take-profit or stop-loss has closed them). An empty result
        is NOT an error and should NOT be treated as a failure to retrieve data.
        Only a dict containing a "msg" key indicates an actual error.

        Returns:
            dict keyed by position ID string. Each value contains:
                price (str): average open price of the position.
                volume (str): position size in lots.
                symbol (str): instrument symbol, e.g. "USDJPY".
                is_buy (bool): True for a long position, False for a short position.
                tp (str): take-profit price, or "0" if not set.
                sl (str): stop-loss price, or "0" if not set.
            Example with one position: {"789": {"price": "154.80", "volume": "0.1", "symbol": "USDJPY", "is_buy": true, "tp": "156.00", "sl": "153.50"}}
            Example with no positions (flat / all closed): {}
            On error: {"msg": "failed to get positions"}
        """
        logger.debug("tool: get_positions called")
        try:
            self.__advance_step(None)
        except Exception as e:
            logger.error(f"Error in advancing step for positions: {e}")
        try:
            positions = self.client.get_positions()
        except Exception:
            logger.exception("tool: get_positions raised exception")
            return {"msg": "failed to get positions"}
        logger.debug("tool: get_positions raw result count=%s", len(positions) if positions is not None else "None")
        return_positions_dict = {}
        for position in positions:
            return_positions_dict[str(position.id)] = {
                "price": str(position.price),
                "volume": str(position.volume),
                "symbol": position.symbol,
                "is_buy": True if position.position_side == POSITION_SIDE.long else False,
                "tp": "0" if position.tp is None else str(position.tp),
                "sl": "0" if position.sl is None else str(position.sl),
            }
        logger.debug("tool: get_positions returning %s positions: %s", len(return_positions_dict), list(return_positions_dict.keys()))
        return return_positions_dict

    def cancel_order(self, id: str):
        """Cancel a pending (unfilled) order by its ID.

        Args:
            id (str): order ID as returned by order() when a limit or stop order was placed.

        Returns:
            {"result": bool, "message": str}
                result  — True if the order was successfully cancelled.
                message — "cancel_order success" on success, or a description of the problem
                          (e.g. "already canceled" if the order no longer exists).
        """
        logger.debug(f"tool: cancel_order for {id}")
        try:
            suc = self.client.cancel_order(id)
            message = "cancel_order success" if suc else "already canceled"
        except Exception:
            logger.exception(f"Error in cancel_order")
            suc = False
            message = str(e)
        logger.debug(f"cancel_order result {suc}")
        return {"result": suc, "message": message}

    def get_ask_rate(self, symbol: str):
        """Return the current ask (offer) price for a symbol.

        Use this for buy orders — the ask is the price you pay when buying.

        Args:
            symbol (str): instrument symbol, e.g. "USDJPY".

        Returns:
            str: current ask price as a string (e.g. "154.823").
                 Returns "failed to get ask rate" if the price could not be retrieved.
        """
        logger.debug(f"tool: get_ask_rate for {symbol}")
        self.__advance_step(symbol)
        rate = self.client.get_current_ask(symbol)
        logger.debug(f"get_ask_rate result: {rate}")
        try:
            ask_rate = str(rate)
        except Exception:
            ask_rate = "failed to get ask rate"
        return ask_rate

    def get_bid_rate(self, symbol: str):
        """Return the current bid price for a symbol.

        Use this for sell orders — the bid is the price you receive when selling.

        Args:
            symbol (str): instrument symbol, e.g. "USDJPY".

        Returns:
            str: current bid price as a string (e.g. "154.820").
                 Returns "failed to get bid rate" if the price could not be retrieved.
        """
        logger.debug(f"tool: get_bid_rate for {symbol}")
        self.__advance_step(symbol)
        rates = self.client.get_current_bid(symbol)
        logger.debug(f"get_bid_rate result: {rates}")
        try:
            rates = str(rates)
        except Exception:
            rates = "failed to get bid rate"
        return rates

    def get_current_spread(self, symbol: str):
        """return current spread of specified symbol

        Args:
            symbol (str): symbol of currency, stock etc. ex USDJPY.]
        Returns:
            rate (str)
        """
        logger.debug(f"tool: get_current_spread for {symbol}")
        if hasattr(self.client, "get_current_spread"):
            spread = self.client.get_current_spread(symbol)
        else:
            ask_rates = self.client.get_current_ask(symbol)
            bid_rates = self.client.get_current_bid(symbol)
            spread = ask_rates - bid_rates
        logger.debug(f"get_current_spread result: {spread}")
        try:
            spread = str(spread)
        except Exception:
            spread = "failed to get spread"
        return spread

    def get_unit_size(self, symbol: str) -> str:
        """return unit size of specified symbol

        Args:
            symbol (str): symbol of currency, stock etc. ex USDJPY.]
        Returns:
            unit_size (float)
        """
        logger.debug(f"tool: get_unit_size for {symbol}")
        unit_size = self.client.get_unit_size(symbol)
        logger.debug(f"get_unit_size result: {unit_size}")
        try:
            unit_size = str(unit_size)
        except Exception:
            unit_size = "failed to get unit size"
        return unit_size

    def __get_ohlc(self, symbol: str, length: int, frame: str):
        """Internal method to get OHLC data from the client.
        Args:
            symbol (str): symbol of currency, stock etc. ex USDJPY.
            length (int): specify data length > 0.
            frame (str): specify frame to get time series data. any of Xmin (e.g. 1min), Xh(e.g. 1h), XD(e.g. 1D), WX(e.g. W1), MOX(e.g. MO1)
        Returns:
            pd.DataFrame: DataFrame containing OHLC data
        """
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
            length (int): specify data length > 0.
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
        self._step_index += 1
        if isinstance(ohlc_df, dict):
            return ohlc_df
        if isinstance(ohlc_df.index, pd.DatetimeIndex):
            ohlc_df.index = ohlc_df.index.strftime("%Y-%m-%dT%H:%M:%S%z")
        logger.debug(f"get_ohlc result: {ohlc_df.shape}")
        return ohlc_df.T.to_dict()

    def _get_ohlc_with_indicators(self, symbol: str, length: int, frame: str):
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
            ohlc_df = self._SMA20.run(ohlc_df)
        except Exception as e:
            logger.exception("Error occurred while calculating EMA/SMA indicators")

        ohlc_df = ohlc_df.iloc[-length:]  # Get the last 'length' rows
        return ohlc_df

    def get_ohlc_with_indicators(self, symbol: str, length: int, frame: str):
        """
        Args:
            symbol (str): symbol of currency, stock etc. ex USDJPY.
            length (int): specify data length > 0.
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
        logger.debug(f"tool: get_ohlc_with_indicators for {symbol}, {length}, {frame}")
        ohlc_df = self._get_ohlc_with_indicators(symbol, length, frame)
        self._step_index += 1
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

    def get_MACD(self, symbol: str, length: int, frame: str, short_window: int, long_window: int, signal_window: int):
        """get MACD and it's signal values based on close value.

        Args:
            symbol (str): symbol of currency, stock etc. ex USDJPY.
            length (int): specify data length > 0.
            frame (str): specify frame to get time series data. any of Xmin (e.g. 1min), Xh (e.g. 1h), XD (e.g. 1D), WX (e.g. W1), MOX (e.g. MO1)
            short_window (int): window for short EMA
            long_window (int): window for long EMA
            signal_window (int): window for signal line EMA

        Returns:
            str: CSV format data with index, MACD, SIGNAL columns
        """
        logger.debug(f"tool: get_MACD for {symbol}, {length}, {frame}, {short_window}, {long_window}, {signal_window}")
        process = idcprocess.MACDProcess(target_column="close", short_window=short_window, long_window=long_window, signal_window=signal_window)
        # clip length by max_length
        if length > self.max_length:
            length = self.max_length

        query_length = length + long_window + signal_window
        ohlc_df = self.__get_ohlc(symbol, query_length, frame)
        macd_df = process.run(ohlc_df)
        macd_df = macd_df[[process.KEY_MACD, process.KEY_SIGNAL]]
        macd_df.columns = ["MACD", "SIGNAL"]
        macd_df = macd_df.iloc[-length:]
        macd_df = macd_df.map(lambda x: f"{x:.5f}" if isinstance(x, float) else str(x))
        if isinstance(macd_df.index, pd.DatetimeIndex):
            macd_df.index = macd_df.index.strftime("%Y-%m-%dT%H:%M:%S%z")
        csv_data = macd_df.to_csv()
        return csv_data

    # idcprocess.ATRProcess
    def get_ATR(self, symbol: str, length: int, frame: str, window: int):
        """get ATR values based on OHLC values.

        Args:
            symbol (str): symbol of currency, stock etc. ex USDJPY.
            length (int): specify data length > 0.
            frame (str): specify frame to get time series data. any of Xmin (e.g. 1min), Xh (e.g. 1h), XD (e.g. 1D), WX (e.g. W1), MOX (e.g. MO1)
            window (int): window for ATR calculation

        Returns:
            str: CSV format data with index, ATR columns
        """
        logger.debug(f"tool: get_ATR for {symbol}, {length}, {frame}, {window}")
        process = idcprocess.ATRProcess(window=window, key="ATR", ohlc_column_name=("open", "high", "low", "close"))
        # clip length by max_length
        if length > self.max_length:
            length = self.max_length

        query_length = length + window
        ohlc_df = self.__get_ohlc(symbol, query_length, frame)
        atr_df = process.run(ohlc_df)
        atr_df = atr_df[[process.KEY_ATR]]
        atr_df.columns = ["ATR"]
        atr_df = atr_df.iloc[-length:]
        atr_df = atr_df.map(lambda x: f"{x:.5f}" if isinstance(x, float) else str(x))
        if isinstance(atr_df.index, pd.DatetimeIndex):
            atr_df.index = atr_df.index.strftime("%Y-%m-%dT%H:%M:%S%z")
        csv_data = atr_df.to_csv()
        return csv_data

    # idcprocess.BBANDProcess
    def get_BollingerBands(self, symbol: str, length: int, frame: str, window: int, alpha: float):
        """get Bollinger Bands values based on close values.

        Args:
            symbol (str): symbol of currency, stock etc. ex USDJPY.
            length (int): specify data length > 0.
            frame (str): specify frame to get time series data. any of Xmin (e.g. 1min), Xh (e.g. 1h), XD (e.g. 1D), WX (e.g. W1), MOX (e.g. MO1)
            window (int): window for Bollinger Bands calculation
            alpha (float): alpha value for Bollinger Bands calculation

        Returns:
            str: CSV format data with index, UpperBand, LowerBand, Width, StdDev columns
        """
        logger.debug(f"tool: get_BollingerBands for {symbol}, {length}, {frame}, {window}, {alpha}")
        process = idcprocess.BBANDProcess(window=window, key="Bollinger", target_column="close", alpha=alpha)
        # clip length by max_length
        if length > self.max_length:
            length = self.max_length

        query_length = length + window
        ohlc_df = self.__get_ohlc(symbol, query_length, frame)
        bband_df = process.run(ohlc_df)
        bband_df = bband_df[[process.KEY_UPPER_VALUE, process.KEY_LOWER_VALUE, process.KEY_WIDTH_VALUE, process.KEY_STD_VALUE]]
        bband_df.columns = ["UpperBand", "LowerBand", "Width", "StdDev"]
        bband_df = bband_df.iloc[-length:]
        bband_df = bband_df.map(lambda x: f"{x:.5f}" if isinstance(x, float) else str(x))
        if isinstance(bband_df.index, pd.DatetimeIndex):
            bband_df.index = bband_df.index.strftime("%Y-%m-%dT%H:%M:%S%z")
        csv_data = bband_df.to_csv()
        return csv_data

    # idcprocess.RSIProcess
    def get_RSI(self, symbol: str, length: int, frame: str, window: int):
        """get RSI values based on close values.

        Args:
            symbol (str): symbol of currency, stock etc. ex USDJPY.
            length (int): specify data length > 0.
            frame (str): specify frame to get time series data. any of Xmin (e.g. 1min), Xh (e.g. 1h), XD (e.g. 1D), WX (e.g. W1), MOX (e.g. MO1)
            window (int): window for RSI calculation

        Returns:
            str: CSV format data with index, RSI, Gain, Loss columns
        """
        logger.debug(f"tool: get_RSI for {symbol}, {length}, {frame}, {window}")
        process = idcprocess.RSIProcess(window=window, key="RSI", ohlc_column_name=("open", "high", "low", "close"))
        # clip length by max_length
        if length > self.max_length:
            length = self.max_length

        query_length = length + window
        ohlc_df = self.__get_ohlc(symbol, query_length, frame)
        rsi_df = process.run(ohlc_df)
        rsi_df = rsi_df[[process.KEY_RSI, process.KEY_GAIN, process.KEY_LOSS]]
        rsi_df.columns = ["RSI", "Gain", "Loss"]
        rsi_df = rsi_df.iloc[-length:]
        rsi_df = rsi_df.map(lambda x: f"{x:.5f}" if isinstance(x, float) else str(x))
        if isinstance(rsi_df.index, pd.DatetimeIndex):
            rsi_df.index = rsi_df.index.strftime("%Y-%m-%dT%H:%M:%S%z")
        csv_data = rsi_df.to_csv()
        return csv_data

    # idcprocess.MAProcess
    def get_SMA(self, symbol: str, length: int, frame: str, window: int):
        """get Simple Mean Average values based on close values.

        Args:
            symbol (str): symbol of currency, stock etc. ex USDJPY.
            length (int): specify data length > 0.
            frame (str): specify frame to get time series data. any of Xmin (e.g. 1min), Xh (e.g. 1h), XD (e.g. 1D), WX (e.g. W1), MOX (e.g. MO1)
            window (int): window for MA calculation

        Returns:
            str: CSV format data with index, MA columns
        """
        logger.debug(f"tool: get_SMA for {symbol}, {length}, {frame}, {window}")
        process = idcprocess.MAProcess(window=window, key="MA", column="close")
        # clip length by max_length
        if length > self.max_length:
            length = self.max_length

        query_length = length + window
        ohlc_df = self.__get_ohlc(symbol, query_length, frame)
        ma_df = process.run(ohlc_df)
        ma_df = ma_df[[process.KEY_EMA]]
        ma_df.columns = ["MA"]
        ma_df = ma_df.iloc[-length:]
        ma_df = ma_df.map(lambda x: f"{x:.5f}" if isinstance(x, float) else str(x))
        if isinstance(ma_df.index, pd.DatetimeIndex):
            ma_df.index = ma_df.index.strftime("%Y-%m-%dT%H:%M:%S%z")
        csv_data = ma_df.to_csv()
        return csv_data

    # idcprocess.EMAProcess
    def get_EMA(self, symbol: str, length: int, frame: str, window: int):
        """get Exponential Mean Average values based on close values.

        Args:
            symbol (str): symbol of currency, stock etc. ex USDJPY.
            length (int): specify data length > 0.
            frame (str): specify frame to get time series data. any of Xmin (e.g. 1min), Xh (e.g. 1h), XD (e.g. 1D), WX (e.g. W1), MOX (e.g. MO1)
            window (int): window for EMA calculation

        Returns:
            str: CSV format data with index, EMA columns
        """
        logger.debug(f"tool: get_EMA for {symbol}, {length}, {frame}, {window}")
        process = idcprocess.EMAProcess(window=window, key="EMA", column="close")
        # clip length by max_length
        if length > self.max_length:
            length = self.max_length

        query_length = length + window
        ohlc_df = self.__get_ohlc(symbol, query_length, frame)
        ema_df = process.run(ohlc_df)
        ema_df = ema_df[[process.key]]
        ema_df.columns = ["EMA"]
        ema_df = ema_df.iloc[-length:]
        ema_df = ema_df.map(lambda x: f"{x:.5f}" if isinstance(x, float) else str(x))
        if isinstance(ema_df.index, pd.DatetimeIndex):
            ema_df.index = ema_df.index.strftime("%Y-%m-%dT%H:%M:%S%z")
        csv_data = ema_df.to_csv()
        return csv_data

    # idcprocess.CCIProcess
    def get_CCI(self, symbol: str, length: int, frame: str, window: int):
        """get CCI values based on OHLC values.

        Args:
            symbol (str): symbol of currency, stock etc. ex USDJPY.
            length (int): specify data length > 0.
            frame (str): specify frame to get time series data. any of Xmin (e.g. 1min), Xh (e.g. 1h), XD (e.g. 1D), WX (e.g. W1), MOX (e.g. MO1)
            window (int): window for CCI calculation

        Returns:
            str: CSV format data with index, CCI columns
        """
        logger.debug(f"tool: get_CCI for {symbol}, {length}, {frame}, {window}")
        process = idcprocess.CCIProcess(window=window, key="CCI", ohlc_column=("open", "high", "low", "close"))
        # clip length by max_length
        if length > self.max_length:
            length = self.max_length

        query_length = length + window
        ohlc_df = self.__get_ohlc(symbol, query_length, frame)
        cci_df = process.run(ohlc_df)
        cci_df = cci_df[[process.KEY_CCI]]
        cci_df.columns = ["CCI"]
        cci_df = cci_df.iloc[-length:]
        cci_df = cci_df.map(lambda x: f"{x:.5f}" if isinstance(x, float) else str(x))
        if isinstance(cci_df.index, pd.DatetimeIndex):
            cci_df.index = cci_df.index.strftime("%Y-%m-%dT%H:%M:%S%z")
        csv_data = cci_df.to_csv()
        return csv_data

    # idcprocess.LinearRegressionMomentumProcess
    def get_LinearRegressionMomentum(self, symbol: str, length: int, frame: str, window: int):
        """get Linear Regression Momentum values based on close values.

        Args:
            symbol (str): symbol of currency, stock etc. ex USDJPY.
            length (int): specify data length > 0.
            frame (str): specify frame to get time series data. any of Xmin (e.g. 1min), Xh (e.g. 1h), XD (e.g. 1D), WX (e.g. W1), MOX (e.g. MO1)
            window (int): window for Linear Regression Momentum calculation

        Returns:
            str: CSV format data with index, LinearRegressionMomentum columns
        """
        logger.debug(f"tool: get_LinearRegressionMomentum for {symbol}, {length}, {frame}, {window}")
        process = idcprocess.LinearRegressionMomentumProcess(window=window, key="LRM", column="close")
        # clip length by max_length
        if length > self.max_length:
            length = self.max_length

        query_length = length + window
        ohlc_df = self.__get_ohlc(symbol, query_length, frame)
        lrm_df = process.run(ohlc_df)
        lrm_df = lrm_df[[process.KEY_MOMENTUM]]
        lrm_df.columns = ["LRM"]
        lrm_df = lrm_df.iloc[-length:]
        lrm_df = lrm_df.map(lambda x: f"{x:.5f}" if isinstance(x, float) else str(x))
        if isinstance(lrm_df.index, pd.DatetimeIndex):
            lrm_df.index = lrm_df.index.strftime("%Y-%m-%dT%H:%M:%S%z")
        csv_data = lrm_df.to_csv()
        return csv_data

    # idcprocess.RenkoProcess
    def get_Renko(self, symbol: str, length: int, frame: str, window: int):
        """get Renko values based on close values. Brick size is calculated by ATR of specified window.

        Args:
            symbol (str): symbol of currency, stock etc. ex USDJPY.
            length (int): specify data length > 0.
            frame (str): specify frame to get time series data. any of Xmin (e.g. 1min), Xh (e.g. 1h), XD (e.g. 1D), WX (e.g. W1), MOX (e.g. MO1)
            window (int): window for ATR to calculate brick size

        Returns:
            str: CSV format data with index, Renko columns
        """
        logger.debug(f"tool: get_Renko for {symbol}, {length}, {frame}, {window}")
        process = idcprocess.RenkoProcess(window=window, key="Renko", ohlc_column=("open", "high", "low", "close"))
        # clip length by max_length
        if length > self.max_length:
            length = self.max_length

        query_length = length + process.get_minimum_required_length()
        ohlc_df = self.__get_ohlc(symbol, query_length, frame)
        renko_df = process.run(ohlc_df)
        renko_df = renko_df[[process.KEY_VALUE]]
        renko_df.columns = ["Renko"]
        renko_df = renko_df.iloc[-length:]
        renko_df = renko_df.map(lambda x: f"{x:.5f}" if isinstance(x, float) else str(x))
        if isinstance(renko_df.index, pd.DatetimeIndex):
            renko_df.index = renko_df.index.strftime("%Y-%m-%dT%H:%M:%S%z")
        csv_data = renko_df.to_csv()
        return csv_data

    def get_indicator_params(self, indicator: str) -> dict:
        """Get the required parameters for a given indicator to use with get_indicator.

        Args:
            indicator (str): Name of the indicator. One of: MACD, EMA, SMA, MA, BBAND, ATR, RSI, Renko, Slope, LRM, CCI

        Returns:
            dict: {
                param_name (str): description of the parameter
            }
            or {"error": "..."} if the indicator is unknown.
        """
        indicator_upper = indicator.upper()
        params_map = {
            "MACD": {
                "short_window": "short EMA window. Typical: 12",
                "long_window": "long EMA window. Typical: 26",
                "signal_window": "signal line window. Typical: 9",
            },
            "EMA": {"window": "window size. Typical: 14"},
            "SMA": {"window": "window size. Typical: 14"},
            "MA": {"window": "window size. Typical: 14"},
            "BBAND": {
                "window": "window size. Typical: 20",
                "alpha": "standard deviation multiplier. Typical: 2.0",
            },
            "BB": {
                "window": "window size. Typical: 20",
                "alpha": "standard deviation multiplier. Typical: 2.0",
            },
            "BOLLINGERBANDS": {
                "window": "window size. Typical: 20",
                "alpha": "standard deviation multiplier. Typical: 2.0",
            },
            "ATR": {"window": "window size. Typical: 14"},
            "RSI": {"window": "window size. Typical: 14"},
            "RENKO": {"window": "ATR window for brick size calculation. Typical: 14"},
            "SLOPE": {"window": "window size. Typical: 14"},
            "LRM": {"window": "window size. Typical: 14"},
            "LRMOMENTUM": {"window": "window size. Typical: 14"},
            "LINEARREGRESSIONMOMENTUM": {"window": "window size. Typical: 14"},
            "CCI": {"window": "window size. Typical: 20"},
        }
        if indicator_upper not in params_map:
            available = ["MACD", "EMA", "SMA", "MA", "BBAND", "ATR", "RSI", "Renko", "Slope", "LRM", "CCI"]
            return {"error": f"Unknown indicator '{indicator}'. Available indicators: {', '.join(available)}"}
        return params_map[indicator_upper]

    def get_indicator(self, indicator: str, symbol: str, length: int, frame: str, params: dict) -> str:
        """Get indicator values for a given symbol. Call get_indicator_params first to know which params to pass.

        Args:
            indicator (str): Name of the indicator. One of: MACD, EMA, SMA, MA, BBAND, ATR, RSI, Renko, Slope, LRM, CCI
            symbol (str): symbol of currency, stock etc. ex USDJPY.
            length (int): specify data length > 0.
            frame (str): specify frame to get time series data. any of Xmin (e.g. 1min), Xh (e.g. 1h), XD (e.g. 1D), WX (e.g. W1), MOX (e.g. MO1)
            params (dict): indicator-specific parameters. Use get_indicator_params to get required keys.

        Returns:
            str: CSV format data with index and indicator value columns.
                 MACD -> MACD, SIGNAL
                 EMA -> EMA
                 SMA/MA -> MA
                 BBAND -> UpperBand, LowerBand, Width, StdDev
                 ATR -> ATR
                 RSI -> RSI, Gain, Loss
                 Renko -> Renko
                 Slope -> Slope
                 LRM -> LRM
                 CCI -> CCI
        """
        logger.debug(f"tool: get_indicator {indicator} for {symbol}, {length}, {frame}")
        if length > self.max_length:
            length = self.max_length

        indicator_upper = indicator.upper()

        if indicator_upper == "MACD":
            short_window = params["short_window"]
            long_window = params["long_window"]
            signal_window = params["signal_window"]
            process = idcprocess.MACDProcess(target_column="close", short_window=short_window, long_window=long_window, signal_window=signal_window)
            query_length = length + long_window + signal_window
            ohlc_df = self.__get_ohlc(symbol, query_length, frame)
            result_df = process.run(ohlc_df)
            result_df = result_df[[process.KEY_MACD, process.KEY_SIGNAL]]
            result_df.columns = ["MACD", "SIGNAL"]

        elif indicator_upper == "EMA":
            window = params["window"]
            process = idcprocess.EMAProcess(window=window, key="EMA", column="close")
            query_length = length + window
            ohlc_df = self.__get_ohlc(symbol, query_length, frame)
            result_df = process.run(ohlc_df)
            result_df = result_df[[process.key]]
            result_df.columns = ["EMA"]

        elif indicator_upper in ("SMA", "MA"):
            window = params["window"]
            process = idcprocess.MAProcess(window=window, key="MA", column="close")
            query_length = length + window
            ohlc_df = self.__get_ohlc(symbol, query_length, frame)
            result_df = process.run(ohlc_df)
            result_df = result_df[[process.KEY_EMA]]
            result_df.columns = ["MA"]

        elif indicator_upper in ("BBAND", "BB", "BOLLINGERBANDS"):
            window = params["window"]
            alpha = params["alpha"]
            process = idcprocess.BBANDProcess(window=window, key="Bollinger", target_column="close", alpha=alpha)
            query_length = length + window
            ohlc_df = self.__get_ohlc(symbol, query_length, frame)
            result_df = process.run(ohlc_df)
            result_df = result_df[[process.KEY_UPPER_VALUE, process.KEY_LOWER_VALUE, process.KEY_WIDTH_VALUE, process.KEY_STD_VALUE]]
            result_df.columns = ["UpperBand", "LowerBand", "Width", "StdDev"]

        elif indicator_upper == "ATR":
            window = params["window"]
            process = idcprocess.ATRProcess(window=window, key="ATR", ohlc_column_name=("open", "high", "low", "close"))
            query_length = length + window
            ohlc_df = self.__get_ohlc(symbol, query_length, frame)
            result_df = process.run(ohlc_df)
            result_df = result_df[[process.KEY_ATR]]
            result_df.columns = ["ATR"]

        elif indicator_upper == "RSI":
            window = params["window"]
            process = idcprocess.RSIProcess(window=window, key="RSI", ohlc_column_name=("open", "high", "low", "close"))
            query_length = length + window
            ohlc_df = self.__get_ohlc(symbol, query_length, frame)
            result_df = process.run(ohlc_df)
            result_df = result_df[[process.KEY_RSI, process.KEY_GAIN, process.KEY_LOSS]]
            result_df.columns = ["RSI", "Gain", "Loss"]

        elif indicator_upper == "RENKO":
            window = params["window"]
            process = idcprocess.RenkoProcess(window=window, key="Renko", ohlc_column=("open", "high", "low", "close"))
            query_length = length + process.get_minimum_required_length()
            ohlc_df = self.__get_ohlc(symbol, query_length, frame)
            result_df = process.run(ohlc_df)
            result_df = result_df[[process.KEY_VALUE]]
            result_df.columns = ["Renko"]

        elif indicator_upper == "SLOPE":
            window = params["window"]
            process = idcprocess.SlopeProcess(window=window, key="Slope", column="close")
            query_length = length + window
            ohlc_df = self.__get_ohlc(symbol, query_length, frame)
            result_df = process.run(ohlc_df)
            result_df = result_df[[process.KEY_SLOPE]]
            result_df.columns = ["Slope"]

        elif indicator_upper in ("LRM", "LRMOMENTUM", "LINEARREGRESSIONMOMENTUM"):
            window = params["window"]
            process = idcprocess.LinearRegressionMomentumProcess(window=window, key="LRM", column="close")
            query_length = length + window
            ohlc_df = self.__get_ohlc(symbol, query_length, frame)
            result_df = process.run(ohlc_df)
            result_df = result_df[[process.KEY_MOMENTUM]]
            result_df.columns = ["LRM"]

        elif indicator_upper == "CCI":
            window = params["window"]
            process = idcprocess.CCIProcess(window=window, key="CCI", ohlc_column=("open", "high", "low", "close"))
            query_length = length + window
            ohlc_df = self.__get_ohlc(symbol, query_length, frame)
            result_df = process.run(ohlc_df)
            result_df = result_df[[process.KEY_CCI]]
            result_df.columns = ["CCI"]

        else:
            available = ["MACD", "EMA", "SMA", "MA", "BBAND", "ATR", "RSI", "Renko", "Slope", "LRM", "CCI"]
            return f"Unknown indicator '{indicator}'. Available indicators: {', '.join(available)}"

        result_df = result_df.iloc[-length:]
        result_df = result_df.map(lambda x: f"{x:.5f}" if isinstance(x, float) else str(x))
        if isinstance(result_df.index, pd.DatetimeIndex):
            result_df.index = result_df.index.strftime("%Y-%m-%dT%H:%M:%S%z")
        return result_df.to_csv()

    def get_budget(self):
        """Return the current free margin available for new trades.

        Free margin = account equity minus the margin already used by open positions.
        This is the maximum capital available to place new orders.

        Returns:
            str: free margin in account currency (e.g. "250000.00").
                 Returns "failed to get budget" if the value could not be retrieved.
        """
        logger.debug(f"tool: get_budget")
        budget = self.client.get_free_margin()
        logger.debug(f"get_budget result: {budget}")
        try:
            budget = str(budget)
        except Exception:
            budget = "failed to get budget"
        return budget


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
