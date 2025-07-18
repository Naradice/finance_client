import datetime
import os
import queue
import random
import threading
import time
from abc import ABCMeta, abstractmethod

import numpy as np
import pandas as pd

from . import db
from . import frames as Frame
from . import graph
from . import logger as lg
from . import wallet
from .position import ORDER_TYPE, POSITION_TYPE, Position

try:
    from .fprocess import fprocess
except ImportError:
    from . import fprocess


class Client(metaclass=ABCMeta):
    def __init__(
        self,
        budget=1000000.0,
        provider="Default",
        symbols=[],
        out_ohlc_columns=("Open", "High", "Low", "Close"),
        idc_process=None,
        pre_process=None,
        economic_keys=None,
        frame=None,
        start_index=0,
        observation_length=None,
        do_render=False,
        enable_trade_log=False,
        storage: db.BaseStorage = None,
        logger=None,
    ):
        self.auto_index = None
        self._step_index = start_index
        self.__data_queue = None
        self.do_render = do_render
        self.__pending_order_results = {}
        self.frame = frame
        self.observation_length = observation_length
        if type(symbols) == str:
            self.symbols = [symbols]
        else:
            self.symbols = symbols
        self.enable_trade_log = enable_trade_log
        if self.do_render:
            self.__rendere = graph.Rendere()
            self.__ohlc_index = -1
            self.__is_graph_initialized = False
        self.ohlc_columns = None
        self.out_ohlc_columns = out_ohlc_columns

        self.logger = lg if logger is None else logger
        if storage is None:
            db_path = os.path.join(os.getcwd(), "finance_client.db")
            storage = db.SQLiteStorage(db_path, provider)
            # storage = db.FileStorage(provider)
        self.wallet = wallet.Manager(budget=budget, storage=storage, logger=logger, provider=provider)

        if idc_process is None:
            self.idc_process = []
        else:
            self.idc_process = idc_process
        if pre_process is None:
            self.pre_process = []
        else:
            self.pre_process = pre_process
        # Note: Economic indicater implementation is in progress
        if economic_keys is None:
            self.eco_keys = []
        else:
            self.eco_keys = economic_keys

        self._indices = None

    def open_trade(
        self,
        is_buy,
        amount: float,
        symbol: str,
        price: float = None,
        tp=None,
        sl=None,
        option_info=None,
        order_type: ORDER_TYPE = ORDER_TYPE.market,
    ):
        """by calling this in your client, order function is called and position is stored

        Args:
            is_buy (bool): buy order or not
            amount (float): amount of trade unit
            stop (_type_): _description_
            order_type (int): Market,
            symbol (str): symbol of currency, stock etc. ex USDJPY.
            option_info (any, optional): store info you want to position. Defaults to None.

        Returns:
            Success (bool): True if order is completed
            Position (Position): you can specify position or position.id to close the position
        """
        if order_type == ORDER_TYPE.market or order_type == ORDER_TYPE.market.value:
            self.logger.debug("budget order is requested.")
            if is_buy:
                if price is None:
                    ask_rate = self.get_current_ask(symbol)
                else:
                    ask_rate = price
                self.logger.debug(f"order price: {ask_rate}")
                suc, result = self._market_buy(symbol, ask_rate, amount, tp, sl, option_info)
                if suc:
                    return True, self.__open_long_position(
                        symbol=symbol, boughtRate=ask_rate, amount=amount, tp=tp, sl=sl, option_info=option_info, result=result
                    )
                else:
                    self.logger.error(f"Order is failed as {result}")
                    return False, result
            else:
                if price is None:
                    bid_rate = self.get_current_bid(symbol)
                else:
                    bid_rate = price
                self.logger.debug(f"order price: {bid_rate}")
                suc, result = self._market_sell(symbol, bid_rate, amount, tp, sl, option_info)
                if suc:
                    return True, self.__open_short_position(
                        symbol=symbol, soldRate=bid_rate, amount=amount, tp=tp, sl=sl, option_info=option_info, result=result
                    )
                else:
                    self.logger.error(f"Order is failed as {result}")
                    return False, result
        else:
            self.logger.error(f"{order_type} is not defined/implemented.")
            return False, None

    def _trading_log(self, position: Position, price, amount, is_open):
        pass

    def close_position(self, price: float = None, position: Position = None, id=None, amount=None, symbol=None, position_type=None):
        """close open_position. If specified amount is less then position, close the specified amount only
        Either position or id must be specified.
        _sell_for_settlement or _buy_for_settlement is calleds

        Args:
            price (float, optional): price for settlement. If not specified, current value is used.
            position (Position, optional): Position returned by open_trade. Defaults to None.
            id (uuid, optional): Position.id. Ignored if position is specified. Defaults to None.
            amount (float, optional): amount of close position. use all if None. Defaults to None.
            symbols (str, optional):
            position_type (str, optional)
        """
        if position is not None:
            id = position.id

        if id is not None:
            if id in self.__pending_order_results:
                closed_result = self.__pending_order_results.pop(id)
                self.logger.info("Specified position was already closed.")
                return closed_result, False
            if position is None:
                position = self.wallet.storage.get_position(id)
            if amount is None:
                if position is not None:
                    amount = position.amount
                else:
                    self.logger.error(f"both amount and position is None: {id}")
                    return None, None, None, None, False
        else:
            if symbol is None or position_type is None:
                self.logger.error("Either id or symbol and position_type should be specified.")
                return None, None, None, None, False
            if amount is None:
                amount = 1
            position = Position(
                position_type=position_type,
                symbol=symbol,
                price=price,
                amount=amount,
                tp=None,
                sl=None,
                option=None,
                result=None,
                time_index=self.get_current_datetime(),
                id=None,
            )

        position_plot = 0
        if position.position_type == POSITION_TYPE.long:
            self.logger.debug(f"close long position is ordered for {id}")
            if price is None:
                price = self.get_current_bid(position.symbol)
                self.logger.debug(f"order close with current ask rate {price}")
            result = self._sell_for_settlment(position.symbol, price, amount, position.option, position.result)
            if result is False:
                return None, None, None, None, False
            position_plot = -2
        elif position.position_type == POSITION_TYPE.short:
            self.logger.debug(f"close short position is ordered for {id}")
            if price is None:
                self.logger.debug(f"order close with current bid rate {price}")
                price = self.get_current_ask(position.symbol)
            result = self._buy_for_settlement(position.symbol, price, amount, position.option, position.result)
            if result is False:
                return None, None, None, None, False
            position_plot = -1
        else:
            self.logger.warning(f"Unkown position_type {position.position_type} is specified on close_position.")
        if self.do_render:
            self.__rendere.add_trade_history_to_latest_tick(position_plot, price, self.__ohlc_index)
        price, position.price, price_diff, profit = self.wallet.close_position(
            position.id, price, amount=amount, position=position, index=self.get_current_datetime()
        )
        return price, position.price, price_diff, profit, True

    def close_all_positions(self, symbols: list = None):
        """close all open_position.
        sell_for_settlement or _buy_for_settlement is calleds for each position
        """

        if symbols is None:
            symbols = []
        if isinstance(symbols, str):
            symbols = [symbols]

        both_positions = self.wallet.storage.get_positions(symbols=symbols)
        results = []
        for positions in both_positions:
            for position in positions:
                result = self.close_position(position=position)
                results.append(result)
        pending_results = self.__pending_order_results.copy()
        for id, result in pending_results.items():
            results.append((result, False))
            self.__pending_order_results.pop(id)
        return results

    def close_long_positions(self, symbols: list = None):
        """close all open_long_position.
        sell_for_settlement is calleds for each position
        """
        if isinstance(symbols, str):
            symbols = [symbols]
        positions = self.wallet.storage.get_long_positions(symbols=symbols)
        results = []
        for position in positions:
            result = self.close_position(position=position)
            results.append(result)
        return results

    def close_short_positions(self, symbols: list = None):
        """close all open_long_position.
        sell_for_settlement is calleds for each position
        """
        if isinstance(symbols, str):
            symbols = [symbols]
        positions = self.wallet.storage.get_short_positions(symbols=symbols)
        results = []
        for position in positions:
            result = self.close_position(position=position)
            results.append(result)
        return results

    def get_positions(self) -> list:
        long_positions, short_positions = self.wallet.storage.get_positions()
        long_positions.extend(short_positions)
        return long_positions

    def _get_required_length(self, processes: list) -> int:
        required_length_list = [0]
        for process in processes:
            required_length_list.append(process.get_minimum_required_length())
        return max(required_length_list)

    def run_processes(self, data: pd.DataFrame, symbols: list = [], idc_processes=[], pre_processes=[], grouped_by_symbol=False) -> pd.DataFrame:
        """
        Ex. you can define and provide MACD as process. The results of the process are stored as dataframe[key] = values
        """
        data_cp = data.copy()

        if idc_processes is not None:
            for process in idc_processes:
                data_cp = process(data_cp, symbols, grouped_by_symbol)

        if pre_processes is not None:
            for process in pre_processes:
                data_cp = process(data_cp, symbols, grouped_by_symbol)
        return data_cp

    def get_economic_idc(self, keys, start, end):
        data = []
        for key in keys:
            idc_data = fprocess.get_indicater(key, start, end)
            if idc_data is not None:
                data.append(idc_data)
        if len(data) > 0:
            return pd.concat(data, axis=1)
        return pd.DataFrame()

    def get_data_queue(self, data_length: int, symbols: list = [], frame: int = None):
        """
            to get data when frame time past
            when this function is called multiple times
        Args:
            symbols (list): _description_
            data_length (int, optional): data length of rates. get all rates are not allowed. Defaults to .

        Returns:
            Queue: Queue return data by Queue.get() when frame time past
        """
        if data_length <= 0:
            self.logger.warning("data_length must be greater than 1. change length to 1")
            data_length = 1
        if frame is None:
            frame = self.frame

        if self.__data_queue is None:
            self.__data_queue = queue.Queue()
        timer_thread = threading.Thread(target=self.__timer, args=(data_length, symbols, frame), daemon=True)
        timer_thread.start()
        return self.__data_queue

    def __timer(self, data_length: int, symbols: list, frame: int):
        next_time = 0
        frame_seconds = frame * 60
        sleep_time = 0

        if frame <= 60:
            base_time = datetime.datetime.now()
            target_min = datetime.timedelta(minutes=(frame - base_time.minute % frame))
            target_time = base_time + target_min
            sleep_time = datetime.datetime.timestamp(target_time) - datetime.datetime.timestamp(base_time) - base_time.second
        if sleep_time > 0:
            time.sleep(sleep_time)
        base_time = time.time()
        while self.__data_queue is not None:
            t = threading.Thread(target=self.__put_data_to_queue, args=(data_length, symbols, frame), daemon=True)
            t.start()
            next_time = ((base_time - time.time()) % frame_seconds) or frame_seconds
            time.sleep(next_time)

    def stop_quing(self):
        self.__data_queue = None

    def __put_data_to_queue(self, data_length: int, symbols: list, frame: int, idc_processes=[], pre_processes=[]):
        data = self.get_ohlc(data_length, symbols, frame, idc_processes=idc_processes, pre_processes=pre_processes)
        self.__data_queue.put(data)

    def get_client_params(self):
        common_args = {"budget": self.wallet.budget, "frame": self.frame, "provider": self.wallet.provider}
        add_params = self.get_additional_params()
        common_args.update(add_params)
        return common_args

    # override if actual client can check if position is closed by tp/sl
    def _check_position(self, position: Position, **kwargs):
        low_price = kwargs.get("low_price")
        high_price = kwargs.get("high_price")

        closed_price = None
        # start checking stop loss at first.
        if position.sl is not None:
            self.logger.debug(f"sl: {position.tp}, high: {high_price}, low: {low_price}")
            if position.position_type == POSITION_TYPE.long:
                if position.sl >= low_price:
                    closed_price = position.sl
            elif position.position_type == POSITION_TYPE.short:
                if position.sl <= high_price:
                    closed_price = position.sl
            else:
                self.logger.error(f"unkown position_type: {position.position_type}")

        if position.tp is not None:
            self.logger.debug(f"tp: {position.tp}, high: {high_price}, low: {low_price}")
            if position.position_type == POSITION_TYPE.long:
                if position.tp <= high_price:
                    closed_price = position.tp
            elif position.position_type == POSITION_TYPE.short:
                if position.tp >= low_price:
                    closed_price = position.tp
            else:
                self.logger.error(f"unkown position_type: {position.position_type}")
        return closed_price

    def __check_position_completion(self, ohlc_df: pd.DataFrame, symbols: list):
        if len(self.wallet.listening_positions) > 0:
            positions = self.wallet.listening_positions.copy()
            self.logger.debug("start checking the tp and sl of positions")
            if self.ohlc_columns is None:
                self.ohlc_columns = self.get_ohlc_columns()
            high_column = self.ohlc_columns["High"]
            low_column = self.ohlc_columns["Low"]
            # assume trading data is retrieved every frame
            tick = ohlc_df.iloc[-1]
            for id, position in positions.items():
                closed_price = self._check_position(position, low_price=tick[low_column], high_price=tick[high_column])
                if closed_price is not None:
                    result = self.wallet.close_position(
                        id=position.id,
                        price=closed_price,
                        amount=position.amount,
                        position=position,
                        index=self.get_current_datetime(),
                    )
                    if result is not None:
                        self.__pending_order_results[position.id] = result
                        self.logger.info(f"Position is closed by limit: {result}")
                        if self.do_render:
                            if position.position_type == POSITION_TYPE.long:
                                self.__rendere.add_trade_history_to_latest_tick(-2, position.sl, self.__ohlc_index)
                            else:
                                self.__rendere.add_trade_history_to_latest_tick(-1, position.sl, self.__ohlc_index)
                    self.wallet.remove_position_from_listening(position.id)

    def __plot_data(self, symbols: list, data_df: pd.DataFrame):
        if self.__is_graph_initialized is False:
            ohlc_columns = self.get_ohlc_columns()
            args_dict = {"ohlc_columns": (ohlc_columns["Open"], ohlc_columns["High"], ohlc_columns["Low"], ohlc_columns["Close"])}
            result = self.__rendere.register_ohlc(symbols, data_df, **args_dict)
            self.__ohlc_index = result
            self.__is_graph_initialized = True
        else:
            self.__rendere.update_ohlc(data_df, self.__ohlc_index)
        self.__rendere.plot()

    def __plot_data_width_indicaters(self, symbols: list, data_df: pd.DataFrame):
        if self.__is_graph_initialized is False:
            ohlc_columns = self.get_ohlc_columns()
            args_dict = {
                "ohlc_columns": (ohlc_columns["Open"], ohlc_columns["High"], ohlc_columns["Low"], ohlc_columns["Close"]),
                "idc_processes": self.idc_process,
                "index": self.__ohlc_index,
            }
            result = self.__rendere.register_ohlc_with_indicaters(symbols, data_df, **args_dict)
            self.__ohlc_index = result
            self.__is_graph_initialized = True
        else:
            self.__rendere.update_ohlc(data_df, self.__ohlc_index)
        self.__rendere.plot()

    def __get_training_data(
        self,
        length,
        symbols,
        frame,
        columns,
        indices,
        idc_processes,
        pre_processes,
        economic_keys,
        grouped_by_symbol,
        do_run_process,
        do_add_eco_idc,
        data_freq,
    ):
        if length is None:
            length = 1
        chunk_data = []
        for index in indices:
            required_length = 0
            if do_run_process:
                required_length += self._get_required_length(idc_processes + pre_processes)
            if length < required_length:
                target_length = required_length
            else:
                target_length = length
            ohlc_df = self._get_ohlc_from_client(
                length=target_length,
                symbols=symbols,
                frame=frame,
                columns=columns,
                index=index,
                grouped_by_symbol=grouped_by_symbol,
            )

            if do_run_process:
                data = self.run_processes(ohlc_df, symbols, idc_processes, pre_processes, grouped_by_symbol)
            else:
                data = ohlc_df

            if do_add_eco_idc:
                first_index = ohlc_df.index[0]
                end_index = ohlc_df.index[-1]
                indicaters_df = self.get_economic_idc(economic_keys, first_index, end_index)
                indicaters_df = indicaters_df.groupby(pd.Grouper(level=0, freq=data_freq)).first()
                if frame < Frame.D1:
                    if indicaters_df.index.tzinfo is None:
                        indicaters_df.index = pd.to_datetime(indicaters_df.index, utc=True)
                indicaters_df = indicaters_df.ffill()
                data = pd.concat([data, indicaters_df], axis=1)
                data.dropna(thresh=len(data.columns), inplace=True)

            chunk_data.append(data.iloc[-length:].values)
        chunk_data = np.array(chunk_data)
        return chunk_data

    def __get_trading_data(
        self,
        length,
        symbols,
        frame,
        columns,
        index,
        idc_processes,
        pre_processes,
        economic_keys,
        grouped_by_symbol,
        do_run_process,
        do_add_eco_idc,
        data_freq,
    ):
        required_length = 0
        if do_run_process:
            required_length += self._get_required_length(idc_processes + pre_processes)

        if length is None:
            ohlc_df = self._get_ohlc_from_client(
                length=length, symbols=symbols, frame=frame, columns=columns, index=index, grouped_by_symbol=grouped_by_symbol
            )
        else:
            if length < required_length:
                target_length = required_length
            else:
                target_length = length
            ohlc_df = self._get_ohlc_from_client(length=target_length, symbols=symbols, frame=frame, index=index, grouped_by_symbol=grouped_by_symbol)

        if isinstance(ohlc_df.index, pd.DatetimeIndex) and data_freq is not None:
            # drop duplicates
            ohlc_df = ohlc_df.groupby(pd.Grouper(level=0, freq=data_freq)).first()
            ohlc_df.dropna(thresh=len(ohlc_df.columns), inplace=True)

        t = threading.Thread(target=self.__check_position_completion, args=(ohlc_df, symbols), daemon=True)
        t.start()

        if do_run_process:
            if isinstance(ohlc_df, pd.DataFrame) and len(ohlc_df) >= required_length:
                data = self.run_processes(ohlc_df, symbols, idc_processes, pre_processes, grouped_by_symbol)
                if self.do_render:
                    self.__plot_data_width_indicaters(symbols, data)
            else:
                self.logger.error("data length is insufficient to caliculate indicaters.")
                data = ohlc_df
        else:
            data = ohlc_df
            if self.do_render:
                self.__plot_data(symbols, data)

        if do_add_eco_idc:
            first_index = ohlc_df.index[0]
            end_index = ohlc_df.index[-1]
            indicaters_df = self.get_economic_idc(economic_keys, first_index, end_index)
            if isinstance(indicaters_df.index, (pd.DatetimeIndex, pd.TimedeltaIndex, pd.PeriodIndex)) and data_freq is not None:
                indicaters_df = indicaters_df.groupby(pd.Grouper(level=0, freq=data_freq)).first()
                if frame < Frame.D1:
                    if indicaters_df.index.tzinfo is None:
                        indicaters_df.index = pd.to_datetime(indicaters_df.index, utc=True)
                indicaters_df = indicaters_df.ffill()
            data = pd.concat([data, indicaters_df], axis=1)
            data.dropna(thresh=len(data.columns), inplace=True)

        if length is None:
            return data
        else:
            return data.iloc[-length:]

    def download(self, length: int = None, symbols: list = None, frame: int = None, grouped_by_symbol=True, file_path=None):
        """download symbol data with specified length. This function omit processing and return raw data, saving data into data folder.

        Args:
            length (int | None): specify data length > 1. If None is specified, return all date.
            symbols (list[str]): list of symbols. Defaults to [].
            frame (int | None): specify frame to get time series data. If None, default value is used instead.
            grouped_by_symbol (bool): if True, return data is grouped by symbol. Defaults to True.
            file_path (str): file path to save data. If None, file is save on default path. Default path is specified by enviornment variables by data_path, it environment variable is not set, data is saved on os.getcwd()/data_source

        Returns:
            pd.DataFrame: ohlc data of symbols which is sorted from older to latest. data are returned with length from latest data
            Column name is depend on actual client. You can get column name dict by get_ohlc_columns function

        """

        if symbols is None:
            symbols = self.symbols
        if type(symbols) == str:
            symbols = [symbols]
        if len(symbols) == 0:
            raise ValueError("symbols must be specified by either initial parameter or function parameter.")
        if frame is None:
            frame = self.frame
        # data save should be handled by each client
        ohlc_df = self._get_ohlc_from_client(length=length, symbols=symbols, frame=frame, columns=None, index=None, grouped_by_symbol=True)
        if file_path is not None:
            ohlc_df.to_csv(file_path, index=True)
            self.logger.info(f"file is saved on {file_path}")
        else:
            default_path = self._get_default_path()
            if default_path is None:
                self.logger.info("save is not supported by this client.")
            else:
                self.logger.info(f"file is saved on {default_path}")
        return ohlc_df

    def get_ohlc(
        self,
        length: int = None,
        symbols: list = None,
        frame: int = None,
        columns: list = None,
        index=None,
        idc_processes=None,
        pre_processes=None,
        economic_keys=None,
        grouped_by_symbol=True,
    ) -> pd.DataFrame or np.array:
        """get ohlc data with specified length

        Args:
            length (int | None): specify data length > 1. If None is specified, return all date.
            symbols (list[str]): list of symbols. Defaults to [].
            frame (int | None): specify frame to get time series data. If None, default value is used instead.
            index (int or list[int]): specify index copy from. If list has multiple indices, return numpy array as (indices_size, column_size, data_length).
            columns: specify columns. If not specified, return open, high, low, close.
              step_index is not change if index is specified even auto_indx is True as typically this option is used for machine learning.
              Defaults None and this case step_index is used.
            idc_processes (Process, optional) : list of indicater process. Dafaults to []
            pre_processes (Process, optional) : list of pre process. Defaults to []

        Returns:
            pd.DataFrame: ohlc data of symbols which is sorted from older to latest. data are returned with length from latest data
            Column name is depend on actual client. You can get column name dict by get_ohlc_columns function
        """
        if length is None and self.observation_length is not None:
            length = self.observation_length

        if symbols is None:
            symbols = []

        if frame is None:
            frame = self.frame

        if idc_processes is None:
            if self.idc_process is not None and len(self.idc_process) > 0:
                idc_processes = self.idc_process
            else:
                idc_processes = []
        if pre_processes is None:
            if self.pre_process is not None and len(self.pre_process) > 0:
                pre_processes = self.pre_process
            else:
                pre_processes = []
        if economic_keys is None:
            economic_keys = self.eco_keys

        do_run_process = False
        if len(idc_processes) > 0 or len(pre_processes) > 0:
            do_run_process = True

        do_add_eco_idc = False
        if len(economic_keys) > 0:
            do_add_eco_idc = True

        if type(symbols) == str:
            symbols = [symbols]
        if len(symbols) == 0:
            symbols = self.symbols

        data_freq = Frame.freq_str[frame]

        if index is None or type(index) is int:
            # return DataFrame for trading
            return self.__get_trading_data(
                length=length,
                symbols=symbols,
                frame=frame,
                columns=columns,
                index=index,
                idc_processes=idc_processes,
                pre_processes=pre_processes,
                economic_keys=economic_keys,
                grouped_by_symbol=grouped_by_symbol,
                do_run_process=do_run_process,
                do_add_eco_idc=do_add_eco_idc,
                data_freq=data_freq,
            )
        else:
            # training
            return self.__get_training_data(
                length=length,
                symbols=symbols,
                frame=frame,
                columns=columns,
                indices=index,
                idc_processes=idc_processes,
                pre_processes=pre_processes,
                economic_keys=economic_keys,
                grouped_by_symbol=grouped_by_symbol,
                do_run_process=do_run_process,
                do_add_eco_idc=do_add_eco_idc,
                data_freq=data_freq,
            )

    # Need to implement in the actual client

    @abstractmethod
    def get_additional_params(self):
        return {}

    @abstractmethod
    def _get_ohlc_from_client(self, length, symbols: list, frame: int, columns: list, index: list, grouped_by_symbol: bool):
        return pd.DataFrame()

    def get_future_rates(self, interval) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_current_ask(self, symbols=[]) -> pd.Series:
        print("Need to implement get_current_ask on your client")

    @abstractmethod
    def get_current_bid(self, symbols=[]) -> pd.Series:
        print("Need to implement get_current_bid on your client")

    # Override if provider has datetime index
    def get_current_datetime(self):
        return Frame.get_frame_time(datetime.datetime.now(datetime.UTC), self.frame)

    def get_params(self) -> dict:
        print("Need to implement get_params")

    def get_next_tick(self, frame=5):
        print("Need to implement get_next_tick")

    def _get_default_path(self):
        return None

    @abstractmethod
    def __len__(self):
        print("Need to implement __len__")

    @property
    def indices(self):
        if self._indices is None:
            required_length = [0]
            for process in self.idc_process:
                required_length.append(process.get_minimum_required_length())
            for process in self.pre_process:
                required_length.append(process.get_minimum_required_length())
            minimum_length = max(required_length)
            if self.observation_length:
                self._indices = [index for index in range(self.observation_length + minimum_length, len(self))]
            else:
                self._indices = [index for index in range(minimum_length, len(self))]
        return self._indices

    def __getitem__(self, batch_idx):
        if type(batch_idx) is tuple:
            idx = batch_idx[0]
            columns = batch_idx[1]
        else:
            idx = batch_idx
            columns = None

        indices = self.indices[idx]
        return self.get_ohlc(self.observation_length, self.symbols, self.frame, columns, indices, self.idc_process, self.pre_process, self.eco_keys)

    # define wallet client
    def _market_buy(self, symbol, ask_rate, amount, tp, sl, option_info):
        return True, None

    def _market_sell(self, symbol, bid_rate, amount, tp, sl, option_info):
        return True, None

    def _buy_for_settlement(self, symbol, ask_rate, amount, option_info, result):
        pass

    def _sell_for_settlment(self, symbol, bid_rate, amount, option_info, result):
        pass

    # defined by the actual client for dataset or env if needed
    def reset(self, mode=None):
        print("Need to implement reset")

    def close_client(self):
        try:
            self.wallet.storage.close()
        except Exception:
            pass

    @property
    def max(self):
        print("Need to implement max")
        return 1

    @property
    def min(self):
        print("Need to implement min")
        return -1

    def __get_long_position_diffs(self, standalization="minimax"):
        positions = self.wallet.storage.get_long_positions()
        if len(positions) > 0:
            diffs = []
            current_bid = self.get_current_bid()
            if standalization == "minimax":
                current_bid = fprocess.standalization.mini_max(current_bid, self.min, self.max, (0, 1))
                for key, position in positions.items():
                    price = fprocess.standalization.mini_max(position.price, self.min, self.max, (0, 1))
                    diffs.append(current_bid - price)
            else:
                for key, position in positions.items():
                    diffs.append(current_bid - position.price)
            return diffs
        else:
            return []

    def __get_short_position_diffs(self, standalization="minimax"):
        positions = self.wallet.storage.get_short_positions()
        if len(positions) > 0:
            diffs = []
            current_ask = self.get_current_ask()
            if standalization == "minimax":
                current_ask = fprocess.standalization.mini_max(current_ask, self.min, self.max, (0, 1))
                for key, position in positions.items():
                    price = fprocess.standalization.mini_max(position.price, self.min, self.max, (0, 1))
                    diffs.append(price - current_ask)
            else:
                for key, position in positions.items():
                    diffs.append(position.price - current_ask)
            return diffs
        else:
            return []

    def get_diffs(self, position_type=None) -> list:
        if position_type == POSITION_TYPE.long:
            return self.__get_long_position_diffs()
        elif position_type == POSITION_TYPE.short:
            return self.__get_short_position_diffs()
        else:
            diffs = self.__get_long_position_diffs()
            bid_diffs = self.__get_short_position_diffs()
            if len(bid_diffs) > 0:
                diffs.append(*bid_diffs)
            return diffs

    def get_diffs_with_minmax(self, position_type=None) -> list:
        if position_type == POSITION_TYPE.long:
            return self.__get_long_position_diffs(standalization="minimax")
        elif position_type == POSITION_TYPE.short:
            return self.__get_short_position_diffs(standalization="minimax")
        else:
            diffs = self.__get_long_position_diffs(standalization="minimax")
            bid_diffs = self.__get_short_position_diffs(standalization="minimax")
            if len(bid_diffs) > 0:
                diffs.append(*bid_diffs)
            return diffs

    def __open_long_position(self, symbol, boughtRate, amount, tp=None, sl=None, option_info=None, result=None):
        self.logger.debug(f"open long position is created: {symbol}, {boughtRate}, {amount}, {tp}, {sl}, {option_info}, {result}")
        id = self.wallet.open_position(
            position_type=POSITION_TYPE.long,
            symbol=symbol,
            price=boughtRate,
            amount=amount,
            tp=tp,
            sl=sl,
            index=self.get_current_datetime(),
            option=option_info,
            result=result,
        )
        if id is not None:
            if self.do_render:
                self.__rendere.add_trade_history_to_latest_tick(1, boughtRate, self.__ohlc_index)
        return id

    def __open_short_position(self, symbol, soldRate, amount, option_info=None, tp=None, sl=None, result=None):
        self.logger.debug(f"open short position is created: {symbol}, {soldRate}, {amount}, {tp}, {sl}, {option_info}, {result}")
        id = self.wallet.open_position(
            position_type=POSITION_TYPE.short,
            symbol=symbol,
            price=soldRate,
            amount=amount,
            tp=tp,
            sl=sl,
            index=self.get_current_datetime(),
            option=option_info,
            result=result,
        )
        if id is not None:
            if self.do_render:
                self.__rendere.add_trade_history_to_latest_tick(2, soldRate, self.__ohlc_index)
            if self.enable_trade_log:
                self._trading_log(id, soldRate, amount, True)
        return id

    def get_ohlc_columns(self, symbol: str = None, out_type="dict", ignore=None) -> dict:
        """returns column names of ohlc data.

        Returns:
            dict: format is {"Open": ${open_column}, "High": ${high_column}, "Low": ${low_column}, "Close": ${close_column}, "Time": ${time_column} (Optional), "Volume": ${volume_column} (Optional)}
        """
        if self.ohlc_columns is None:
            self.ohlc_columns = {}
            temp = self.auto_index
            self.auto_index = False
            temp_r = self.do_render
            self.do_render = False
            data = self.get_ohlc(1)
            self.auto_index = temp
            self.do_render = temp_r
            columns = data.columns
            if type(columns) == pd.MultiIndex:
                if symbol is None:
                    if fprocess.ohlc.is_grouped_by_symbol(columns):
                        columns = set(columns.droplevel(0))
                    else:
                        columns = set(columns.droplevel(1))
                else:
                    if symbol in columns.droplevel(0):
                        # grouped_by_symbol = False
                        columns = columns.swaplevel(0, 1)
                    elif symbol not in columns.droplevel(1):
                        raise ValueError(f"Specified symbol {symbol} not found on columns.")
                    columns = columns[symbol]
            for column in columns:
                column_ = str(column).lower()
                if column_ == "open":
                    self.ohlc_columns["Open"] = column
                elif column_ == "high":
                    self.ohlc_columns["High"] = column
                elif column_ == "low":
                    self.ohlc_columns["Low"] = column
                elif "close" in column_:
                    self.ohlc_columns["Close"] = column
                elif "time" in column_:  # assume time, timestamp or datetime
                    self.ohlc_columns["Time"] = column
                elif "volume" in column_:
                    self.ohlc_columns["Volume"] = column
                elif "spread" in column_:
                    self.ohlc_columns["Spread"] = column

        ohlc_columns = self.ohlc_columns.copy()
        if ignore is not None:
            if type(ignore) == str and ignore in ohlc_columns:
                ohlc_columns.pop(ignore)
            elif type(ignore):
                for key in ignore:
                    if key in ohlc_columns:
                        ohlc_columns.pop(key)
        if out_type == "dict":
            return ohlc_columns
        else:
            columns = [item for key, item in ohlc_columns.items()]
            return columns

    def revert_preprocesses(self, data: pd.DataFrame = None):
        if data is None:
            data = self.get_ohlc()
        data = data.copy()

        columns_dict = self.get_ohlc_columns()
        if "Time" in columns_dict and columns_dict["Time"] is not None:
            date_column = columns_dict["Time"]
            columns = list(data.columns.copy())
            if date_column in columns:
                # date_data = data[date_column].copy()
                # df = df.set_index(date_column)
                columns.remove(date_column)
                data = data[columns]
        for ps in self.pre_process:
            data = ps.revert(data)
        return data

    def roll_ohlc_data(
        self,
        data: pd.DataFrame,
        to_frame: int = None,
        to_freq: str = None,
        grouped_by_symbol=True,
        Open="Open",
        High="High",
        Low="Low",
        Close="Close",
        Volume=None,
    ) -> pd.DataFrame:
        """Roll time series data by specified frequency.

        Args:
            data (pd.DataFrame): time series data. columns should be same as what get_ohlc_columns returns
            to_frame (int, optional): target frame minutes to roll data. If None, to_freq should be specified.
            freq (str, optional): target freq value defined in pandas. Defaults to None.
            grouped_by_symbol (bool, optional): specify if data is grouped_by_symbol or not. Defaults to True

        Raises:
            Exception: if both to_frame and to_freq are None

        Returns:
            pd.DataFrame: rolled data. Only columns handled on get_ohlc_columns are returned
        """
        if to_frame is None and to_freq is None:
            raise Exception("Either to_frame or freq should be provided.")

        if to_freq is None:
            freq = Frame.freq_str[to_frame]
        else:
            freq = to_freq

        if grouped_by_symbol:
            data.columns = data.columns.swaplevel(0, 1)

        ohlc_columns_dict = self.get_ohlc_columns()
        rolled_data_dict = {}
        if Open:
            if Open in ohlc_columns_dict:
                opn_clm = ohlc_columns_dict[Open]
            else:
                opn_clm = Open
            rolled_opn = data[opn_clm].groupby(pd.Grouper(level=0, freq=freq)).first()
            rolled_data_dict[opn_clm] = rolled_opn
        if High:
            if High in ohlc_columns_dict:
                high_clm = ohlc_columns_dict[High]
            else:
                high_clm = High
            rolled_high = data[high_clm].groupby(pd.Grouper(level=0, freq=freq)).max()
            rolled_data_dict[high_clm] = rolled_high
        if Low:
            if Low in ohlc_columns_dict:
                low_clm = ohlc_columns_dict[Low]
            else:
                low_clm = Low
            rolled_low = data[low_clm].groupby(pd.Grouper(level=0, freq=freq)).min()
            rolled_data_dict[low_clm] = rolled_low
        if Close:
            if Close in ohlc_columns_dict:
                cls_clm = ohlc_columns_dict[Close]
            else:
                cls_clm = Close
            rolled_cls = data[cls_clm].groupby(pd.Grouper(level=0, freq=freq)).last()
            rolled_data_dict[cls_clm] = rolled_cls
        if Volume:
            if Volume in ohlc_columns_dict:
                vlm_clm = ohlc_columns_dict[Volume]
            else:
                vlm_clm = Volume
            rolled_vlm = data[vlm_clm].groupby(pd.Grouper(level=0, freq=freq)).sum()
            rolled_data_dict[vlm_clm] = rolled_vlm

        if len(rolled_data_dict) > 0:
            rolled_df = pd.concat(rolled_data_dict.values(), axis=1, keys=rolled_data_dict.keys())
            if grouped_by_symbol:
                data.columns = data.columns.swaplevel(0, 1)
                rolled_df.columns = rolled_df.columns.swaplevel(0, 1)
                rolled_df.sort_index(level=0, axis=1, inplace=True)
            return rolled_df
        else:
            self.logger.warning(f"no column found to roll. currently {ohlc_columns_dict}")
            return pd.DataFrame()

    def get_budget(self) -> tuple:
        ask_states, bid_states = self.get_portfolio()
        in_use = 0
        profit = 0
        for state in ask_states:
            in_use += state[1] * state[2]
            profit += state[4]

        for state in bid_states:
            in_use += state[1]
            profit += state[4]
        return self.wallet.budget, in_use, profit

    def get_portfolio(self) -> tuple:
        portfolio = {"long": {}, "short": {}}
        long_positions, short_positions = self.wallet.storage.get_positions()
        ask_symbols = []
        if len(long_positions) > 0:
            for position in long_positions:
                symbol = position.symbol
                ask_symbols.append(symbol)
                if symbol in portfolio["long"]:
                    portfolio["long"][symbol].append(position)
                else:
                    portfolio["long"][symbol] = [position]

        bid_symbols = []
        if len(short_positions) > 0:
            for position in short_positions:
                symbol = position.symbol
                bid_symbols.append(symbol)
                if symbol in portfolio["short"]:
                    portfolio["short"][symbol].append(position)
                else:
                    portfolio["short"][symbol] = [position]

        bid_rates = self.get_current_bid(list(set(ask_symbols)))
        if isinstance(bid_rates, pd.Series):
            get_bid_rate = lambda symbol: bid_rates[symbol]
        else:
            get_bid_rate = lambda symbol: bid_rates
        ask_position_states = []
        for symbol, positions in portfolio["long"].items():
            bid_rate = get_bid_rate(symbol)
            for position in positions:
                profit = (bid_rate - position.price) * position.amount
                profit_rate = bid_rate / position.price
                ask_position_states.append((symbol, position.price, position.amount, bid_rate, profit, profit_rate))

        ask_rates = self.get_current_ask(list(set(bid_symbols)))
        if isinstance(ask_rates, pd.Series):
            get_ask_rate = lambda symbol: ask_rates[symbol]
        else:
            get_ask_rate = lambda symbol: ask_rates
        bid_position_states = []
        for symbol, positions in portfolio["short"].items():
            ask_rate = get_ask_rate(symbol)
            for position in positions:
                profit = (position.price - ask_rate) * position.amount
                profit_rate = position.price / ask_rate
                bid_position_states.append((symbol, position.price, position.amount, ask_rate, profit, profit_rate))
        return ask_position_states, bid_position_states

    def seed(self, seed=None):
        if seed is None:
            seed = 1017
        random.seed(seed)
        np.random.seed(seed)
        self.seed_value = seed
