import queue
import threading
import pandas as pd
import finance_client.utils as utils
import finance_client.market as market
import finance_client.frames as Frame
from finance_client.render import graph
from logging import getLogger, config
import os, json, datetime
import time

class Client:
    
    def __init__(self, budget=1000000.0, provider="Default", symbols=[], out_ohlc_columns = ("Open", "High", "Low", "Close"), frame=None, do_render=True, logger_name=None, logger=None):
        self.auto_index = None
        dir = os.path.dirname(__file__)
        self.__data_queue = None
        self.do_render = do_render
        self.__pending_order_results = {}
        self.frame = frame
        if type(symbols) == str:
            self.symbols = [symbols]
        else:
            self.symbols = symbols
        if self.do_render:
            self.__rendere = graph.Rendere()
            self.__ohlc_index = -1
            self.__is_graph_initialized = False
        
        try:
            with open(os.path.join(dir, './settings.json'), 'r') as f:
                    settings = json.load(f)
        except Exception as e:
            print(f"fail to load settings file on client: {e}")
            raise e
        self.ohlc_columns = None
        
        if logger is None:
            logger_config = settings["log"]
        
            try:
                log_file_base_name = logger_config["handlers"]["fileHandler"]["filename"]
                log_folder = os.path.join(os.path.dirname(__file__), 'logs')
                if os.path.exists(log_folder) == False:
                    os.makedirs(log_folder)
                logger_config["handlers"]["fileHandler"]["filename"] = f'{log_folder}/{log_file_base_name}_{datetime.datetime.utcnow().strftime("%Y%m%d")}.logs'
                config.dictConfig(logger_config)
                if logger_name == None:
                    logger_name == __name__
                self.logger = getLogger(logger_name)
                self.market = market.Manager(budget)
            except Exception as e:
                print(f"fail to set configure file: {e}")
                raise e
        else:
            self.logger = logger
            self.market = market.Manager(budget, logger=logger, provider=provider)
        
        self.out_ohlc_columns = out_ohlc_columns
    
    def initialize_budget(self, budget):
        self.market(budget)
        
    def open_trade(self, is_buy, amount:float, order_type:str, symbol:str, price:float=None, tp=None, sl=None, option_info=None):
        """ by calling this in your client, order function is called and position is stored

        Args:
            is_buy (bool): buy order or not
            amount (float): amount of trade unit
            stop (_type_): _description_
            order_type (str): Market, 
            symbol (str): symbol of currency, stock etc. ex USDJPY.
            option_info (any, optional): store info you want to position. Defaults to None.

        Returns:
            Success (bool): True if order is completed
            Position (Position): you can specify position or position.id to close the position
        """
        if order_type == "Market":
            self.logger.debug("market order is requested.")
            if is_buy:
                if price == None:
                    ask_rate = self.get_current_ask(symbol)
                else:
                    ask_rate = price
                if ask_rate and ask_rate > 0:
                    suc, result = self._market_buy(symbol, ask_rate, amount, tp, sl, option_info)
                    if suc:
                        return True, self.__open_long_position(symbol=symbol, boughtRate=ask_rate, amount=amount, tp=tp, sl=sl, option_info=option_info, result=result)
                    else:
                        self.logger.error(f"Order is failed as {result}")
                        return False, result
                else:
                    err_msg = f"current rate is not valid: {ask_rate}"
                    self.logger.error(f"Order is failed as {err_msg}")
                    return False, err_msg
            else:
                if price == None:
                    bid_rate = self.get_current_bid(symbol)
                else:
                    bid_rate = price
                if bid_rate and bid_rate > 0:
                    suc, result = self._market_sell(symbol, bid_rate, amount, tp, sl, option_info)
                    if suc:
                        return True, self.__open_short_position(symbol=symbol, soldRate=bid_rate, amount=amount, tp=tp, sl=sl, option_info=option_info, result=result)
                    else:
                        self.logger(f"Order is failed as {result}")
                        return False, result
                else:
                    err_msg = f"current rate is not valid: {ask_rate}"
                    self.logger(f"Order is failed as {err_msg}")
                    return False, err_msg
        else:
            self.logger.debug(f"{order_type} is not defined/implemented.")
            
    def close_position(self, price:float=None, position:market.Position=None, id=None, amount=None):
        """ close open_position. If specified amount is less then position, close the specified amount only
        Either position or id must be specified.
        sell_for_settlement or _buy_for_settlement is calleds

        Args:
            price (float, optional): price for settlement. If not specified, current value is used.
            position (Position, optional): Position returned by open_trade. Defaults to None.
            id (uuid, optional): Position.id. Ignored if position is specified. Defaults to None.
            amount (float, optional): amount of close position. use all if None. Defaults to None.
        """
        if position is not None:
            id = position.id
        
        if id is not None:
            if id in self.__pending_order_results:
                closed_result = self.__pending_order_results[id]
                self.logger.info("Specified position was already closed.")
                self.__pending_order_results.pop(id)
                return closed_result
            if position is None:
                position = self.market.get_position(id)
        else:
            self.logger.error("Either position or id should be specified.")
            return None
        if amount is None:
            amount = position.amount
        position_plot = 0
        if position.order_type == "ask":
            self.logger.debug(f"close long position is ordered for {id}")
            if price is None:
                price = self.get_current_bid(position.symbol)
                self.logger.debug(f"order close with current ask rate {price} if market sell is not allowed")
            self._sell_for_settlment(position.symbol , price, amount, position.option, position.result)
            position_plot = -2
        elif position.order_type == "bid":
            self.logger.debug(f"close long position is ordered for {id}")
            if price is None:
                self.logger.debug(f"order close with current bid rate {price} if market sell is not allowed")
                price = self.get_current_ask(position.symbol)
            self._buy_for_settlement(position.symbol, price, amount, position.option, position.result)
            position_plot = -1
        else:
            self.logger.warning(f"Unkown order_type {position.order_type} is specified on close_position.")
        if self.do_render:
            self.__rendere.add_trade_history_to_latest_tick(position_plot, price, self.__ohlc_index)
        return self.market.close_position(position, price, amount)
    
    def close_all_positions(self, symbols=[]):
        """close all open_position.
        sell_for_settlement or _buy_for_settlement is calleds for each position
        """
        positions = self.market.get_open_positions(symbols=symbols)
        results = []
        for position in positions:
            result = self.close_position(position=position)
            results.append(result)
        return results

    def close_long_positions(self, symbols=[]):
        """close all open_long_position.
        sell_for_settlement is calleds for each position
        """
        positions = self.market.get_open_positions(order_type="ask", symbols=symbols)
        results = []
        for position in positions:
            result = self.close_position(position=position)
            results.append(result)
        return results

    def close_short_positions(self, symbols=[]):
        """close all open_long_position.
        sell_for_settlement is calleds for each position
        """
        positions = self.market.get_open_positions(order_type="bid", symbols=symbols)
        results = []
        for position in positions:
            result = self.close_position(position=position)
            results.append(result)
        return results
    
    def get_positions(self) -> list:
        return self.market.get_open_positions()
    
    def _get_required_length(self, processes:list) -> int:
        required_length_list = [0]
        for process in processes:
            required_length_list.append(process.get_minimum_required_length())
        return max(required_length_list)
    
    def run_processes(self, data:pd.DataFrame, symbols:list = [], idc_processes = [], pre_processes=[], grouped_by_symbol=False) -> pd.DataFrame:
        """
        Ex. you can define and provide MACD as process. The results of the process are stored as dataframe[key] = values
        """
        data_cp = data.copy()
        
        for process in idc_processes:
            idc_df = process.run(data_cp, symbols, grouped_by_symbol)
            data_cp = pd.concat([data_cp, idc_df], axis=1)
        
        for process in pre_processes:
            data_cp = process.run(data_cp)
        return data_cp
              
    def get_data_queue(self, data_length:int, symbols:list=[], frame:int=None):
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
            self.logger.warning(f"data_length must be greater than 1. change length to 1")
            data_length = 1
        if frame is None:
            frame = self.frame
            
        if self.__data_queue is None:
            self.__data_queue = queue.Queue()    
        timer_thread = threading.Thread(target=self.__timer, args=(data_length, symbols, frame), daemon=True)
        timer_thread.start()
        return self.__data_queue
        
    def __timer(self, data_length:int, symbols:list, frame:int):
        next_time = 0
        frame_seconds = frame*60
        sleep_time = 0
        
        if frame <= 60:
            base_time = datetime.datetime.now()
            target_min = datetime.timedelta(minutes=(frame- base_time.minute % frame))
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
        
    def __put_data_to_queue(self, data_length:int, symbols:list, frame:int, idc_processes=[], pre_processes=[]):
        data = self.get_ohlc(data_length, symbols, frame, idc_processes, pre_processes)
        self.__data_queue.put(data)
    
    def get_client_params(self):
        common_args = { "budget": self.market.positions["budget"], "frame": self.frame, "provider": self.market.provider }
        add_params = self.get_additional_params()
        common_args.update(add_params)
        return common_args
    
    def __check_order_completion(self, ohlc_df: pd.DataFrame, symbols:list):
        if len(self.market.listening_positions) > 0:
            positions = self.market.listening_positions.copy()
            self.logger.debug("start checking the tp and sl of positions")
            if self.ohlc_columns is None:
                self.ohlc_columns = self.get_ohlc_columns()
            high_column = self.ohlc_columns["High"]
            low_column = self.ohlc_columns["Low"]
            tick = ohlc_df.iloc[-1]
            for id, position in positions.items():
                # start checking stop loss at first.
                if position.sl is not None:
                    if position.order_type == "ask":
                        if position.sl >= tick[low_column]:
                            result = self.market.close_position(position, position.sl)
                            self.__pending_order_results[position.id] = result
                            self.logger.info("Position is closed to stop loss")
                            if self.do_render:
                                self.__rendere.add_trade_history_to_latest_tick(-2, position.sl, self.__ohlc_index)
                            continue
                    elif position.order_type == "bid":
                        if position.sl <= tick[high_column]:
                            result = self.market.close_position(position, position.sl)
                            self.__pending_order_results[position.id] = result
                            self.logger.info("Position is closed to stop loss")
                            if self.do_render:
                                self.__rendere.add_trade_history_to_latest_tick(-1, position.sl, self.__ohlc_index)
                            continue
                    else:
                        self.logger.error(f"unkown order_type: {position.order_type}")
                        continue
                        
                if position.tp is not None:
                    if position.order_type == "ask":
                        self.logger.debug(f"tp: {position.tp}, high: {tick[high_column]}")
                        if position.tp <= tick[high_column]:
                            result = self.market.close_position(position, position.tp)
                            self.__pending_order_results[position.id] = result
                            self.logger.info("Position is closed to take profit")
                            if self.do_render:
                                self.__rendere.add_trade_history_to_latest_tick(-2, position.tp, self.__ohlc_index)
                    elif position.order_type == "bid":
                        self.logger.debug(f"tp: {position.tp}, low: {tick[low_column]}")
                        if position.tp >= tick[low_column]:
                            result = self.market.close_position(position, position.tp)
                            self.__pending_order_results[position.id] = result
                            self.logger.info("Position is closed to take profit")
                            if self.do_render:
                                self.__rendere.add_trade_history_to_latest_tick(-1, position.tp, self.__ohlc_index)
                    else:
                        self.logger.error(f"unkown order_type: {position.order_type}")

    def __plot_data(self, symbols:list, data_df: pd.DataFrame):
        if self.__is_graph_initialized is False:
            ohlc_columns = self.get_ohlc_columns()
            args_dict = {
                "ohlc_columns": (
                    ohlc_columns['Open'],
                    ohlc_columns['High'],
                    ohlc_columns['Low'],
                    ohlc_columns['Close']
                )
            }
            result = self.__rendere.register_ohlc(symbols, data_df, **args_dict)
            self.__ohlc_index = result
            self.__is_graph_initialized = True
        else:
            self.__rendere.update_ohlc(data_df, self.__ohlc_index)
        self.__rendere.plot()
    
    def __plot_data_width_indicaters(self, symbols:list, data_df: pd.DataFrame):
        if self.__is_graph_initialized is False:
            ohlc_columns = self.get_ohlc_columns()
            args_dict = {
                "ohlc_columns": (
                    ohlc_columns['Open'],
                    ohlc_columns['High'],
                    ohlc_columns['Low'],
                    ohlc_columns['Close']
                ),
                "idc_processes": self.__idc_processes,
                "index": self.__ohlc_index
            }
            result = self.__rendere.register_ohlc_with_indicaters(symbols, data_df, **args_dict)
            self.__ohlc_index = result
            self.__is_graph_initialized = True
        else:
            self.__rendere.update_ohlc(data_df, self.__ohlc_index)
        self.__rendere.plot()
        
    def get_ohlc(self, length:int = None, symbols:list=[], frame:int=None, idc_processes=[], pre_processes=[], grouped_by_symbol=True) -> pd.DataFrame:
        """ get ohlc data with length length

        Args:
            length (int | None): specify data length > 1. If None is specified, return all date.
            symbols (list<str>): list of symbols. Defaults to [].
            frame (int | None): specify frame to get time series data. If None, default value is used instead.
            idc_processes (Process, optional) : list of indicater process. Dafaults to []
            pre_processes (Process, optional) : list of pre process. Defaults to []
            
        Returns:
            pd.DataFrame: ohlc data of symbols which is sorted from older to latest. data are returned with length from latest data
            Column name is depend on actual client. You can get column name dict by get_ohlc_columns function
        """
        do_run_process = False
        if len(idc_processes) > 0 or len(pre_processes) > 0:
            do_run_process = True
            
        if type(symbols) == str:
            symbols = [symbols]
        if len(symbols) == 0:
            symbols = self.symbols
        
        if length is None:
            ohlc_df = self._get_ohlc_from_client(length=length, symbols=symbols, frame=frame, grouped_by_symbol=grouped_by_symbol)
            required_length = 0
            if do_run_process:
                required_length += self._get_required_length(idc_processes + pre_processes)
        else:
            required_length = 0
            if do_run_process:
                required_length += self._get_required_length(idc_processes + pre_processes)
            if length < required_length:
                target_length = required_length
            else:
                target_length = length
            ohlc_df = self._get_ohlc_from_client(length=target_length, symbols=symbols, frame=frame, grouped_by_symbol=grouped_by_symbol)
        
        t = threading.Thread(target=self.__check_order_completion, args=(ohlc_df, symbols), daemon=True)
        t.start()
        
        if do_run_process:
            if type(ohlc_df) == pd.DataFrame and len(ohlc_df) >= required_length:
                data = self.run_processes(ohlc_df, symbols, idc_processes, pre_processes, grouped_by_symbol)
                if self.do_render:
                    self.__plot_data_width_indicaters(symbols, data)
                if length is None:
                    return data
                else:
                    return data.iloc[-length:]
            else:
                self.logger.error(f"data length is insufficient to caliculate indicaters.")
        elif self.do_render:
            self.__plot_data(symbols, ohlc_df)
        return ohlc_df
      
    ## Need to implement in the actual client ##
    
    def get_additional_params(self):
        return {}

    def _get_ohlc_from_client(self,length, symbols:list, frame:int, grouped_by_symbol:bool):
        return {}
    
    def get_future_rates(self, interval) -> pd.DataFrame:
        pass
    
    def get_current_ask(self) -> float:
        print("Need to implement get_current_ask on your client")
    
    def get_current_bid(self) -> float:
        print("Need to implement get_current_bid on your client")
            
    def _market_buy(self, symbol, ask_rate, amount, tp, sl, option_info):
        return True, None
    
    def _market_sell(self, symbol, bid_rate, amount, tp, sl, option_info):
        return True, None
    
    def _buy_for_settlement(self, symbol, ask_rate, amount, option_info, result):
        pass
    
    def _sell_for_settlment(self, symbol, bid_rate, amount, option_info, result):
        pass
    
    def get_params(self) -> dict:
        print("Need to implement get_params")
    
    ## defined by the actual client for dataset or env
    def close_client(self):
        pass
    
    def get_next_tick(self, frame=5):
        print("Need to implement get_next_tick")

    def reset(self, mode=None):
        print("Need to implement reset")
    
    def get_min_max(column, data_length = 0):
        pass
    
    @property
    def max(self):
        print("Need to implement max")
        return 1
        
    @property
    def min(self):
        print("Need to implement min")
        return -1
    
    def __getitem__(self, ndx):
        return None
    
    ###
    
    def __get_long_position_diffs(self, standalization="minimax"):
        positions = self.market.positions["ask"]
        if len(positions) > 0:
            diffs = []
            current_bid = self.get_current_bid()
            if standalization == "minimax":
                current_bid = utils.mini_max(current_bid, self.min, self.max, (0, 1))
                for key, position in positions.items():
                    price = utils.mini_max(position.price, self.min, self.max, (0, 1))
                    diffs.append(current_bid - price)
            else:
                for key, position in positions.items():
                    diffs.append(current_bid - position.price)
            return diffs
        else:
            return []
    
    def __get_short_position_diffs(self, standalization="minimax"):
        positions = self.market.positions["bid"]
        if len(positions) > 0:
            diffs = []
            current_ask = self.get_current_ask()
            if standalization == "minimax":
                current_ask = utils.mini_max(current_ask, self.min, self.max, (0, 1))
                for key, position in positions.items():
                    price = utils.mini_max(position.price, self.min, self.max, (0, 1))
                    diffs.append(price - current_ask)
            else:
                for key, position in positions.items():
                    diffs.append(position.price - current_ask)
            return diffs
        else:
            return []

    def get_diffs(self, position_type=None) -> list:
        if position_type == 'ask' or position_type == 'long':
            return self.__get_long_position_diffs()
        elif position_type == "bid" or position_type == 'short':
            return self.__get_short_position_diffs()
        else:
            diffs = self.__get_long_position_diffs()
            bid_diffs = self.__get_short_position_diffs()
            if len(bid_diffs) > 0:
                diffs.append(*bid_diffs)
            return diffs
        
    
    def get_diffs_with_minmax(self, position_type=None)-> list:
        if position_type == 'ask' or position_type == 'long':
            return self.__get_long_position_diffs(standalization="minimax")
        elif position_type == "bid" or position_type == 'short':
            return self.__get_short_position_diffs(standalization="minimax")
        else:
            diffs = self.__get_long_position_diffs(standalization="minimax")
            bid_diffs = self.__get_short_position_diffs(standalization="minimax")
            if len(bid_diffs) > 0:
                diffs.append(*bid_diffs)
            return diffs
    
    def __open_long_position(self, symbol, boughtRate, amount, tp=None, sl=None, option_info=None, result=None):
        self.logger.debug(f"open long position is created: {symbol}, {boughtRate}, {amount}, {tp}, {sl}, {option_info}, {result}")
        position = self.market.open_position(order_type="ask", symbol=symbol,  price=boughtRate, amount=amount, tp=tp, sl=sl, option=option_info, result=result)
        if self.do_render:
            self.__rendere.add_trade_history_to_latest_tick(1, boughtRate, self.__ohlc_index)
        return position

    def __open_short_position(self, symbol, soldRate, amount, option_info=None, tp=None, sl=None, result = None):
        self.logger.debug(f"open short position is created: {symbol}, {soldRate}, {amount}, {tp}, {sl}, {option_info}, {result}")
        position = self.market.open_position(order_type="bid", symbol=symbol,  price=soldRate, amount=amount, tp=tp, sl=sl, option=option_info, result=result)
        if self.do_render:
            self.__rendere.add_trade_history_to_latest_tick(2, soldRate, self.__ohlc_index)
        return position
    
    def _is_grouped_by_symbol(self, columns: pd.MultiIndex):
        if type(columns) == pd.MultiIndex:
            open_column = "open"
            for column in columns.droplevel(0):
                if open_column == str(column).lower():
                    return True
            for column in columns.droplevel(1):
                if open_column == str(column).lower():
                    return False
            print("open column is not found on column")
            return None
        else:
            raise TypeError(f"{type(columns)} is not supported")
    
    def get_ohlc_columns(self, symbol:str=None) -> dict:
        """ returns column names of ohlc data.
        
        Returns:
            dict: format is {"Open": ${open_column}, "High": ${high_column}, "Low": ${low_column}, "Close": ${close_column}, "Time": ${time_column} (Optional), "Volume": ${volume_column} (Optional)}
        """
        if self.ohlc_columns == None:
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
                    if self._is_grouped_by_symbol(columns):
                        columns = set(columns.droplevel(0))
                    else:
                        columns = set(columns.droplevel(1))
                else:
                    if symbol in columns.droplevel(0):
                        #grouped_by_symbol = False
                        columns = columns.swaplevel(0, 1)
                    elif symbol not in columns.droplevel(1):
                        raise ValueError(f"Specified symbol {symbol} not found on columns.")
                    columns = columns[symbol]
            for column in columns:
                column_ = str(column).lower()
                if column_ == 'open':
                    self.ohlc_columns['Open'] = column
                elif column_ == 'high':
                    self.ohlc_columns['High'] = column
                elif column_ == 'low':
                    self.ohlc_columns['Low'] = column
                elif 'close' in column_:
                    self.ohlc_columns['Close'] = column
                elif "time" in column_:#assume time, timestamp or datetime
                    self.ohlc_columns["Time"] = column
                elif "volume" in column_:
                    self.ohlc_columns["Volume"] = column
        return self.ohlc_columns
    
    def revert_preprocesses(self, data: pd.DataFrame=None):
        if data is None:
            data = self.get_rates()
        data = data.copy()
        
        columns_dict = self.get_ohlc_columns()
        if "Time" in columns_dict and columns_dict["Time"] != None:
            date_column = columns_dict["Time"]
            columns = list(data.columns.copy())
            if date_column in columns:
                date_data = data[date_column].copy()
                #df = df.set_index(date_column)
                columns.remove(date_column)
                data = data[columns]
        for ps in self.__preprocesses:
            data = ps.revert(data)
        return data
    
    def roll_data(self, data: pd.DataFrame, to_frame:int=None, to_freq:str=None, grouped_by_symbol=True) -> pd.DataFrame:
        """ Roll time series data by specified frequency.

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
            freq = Frame.to_panda_freq(to_frame)
        else:
            freq = to_freq
        
        if grouped_by_symbol:
            data.columns = data.columns.swaplevel(0, 1)
            
        ohlc_columns_dict = self.get_ohlc_columns()
        rolled_data_dict = {}
        if "Open" in ohlc_columns_dict:
            opn_clm = ohlc_columns_dict["Open"]
            ##assume index isn't Multi index
            rolled_opn = data[opn_clm].groupby(pd.Grouper(level=0, freq=freq)).first()
            rolled_data_dict[opn_clm] = rolled_opn
        if "High" in ohlc_columns_dict:
            high_clm = ohlc_columns_dict["High"]
            rolled_high = data[high_clm].groupby(pd.Grouper(level=0, freq=freq)).max()
            rolled_data_dict[high_clm] = rolled_high
        if "Low" in ohlc_columns_dict:
            low_clm = ohlc_columns_dict["Low"]
            rolled_low = data[low_clm].groupby(pd.Grouper(level=0, freq=freq)).min()
            rolled_data_dict[low_clm] = rolled_low
        if "Close" in ohlc_columns_dict:
            cls_clm = ohlc_columns_dict["Close"]
            rolled_cls = data[cls_clm].groupby(pd.Grouper(level=0, freq=freq)).last()
            rolled_data_dict[cls_clm] = rolled_cls
        if "Volume" in ohlc_columns_dict:
            vlm_clm = ohlc_columns_dict["Volume"]
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