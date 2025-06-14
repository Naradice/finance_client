import datetime
import os
import random
from time import sleep

import MetaTrader5 as mt5
import numpy
import pandas as pd

from .. import enum
from .. import frames as Frame
from ..client_base import Client

try:
    from ..fprocess.fprocess.csvrw import get_datafolder_path, read_csv, write_df_to_csv
except ImportError:
    from ..fprocess.csvrw import get_datafolder_path, read_csv, write_df_to_csv


class MT5Client(Client):
    kinds = "mt5"

    AVAILABLE_FRAMES = {
        Frame.MIN1: mt5.TIMEFRAME_M1,
        Frame.MIN5: mt5.TIMEFRAME_M5,
        Frame.MIN10: mt5.TIMEFRAME_M10,
        Frame.MIN30: mt5.TIMEFRAME_M30,
        Frame.H1: mt5.TIMEFRAME_H1,
        Frame.H2: mt5.TIMEFRAME_H2,
        Frame.H4: mt5.TIMEFRAME_H4,
        Frame.H8: mt5.TIMEFRAME_H8,
        Frame.D1: mt5.TIMEFRAME_D1,
        Frame.W1: mt5.TIMEFRAME_W1,
        Frame.MO1: mt5.TIMEFRAME_MN1,
    }

    AVAILABLE_FRAMES_STR = {
        Frame.MIN1: "min1",
        Frame.MIN5: "min5",
        Frame.MIN10: "min10",
        Frame.MIN30: "min30",
        Frame.H1: "h1",
        Frame.H2: "h2",
        Frame.H4: "h4",
        Frame.H8: "h8",
        Frame.D1: "d1",
        Frame.W1: "w1",
        Frame.MO1: "m1",
    }

    LAST_IDX = {
        Frame.H2: 12 * 12 * 560,
        Frame.H4: 43930,
        Frame.H8: int(12 * 12 * 560 / 4) + 5400,
        Frame.D1: 13301,
        Frame.W1: 2681,
    }

    def login(self, id, password, server):
        return mt5.login(
            id,
            password=password,
            server=server,
        )

    def get_additional_params(self):
        self.logger.warn("parameters are not saved for mt5 as credentials are included.")
        return {}

    def __init__(
        self,
        id,
        password,
        server,
        auto_index=True,
        simulation=True,
        frame=5,
        observation_length=None,
        symbols=["USDJPY"],
        volume_unit=0.01,
        back_test=False,
        do_render=False,
        budget=1000000,
        storage=None,
        enable_trade_log=False,
        logger=None,
        seed=1017,
        idc_process=None,
        std_processes=None,
    ):
        super().__init__(
            budget=budget,
            frame=frame,
            symbols=symbols,
            observation_length=observation_length,
            provider=server,
            do_render=do_render,
            enable_trade_log=enable_trade_log,
            logger=logger,
            storage=storage,
            idc_process=idc_process,
            pre_process=std_processes,
        )
        self.back_test = back_test
        self.debug = False
        self.provider = server
        self.isWorking = mt5.initialize()
        if not self.isWorking:
            err_txt = f"initialize() failed, error code = {mt5.last_error()}"
            self.logger.error(err_txt)
            raise Exception(err_txt)
        self.logger.info(f"MetaTrader5 package version {mt5.__version__}")
        authorized = mt5.login(
            id,
            password=password,
            server=server,
        )
        if not authorized:
            err_txt = "User Authorization Failed"
            self.logger.error(err_txt)
            raise Exception(err_txt)
        if type(symbols) == str:
            symbols = [symbols]
        if type(symbols) != list:
            try:
                symbols = list(symbols)
            except Exception:
                raise TypeError(f"{type(symbols)} is not supported as symbols")

        self.symbols = symbols
        self.frame = frame
        self.volume_unit = volume_unit
        try:
            self.mt5_frame = self.AVAILABLE_FRAMES[frame]
        except Exception as e:
            raise e

        account_info = mt5.account_info()
        if account_info is None:
            self.logger.warning("Retreiving account information failed. Please check your internet connection.")
        self.logger.info(f"Balance: {account_info}")

        # check all symbol available
        self.points = {}
        self.orders = {}
        for symbol in self.symbols:
            symbol_info = mt5.symbol_info(symbol)
            if symbol_info is None:
                err_txt = f"Symbol, {symbol}, not found"
                self.logger.error(err_txt)
                raise Exception(err_txt)
            self.points[symbol] = symbol_info.point
            self.orders[symbol] = {"ask": [], "bid": []}

        if self.back_test:
            if frame > Frame.W1:
                raise ValueError("back_test mode is available only for less than W1 for now")
            if type(seed) == int:
                random.seed(seed)
            else:
                random.seed(1017)
            # self.sim_index = random.randrange(int(12*24*347*0.2), 12*24*347 - 30) ##only for M5
            if frame >= Frame.H2:
                if frame in self.LAST_IDX:
                    self.sim_index = self.LAST_IDX[frame]
                else:
                    raise ValueError(f"unexpected Frame is specified: {frame}")
            else:
                self.sim_index = 12 * 24 * 345
            self.sim_initial_index = self.sim_index
            self.auto_index = auto_index
            if auto_index:
                self.__next_time = None

        if simulation and auto_index:
            self.logger.warning("auto index feature is applied only for back test.")

        if back_test or simulation:
            self.__ignore_order = True
        else:
            self.__ignore_order = False

    def __get_provider_string(self):
        return os.path.join(self.kinds, self.provider)

    def _get_default_path(self):
        data_folder = get_datafolder_path()
        os.path.join(data_folder, self.__get_provider_string())

    def __generate_common_request(self, action, symbol, _type, vol, price, dev, sl=None, tp=None, position=None, order=None):
        request = {
            "action": action,
            "symbol": symbol,
            "volume": vol,
            "price": price,
            "deviation": dev,
            "magic": 100000000,
            "type_time": mt5.ORDER_TIME_GTC,
            "type": _type,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }
        if sl is not None:
            request["sl"] = sl
        if tp is not None:
            request["tp"] = tp
        if position is not None:
            request["position"] = position
        if order is not None:
            request["order"] = order
        return request

    def __check_trade_result(self, result):
        self.logger.debug(f"order result: {result}")
        if result is None:
            self.logger.error(f"order failed. result is None.")
            return enum.TRADE_ERROR
        if result.order == 0:
            # order failed
            self.logger.error(f"order failed due to {result.comment}")
            retcode = result.retcode
            if retcode in [mt5.TRADE_RETCODE_REQUOTE, mt5.TRADE_RETCODE_PRICE_CHANGED]:
                # if client changed order price, it may be accepted
                return enum.TRADE_PRICE_CHANGED
            if retcode in [mt5.TRADE_RETCODE_TOO_MANY_REQUESTS]:
                # if client try again, it may be accepted
                return enum.TRADE_TOO_MANY_REQUESTS
            if retcode in [mt5.TRADE_RETCODE_REJECT, mt5.TRADE_RETCODE_TIMEOUT, mt5.TRADE_RETCODE_CONNECTION]:
                # if client try again later, it may be accepted
                return enum.TRADE_CONTEXT_BUSY
            if retcode in [mt5.TRADE_RETCODE_NO_MONEY]:
                return enum.TRADE_NO_MONEY
        else:
            if result.retcode == mt5.TRADE_RETCODE_DONE:
                # success
                self.logger.info(f"order success {result.comment}")
                return enum.TRADE_DONE
            elif result.retcode == mt5.TRADE_RETCODE_DONE_PARTIAL:
                self.logger.warning(f"order partially done {result.comment}")
                return enum.TRADE_PARTIAL_DONE
            else:
                # unkown state
                self.logger.error(f"order failed due to unkown reason: {result.comment}")
                return enum.TRADE_ERROR

    def __request_order(self, request):
        result = mt5.order_send(request)
        retcode = self.__check_trade_result(result)
        if retcode in [enum.TRADE_DONE, enum.TRADE_PARTIAL_DONE]:
            return True, result
        elif retcode == enum.TRADE_CONTEXT_BUSY:
            sleep(1)
            return self.__request_order(request)
        else:
            return False, result

    def __post_pending_order(self, symbol, _type, vol, price, dev, sl=None, tp=None, position=None):
        request = self.__generate_common_request(symbol, _type, vol, price, dev, sl, tp, position)
        request["action"] = mt5.TRADE_ACTION_PENDING
        result = mt5.order_send(request)
        self.logger.debug(f"order result: {result}")
        return result

    def __get_ask(self, symbol, retry=1):
        info = mt5.symbol_info_tick(symbol)
        if info is None:
            sleep(pow(3, retry))
            return self.__get_ask(symbol, retry + 1)
        else:
            return info.ask

    def get_current_ask(self, symbols=None):
        if symbols is None or len(symbols) == 0:
            symbols = self.symbols.copy()
        if isinstance(symbols, str):
            symbols = [symbols]
        if self.back_test:
            if len(symbols) == 1:
                df = self.__download(length=1, symbol=symbols[0], frame=self.mt5_frame, index=self.sim_index - 1)
            else:
                df = pd.DataFrame()
                for symbol in symbols:
                    symbol_tick = self.__download(length=1, symbol=symbol, frame=self.mt5_frame, index=self.sim_index - 1)
                    symbol_tick.index = [symbol]
                    df = pd.concat(df, symbol_tick)
            ohlc_columns = self.get_ohlc_columns()
            open_column = ohlc_columns["Open"]
            high_column = ohlc_columns["High"]

            ask_srs = random.uniform(df[open_column].iloc[0], df[high_column].iloc[0])
            return ask_srs
        else:
            ask_values = {}
            for symbol in symbols:
                ask_values[symbol] = [self.__get_ask(symbol)]
            ask_srs = pd.DataFrame.from_dict(ask_values).iloc[0]
        if len(symbols) == 1:
            ask_srs = ask_srs[symbols[0]]
        return ask_srs

    def __get_bid(self, symbol, retry=1):
        info = mt5.symbol_info_tick(symbol)
        if info is None:
            sleep(pow(3, retry))
            return self.__get_bid(symbol, retry + 1)
        else:
            return info.bid

    def get_current_bid(self, symbols=None):
        if symbols is None or len(symbols) == 0:
            symbols = self.symbols.copy()
        if isinstance(symbols, str):
            symbols = [symbols]
        if self.back_test:
            if len(symbols) == 1:
                df = self.__download(length=1, symbol=symbols[0], frame=self.mt5_frame, index=self.sim_index - 1)
            else:
                df = pd.DataFrame()
                for symbol in symbols:
                    symbol_tick = self.__download(length=1, symbol=symbol, frame=self.mt5_frame, index=self.sim_index - 1)
                    symbol_tick.index = [symbol]
                    df = pd.concat(df, symbol_tick)
            ohlc_columns = self.get_ohlc_columns()
            open_column = ohlc_columns["Open"]
            low_column = ohlc_columns["Low"]
            bid_srs = random.uniform(df[low_column].iloc[0], df[open_column].iloc[0])
            return bid_srs
        else:
            bid_values = {}
            for symbol in symbols:
                bid_values[symbol] = [self.__get_bid(symbol)]
            bid_srs = pd.DataFrame.from_dict(bid_values).iloc[0]
        if len(symbols) == 1:
            bid_srs = bid_srs[symbols[0]]
        return bid_srs

    def get_current_spread(self, symbols=None):
        if symbols is None or len(symbols) == 0:
            symbols = self.symbols.copy()
        if self.back_test:
            df = pd.DataFrame()
            for symbol in symbols:
                symbol_tick = self.__download(length=1, symbol=symbol, frame=self.mt5_frame, index=self.sim_index - 1)
                symbol_tick.index = [symbol]
                df = pd.concat(df, symbol_tick)
            spread_srs = df["spread"].iloc[0]
        else:
            spread_values = {}
            for symbol in symbols:
                spread_values[symbol] = [mt5.symbol_info_tick(symbol).spread]
            spread_srs = pd.DataFrame.from_dict(spread_values).iloc[0]
        if len(symbols) == 1:
            spread_srs = spread_srs[symbols[0]]
        return spread_srs

    def __check_params(self, buy_order: bool, rate: float, tp=None, sl=None):
        if buy_order:
            pass
        else:
            if tp is not None:
                if rate <= tp:
                    self.logger.error("tp should be lower than price for sell order")
                    return False
            if sl is not None:
                if rate >= sl:
                    self.logger.error("sl should be higer than price for sell order")
                    return False
        return True

    def _market_sell(self, symbol, price, amount, tp=None, sl=None, **kwargs):
        if self.__ignore_order is False:
            suc = self.__check_params(False, price, tp, sl)
            if suc is False:
                return False, None
            request = self.__generate_common_request(
                action=mt5.TRADE_ACTION_DEAL,
                symbol=symbol,
                _type=mt5.ORDER_TYPE_SELL,
                vol=amount * self.volume_unit,
                price=price,
                dev=20,
                sl=sl,
                tp=tp,
            )
            order_suc, result = self.__request_order(request)
            if order_suc:
                return True, result.order
            else:
                return False, None
        else:
            return True, numpy.random.randint(100, 100000)

    def _pending_sell(self, symbol, price, amount, tp=None, sl=None, option_info=None):
        if self.__ignore_order is False:
            suc = self.__check_params(False, price, tp, sl)
            if suc is False:
                return False, None
            request = self.__generate_common_request(
                action=mt5.TRADE_ACTION_PENDING,
                symbol=symbol,
                _type=mt5.ORDER_TYPE_SELL_LIMIT,
                vol=amount * self.volume_unit,
                price=price,
                dev=20,
                sl=sl,
                tp=tp,
            )
            order_suc, result = self.__request_order(request)
            if order_suc:
                return True, result.order
            else:
                return False, None
        else:
            return True, numpy.random.randint(100, 100000)

    def _buy_for_settlement(self, symbol, price, amount, option, result):
        if self.__ignore_order is False:
            if result is not None:
                if hasattr(result, "order"):
                    position_id = result.order
                else:
                    position_id = result
                request = self.__generate_common_request(
                    action=mt5.TRADE_ACTION_DEAL,
                    symbol=symbol,
                    _type=mt5.ORDER_TYPE_BUY,
                    vol=amount * self.volume_unit,
                    price=price,
                    dev=20,
                    position=position_id,
                )
                order_suc, result = self.__request_order(request=request)
                if order_suc:
                    return result
                else:
                    return False
            else:
                return False
        else:
            return True

    def _market_buy(self, symbol, price, amount, tp=None, sl=None, option_info=None):
        if self.__ignore_order is False:
            suc = self.__check_params(True, price, tp, sl)
            if suc is False:
                return False, None
            request = self.__generate_common_request(
                action=mt5.TRADE_ACTION_DEAL, symbol=symbol, _type=mt5.ORDER_TYPE_BUY, vol=0.1 * amount, price=price, dev=20, sl=sl, tp=tp
            )
            order_suc, result = self.__request_order(request)
            if order_suc:
                return True, result.order
            else:
                return False, None
        else:
            return True, numpy.random.randint(100, 100000)

    def _pending_sell(self, symbol, price, amount, tp=None, sl=None, option_info=None):
        if self.__ignore_order is False:
            suc = self.__check_params(False, price, tp, sl)
            if suc is False:
                return False, None
            request = self.__generate_common_request(
                action=mt5.TRADE_ACTION_PENDING,
                symbol=symbol,
                _type=mt5.ORDER_TYPE_BUY_LIMIT,
                vol=amount * self.volume_unit,
                price=price,
                dev=20,
                sl=sl,
                tp=tp,
            )
            order_suc, result = self.__request_order(request)
            if order_suc:
                return True, result.order
            else:
                return False, None
        else:
            return True, numpy.random.randint(100, 100000)

    def _sell_for_settlment(self, symbol, price, amount, option, result):
        if self.__ignore_order is False:
            if result is not None:
                if hasattr(result, "order"):
                    position_id = result.order
                else:
                    position_id = result
                request = self.__generate_common_request(
                    action=mt5.TRADE_ACTION_DEAL,
                    symbol=symbol,
                    _type=mt5.ORDER_TYPE_SELL,
                    vol=amount * self.volume_unit,
                    price=price,
                    dev=20,
                    position=position_id,
                )
                order_suc, result = self.__request_order(request=request)
                if order_suc:
                    return result
                else:
                    return False
            else:
                return False
        else:
            return True

    def __generate_file_name(self, symbol, frame):
        if frame in self.AVAILABLE_FRAMES_STR:
            frame_str = self.AVAILABLE_FRAMES_STR[frame]
        elif type(frame) == int:
            if frame > 16384 * 3:
                frame_str = self.AVAILABLE_FRAMES_STR[Frame.MO1]
            elif frame > 16384 * 2:
                weeks = frame - 16384 * 2
                frame_minutes = Frame.W1 * weeks
                frame_str = self.AVAILABLE_FRAMES_STR[frame_minutes]
            elif frame > 16384:
                hours = frame - 16384
                frame_minutes = Frame.H1 * hours
                frame_str = self.AVAILABLE_FRAMES_STR[frame_minutes]
        return f"mt5_{symbol}_{frame_str}.csv"

    def __download_entire(self, symbol, frame):
        existing_rate_df = None
        file_name = self.__generate_file_name(symbol, frame)
        existing_rate_df = read_csv(self.__get_provider_string(), file_name, ["time"])

        MAX_LENGTH = 12 * 24 * 345  # not accurate, may depend on server

        if existing_rate_df is None:
            interval = MAX_LENGTH
        else:
            delta = datetime.datetime.now(datetime.UTC) - existing_rate_df["time"].iloc[-1]
            total_seconds = delta.total_seconds()
            if (total_seconds / (60 * 60 * 24 * 7)) >= 1:
                total_seconds = total_seconds * 5 / 7  # remove sat,sun
            interval = int((total_seconds / 60) / self.frame)

        if interval > 0:
            start_index = 0
            if interval > MAX_LENGTH:
                interval = MAX_LENGTH
                self.logger.warn("data may have vacant")

            rates = mt5.copy_rates_from_pos(symbol, frame, start_index, interval)
            new_rates = rates

            while new_rates is not None:
                interval = len(new_rates)
                start_index += interval
                new_rates = mt5.copy_rates_from_pos(symbol, frame, start_index, interval)
                if new_rates is not None:
                    rates = numpy.concatenate([new_rates, rates])
                else:
                    break
            rate_df = pd.DataFrame(rates)
            rate_df["time"] = pd.to_datetime(rate_df["time"], unit="s")

            if existing_rate_df is not None:
                rate_df = pd.concat([existing_rate_df, rate_df])

            rate_df = rate_df.sort_values("time")
            rate_df = rate_df.drop_duplicates(keep="last", subset="time")

            write_df_to_csv(
                rate_df,
                self.__get_provider_string(),
                file_name,
                panda_option={"mode": "w", "index": False, "header": True},
            )
            rate_df.set_index("time", inplace=True)
            return rate_df
        else:
            self.logger.info(f"no new data found for {symbol}")
            return existing_rate_df

    def __download(self, length, symbol, frame, index=None):
        if index is None:
            if self.back_test:
                # simu index will be reduced by get_ohlc_from_client
                start_index = self.sim_index
            else:
                start_index = 0
        else:
            start_index = index
        _length = None

        if self.auto_index and length == 1:
            _length = length
            length += 1

        # save data when mode is back test
        # if length is less than stored length - step_index. Then update time fit logic
        rates = mt5.copy_rates_from_pos(symbol, frame, start_index, length)
        df_rates = pd.DataFrame(rates)
        if len(df_rates) > 0:
            df_rates["time"] = pd.to_datetime(df_rates["time"], unit="s")
            # df_rates = df_rates.set_index('time')

            # MT5 index based on current time. So we need to update the index based on past time.
            if self.back_test and self.auto_index:
                if self.__next_time is None:
                    self.__next_time = df_rates["time"].iloc[1]
                    self.logger.debug(f"auto index: initialized with {df_rates['time'].iloc[0]}")
                else:
                    current_time = df_rates["time"].iloc[0]
                    if current_time == self.__next_time:
                        self.logger.debug(f"auto index: index is ongoing on {current_time}")
                        self.__next_time = df_rates["time"].iloc[1]
                    elif current_time > self.__next_time:
                        self.logger.debug(f"auto index: {current_time} > {self.__next_time}. may time past.")
                        candidate = self.sim_index
                        while current_time != self.__next_time:
                            candidate += 1
                            rates = mt5.copy_rates_from_pos(symbol, frame, candidate, length)
                            df_rates = pd.DataFrame(rates)
                            df_rates["time"] = pd.to_datetime(df_rates["time"], unit="s")
                            current_time = df_rates["time"].iloc[0]
                        self.sim_index = candidate

                        self.logger.debug(f"auto index: fixed to {current_time}")
                        self.__next_time = df_rates["time"].iloc[1]
                        # to avoid infinite loop, don't call oneself
                    else:
                        self.logger.debug(f"auto index: {current_time} < {self.__next_time} somehow.")
                        candidate = self.sim_index
                        while current_time != self.__next_time:
                            candidate = candidate - 1
                            rates = mt5.copy_rates_from_pos(symbol, frame, candidate, length)
                            df_rates = pd.DataFrame(rates)
                            df_rates["time"] = pd.to_datetime(df_rates["time"], unit="s")
                            current_time = df_rates["time"].iloc[0]
                        self.sim_index = candidate

                        self.logger.debug(f"auto index: fixed to {current_time}")
                        self.__next_time = df_rates["time"].iloc[1]

            df_rates.set_index("time", inplace=True)
            if self.auto_index and _length is not None:
                return df_rates.iloc[:length]
        return df_rates

    def __get_ohlc(self, length, symbols, frame, columns=None, index=None, grouped_by_symbol=True):
        if frame is None:
            frame = self.mt5_frame
        if frame != self.mt5_frame:
            if frame in self.AVAILABLE_FRAMES:
                frame = self.AVAILABLE_FRAMES[frame]

        if length is None:
            download_func = self.__download_entire
            kwargs = {"frame": frame}
        elif length > 0:
            download_func = self.__download
            kwargs = {"length": length, "frame": frame, "index": index}
        DFS = {}
        df = pd.DataFrame()
        for symbol in symbols:
            temp_df = download_func(symbol=symbol, **kwargs)
            if columns is not None:
                temp_df = temp_df[columns]
            DFS[symbol] = temp_df
        if len(DFS) > 0:
            df = pd.concat(DFS.values(), axis=1, keys=DFS.keys())
        if len(symbols) > 1:
            if grouped_by_symbol is False:
                df.columns = df.columns.swaplevel(0, 1)
        elif len(symbols) == 1:
            df = df[symbols[0]]
        return df

    def _get_ohlc_from_client(self, length: int = None, symbols: list = [], frame: int = None, columns=None, index=None, grouped_by_symbol=True):
        df_rates = self.__get_ohlc(length, symbols, frame, columns, index, grouped_by_symbol)
        if self.auto_index:
            self.sim_index = self.sim_index - 1
        return df_rates

    def update_order(self, order_id, price, tp=None, sl=None):
        if self.__ignore_order is False:
            orders = mt5.orders_get()
            for order in orders:
                if order.ticket == order_id:
                    request = {"action": mt5.TRADE_ACTION_MODIFY, "price": price, "order": order_id}
                    if tp is not None:
                        request["tp"] = tp
                    if sl is not None:
                        request["sl"] = sl
                    suc, _ = self.__request_order(request)
                    return suc
            return False
        else:
            return True

    def cancel_order(self, order):
        if self.__ignore_order is False:
            if hasattr(order, "order"):
                position = order.order
            else:
                position = order
            request = {"action": mt5.TRADE_ACTION_REMOVE, "order": position}
            suc, _ = self.__request_order(request)
            return suc
        else:
            self.logger.warning("pending order is not available on backtest and simulator")
            return True

    def update_position(self, position, tp=None, sl=None):
        if tp is None and sl is None:
            self.logger.error("update position require tp or sl")
            return False
        if self.__ignore_order is False:
            if hasattr(position, "order"):
                position = position.order
            request = {"action": mt5.TRADE_ACTION_SLTP, "position": position}
            if tp is not None:
                request["tp"] = tp
            if sl is not None:
                request["sl"] = sl
            suc, _ = self.__request_order(request)
            return suc
        else:
            return True

    def __len__(self):
        if self.frame in self.LAST_IDX:
            return self.LAST_IDX[self.frame]
        else:
            MAX_LENGTH = 12 * 24 * 345
            return MAX_LENGTH
