import datetime
import logging
import os
import random
from time import sleep

import MetaTrader5 as mt5
import numpy
import pandas as pd

from .. import enum
from .. import frames as Frame
from ..client_base import ClientBase
from ..position import Position

try:
    from ..fprocess.fprocess.csvrw import get_datafolder_path, read_csv, write_df_to_csv
except ImportError:
    from ..fprocess.csvrw import get_datafolder_path, read_csv, write_df_to_csv

logger = logging.getLogger(__name__)


class MT5Client(ClientBase):
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
        logger.warning("parameters are not saved for mt5 as credentials are included.")
        return {}

    def __init__(
        self,
        id,
        password,
        server,
        simulation=True,
        frame=5,
        observation_length=None,
        point_unit=None,
        back_test=False,
        auto_index=False,
        do_render=False,
        budget=1000000,
        storage=None,
        seed=1017,
        idc_process=None,
        std_processes=None,
    ):
        """Trade Client for MT5 server

        Args:
            id (int): login id of MT5 server
            password (str): login password of MT5 server
            server (str): name of MT5 server
            simulation (bool, optional): If simulation is True, don't send an order to MT5 server. Defaults to True.
            frame (int, optional): specify default frame. Defaults to 5.
            observation_length (int, optional): specify default length to get ohlc. Defaults to None.
            point_unit (float, optional): specify order unit to override default one if you need. If None, the unit is automatically detected.
            back_test (bool, optional): if back_test is True, OHLC is retrieved based on sim_index. Defaults to False.
            auto_index (bool, optional): automatically step index when backtest is True. Defaults to False.
            do_render (bool, optional): If True, rendere OHLC when it is retrieved by get_ohlc. Defaults to False.
            budget (int, optional): Defaults to 1000000.
            storage (db.BaseStorage, optional): Storage to store positions. Defaults to None.
            seed (int, optional): _description_. Defaults to 1017.
            idc_process (list[fprocess.ProcessBase], optional): list of indicator process to apply them when get_ohlc is called. Defaults to None.
            std_processes (list[fprocess.ProcessBase], optional): list of standalization process to apply them when get_ohlc is called. Defaults to None.
        """
        super().__init__(
            budget=budget,
            frame=frame,
            observation_length=observation_length,
            provider=server,
            do_render=do_render,
            enable_trade_log=False,
            storage=storage,
            idc_process=idc_process,
            pre_process=std_processes,
        )
        self.back_test = back_test
        self.debug = False
        self.provider = server
        isWorking = mt5.initialize()
        if not isWorking:
            err_txt = f"initialize() failed, error code = {mt5.last_error()}"
            logger.error(err_txt)
            raise Exception(err_txt)
        logger.info(f"MetaTrader5 package version {mt5.__version__}")
        authorized = mt5.login(
            id,
            password=password,
            server=server,
        )
        if not authorized:
            err_txt = "User Authorization Failed"
            logger.error(err_txt)
            raise Exception(err_txt)
        self.frame = frame
        self.point_unit = point_unit
        try:
            self.mt5_frame = self.AVAILABLE_FRAMES[frame]
        except Exception as e:
            raise e

        account_info = mt5.account_info()
        if account_info is None:
            logger.warning("Retreiving account information failed. Please check your internet connection.")
        logger.info(f"Balance: {account_info}")

        if self.back_test:
            if frame is None or frame > Frame.W1:
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
            logger.warning("auto index feature is applied only for back test.")

        if back_test or simulation:
            self.__ignore_order = True
        else:
            self.__ignore_order = False

    def __get_provider_string(self):
        return os.path.join(self.kinds, self.provider)

    def __get_point(self, symbol):
        if self.point_unit is None:
            symbol_info = mt5.symbol_info(symbol)
            if symbol_info is None:
                logger.error(f"symbol info is not available for {symbol}")
                return None
            else:
                point_for_symbol = symbol_info.point
                logger.debug(f"detected point for symbol: {point_for_symbol}")
                return point_for_symbol
        else:
            return self.point_unit

    def _get_default_path(self):
        data_folder = get_datafolder_path()
        return os.path.join(data_folder, self.__get_provider_string())

    def _get_columns_from_data(self, symbol=slice(None)):
        # disable auto index and rendering option
        temp = self.auto_index
        self.auto_index = False
        temp_r = self.do_render
        self.do_render = False
        if symbol is None or symbol == slice(None):
            symbol = ["USDJPY"]
        data = self.get_ohlc(symbol, 1)
        # revert options
        self.auto_index = temp
        self.do_render = temp_r

        return data.columns

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
        logger.debug(f"order result: {result}")
        if result is None:
            logger.error(f"order failed. result is None.")
            return enum.TRADE_ERROR
        if result.order == 0:
            # order failed
            logger.error(f"order failed due to {result.comment}")
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
                logger.info(f"order success {result.comment}")
                return enum.TRADE_DONE
            elif result.retcode == mt5.TRADE_RETCODE_DONE_PARTIAL:
                logger.warning(f"order partially done {result.comment}")
                return enum.TRADE_PARTIAL_DONE
            else:
                # unkown state
                logger.error(f"order failed due to unkown reason: {result.comment}")
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
        logger.debug(f"order result: {result}")
        return result

    def __get_attr_from_info(self, symbol, attr: str, retry=1):
        info = mt5.symbol_info(symbol)
        if info is None:
            logger.debug(f"failed to get {attr} from {symbol}")
            sleep(pow(2, retry))
            if retry <= 3:
                return self.__get_attr_from_info(symbol, attr, retry + 1)
            else:
                return pd.NA
        else:
            return getattr(info, attr)

    def get_current_ask(self, symbols):
        if symbols is None or symbols == slice(None) or len(symbols) == 0:
            logger.error("symbol is mandatory to get current ask")
            return None
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
                ask_values[symbol] = [self.__get_attr_from_info(symbol, "ask")]
            ask_srs = pd.DataFrame.from_dict(ask_values).iloc[0]
        if len(symbols) == 1:
            ask_srs = ask_srs[symbols[0]]
        return ask_srs

    def get_current_bid(self, symbols):
        if symbols is None or len(symbols) == 0:
            logger.error("symbol is mandatory to get current bid")
            return None
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
                bid_values[symbol] = [self.__get_attr_from_info(symbol, "bid")]
            bid_srs = pd.DataFrame.from_dict(bid_values).iloc[0]
        if len(symbols) == 1:
            bid_srs = bid_srs[symbols[0]]
        return bid_srs

    def get_current_spread(self, symbols):
        if symbols is None or len(symbols) == 0:
            logger.error("symbol is mandatory to get current spread")
            return None
        if isinstance(symbols, str):
            symbols = [symbols]
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
                spread_values[symbol] = [self.__get_attr_from_info(symbol, "spread")]
            spread_srs = pd.DataFrame.from_dict(spread_values).iloc[0]
        if len(symbols) == 1:
            spread_srs = spread_srs[symbols[0]]
        return spread_srs

    def get_symbols(self):
        symbols_info = mt5.symbols_get()
        symbols = [info.name for info in symbols_info]
        return symbols

    def __check_params(self, buy_order: bool, rate: float, tp=None, sl=None):
        if buy_order:
            if tp is not None:
                if rate >= tp:
                    logger.error("tp should be higer than price for buy order")
                    return False
            if sl is not None:
                if rate <= sl:
                    logger.error("sl should be lower than price for buy order")
                    return False
        else:
            if tp is not None:
                if rate <= tp:
                    logger.error("tp should be lower than price for sell order")
                    return False
            if sl is not None:
                if rate >= sl:
                    logger.error("sl should be higer than price for sell order")
                    return False
        return True

    def _market_sell(self, symbol, price, amount, tp=None, sl=None, **kwargs):
        suc = self.__check_params(False, price, tp, sl)
        if suc is False:
            return False, None
        point = self.__get_point(symbol)
        if point is None:
            return False, None

        if self.__ignore_order is False:
            request = self.__generate_common_request(
                action=mt5.TRADE_ACTION_DEAL,
                symbol=symbol,
                _type=mt5.ORDER_TYPE_SELL,
                vol=amount * point,
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
        suc = self.__check_params(False, price, tp, sl)
        if suc is False:
            return False, None
        point = self.__get_point(symbol)
        if point is None:
            return False, None

        if self.__ignore_order is False:
            request = self.__generate_common_request(
                action=mt5.TRADE_ACTION_PENDING,
                symbol=symbol,
                _type=mt5.ORDER_TYPE_SELL_LIMIT,
                vol=amount * point,
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
        point = self.__get_point(symbol)
        if point is None:
            return False

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
                    vol=amount * point,
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

    def _market_buy(self, symbol, price, amount, tp=None, sl=None, option_info=None, *args, **kwargs):
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

    def _pending_buy(self, symbol, price, amount, tp=None, sl=None, option_info=None):
        suc = self.__check_params(False, price, tp, sl)
        if suc is False:
            return False, None
        point = self.__get_point(symbol)
        if point is None:
            return False, None

        if self.__ignore_order is False:
            request = self.__generate_common_request(
                action=mt5.TRADE_ACTION_PENDING,
                symbol=symbol,
                _type=mt5.ORDER_TYPE_BUY_LIMIT,
                vol=amount * point,
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
        point = self.__get_point(symbol)
        if point is None:
            return False
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
                    vol=amount * point,
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

    def _check_position(self, position: Position, **kwargs):
        position_id = position.result
        deals = mt5.history_deals_get(position=position_id)
        if len(deals) > 1:
            # if the deals has 2 length, it would have closed result
            for deal in deals:
                entry_type = deal.entry
                if entry_type == mt5.DEAL_ENTRY_IN:
                    continue
                if entry_type == mt5.DEAL_ENTRY_OUT:
                    return deal.price
                logger.warning(f"{entry_type} is not handled correctly in finance_client")
                return deal.price
        elif len(deals) == 0:
            logger.error("the specified position is not stored in mt5 client.")
            return None
        else:
            return None

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
            latest_frame_timestamp = existing_rate_df["time"].iloc[-1]
            current_time = datetime.datetime.now(tz=datetime.UTC)
            delta = current_time - latest_frame_timestamp
            total_seconds = delta.total_seconds()
            if (total_seconds / (60 * 60 * 24 * 7)) >= 1:
                total_seconds = total_seconds * 5 / 7  # remove sat,sun
            interval = int((total_seconds / 60) / frame)

        if interval > 0:
            start_index = 0
            if interval > MAX_LENGTH:
                interval = MAX_LENGTH
                logger.warning("data may have vacant")

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
            rate_df["time"] = pd.to_datetime(rate_df["time"], unit="s", utc=True)

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
            logger.info(f"no new data found for {symbol}")
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
            df_rates["time"] = pd.to_datetime(df_rates["time"], unit="s", utc=True)
            # df_rates = df_rates.set_index('time')

            # MT5 index based on current time. So we need to update the index based on past time.
            if self.back_test and self.auto_index:
                if self.__next_time is None:
                    self.__next_time = df_rates["time"].iloc[1]
                    logger.debug(f"auto index: initialized with {df_rates['time'].iloc[0]}")
                else:
                    current_time = df_rates["time"].iloc[0]
                    if current_time == self.__next_time:
                        logger.debug(f"auto index: index is ongoing on {current_time}")
                        self.__next_time = df_rates["time"].iloc[1]
                    elif current_time > self.__next_time:
                        logger.debug(f"auto index: {current_time} > {self.__next_time}. may time past.")
                        candidate = self.sim_index
                        while current_time != self.__next_time:
                            candidate += 1
                            rates = mt5.copy_rates_from_pos(symbol, frame, candidate, length)
                            df_rates = pd.DataFrame(rates)
                            df_rates["time"] = pd.to_datetime(df_rates["time"], unit="s", utc=True)
                            current_time = df_rates["time"].iloc[0]
                        self.sim_index = candidate

                        logger.debug(f"auto index: fixed to {current_time}")
                        self.__next_time = df_rates["time"].iloc[1]
                        # to avoid infinite loop, don't call oneself
                    else:
                        logger.debug(f"auto index: {current_time} < {self.__next_time} somehow.")
                        candidate = self.sim_index
                        while current_time != self.__next_time:
                            candidate = candidate - 1
                            rates = mt5.copy_rates_from_pos(symbol, frame, candidate, length)
                            df_rates = pd.DataFrame(rates)
                            df_rates["time"] = pd.to_datetime(df_rates["time"], unit="s", utc=True)
                            current_time = df_rates["time"].iloc[0]
                        self.sim_index = candidate

                        logger.debug(f"auto index: fixed to {current_time}")
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

    def _get_ohlc_from_client(self, length: int = None, symbols=slice(None), frame: int = None, columns=None, index=None, grouped_by_symbol=True):
        if symbols is None or symbols == slice(None) or len(symbols) == 0:
            logger.error(f"symbols are mandatory to get_ohlc: {symbols}")
            return pd.DataFrame()
        df_rates = self.__get_ohlc(length, symbols, frame, columns, index, grouped_by_symbol)
        if self.auto_index:
            if len(df_rates) > 0:
                self.sim_index = self.sim_index - 1
            else:
                logger.error(f"auto index is not applied for {length}, {symbols}, {frame}, {columns}, {index}, {grouped_by_symbol}")
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
            logger.warning("pending order is not available on backtest and simulator")
            return True

    def update_position(self, position, tp=None, sl=None):
        if tp is None and sl is None:
            logger.error("update position require tp or sl")
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
