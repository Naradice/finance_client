import datetime
import time

import pandas as pd

import finance_client.frames as Frame
from finance_client.client_base import Client

try:
    from ..fprocess.fprocess.csvrw import write_df_to_csv
except ImportError:
    from ..fprocess.csvrw import write_df_to_csv

from . import apis
from .apis.servicebase import ServiceBase
from .apis.ws import TradeHistory


class CoinCheckClient(Client):
    kinds = "cc"
    provider = "CoinCheck"

    def __store_ticks(self, tick):
        tick_time = tick["time"]
        tick_price = tick["price"]
        tick_volume = tick["volume"]

        if self.frame_ohlcv is None:
            # update frame if frame is changed before this method is called
            if tick_time >= self.next_frame:
                self.current_frame = self.next_frame
                self.__update_next_frame()
            # initialization at start script
            self.frame_ohlcv = pd.DataFrame(
                {"open": tick_price, "high": tick_price, "low": tick_price, "close": tick_price, "volume": tick_volume},
                index=[self.current_frame],
            )
            self.frame_ohlcv.index.name = self.time_column_name

        if tick_time >= self.next_frame:
            # self.frame_ohlcv.close = self.last_tick.price
            # concat frame_data to data
            self.data = pd.concat([self.data, self.frame_ohlcv])
            self.current_frame = self.next_frame
            self.__update_next_frame()
            # initialization on frame
            self.frame_ohlcv = pd.DataFrame(
                {"open": tick_price, "high": tick_price, "low": tick_price, "close": tick_price, "volume": tick_volume},
                index=[self.current_frame],
            )
            self.frame_ohlcv.index.name = self.time_column_name
        else:
            if self.frame_ohlcv.iloc[-1].high < tick_price:
                self.frame_ohlcv.high = tick_price
            if self.frame_ohlcv.iloc[-1].low > tick_price:
                self.frame_ohlcv.low = tick_price

            self.frame_ohlcv.volume += tick_volume
            self.frame_ohlcv.close = tick_price
        # self.last_tick = tick

    def __update_next_frame(self):
        if self.frame_delta is None:
            if self.frame == Frame.MO1:
                year = self.next_frame.year
                if self.next_frame.month == 12:
                    year += 1
                    month = 1
                else:
                    month = self.next_frame.month + 1
                self.next_frame = datetime.datetime(year=year, month=month, day=1, tzinfo=datetime.timezone.utc)
            else:
                err_txt = f"frame_delta is not defined somehow for {self.frame}"
                self.logger.error(err_txt)
                raise Exception(err_txt)
        else:
            self.next_frame = self.next_frame + self.frame_delta

    def __init__(
        self,
        ACCESS_ID=None,
        ACCESS_SECRET=None,
        initialized_with=None,
        return_intermidiate_data=False,
        simulation=False,
        budget=1000000,
        frame: int = 30,
        observation_length=None,
        do_render=False,
        enable_trade_log=False,
        logger=None,
    ):
        """CoinCheck Client. Create OHLCV data from tick data obtained from websocket.
        Create order with API. Need to specify the credentials
        Currentry BTC/JPY is only supported

        Args:
            budget (int, optional): Defaults to 1000000.
            indicater_processes (list, optional): Indicaters made by finance_client.fprocess.idcprocess. Defaults to [].
            post_processes (list, optional): _description_. Defaults to [].
            frame (int, optional): Frame minutes. finance_client.frames is also available. Defaults to 30.
            initialized_with (Client | None, optional): CoinCheck API don't provide history data. You can specify other Client to initialized ohlc with history data of the client. Defaults to None.
            do_render (bool, optional): If true, plot ohlc data by matplotlib. Defaults to False.
            logger (_type_, optional): you can pass your logger. Defaults to None and use default logger.
        """
        super().__init__(
            budget,
            "CoinCheck",
            [],
            frame=frame,
            observation_length=observation_length,
            do_render=do_render,
            enable_trade_log=enable_trade_log,
            logger_name="ccheck",
            logger=logger,
        )
        ServiceBase(ACCESS_ID=ACCESS_ID, ACCESS_SECRET=ACCESS_SECRET)

        self.ticker = apis.Ticker()

        # Initialize required params
        current_time = datetime.datetime.now(tz=datetime.timezone.utc)
        if self.frame < Frame.H1:
            additional_mins = current_time.minute % self.frame
            delta = datetime.timedelta(
                minutes=additional_mins, seconds=current_time.second, microseconds=current_time.microsecond
            )
            self.current_frame = current_time - delta
            self.frame_delta = datetime.timedelta(minutes=self.frame)
        elif self.frame < Frame.D1:
            additional_hours = current_time.hour % (self.frame / 60)
            delta = datetime.timedelta(
                hours=additional_hours,
                minutes=current_time.minute,
                seconds=current_time.second,
                microseconds=current_time.microsecond,
            )
            self.current_frame = current_time - delta
            self.frame_delta = datetime.timedelta(hours=self.frame / 60)
        elif self.frame < Frame.MO1:
            additional_days = current_time.day % (self.frame / (60 * 24))
            delta = datetime.timedelta(
                days=additional_days,
                hours=current_time.hour,
                minutes=current_time.minute,
                seconds=current_time.second,
                microseconds=current_time.microsecond,
            )
            self.current_frame = current_time - delta
            self.frame_delta = datetime.timedelta(days=self.frame / (60 * 24))
        else:  # Frame.MO1
            self.current_frame = datetime.datetime(
                year=current_time.year, month=current_time.month, day=1, tzinfo=datetime.timezone.utc
            )
            self.frame_delta = None
        self.__return_intermidiate_data = return_intermidiate_data
        self.next_frame = self.current_frame
        self.__update_next_frame()
        # self.last_tick = None
        self.frame_ohlcv = None
        self.data = pd.DataFrame({})
        self.time_column_name = "time"
        self.simulation = simulation

        if initialized_with is None:
            self.logger.info(
                "get_rates returns shortened or filled with 0 data without initialization as Coincheck don't provide historical data API."
            )
        elif isinstance(initialized_with, Client):
            if type(initialized_with.symbols) is list:
                self.symbols = initialized_with.symbols
            if initialized_with.frame != frame:
                raise ValueError("initialize client and frame should be same.")
            self.data = initialized_with.get_ohlc()
            # update columns name with cc policy
            ohlc_dict = initialized_with.get_ohlc_columns()
            new_column_dict = {
                ohlc_dict["Open"]: "open",
                ohlc_dict["High"]: "high",
                ohlc_dict["Low"]: "low",
                ohlc_dict["Close"]: "close",
            }
            if "Volume" in ohlc_dict:
                new_column_dict.update({ohlc_dict["Volume"]: "volume"})
            self.data.rename(columns=new_column_dict)

            if "Time" in ohlc_dict:
                self.data = self.data.set_index(ohlc_dict["Time"])
                self.data.index.name = self.time_column_name

                # check type of time column. If str, convert it to pd.datetime
                if type(self.data.iloc[-1].index) is str:
                    try:
                        self.data.index = pd.to_datetime(self.data.index, utc=True)
                    except Exception:
                        self.logger.error("can't convert str to datetime")

            # convert timezone
            if self.data.index[-1].tzinfo != "UTC":
                # convert them to UTC
                try:
                    self.data.index = self.data.index.tz_convert("UTC")
                except Exception:
                    self.logger.info("can't convert timezone on initialization")

            # fit initialization client to current time frame
            last_frame_time = None
            try:
                last_frame_time = self.data.index[-1].to_pydatetime()
            except Exception:
                self.logger.error("can't find time column or index. ignore time.")
            if last_frame_time is not None:
                # If last_frame_time equal to currenet_frame, set it as frame_ohlc. Then remove last ohlc
                if last_frame_time == self.current_frame:
                    self.logger.info(
                        "initialize client returned current time frame data. try to merge it with tick data obtained from coincheck."
                    )
                    self.frame_ohlcv = self.data.iloc[-1:]  # store last tick df
                    self.data = self.data.drop([self.data.index[-1]])
                # If last_frame_time is greater than current_time, assuming server time and client time are mismatching. so convert(reduce) server datetime
                elif last_frame_time > self.current_frame:
                    self.logger.info(
                        f"initialize client returns {last_frame_time} as last frame. But current frame based on device time is {self.current_frame}. Try to fit server time to device time."
                    )
                    delta = last_frame_time - self.current_frame
                    self.data.index = (
                        self.data.index - delta - datetime.timedelta(minutes=self.frame)
                    )  # Not sure last tick is actually on time. So put it on 1 frame before
                    if self.data.index[-1].to_pydatetime() == self.current_frame:
                        self.frame_ohlcv = self.data.iloc[-1:]
                        self.data = self.data.drop([self.data.index[-1]])
                    else:
                        self.logger.error(
                            f"Unexpectedly time isn't fitted. last frame time is {self.data.index[-1]} and current time frame is {self.current_frame}"
                        )
                        exit()  # exit to prevent caliculating indicaters based on bad data
                # When last_frame_time is less than current_Time, if difference is just a frame ignore it otherwise assuming server time and client time are mismatching. so convert(reduce) server datetime
                else:
                    delta = self.current_frame - last_frame_time
                    diff_mins = int(delta.total_seconds() / 60)
                    if diff_mins != self.frame:
                        self.logger.warn(
                            f"Time difference between initialize client and coincheck server is {diff_mins} mins. try to fit it to local time. If you feel this is strange, please stop this script."
                        )
                        fitting_time = delta - datetime.timedelta(
                            minutes=self.frame
                        )  # Not sure last tick is actually on time. So put it on 1 frame before
                        self.data.index = self.data.index + fitting_time
                    else:
                        self.logger.info(
                            f"initialize client retuened {last_frame_time} for latest frame data. It is working as expected."
                        )
                if self.frame_ohlcv is not None and "volume" not in self.frame_ohlcv.columns:
                    self.frame_ohlcv["volume"] = 0

        th = TradeHistory()
        th.subscribe(on_tick=self.__store_ticks)

    # TODO: use cctx

    def get_additional_params(self):
        return {}

    def _get_ohlc_from_client(
        self, length: int = None, symbols: list = [], frame: int = None, index=None, grouped_by_symbol=True
    ):
        try:
            write_df_to_csv(
                self.data, self.provider, f"CC_BTC_{self.frame}.csv"
            )
        except Exception as e:
            self.logger.error(e)
        if length is None:
            if index is None:
                if self.__return_intermidiate_data:
                    return pd.concat([self.data, self.frame_ohlcv])
                else:
                    return self.data.copy()
            else:
                return self.data.iloc[:index]
        elif length > 0:
            if index is None:
                if self.__return_intermidiate_data:
                    return pd.concat([self.data, self.frame_ohlcv]).iloc[-length:]
                else:
                    return self.data.iloc[-length:]
            else:
                return self.data.iloc[-length:index]
        else:
            self.logger.error("intervl should be greater than 0.")

    def get_future_rates(self, interval) -> pd.DataFrame:
        self.logger.info("This is not available on this client type.")

    def get_current_ask(self, symbols=[]) -> float:
        tick = self.ticker.get()
        return tick["ask"]

    def get_current_bid(self, symbols=[]) -> float:
        tick = self.ticker.get()
        return tick["bid"]

    def _market_buy(self, symbol, ask_rate, amount, tp, sl, option_info):
        # buy_amount = ask_rate * amount
        # response = apis.create_market_buy_order(amount=buy_amount, stop_loss_rate=sl)
        # api don't return amount, so use pending order instead.
        # TODO: use marketbuy and check amount from trade_history
        if self.simulation:
            return datetime.datetime.now().timestamp()
        else:
            if ask_rate is None:
                ask_rate = self.get_current_ask()
            response = apis.create_pending_buy_order(rate=ask_rate, amount=amount, stop_loss_rate=sl)
            if response["success"]:
                return True, response["id"]
            else:
                err_msg = f"error happened: {response}"
                print(err_msg)
                return False, err_msg

    def _market_sell(self, symbol, bid_rate, amount, tp, sl, option_info):
        err_meg = "sell is not allowed."
        self.logger.error(err_meg)
        return False, err_meg

    def _buy_for_settlement(self, symbol, ask_rate, amount, option_info, result):
        self.logger.error("sell is not allowed, so buy settlement is not available.")

    def _sell_for_settlment(self, symbol, bid_rate, amount, option_info, result_id):
        if self.simulation:
            pass
        else:
            if bid_rate is not None:
                print("bid_rate is ignored for the settlement")
            cancel_result = apis.cancel(result_id)
            if cancel_result["success"]:
                print("had failed to buy")
                return cancel_result
            else:
                result = apis.create_market_sell_order(amount=amount)
                if result["success"]:
                    print(result)
                    return result
                else:
                    time.sleep(10)
                    return self._sell_for_settlment(symbol, bid_rate, amount, option_info, result_id)

    def get_params(self) -> dict:
        return {}

    # defined by the actual client for dataset or env
    def close_client(self):
        pass

    def get_next_tick(self, frame=5):
        self.logger.error("get_next_tick is not available for now")

    def reset(self, mode=None):
        pass

    @property
    def max(self):
        print("Need to implement max")
        return 1

    @property
    def min(self):
        print("Need to implement min")
        return -1

    ###
