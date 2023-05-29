import datetime
from time import sleep

import pandas as pd
import yfinance as yf

from .. import frames as Frame
from ..csv.client import CSVClient

try:
    from ..fprocess.fprocess.csvrw import get_file_path, read_csv, write_df_to_csv
except ImportError:
    from ..fprocess.csvrw import get_file_path, read_csv, write_df_to_csv


class YahooClient(CSVClient):
    kinds = "yfinance"

    # max period to call with interval. Confirmed on 2022-09
    max_periods = {
        "1m": datetime.timedelta(days=6),
        "2m": datetime.timedelta(days=59),
        "5m": datetime.timedelta(days=59),
        "15m": datetime.timedelta(days=59),
        "30m": datetime.timedelta(days=59),
        "60m": datetime.timedelta(days=721),
        "90m": datetime.timedelta(days=59),
    }

    max_range = {
        "1m": datetime.timedelta(days=29),
        "5m": datetime.timedelta(days=59),
        "15m": datetime.timedelta(days=59),
        "60m": datetime.timedelta(days=729),
    }
    available_frames = [Frame.MIN1, Frame.MIN5, Frame.MIN15, Frame.MIN30, Frame.H1, Frame.D1, Frame.W1, Frame.MO1]

    def frame_to_str(self, frame: int) -> str:
        availables = {
            Frame.MIN1: "1m",
            Frame.MIN5: "5m",
            Frame.MIN15: "15m",
            Frame.MIN30: "30m",
            Frame.H1: "h1",
            Frame.D1: "1d",
            Frame.W1: "1wk",
            Frame.MO1: "1mo",
        }
        if frame in availables:
            return availables[frame]

    def _create_filename(self, symbol: str):
        file_name = f"yfinance_{symbol}_{Frame.to_str(self.frame)}.csv"
        return file_name

    def _file_name_generator(self, symbol):
        file_name = self._create_filename(symbol)
        file_path = get_file_path(self.kinds, file_name=file_name)
        return file_path

    def get_additional_params(self):
        return {}

    def __init__(
        self,
        symbols=[],
        auto_step_index=False,
        frame: int = Frame.MIN5,
        adjust_close: bool = False,
        start_index=None,
        seed=1017,
        slip_type="random",
        do_render=False,
        idc_processes=[],
        post_process=[],
        budget=1000000,
        logger=None,
    ):
        """Get ohlc rate from yfinance
        Args:
            symbols (str|list): stock symbol.
            auto_step_index (bool, optional): increase step when get_rates is called. Defaults to False.
            frame (int, optional): Frame of ohlc rates. Defaults to Frame.M5.
            post_process (list, optional): process to add indicater for output when get_rate_with_indicater is called. Defaults to [].
            budget (int, optional): budget for the simulation. Defaults to 1000000.
            logger (logger, optional): you can pass your logger if needed. Defaults to None.
            seed (int, optional): random seed. Defaults to 1017.

        Raises:
            ValueError: other than 1, 5, 15, 30, 60, 60*24, 60*24*7, 60*24*7*30 is specified as frame
            ValueError: length of symbol(tuple) isn't 2 when target is FX or CRYPT_CURRENCY
            Exception: length of symol(tuple) is other than 1 when target is
            ValueError: symbol(str) is not from_symvol/to_symbol foramt when target is FX
            TypeError: symbol is neither tuple nor str
        """
        self.OHLC_COLUMNS = ["Open", "High", "Low", "Close"]
        if adjust_close:
            self.OHLC_COLUMNS[3] = "Adj Close"
        self.VOLUME_COLUMN = ["Volume"]
        self.TIME_INDEX_NAME = "Datetime"
        self.__updated_time = {}

        if frame not in self.available_frames:
            raise ValueError(f"{frame} is not supported")
        self.frame = frame
        self.__frame_delta = datetime.timedelta(minutes=frame)
        self.debug = False

        if type(symbols) == list:
            self.symbols = symbols
        elif type(symbols) == str:
            symbols = [symbols]
            self.symbols = symbols
        else:
            # TODO: initialize logger
            raise TypeError("symbol must be str or list.")

        self.__get_rates(self.symbols)
        super().__init__(
            auto_step_index=auto_step_index,
            file_name_generator=self._file_name_generator,
            symbols=self.symbols,
            frame=frame,
            provider="csv",
            out_frame=None,
            columns=self.OHLC_COLUMNS,
            date_column=self.TIME_INDEX_NAME,
            start_index=start_index,
            do_render=do_render,
            seed=seed,
            slip_type=slip_type,
            budget=budget,
            logger=logger,
        )

    def __tz_convert(self, df):
        if "tz_convert" in dir(df.index):
            try:
                df.index = df.index.tz_convert("UTC")
            except Exception:
                try:
                    df.index = df.index.tz_localize("UTC")
                except Exception as e:
                    print(f"failed tz_convert of index: ({type(df.index)}) by {e}")
        else:
            try:
                df.index = pd.DatetimeIndex(df.index)
                df.index = df.index.tz_localize("UTC")
            except Exception as e:
                print(f"failed tz_convert of index: ({type(df.index)}) by {e}")
        return df

    def __download(self, symbol, interval):
        existing_last_date = datetime.datetime.min.date()
        existing_rate_df = read_csv(
            self.kinds, self._create_filename(symbol), [self.TIME_INDEX_NAME], pandas_option={"index_col": self.TIME_INDEX_NAME}
        )
        if existing_rate_df is not None:
            if len(existing_rate_df) > 0:
                if self.frame < Frame.D1:
                    existing_rate_df = self.__tz_convert(existing_rate_df)
                existing_rate_df = existing_rate_df.sort_index()
                # get last date of existing data
                existing_last_date = existing_rate_df.index[-1].date()
            else:
                existing_rate_df = None

        end = datetime.datetime.utcnow().date()
        delta = None
        kwargs = {"group_by": "ticker"}
        isDataRemaining = False
        if interval in self.max_periods:
            delta = self.max_periods[interval]
            start = end - delta
            kwargs["end"] = end
            kwargs["start"] = start
            isDataRemaining = True

        mrange_delta = None
        mrange = datetime.datetime.min.date()
        if interval in self.max_range:
            mrange_delta = self.max_range[interval]
            mrange = end - mrange_delta
        # compare today - max range with last date of exsisting date to decide range
        if existing_last_date > mrange:
            mrange = existing_last_date
            start = mrange
            kwargs["start"] = start
        # compare start and range. If today- range > start, change start to range then change flag true
        if "start" in kwargs:
            if mrange >= start:
                start = mrange
                kwargs["start"] = start
                isDataRemaining = False
            print(f"from {start} to {end} of {symbol}")
        df = yf.download(symbol, interval=interval, **kwargs)
        ticks_df = df.copy()
        if self.frame < Frame.D1:
            ticks_df = self.__tz_convert(ticks_df)
        if len(df) > 0:
            while isDataRemaining:
                end = end - delta
                start = end - delta
                if start < mrange:
                    start = mrange
                    isDataRemaining = False

                print(f"from {start} to {end} of {symbol}")
                sleep(1)  # to avoid a load
                df = yf.download(symbol, interval=interval, group_by="ticker", start=start, end=end)
                if len(df) != 0:
                    if self.frame < Frame.D1:
                        df = self.__tz_convert(df)
                    ticks_df = pd.concat([df, ticks_df])
                    ticks_df = ticks_df[~ticks_df.index.duplicated(keep="first")]
                else:
                    isDataRemaining = False
            ticks_df = ticks_df.sort_index()
        if len(ticks_df) > 0:
            if existing_rate_df is not None:
                ticks_df = pd.concat([existing_rate_df, ticks_df])
                ticks_df = ticks_df[~ticks_df.index.duplicated(keep="first")]
                ticks_df = ticks_df.sort_index()
        return ticks_df

    def __get_rates(self, symbols):
        if len(symbols) > 0:
            interval = self.frame_to_str(self.frame)
            DFS = {}
            for symbol in symbols:
                ticks_df = self.__download(symbol, interval)
                write_df_to_csv(
                    ticks_df, self.kinds, self._file_name_generator(symbol), panda_option={"index_label": self.TIME_INDEX_NAME}
                )
                DFS[symbol] = ticks_df
                self.__updated_time[symbol] = datetime.datetime.now()
            if len(DFS) > 1:
                df = pd.concat(DFS.values(), axis=1, keys=DFS.keys())
            else:
                df = ticks_df
            return df
        return pd.DataFrame()

    def _update_rates(self, symbols=[]):
        _symbols = set(symbols) & set(self.symbols)
        isUpdated = False
        if len(_symbols) > 0:
            symbols_require_update = []
            for _symbol in _symbols:
                current_time = datetime.datetime.now()
                delta = current_time - self.__updated_time[_symbol]
                if delta > self.__frame_delta:
                    symbols_require_update.append(_symbol)
            if len(symbols_require_update) > 0:
                df = self.__get_rates(symbols_require_update)
                if len(df) > 0:
                    isUpdated = True
        return isUpdated

    def _get_ohlc_from_client(self, length, symbols: list, frame: int, index, grouped_by_symbol: bool, columns=None):
        # frame rolling is handled in csv client
        interval = self.frame_to_str(self.frame)
        for symbol in symbols:
            if symbol not in self.symbols:
                ticks_df = self.__download(symbol, interval)
                write_df_to_csv(
                    ticks_df, self.kinds, self._file_name_generator(symbol), panda_option={"index_label": self.TIME_INDEX_NAME}
                )
                self.__updated_time[symbol] = datetime.datetime.now()
        return super()._get_ohlc_from_client(length, symbols, frame, columns, index, grouped_by_symbol)

    def cancel_order(self, order):
        pass
