import datetime
import logging

import pandas as pd

from .. import frames as Frame
from ..csv.client import CSVClient
from . import target

try:
    from ..fprocess.fprocess.convert import str_to_currencies
    from ..fprocess.fprocess.csvrw import get_file_path, read_csv, write_df_to_csv
except ImportError:
    from ..fprocess.convert import str_to_currencies
    from ..fprocess.csvrw import get_file_path, read_csv, write_df_to_csv

logger = logging.getLogger(__name__)


class VantageClient(CSVClient):
    OHLC_COLUMNS = ["open", "high", "low", "close"]
    VOLUME_COLUMN = ["volume"]
    TIME_INDEX_NAME = "time"

    kinds = "vantage"

    def get_additional_params(self):
        logger.warning("parameters are not saved for vantage as credentials are included.")
        return {}

    def _generate_file_name(self, symbol):
        file_name = f"vantage_{symbol}_{self.function_name}_{Frame.to_str(self.frame)}.csv"
        file_path = get_file_path(self.kinds, file_name)
        return file_path

    def __init__(
        self,
        api_key,
        symbols,
        auto_step_index=False,
        frame: int = Frame.MIN5,
        finance_target=target.FX,
        start_index=None,
        seed=1017,
        slip_type="random",
        do_render=False,
        enable_trade_log=False,
        budget=1000000,
    ):
        """Get ohlc rate from alpha vantage api. No online download.

        Args:
            api_key (str): apikey of alpha vantage
            symbols (list of str): list of symbol for fx or stock.
            auto_step_index (bool, optional): increase step when get_rates is called. Defaults to False.
            finance_target (Target, optional): Target of finance market. Defaults to Target.FX.
            frame (int, optional): Frame of ohlc rates. Defaults to Frame.M5.
            post_process (list, optional): process to add indicater for output when get_rate_with_indicater is called. Defaults to [].
            budget (int, optional): budget for the simulation. Defaults to 1000000.
            seed (int, optional): random seed. Defaults to 1017.

        Raises:
            ValueError: other than 1, 5, 15, 30, 60, 60*24, 60*24*7, 60*24*7*30 is specified as frame
            TypeError: symbols is neither list nor str
        """
        if frame not in target.available_frame:
            raise ValueError(f"{frame} is not supported")
        self.frame = frame
        self.__frame_delta = datetime.timedelta(minutes=frame)
        if type(symbols) == str:
            symbols = [symbols]
        if type(symbols) != list:
            try:
                symbols = list(symbols)
            except Exception:
                raise TypeError(f"{type(symbols)} is not supported as symbols")
        self.symbols = symbols

        if finance_target == target.FX or finance_target == target.CRYPTO_CURRENCY:
            self.__currency_trade = True
        else:
            self.__currency_trade = False

        self.client = finance_target.create_client(api_key, logger)
        self.function_name = finance_target.to_function_name(finance_target, frame)
        self.__updated_times = {}

        self.__get_rates(self.symbols)
        super().__init__(
            auto_step_index=auto_step_index,
            file_name_generator=self._generate_file_name,
            symbols=self.symbols,
            frame=frame,
            provider="Vantage",
            out_frame=None,
            columns=self.OHLC_COLUMNS,
            date_column=self.TIME_INDEX_NAME,
            start_index=start_index,
            do_render=do_render,
            seed=seed,
            slip_type=slip_type,
            enable_trade_log=enable_trade_log,
            budget=budget,
        )

    def __convert_response_to_df(self, data_json: dict):
        # handle meta data and series column
        columns = list(data_json.keys())
        if len(columns) > 1:
            series_column = columns[1]
            time_zone = "UTC"
            meta_data_column = "Meta Data"
            if meta_data_column in data_json:
                meta_data = data_json[meta_data_column]
                for key, value in meta_data.items():
                    if "Time Zone" in key:
                        time_zone = value
                columns.remove(meta_data_column)
                if len(columns) != 1:
                    logger.warn(f"data key has multiple candidates unexpectedly: {columns}")
                series_column = columns[0]
            else:
                logger.warn("couldn't get meta data info. try to parse the response with UTC")
        else:
            if "Information" in columns:
                raise Exception(f"You might call premium API: {data_json['Information']}")
            raise Exception(f"Unexpected response from vantage api: {data_json.values}")

        # convert dict to data frame
        data_df = pd.DataFrame(data_json[series_column]).T
        data_df.index = pd.to_datetime(data_df.index)
        data_df = data_df.sort_index(ascending=True)

        # apply timezone info obtained from meta_data
        if data_df.index.tz is None:
            try:
                data_df.index = data_df.index.tz_localize(time_zone)
            except Exception as e:
                print(f"can't localize the datetime: {e}")
                print("store datetime without timezone")

        # filter ohlc and volume
        desired_columns = [*self.OHLC_COLUMNS, *self.VOLUME_COLUMN]
        column_index = []
        d_index = 0
        for d_column in desired_columns:
            index = 0
            for column in data_df.columns:
                if d_column in str(column).lower():  # assume 1. open
                    column_index.append((d_index, index))
                    break
                index += 1
            d_index += 1

        column_names = []
        target_columns = []
        for index in column_index:
            column_names.append(desired_columns[index[0]])
            target_columns.append(data_df.columns[index[1]])

        data_df = data_df[target_columns]
        data_df.columns = column_names
        return data_df

    def __get_currency_rates(self, from_symbol, to_symbol, frame, size):
        if frame <= Frame.H1:
            data = self.client.get_interday_rates(from_symbol, to_symbol, interval=frame, output_size=size)
        elif frame == Frame.D1:
            data = self.client.get_daily_rates(from_symbol, to_symbol, output_size=size)
        elif frame == Frame.W1:
            data = self.client.get_weekly_rates(from_symbol, to_symbol, output_size=size)
        elif frame == Frame.MO1:
            data = self.client.get_monthly_rates(from_symbol, to_symbol, output_size=size)
        else:
            raise ValueError(f"{Frame.to_str(frame)} is not supported")

        data_df = self.__convert_response_to_df(data)
        return data_df

    def __get_stock_rates(self, symbol, frame, size):
        if frame <= Frame.H1:
            data = self.client.get_interday_rates(symbol, interval=frame, output_size=size)
        elif frame == Frame.D1:
            data = self.client.get_daily_rates(symbol, output_size=size)
        elif frame == Frame.W1:
            data = self.client.get_weekly_rates(symbol, output_size=size)
        elif frame == Frame.MO1:
            data = self.client.get_monthly_rates(symbol, output_size=size)
        else:
            raise ValueError(f"{Frame.to_str(frame)} is not supported")

        data_df = self.__convert_response_to_df(data)
        return data_df

    def __download(self, symbol):
        file_name = self._generate_file_name(symbol)
        existing_rate_df = read_csv(self.kinds, file_name, [self.TIME_INDEX_NAME], pandas_option={"index_col": self.TIME_INDEX_NAME})
        MAX_LENGTH = 950  # not accurate

        if existing_rate_df is None:
            interval = MAX_LENGTH
            size = "full"
        else:
            delta = datetime.datetime.now(tz=datetime.timezone.utc) - existing_rate_df.index[-1]
            total_seconds = delta.total_seconds()
            if (total_seconds / (60 * 60 * 24 * 7)) >= 1:
                total_seconds = total_seconds * self.client.work_day_in_week / 7  # remove sat,sun
            interval = int((total_seconds / 60) / self.frame)
            if interval <= MAX_LENGTH:
                size = "compact"  # to be safe
            else:
                size = "full"
                if interval > MAX_LENGTH:
                    print("may be some time will be missing")
        if self.__currency_trade:
            try:
                from_symbol, to_symbol = str_to_currencies(symbol)
            except Exception:
                print(f"Faile to parse {symbol} to from/to currency.")
                return pd.DataFrame()
            new_data_df = self.__get_currency_rates(from_symbol, to_symbol, self.frame, size)
        else:
            new_data_df = self.__get_stock_rates(symbol, self.frame, size)

        if existing_rate_df is not None:
            new_data_df = pd.concat([existing_rate_df, new_data_df])
            new_data_df = new_data_df[~new_data_df.index.duplicated(keep="first")]
            new_data_df = new_data_df.sort_index()
        write_df_to_csv(new_data_df, self.kinds, file_name, panda_option={"index_label": self.TIME_INDEX_NAME})
        return new_data_df

    def __get_rates(self, symbols):
        if len(symbols) > 0:
            DFS = {}
            for symbol in symbols:
                df = self.__download(symbol)
                DFS[symbol] = df
                self.__updated_times[symbol] = datetime.datetime.now()
            df = pd.concat(DFS.values(), axis=1, keys=DFS.keys())
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

    def cancel_order(self, order):
        pass
