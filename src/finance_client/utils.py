import logging

import pandas as pd

import finance_client.frames as Frame
from finance_client.frames import to_freq, to_freq_str, to_str

logger = logging.getLogger(__name__)

def roll_ohlc_data(
        data: pd.DataFrame,
        open_column,
        high_column,
        low_column,
        close_column,
        volume_column=None,
        spread_column=None,
        to_frame: int = None,
        to_freq: str = None,
        grouped_by_symbol: bool = True,
    ) -> pd.DataFrame:
        """Roll time series data by specified frequency.

        Args:
            data (pd.DataFrame): time series data. columns should be same as what get_ohlc_columns returns
            open_column: column name for open price
            high_column: column name for high price
            low_column: column name for low price
            close_column: column name for close price
            volume_column: column name for volume
            spread_column: column name for spread
            to_frame (int, optional): target frame minutes to roll data. If None, to_freq should be specified.
            to_freq (str, optional): target freq value defined in pandas. Defaults to None.
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

        if grouped_by_symbol and isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.swaplevel(0, 1)

        rolled_data_dict = {}
        if open_column:
            rolled_opn = data[open_column].groupby(pd.Grouper(level=0, freq=freq)).first()
            rolled_data_dict[open_column] = rolled_opn
        if high_column:
            rolled_high = data[high_column].groupby(pd.Grouper(level=0, freq=freq)).max()
            rolled_data_dict[high_column] = rolled_high
        if low_column:
            rolled_low = data[low_column].groupby(pd.Grouper(level=0, freq=freq)).min()
            rolled_data_dict[low_column] = rolled_low
        if close_column:
            rolled_cls = data[close_column].groupby(pd.Grouper(level=0, freq=freq)).last()
            rolled_data_dict[close_column] = rolled_cls
        if volume_column:
            rolled_vlm = data[volume_column].groupby(pd.Grouper(level=0, freq=freq)).sum()
            rolled_data_dict[volume_column] = rolled_vlm
        if spread_column:
            rolled_spd = data[spread_column].groupby(pd.Grouper(level=0, freq=freq)).last()
            rolled_data_dict[spread_column] = rolled_spd

        if len(rolled_data_dict) > 0:
            rolled_df = pd.concat(rolled_data_dict.values(), axis=1, keys=rolled_data_dict.keys())
            if grouped_by_symbol and isinstance(rolled_df.columns, pd.MultiIndex):
                data.columns = data.columns.swaplevel(0, 1)
                rolled_df.columns = rolled_df.columns.swaplevel(0, 1)
                rolled_df.sort_index(level=0, axis=1, inplace=True)
            return rolled_df
        else:
            logger.warning(f"no data found to roll. currently {rolled_data_dict}")
            return pd.DataFrame()