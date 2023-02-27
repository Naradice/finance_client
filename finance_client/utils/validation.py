import pandas as pd

WEEKDAY_STR = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}


def get_weekly_count(df: pd.DataFrame):
    """count how many days dataframe has
        If a day has multiple data, it is counted as 1
    Args:
        df (pd.DataFrame): Timeseries data. Index should be DatetimeIndex

    Returns:
        pd.DataFrame or None: Index has Weekdays: 0:Mon, 1:Thu, 2: Wed, 3:Thu, 4:Fri, 5:Sat, 6:Sun
    """
    if type(df.index) == pd.DatetimeIndex:
        sample_column = df.columns[0]
        daily_df = df[sample_column].groupby(pd.Grouper(level=0, freq="1D")).first()
        daily_df.dropna(inplace=True)
        weekday_df = daily_df.groupby(daily_df.index.weekday).count()
        weekday_df.name = "day_count"
        return weekday_df
    return None


def get_open_close_weekday(weekdays_index):
    """If Dataframe of get_weekly_count has missing weekday, market typically closed

    Args:
        weekdays_index (Iterale): Typically DatatimeIndex of DataFrame of get_weekly_count

    Returns:
        Tuple[int, int]: weekdays of open, close
    """
    if isinstance(weekdays_index, pd.DataFrame):
        if len(weekdays_index) <= 7:
            weekdays_index = weekdays_index.index
        else:
            if type(weekdays_index) == pd.DataFrame:
                weekdays_index = get_weekly_count(weekdays_index).index
    if len(weekdays_index) == 0:
        return None, None
    missing_weekdays = list(set(WEEKDAY_STR.keys()) - set(weekdays_index))
    if len(missing_weekdays) == 0:
        # no market close date typically.
        return None, None
    elif len(missing_weekdays) == 1:
        holiday = missing_weekdays[0] + 7
        close_weekday = (holiday - 1) % 7
        open_weekday = (holiday + 1) % 7
    else:
        if missing_weekdays[0] == 0:
            open_weekday = missing_weekdays[-2] + 1
            close_weekday = missing_weekdays[-1] - 1
        else:
            close_weekday = missing_weekdays[0] - 1
            open_weekday = (missing_weekdays[-1] + 1) % 7
    return open_weekday, close_weekday


def frequence_count(df: pd.DataFrame, datetime_column: str = None):
    """count difference of DatetimeIndex

    Args:
        df (pd.DataFrame): Timeseries data. Index should be DatetimeIndex
        datetime_column (str, optional): datetime column name. use column values instead of index. Defaults to None.

    Returns:
        pd.Series: timedelta index and count of the delta
    """
    if datetime_column is None:
        if type(df.index) == pd.DatetimeIndex:
            time_indices = df.index
        else:
            if type(df) == pd.Series:
                time_indices = df
            else:
                column = df.columns[0]
                time_indices = df[column]
    else:
        time_indices = pd.DatetimeIndex(df[datetime_column])
    delta_index = time_indices[1:] - time_indices[:-1]
    return delta_index.value_counts()


def get_most_frequent_delta(df: pd.DataFrame, datetime_column: str = None):
    """get mode value of difference of DatetimeIndex

    Args:
        df (pd.DataFrame): Timeseries data. Index should be DatetimeIndex
        datetime_column (str, optional): datetime column name. use column values instead of index. Defaults to None.

    Returns:
        int: minutes of timedelta
    """
    freq = int(frequence_count(df, datetime_column).idxmax().total_seconds() / 60)
    return freq


def get_start_end_time(df: pd.DataFrame, datetime_column: str = None, dropna=True):
    """get typical start time and end time of a weekday in timeseries data

    Args:
        df (pd.DataFrame): Timeseries data. Index should be DatetimeIndex. Frequency should be less than 1 day
        datetime_column (str, optional): datetime column name. use column values instead of index. Defaults to None.

    Returns:
        Tuple[pd.Series, pd.Series]: each value represents hours in a weekday
    """

    if type(df.index) == pd.DatetimeIndex:
        datetime_column = df.index.name
        if datetime_column is None:
            datetime_column = 0
        datetime_df = df.index.to_frame()
    elif datetime_column is not None:
        datetime_df = df[datetime_column]
        datetime_df.index = datetime_df
    else:
        return None, None

    START_AT_KEY = "start_at"
    START_AT_CNT_KEY = "start_at_count"
    END_AT_KEY = "end_at"
    END_AT_CNT_KEY = "end_at_count"

    start_times = {START_AT_KEY: [], START_AT_CNT_KEY: []}
    end_times = {END_AT_KEY: [], END_AT_CNT_KEY: []}

    index = WEEKDAY_STR.keys()
    for weekday in index:
        daily_open_df = datetime_df.groupby(pd.Grouper(level=0, freq="1D")).first()
        daily_close_df = datetime_df.groupby(pd.Grouper(level=0, freq="1D")).last()
        open_datetimes = daily_open_df[datetime_column][daily_open_df.index.weekday == weekday]
        close_datetimes = daily_close_df[datetime_column][daily_close_df.index.weekday == weekday]

        open_datetimes = pd.DatetimeIndex(open_datetimes)
        open_datetimes = open_datetimes.hour + open_datetimes.minute / 60
        open_datetime_counts = open_datetimes.value_counts()
        if len(open_datetime_counts) > 0:
            argmax_id = open_datetime_counts.idxmax()
            count = open_datetime_counts[argmax_id]
            start_times[START_AT_KEY].append(argmax_id)
            start_times[START_AT_CNT_KEY].append(count)
        else:
            start_times[START_AT_KEY].append(None)
            start_times[START_AT_CNT_KEY].append(None)

        close_datetimes = pd.DatetimeIndex(close_datetimes)
        close_datetimes = close_datetimes.hour + close_datetimes.minute / 60
        close_datetime_counts = close_datetimes.value_counts()
        if len(close_datetime_counts) > 0:
            argmax_id = close_datetime_counts.idxmax()
            count = close_datetime_counts[argmax_id]
            end_times[END_AT_KEY].append(argmax_id)
            end_times[END_AT_CNT_KEY].append(count)
        else:
            end_times[END_AT_KEY].append(None)
            end_times[END_AT_CNT_KEY].append(None)

    start = pd.DataFrame.from_dict(start_times)
    start.index = index
    end = pd.DataFrame.from_dict(end_times)
    end.index = index
    if dropna:
        start.dropna(inplace=True)
        end.dropna(inplace=True)
    return start, end


def weekly_summary(df: pd.DataFrame):
    """get typical start time and end time of a weekday in timeseries data

    Args:
        df (pd.DataFrame): Timeseries data. Index should be DatetimeIndex. Frequency should be less than 1 day
        datetime_column (str, optional): datetime column name. use column values instead of index. Defaults to None.

    Returns:
        pd.DataFrame: data count based on weekday, open/close weekday, start/end hours in a day
    """
    if type(df.index) == pd.DatetimeIndex:
        # Aggregate based on weekday
        weekday_df = get_weekly_count(df)
        if len(weekday_df) < 7:
            week_indices = weekday_df.index
            # when market open/close typically in a day
            open_weekday, close_weekday = get_open_close_weekday(week_indices)
            if open_weekday is not None:
                is_open = pd.Series(week_indices == open_weekday, name="open_date")
                is_close = pd.Series(week_indices == close_weekday, name="close_date")
                weekday_df = pd.concat([weekday_df, is_open, is_close], axis=1)
            # summary using hour data
            freq = get_most_frequent_delta(df)
            if freq < 24 * 60:
                start, end = get_start_end_time(df)
                if start is not None:
                    weekday_df = pd.concat([weekday_df, start, end], axis=1)

        new_index = [WEEKDAY_STR[weekday] for weekday in weekday_df.index]
        weekday_df.index = new_index
        return weekday_df
