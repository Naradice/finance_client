import datetime

import pandas_datareader as pdr


def __convert_timestamp(__datetime):
    if 'tzinfo' in dir(__datetime):
        if __datetime.tzinfo:
            return datetime.datetime(__datetime.year, __datetime.month, __datetime.day) 
        else:
            return __datetime
    else:
        try:
            return datetime.datetime(__datetime)
        except Exception:
            return __datetime

def get_SP500(start=None, end=None):
    kwargs = {}
    info = get_SP500_info()
    if start is None:
        start = info["min_date"]
        kwargs["start"] = start
    else:
        start = __convert_timestamp(start)
        #if start < info["min_date"]:
        #    print(f"start {start} is less than min date.")
        kwargs["start"] = start
        
    if end is not None:
        end = __convert_timestamp(end)
        if end < info["min_date"]:
            return None
        if start > end:
            end = datetime.datetime().now()
        kwargs["end"] = end
    
    return pdr.get_data_fred('SP500', **kwargs)

def get_SP500_info():
    info = {
        "min_date":datetime.datetime(year=2012, month=12, day=31),
        "freq": "1D"
    }
    return info