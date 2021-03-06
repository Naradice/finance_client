import numpy, csv, datetime
import pandas as pd

open_value = 0
high_value = 0
low_value = numpy.Infinity
close_value = 0

updated = False
initialized = False
save_header = True
delta = datetime.timedelta(minutes=5)

output = {}
count = 0

def get_value(tick):
    if tick[3]:#ask exist
        return float(tick[3])
    elif tick[2]:
        return float(tick[2])
    elif tick[4]:
        return float(tick[4])
    else:
        return None

with open("./USDJPY_forex_ticks.csv", 'r') as f:
    reader = csv.reader(f, delimiter='\t')
    for i, tick in enumerate(reader):
        if i > 0:
            current_date = datetime.datetime.strptime((tick[0]+'T'+tick[1]), "%Y.%m.%dT%H:%M:%S.%f")
            if initialized == False:
                frame_date = datetime.datetime(year=current_date.year, month=current_date.month, day=current_date.day, hour=current_date.hour, minute=current_date.minute)
                next_frame = frame_date + delta
                initialized = True
                value = get_value(tick)
                open_value = value
                high_value = value
                low_value = value
                
            if next_frame < current_date:
                close_value = get_value(last_tick)
                if updated:
                    ##save ohlc
                    tick_sr = pd.DataFrame({"Time":[frame_date], "Open": [open_value], "High": [high_value], "Low":[low_value], "Close": [close_value]})
                    tick_sr.to_csv("./USDJPY_forex_min5.csv", mode="a", index=False, header=save_header)
                    save_header = False
                    ##
                value = get_value(tick)
                if value:
                    frame_date = datetime.datetime(year=current_date.year, month=current_date.month, day=current_date.day, hour=current_date.hour, minute=current_date.minute)
                    next_frame = frame_date + delta
                    
                    open_value = value
                    high_value = value
                    low_value = value
                else:
                    continue
                
            value = get_value(tick)
            if value:
                if high_value < value:
                    high_value = value
                if low_value > value:
                    low_value = value
                last_tick = tick
                updated = True