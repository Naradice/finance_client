import pandas as pd

from ..frames import to_panda_freq
from .indicaters import indicater_code as ic
from .indicaters.economic import *

__indicaters = {ic.SP500: SP500, ic.PMI: PMI}


def get_indicater(keys, start, end, frame=None, *params):
    if type(keys) is str:
        keys = [keys]
    freq = None
    if frame is None:
        freq = None
    else:
        if type(frame) is int:
            freq = to_panda_freq(frame)
        else:
            freq = frame

    additional_params = []
    for param in params:
        if param in available_additional_params:
            additional_params.append(param)
        else:
            print(f"Unkown param {param} is specified.")
    additional_params = tuple(additional_params)

    e_indicaters = []
    for key in keys:
        if key in __indicaters:
            func = __indicaters[key]
            indicater = func(start=start, end=end)
            e_indicaters.append(indicater)
    eco_idc_df = pd.concat(e_indicaters, axis=1)
    if freq is not None:
        eco_idc_df = eco_idc_df.groupby(pd.Grouper(level=0, freq=freq)).first()
        eco_idc_df = eco_idc_df.fillna(method="ffill")
    return eco_idc_df
