import datetime
import os
import pandas as pd
import json

from ..csvrw import get_economic_file_path, get_economic_state_file_path


def SP500(start=None, end=None, provider="fred"):
    eco_key = "SP500"
    path = get_economic_file_path(eco_key)
    state_file_path = get_economic_state_file_path()
    existing_data_provider = pd.DataFrame()
    existing_data = pd.DataFrame()
    update_required = True
    DFS = {}
    update_state = {provider: {}}
    
    if os.path.exists(state_file_path):
        with open(state_file_path, mode="r") as fp:
            update_state = json.load(fp)
        if os.path.exists(path):
            existing_data = pd.read_csv(path, nrows=5, header=[0,1])
    # check if header exists in the csv
    if provider in existing_data.columns:
        ## Note: can't specify usecol if we specify multi headers
        existing_data = pd.read_csv(path, header=[0,1], index_col=[0], parse_dates=True)
        ## store other providers
        for __provider in existing_data.columns.levels[0]:
            DFS[__provider] = existing_data[__provider]
        existing_data_provider = DFS[provider].dropna()
        del existing_data
        
        updated_date = None
        delta_from_state = datetime.timedelta(days=0)
        if provider in update_state and eco_key in update_state[provider]:
            updated_date_str = update_state[provider][eco_key]
            updated_date = datetime.datetime.fromisoformat(updated_date_str)
            delta_from_state = datetime.datetime.now() - updated_date
        if updated_date is None or delta_from_state >= datetime.timedelta(days=1):
            # compare now date and latest date of existing data
            if type(existing_data_provider.index) is pd.DatetimeIndex:
                timezone = existing_data_provider.index.tzinfo
                delta = datetime.datetime.now(timezone) - existing_data_provider.index[-1]
                freq_delta = datetime.timedelta(days=1)
                if delta >= freq_delta:
                    update_required = True
                else:
                    update_required = False
            else:
                ## TODO: Add logger
                print("Index is not datetime index unexpectedly")
        else:
            update_required = False
    else:
        print("Initialize SP500 data for the provider")
        
    if provider == "fred":
        # if delta have greater than 1 day, read from last date
        if update_required:
            from .fred import get_SP500
            new_data = get_SP500(start, end)
            # concat exsisting data and new data
            data = pd.concat([existing_data_provider, new_data], axis=0)
            del existing_data_provider
            del new_data
            # save new data
            DFS[provider] = data
            entire_data = pd.concat(DFS.values(), axis=1, keys=DFS.keys())
            entire_data.to_csv(path)
            update_state[provider][eco_key] = datetime.datetime.now().isoformat()
            with open(file=state_file_path, mode="w") as fp:
                json.dump(update_state, fp)
        else:
            data = existing_data_provider
        return data
    else:
        return None