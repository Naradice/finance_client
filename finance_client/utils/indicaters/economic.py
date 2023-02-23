import datetime
import json
import os

import pandas as pd

from ..csvrw import get_economic_file_path, get_economic_state_file_path
from . import countory_code as cc
from . import indicater_code as ic

available_additional_params = tuple(["Untill", "Forecast", "Previous"])  # number of remaining indicies till update happends


def __handle_existing_data(key, provider, freq):
    path = get_economic_file_path(key)
    state_file_path = get_economic_state_file_path()
    existing_data = pd.DataFrame()
    update_required = True
    if freq == "D1":
        check_update_required = lambda current_date, last_date: (current_date - last_date) >= datetime.timedelta(days=1)
    elif freq == "W1":
        check_update_required = (
            lambda current_date, last_date: (current_date - last_date) >= datetime.timedelta(days=7)
            or current_date.weekday < last_date.weekday
        )
    elif freq == "M1":
        check_update_required = (
            lambda current_date, last_date: (current_date >= last_date) and current_date.month != last_date.month
        )
    elif freq == "Y1":
        check_update_required = (
            lambda current_date, last_date: (current_date >= last_date) and current_date.year != last_date.year
        )
    else:
        check_update_required = lambda current_date, last_date: current_date >= last_date

    if os.path.exists(state_file_path):
        with open(state_file_path, mode="r") as fp:
            update_state = json.load(fp)
        if os.path.exists(path):
            existing_data = pd.read_csv(path, nrows=5, header=[0, 1])
        if provider in existing_data.columns:
            # Note: can't specify usecol if we specify multi headers
            existing_data = pd.read_csv(path, header=[0, 1], index_col=[0], parse_dates=True)
            # store other providers
            for __provider in existing_data.columns.levels[0]:
                existing_data = existing_data[__provider]
            existing_data = existing_data.dropna()

            updated_date = None
            if provider in update_state and key in update_state[provider]:
                updated_date_str = update_state[provider][key]
                updated_date = datetime.datetime.fromisoformat(updated_date_str)
                current_date = datetime.datetime.now()
            # check if already tried on same day
            if updated_date is None or current_date.day != updated_date.day:
                # compare now date and latest date of existing data
                if type(existing_data.index) is pd.DatetimeIndex:
                    timezone = existing_data.index.tzinfo
                    update_required = check_update_required(datetime.datetime.now(timezone), existing_data.index[-1])
                else:
                    # TODO: Add logger
                    print("Index is not datetime index unexpectedly")
            else:
                update_required = False
    return existing_data, update_required


def SP500(start=None, end=None, provider="fred", *additional_params):
    path = get_economic_file_path(ic.SP500)
    state_file_path = get_economic_state_file_path()
    from .fred import get_SP500_info

    info = get_SP500_info()
    existing_data, update_required = __handle_existing_data(ic.SP500, provider, info["freq"])
    update_state = {provider: {}}
    if os.path.exists(state_file_path):
        with open(state_file_path, mode="r") as fp:
            update_state = json.load(fp)

    if provider == "fred":
        # if delta have greater than 1 day, read from last date
        if update_required:
            from .fred import get_SP500

            new_data = get_SP500(start, end)
            # concat exsisting data and new data
            if len(existing_data) > 0 and existing_data.index[-1] < new_data.index[-1]:
                data = pd.concat([existing_data, new_data], axis=0)
                del existing_data
                del new_data
                # save new data
                entire_data = pd.concat([data], axis=1, keys=[provider])
                entire_data.to_csv(path)
                update_state[provider][ic.SP500] = datetime.datetime.now().isoformat()
                with open(file=state_file_path, mode="w") as fp:
                    json.dump(update_state, fp)
            else:
                data = existing_data
        else:
            data = existing_data
        return data
    else:
        return None


def PMI(start=None, end=None, country=cc.US, provider="mql5", *additional_params):
    path = get_economic_file_path(ic.PMI)
    state_file_path = get_economic_state_file_path()
    from .mql5 import get_indicater_info

    info = get_indicater_info(country, ic.PMI)
    existing_data, update_required = __handle_existing_data(ic.PMI, provider, info["freq"])
    update_state = {provider: {}}
    if os.path.exists(state_file_path):
        with open(state_file_path, mode="r") as fp:
            update_state = json.load(fp)

    if provider == "mql5":
        # if delta have greater than 1 day, read from last date
        if update_required:
            from .mql5 import get_PMI

            new_data = get_PMI(country, start, end)
            end = existing_data.index[-1]
            new_data = new_data.truncate(before=end)
            # concat exsisting data and new data
            if len(new_data) > 1:
                data = pd.concat([existing_data, new_data], axis=0)
            else:
                data = existing_data
            del existing_data
            del new_data
            # save new data
            entire_data = pd.concat([data], axis=1, keys=[provider])
            entire_data.to_csv(path)
            update_state[provider][ic.PMI] = datetime.datetime.now().isoformat()
            with open(file=state_file_path, mode="w") as fp:
                json.dump(update_state, fp)
        else:
            data = existing_data
        kwargs = {}
        if start is not None:
            kwargs["after"] = end
        if end is not None:
            kwargs["before"] = start
        if len(kwargs) > 1:
            data = data.truncate(**kwargs)
        return data
    else:
        return None
