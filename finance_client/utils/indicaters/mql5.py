import datetime
import io

import pandas as pd
import requests

from . import countory_code as cc
from . import indicater_code as ic


def __convert_timestamp(__datetime):
    if "tzinfo" in dir(__datetime):
        if __datetime.tzinfo:
            return datetime.datetime(__datetime.year, __datetime.month, __datetime.day)
        else:
            return __datetime
    else:
        try:
            return datetime.datetime(__datetime)
        except Exception:
            return __datetime


def __convert_country_code(common_code):
    conversion_dict = {
        cc.Australia: "australia",
        cc.Brazil: "brazil",
        cc.Canada: "canada",
        cc.China: "china",
        cc.EU: "european-union",
        cc.France: "france",
        cc.Germany: "germany",
        cc.HongKong: "hong-kong",
        cc.India: "india",
        cc.Italy: "italy",
        cc.Japan: "japan",
        cc.Mexico: "mexico",
        cc.NewZealand: "new-zealand",
        cc.Norway: "norway",
        cc.Singapore: "singapore",
        cc.SouthAfrica: "south-africa",
        cc.SouthKorea: "south-korea",
        cc.Spain: "spain",
        cc.Sweden: "sweden",
        cc.Switzerland: "switzerland",
        cc.UnitedKingdom: "united-kingdom",
        cc.UnitedStates: "united-states",
    }

    if common_code in conversion_dict:
        return conversion_dict[common_code]
    return None


def __convert_indicater_code(country, common_code):
    if country == cc.US:
        conversion_dict = {
            ic.PMI: "ism-manufacturing-pmi",
            ic.GDP_qq: "gross-domestic-product-qq",
            ic.Federal_Reserve_System_Interest_Rate_Decision: "fed-interest-rate-decision",
        }


def get_PMI(country: str, start=None, end=None):
    info = get_indicater_info(country, ic.PMI)
    country_code = __convert_country_code(country)
    if info is not None:
        res = requests.get(f"https://www.mql5.com/en/economic-calendar/{country_code}/ism-manufacturing-pmi/export")
        csvStrIO = io.StringIO(res.text)
        df = pd.read_table(csvStrIO, index_col=0, parse_dates=True)
        df.rename(columns={"ActualValue": "Value", "ForecastValue": "Forecast", "PreviousValue": "Previous"}, inplace=True)
        df.sort_index(ascending=True, inplace=True)
        return df
    return None


def get_indicater_info(country, key):
    if key == ic.PMI:
        return __get_PMI_info(country)
    else:
        return None


def __get_PMI_info(country):
    info = {"freq": "M1"}
    if country == cc.US:
        info["min_date"] = datetime.datetime(year=2007, month=3, day=1)
        return info
