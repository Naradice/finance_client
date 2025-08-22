from . import frames as Frame
from .client_base import ClientBase
from .coincheck.client import CoinCheckClient
from .csv.client import CSVClient
from .logger import setup_logging
from .tool import AgentTool
from .vantage.client import VantageClient
from .yfinance.client import YahooClient

available_clients = {
    CSVClient.kinds: CSVClient,
    VantageClient.kinds: VantageClient,
    CoinCheckClient.kinds: CoinCheckClient,
    YahooClient.kinds: YahooClient,
}

setup_logging()


def client_to_params(client):
    params = {}
    params["kinds"] = client.kinds
    client_params = client.get_client_params()
    params.update(client_params)
    return params


def load_client(params: dict, credentials: tuple = None):
    kinds = params["kinds"]
    if kinds in available_clients:
        dict_args = params["args"]
        _Client = available_clients[kinds]
        if kinds == VantageClient.kinds:
            if credentials is None:
                raise Exception(f"{kinds} need to specify a credential(s)")
            data_client = _Client(*credentials, **dict_args)
        else:
            data_client = _Client(**dict_args)
        return data_client
    return None
