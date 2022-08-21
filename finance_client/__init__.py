from finance_client.coincheck.client import CoinCheckClient
import finance_client.frames as Frame
from finance_client.client_base import Client
from finance_client.csv.client import CSVClient, MultiFrameClient
from finance_client.mt5.client import MT5Client
from finance_client.vantage.client import VantageClient


available_clients = {
    CSVClient.kinds : CSVClient,
    MultiFrameClient.kinds: MultiFrameClient,
    MT5Client.kinds: MT5Client,
    VantageClient.kinds: VantageClient,
    CoinCheckClient.kinds: CoinCheckClient
}

def client_to_params(client):
    params = {}
    params["kinds"] = client.kinds
    client_params = client.get_client_params()
    params.update(client_params)
    return params

def load_client(params:dict, credentials: tuple = None):
    kinds = params["kinds"]
    if kinds in available_clients:
        dict_args = params["args"]
        Client = available_clients[kinds]
        if kinds == MT5Client.kinds or kinds == VantageClient.kinds:
            if credentials is None:
                raise Exception(f"{kinds} need to specify a credential(s)")
            data_client = Client(*credentials, **dict_args)
        else:
            data_client = Client(**dict_args)
        return data_client
    return None
    