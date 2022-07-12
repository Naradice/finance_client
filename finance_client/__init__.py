from finance_client.csv.client import CSVClient, MultiFrameClient
#from finance_client.coincheck import client as CoinCheckClient
import finance_client.frames as Frame
from finance_client.client_base import Client
from finance_client import utils


available_clients = {
    CSVClient.kinds : CSVClient,
    MultiFrameClient.kinds: MultiFrameClient
}

def client_to_params(client):
    params = {}
    params["kinds"] = client.kinds
    params["args"] = client.args
    return params

def load_client(params:dict):
    kinds = params["kinds"]
    if kinds in available_clients:
        args = params["args"]
        Client = available_clients[kinds]
        data_client = Client(*args)
        return data_client
    return None
    