from finance_client.csv.client import CSVClient, MultiFrameClient
import finance_client.coincheck
from finance_client.frames import Frame
from finance_client.client_base import Client


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
    