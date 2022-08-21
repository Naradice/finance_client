from finance_client.coincheck.apis.servicebase import ServiceBase
import os

baseUrl = '/api/exchange/orders'
service = ServiceBase()

def create(body={}):
    response = service.request(ServiceBase.METHOD_POST, baseUrl, _body=body)
    return service.parse_str_to_dict(response)

def __create_pending_order(order_type:str, rate:int, amount:float, stop_loss_rate:int = None, pair:str="btc_jpy"):
    body = {
        "pair": pair,
        "order_type": order_type,
        "rate": rate,
        "amount": amount
    }
    if stop_loss_rate is not None:
        body["stop_loss_rate"] = stop_loss_rate
    return create(body=body)

def create_pending_buy_order(rate:int, amount:float, stop_loss_rate:int = None, pair:str="btc_jpy"):
    return __create_pending_order(order_type="buy", rate=rate, amount=amount, stop_loss_rate=stop_loss_rate, pair=pair)

def create_pending_sell_order(rate:int, amount:float, pair:str="btc_jpy"):
    return __create_pending_order(order_type="sell", rate=rate, amount=amount, stop_loss_rate=None, pair=pair)

def create_market_buy_order(amount:float, stop_loss_rate:int = None, pair:str="btc_jpy"):
    body = {
        "pair": pair,
        "order_type": "market_buy",
        "market_buy_amount": amount
    }
    if stop_loss_rate is not None:
        body["stop_loss_rate"] = stop_loss_rate
    return create(body=body)

def create_market_sell_order(amount:float, pair:str="btc_jpy"):
    body = {
        "pair": pair,
        "order_type": "market_sell",
        "amount": amount
    }
    return create(body=body)

def cancel(id):
    response = service.request(ServiceBase.METHOD_DELETE, baseUrl + '/' + str(id))
    return response

def get_pending_orders():
    """ get orders. GET /opens

    Returns:
        dict format: orders: [{
            "id": 202835,
            "order_type": "buy",
            "rate": 26890,
            "pair": "btc_jpy",
            "pending_amount": "0.5527",
            "pending_market_buy_amount": null,
            "stop_loss_rate": null,
            "created_at": "2015-01-10T05:55:38.000Z"
        }]
    """
    return service.request(ServiceBase.METHOD_GET, baseUrl + '/opens')

def get_my_trade_history():
    response = service.request(ServiceBase.METHOD_GET, baseUrl + '/transactions')
    return service.parse_str_to_dict(response)