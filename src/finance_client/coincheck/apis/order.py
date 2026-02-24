from .servicebase import ServiceBase

baseUrl = "/api/exchange/orders"
service = ServiceBase()


def create(body={}):
    response = service.request(ServiceBase.METHOD_POST, baseUrl, _body=body)
    return service.parse_str_to_dict(response)


def __create_pending_order(order_type: str, rate: int, volume: float, stop_loss_rate: int = None, pair: str = "btc_jpy"):
    body = {"pair": pair, "order_type": order_type, "rate": rate, "volume": volume}
    if stop_loss_rate is not None:
        body["stop_loss_rate"] = stop_loss_rate
    return create(body=body)


def create_pending_buy_order(rate: int, volume: float, stop_loss_rate: int = None, pair: str = "btc_jpy"):
    return __create_pending_order(order_type="buy", rate=rate, volume=volume, stop_loss_rate=stop_loss_rate, pair=pair)


def create_pending_sell_order(rate: int, volume: float, pair: str = "btc_jpy"):
    return __create_pending_order(order_type="sell", rate=rate, volume=volume, stop_loss_rate=None, pair=pair)


def create_market_buy_order(volume: float, stop_loss_rate: int = None, pair: str = "btc_jpy"):
    body = {"pair": pair, "order_type": "market_buy", "market_buy_volume": volume}
    if stop_loss_rate is not None:
        body["stop_loss_rate"] = stop_loss_rate
    return create(body=body)


def create_market_sell_order(volume: float, pair: str = "btc_jpy"):
    body = {"pair": pair, "order_type": "market_sell", "volume": volume}
    return create(body=body)


def cancel(id):
    response = service.request(ServiceBase.METHOD_DELETE, baseUrl + "/" + str(id))
    return service.parse_str_to_dict(response)


def get_pending_orders():
    """get orders. GET /opens

    Returns:
        dict format: orders: [{
            "id": 202835,
            "order_type": "buy",
            "rate": 26890,
            "pair": "btc_jpy",
            "pending_volume": "0.5527",
            "pending_market_buy_volume": null,
            "stop_loss_rate": null,
            "created_at": "2015-01-10T05:55:38.000Z"
        }]
    """
    return service.request(ServiceBase.METHOD_GET, baseUrl + "/opens")


def get_my_trade_history():
    response = service.request(ServiceBase.METHOD_GET, baseUrl + "/transactions")
    return service.parse_str_to_dict(response)
