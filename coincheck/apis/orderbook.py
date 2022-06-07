from coincheck.apis.servicebase import ServiceBase

class OrderBook():
    def __init__(self) -> None:
        self.baseUrl = '/api/order_books'
        self.service = ServiceBase()
    
    def all(self, params = {}):
        return self.service.request(ServiceBase.METHOD_GET, self.baseUrl, params)