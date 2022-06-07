from coincheck.apis.servicebase import ServiceBase

class Trade():
    
    def __init__(self) -> None:
        self.baseUrl = '/api/trades'
        self.__service = ServiceBase()
    
    def all(self, params = {}):
        return self.__service.request(ServiceBase.METHOD_GET, self.baseUrl, params)