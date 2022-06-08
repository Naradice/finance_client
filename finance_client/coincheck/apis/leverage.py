from coincheck.apis.servicebase import ServiceBase

class Leverage():
    def __init__(self) -> None:
        self.baseUrl = '/api/exchange/leverage'
        self.service = ServiceBase()
    
    def positions(self, params = {}):
        return self.service.request(ServiceBase.METHOD_GET, self.baseUrl + '/positions', params)