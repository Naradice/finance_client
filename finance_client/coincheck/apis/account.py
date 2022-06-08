from coincheck.apis.servicebase import ServiceBase

class Account():
    
    def __init__(self):
        self.baseUrl = '/api/accounts'
        self.service = ServiceBase()

    def balance(self, params = {}):
        return self.service.request(ServiceBase.METHOD_GET, self.baseUrl + '/balance', params)

    def leverage_balance(self, params = {}):
        return self.service.request(ServiceBase.METHOD_GET, self.baseUrl + '/leverage_balance', params)

    def info(self, params = {}):
        return self.service.request(ServiceBase.METHOD_GET, self.baseUrl, params)