from coincheck.apis.servicebase import ServiceBase

class Deposit():
    
    def __init__(self) -> None:
        self.baseUrl = '/api/deposit_money'
        self.service = ServiceBase()
    
    def all(self, params = {}):
        defaults = {
            'currency': ""
        }
        defaults.update(params)
        params = defaults.copy()
        return self.service.request(ServiceBase.METHOD_GET, self.baseUrl, params)

    def fast(self, params = {}):
        defaults = {
            'id': ""
        }
        defaults.update(params)
        params = defaults.copy()
        return self.service.request(ServiceBase.METHOD_POST, self.baseUrl + '/' + str(params['id']) + '/fast', params)