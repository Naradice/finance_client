from coincheck.apis.servicebase import ServiceBase

class Send():
    
    def __init__(self) -> None:
        self.baseUrl = '/api/send_money'
        self.service = ServiceBase()

    def create(self, params = {}):
        defaults = {
            'address': "",
            'amount': ""
        }
        defaults.update(params)
        params = defaults.copy()
        return self.service.request(ServiceBase.METHOD_POST, self.baseUrl, params)
    
    def all(self, params = {}):
        defaults = {
            'currency': ''
        }
        defaults.update(params)
        params = defaults.copy()
        return self.service.request(ServiceBase.METHOD_GET, self.baseUrl, params)