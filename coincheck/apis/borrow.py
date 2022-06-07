from coincheck.apis.servicebase import ServiceBase

class Borrow:
    
    def __init__(self) -> None:
        self.baseUrl = '/api/lending/borrows'
        self.service = ServiceBase()

    def create(self, params = {}):
        defaults = {
            'amount': "",
            'currency': ""
        }
        defaults.update(params)
        params = defaults.copy()
        return self.service.request(ServiceBase.METHOD_POST, self.baseUrl, params)

    def matches(self, params = {}):
        return self.service.request(ServiceBase.METHOD_GET, self.baseUrl + '/matches', params)

    def repay(self, params = {}):
        defaults = {
            'id': ""
        }
        defaults.update(params)
        params = defaults.copy()
        return self.service.request(ServiceBase.METHOD_POST, self.baseUrl + '/' + str(params['id']) + '/repay', params)