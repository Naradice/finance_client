from coincheck.apis.servicebase import ServiceBase


class Order():
    
    def __init__(self) -> None:
        self.baseUrl = '/api/exchange/orders'
        self.service = ServiceBase()

    def create(self, params={}):
        return self.service.request(ServiceBase.METHOD_POST, self.baseUrl, params)

    def cancel(self, params={}):
        defaults = {
            'id': ""
        }
        defaults.update(params)
        params = defaults.copy()
        return self.service.request(ServiceBase.METHOD_DELETE, self.baseUrl + '/' + str(params['id']), params)

    def opens(self, params={}):
        return self.service.request(ServiceBase.METHOD_GET, self.baseUrl + '/opens', params)

    def transactions(self, params={}):
        return self.service.request(ServiceBase.METHOD_GET, self.baseUrl + '/transactions', params)

    def rate(self, params={}):
        return self.service.request(ServiceBase.METHOD_GET, self.baseUrl + '/rate', params)