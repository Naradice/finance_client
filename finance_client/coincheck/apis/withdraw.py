from coincheck.apis.servicebase import ServiceBase

class Withdraw():
    
    def __init__(self) -> None:
        self.baseUrl = '/api/withdraws'
        self.__service = ServiceBase()

    def create(self, params = {}):
        return self.__service.request(ServiceBase.METHOD_POST, self.baseUrl, params)
    
    def all(self, params = {}):
        return self.__service.request(ServiceBase.METHOD_GET, self.baseUrl, params)

    def cancel(self, params = {}):
        defaults = {
            'id': ""
        }
        defaults.update(params)
        params = defaults.copy()
        return self.__service.request(ServiceBase.METHOD_DELETE, self.baseUrl + '/' + str(params['id']), params)
