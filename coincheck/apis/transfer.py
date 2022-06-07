from coincheck.apis.servicebase import ServiceBase

class Transfer():
    
    def __init__(self) -> None:
        self.baseUrl = '/api/exchange/transfers'
        self.__service = ServiceBase()

    def to_leverage(self, params = {}):
        defaults = {
            'amount': "",
            'currency': ""
        }
        defaults.update(params)
        params = defaults.copy()
        return self.__service.request(ServiceBase.METHOD_POST, self.baseUrl + '/to_leverage', params)

    def from_leverage(self, params = {}):
        defaults = {
            'amount': "",
            'currency': ""
        }
        defaults.update(params)
        params = defaults.copy()
        return self.__service.request(ServiceBase.METHOD_POST, self.baseUrl + '/from_leverage', params)