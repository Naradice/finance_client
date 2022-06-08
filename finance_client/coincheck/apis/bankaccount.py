from coincheck.apis.servicebase import ServiceBase

class BankAccount():
    baseUrl = '/api/bank_accounts'
    
    def __init__(self) -> None:
        self.service = ServiceBase()
    
    def create(self, params = {}):
        defaults = {
            'bank_name': "",
            'branch_name': "",
            'bank_account_type': "",
            'number': "",
            'name': ""
        }
        defaults.update(params)
        params = defaults.copy()
        return self.service.request(ServiceBase.METHOD_POST, self.baseUrl, params)

    def all(self, params = {}):
        return self.service.request(ServiceBase.METHOD_GET, self.baseUrl, params)

    def delete(self, params = {}):
        defaults = {
            'id': ""
        }
        defaults.update(params)
        params = defaults.copy()
        return self.service.request(ServiceBase.METHOD_DELETE, self.baseUrl + '/' + str(params['id']), params)
