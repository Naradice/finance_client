from coincheck.apis.servicebase import ServiceBase

class Ticker():
    '''
    public API
    get latest tick
    '''
    AVAILABLE_PAIRS = ['btc_jpy', 'plt_jpy']
    
    def __init__(self) -> None:
        self.baseUrl = '/api/ticker'
        self.__service = ServiceBase()
    
    def get(self, pair='btc_jpy'):
        params = {}
        if pair in self.AVAILABLE_PAIRS:
            params['pair'] = pair
        else:
            print(f'Warning: {pair} is not available. use btc_jpy instead.')
            
        return self.__service.request(ServiceBase.METHOD_GET, self.baseUrl, params)