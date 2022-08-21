from finance_client.coincheck.apis.servicebase import ServiceBase

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
        response = self.__service.request(ServiceBase.METHOD_GET, self.baseUrl, params)
        # response sample: '{"last":2915999.0,"bid":2915225.0,"ask":2915905.0,"high":2970000.0,"low":2850481.0,"volume":3065.26893255,"timestamp":1661000623}'
        
        return self.__service.parse_str_to_dict(response)