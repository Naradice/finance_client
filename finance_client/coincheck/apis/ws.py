import json
from typing import Callable
import websocket
import time
import threading
import datetime

class TradeHistory():
    '''
    public API
    get latest trade result
    '''
    AVAILABLE_PAIRS = ['btc_jpy', 'plt_jpy', 'etc_jpy', 'mona_jpy']
    
    def __format_trade_msg(self, message):
        #sample message: [228133744,"btc_jpy","3284400.0","0.05","buy"]
        elements = message.split(',')
        trade = {
            "time":datetime.datetime.utcnow(),
            #"id": int(elements[0][1:-1]),
            "symbol": elements[1][1:-1],
            "price": float(elements[2][1:-1]),
            "volume": float(elements[3][1:-1]),
            "type": elements[4][1:-2]
        }
        return trade
    
    def __on_message(self, ws, message):
        trade = self.__format_trade_msg(message)
        self.on_tick(trade)

    def __on_error(self, ws, error):
        print(f'ws error: {error}')

    def __on_close(self, ws, close_status_code, close_msg):
        print("### closed ###")

    def __on_open(self, ws):
        print("Opened connection")
        self.ready_state = 1#open
    
    def __init__(self, debug=False) -> None:
        self.ready_state = 3#closed
        self.baseUrl = 'wss://ws-api.coincheck.com/'
        self.on_tick = None
        websocket.enableTrace(debug)
        self.ws = websocket.WebSocketApp(url=self.baseUrl,
                                on_open=self.__on_open,
                                on_message=self.__on_message,
                                on_error=self.__on_error,
                                on_close=self.__on_close)
        self.ready_state = 0#connecting
        t = threading.Thread(target=lambda: self.ws.run_forever(), daemon=True)
        t.start()
    
    def subscribe(self, on_tick: Callable, pair='btc_jpy'):
        if on_tick is None or isinstance(on_tick, Callable) is False:
            on_tick = lambda tick: tick
            print("function for on_tick was not provided.")
        self.on_tick = on_tick
        while self.ready_state != 1:
            time.sleep(1)
        self.ws.send(json.dumps(
            {'type': 'subscribe', 'channel': f'{pair}-trades'}
        ))
        
    def close(self):
        self.ready_state = 2#closing
        self.ws.close()
        self.ready_state = 3#closed
        
class Orders():
    '''
    public API
    get latest trade result
    '''
    AVAILABLE_PAIRS = ['btc_jpy', 'plt_jpy', 'etc_jpy', 'mona_jpy']
    
    def __on_message(self, ws, message):
        print('ws message')
        print(message)

    def __on_error(self, ws, error):
        print(f'ws error: {error}')

    def __on_close(self, ws, close_status_code, close_msg):
        print("### closed ###")

    def __on_open(self, ws):
        print("Opened connection")
        self.ready_state = 1#open
    
    def __init__(self, debug=False) -> None:
        self.ready_state = 3#closed
        self.baseUrl = 'wss://ws-api.coincheck.com/'
        websocket.enableTrace(debug)
        self.ws = websocket.WebSocketApp(f"{self.baseUrl}",
                                on_open=self.__on_open,
                                on_message=self.__on_message,
                                on_error=self.__on_error,
                                on_close=self.__on_close)
        self.ready_state = 0#connecting
        t = threading.Thread(target=lambda: self.ws.run_forever(), daemon=True)
        t.start()

    
    def subscribe(self, pair='btc_jpy'):
        while self.ready_state != 1:
            time.sleep(1)
        self.ws.send(json.dumps(
            {'type': 'subscribe', 'channel': f'{pair}-orderbook'}
        ))
        
    def close(self):
        self.ready_state = 2#closing
        self.ws.close()
        self.ready_state = 3#closed