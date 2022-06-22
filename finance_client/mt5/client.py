import datetime
import MetaTrader5 as mt5
import pandas as pd
import random
from finance_client.client_base import Client
from finance_client.frames import Frame

class MT5Client(Client):
    
    AVAILABLE_FRAMES = {
        Frame.MIN1: mt5.TIMEFRAME_M1,
        Frame.MIN5: mt5.TIMEFRAME_M5,
        Frame.MIN10: mt5.TIMEFRAME_M10,
        Frame.MIN30: mt5.TIMEFRAME_M30,
        Frame.H1: mt5.TIMEFRAME_H1,
        Frame.H2: mt5.TIMEFRAME_H2,
        Frame.H4: mt5.TIMEFRAME_H4,
        Frame.H8: mt5.TIMEFRAME_H8,
        Frame.D1: mt5.TIMEFRAME_D1,
        Frame.W1: mt5.TIMEFRAME_W1,
        Frame.MO1: mt5.TIMEFRAME_MN1
    }

    def __init__(self,id, password, server, simulation=True, auto_index=False, frame = 5, symbol = 'USDJPY', post_process = [], budget=1000000, logger = None, simulation_seed = 1017):
        super().__init__( budget=budget, frame=frame, provider=server, post_processes= post_process, logger_name=__name__, logger=logger)
        self.simulation = simulation
        self.debug = False
        self.isWorking = mt5.initialize()
        if not self.isWorking:
            err_txt = f"initialize() failed, error code = {mt5.last_error()}"
            self.logger.error(err_txt)
            raise Exception(err_txt)
        self.logger.info(f"MetaTrader5 package version {mt5.__version__}")
        authorized = mt5.login(
            id,
            password=password,
            server=server,
        )
        if not authorized:
            err_txt = f"User Authorization Failed"
            self.logger.error(err_txt)
            raise Exception(err_txt)
        self.SYMBOL = symbol
        self.frame = frame
        try:
            self.mt5_frame = self.AVAILABLE_FRAMES[frame]
        except Exception as e:
            raise e
        
        account_info = mt5.account_info()
        if account_info is None:
            self.logger.warning(f"Retreiving account information failed. Please check your internet connection.")
        self.logger.info(f"Balance: {account_info}")

        symbol_info = mt5.symbol_info(self.SYMBOL)
        if symbol_info is None:
            err_txt = "Symbol not found"
            self.logger.error(err_txt)
            raise Exception(err_txt)
        self.point = symbol_info.point
        self.orders = {"ask":[], "bid":[]}
        
        if self.simulation:
            if frame != Frame.MIN5:
                self.logger.warning("simulation mode is available only for MIN5 for now")
            if type(simulation_seed) == int:
                random.seed(simulation_seed)
            else:
                random.seed(1017)
            #self.sim_index = random.randrange(int(12*24*347*0.2), 12*24*347 - 30) ##only for M5
            self.sim_index = random.randrange(int(12*24*347*0.2), 12*24*345) ##only for M5
            self.sim_initial_index = self.sim_index
            self.auto_index = auto_index
            if auto_index:
                self.__next_time = None
    
    def __post_market_order(self, _type, vol, price, dev, sl=None, tp=None, position=None):
        request = {
            'action': mt5.TRADE_ACTION_DEAL,
            'symbol': self.SYMBOL,
            'volume': vol,
            'price': price,
            'deviation': dev,
            'magic': 100000000,
            'comment': "python script open",
            'type_time': mt5.ORDER_TIME_GTC,
            'type': _type,
            'type_filling': mt5.ORDER_FILLING_IOC, # depends on broker
        }
        if sl is not None:
            request.update({"sl": sl,})
        if tp is not None:
            request.update({"tp": tp,})
        if position is not None:
            request.update({"position": position})

        result = mt5.order_send(request)
        return result
    
    def __post_order(self, _type, vol, price, dev, sl=None, tp=None, position=None):
        request = {
            'action': mt5.TRADE_ACTION_PENDING,
            'symbol': self.SYMBOL,
            'volume': vol,
            'price': price,
            'deviation': dev,
            'magic': 100000000,
            'comment': "python script open",
            'type_time': mt5.ORDER_TIME_GTC,
            'type': _type,
            'type_filling': mt5.ORDER_FILLING_IOC,
        }
        if sl is not None:
            request.update({"sl": sl,})
        if tp is not None:
            request.update({"tp": tp,})
        if position is not None:
            request.update({"position": position})

        result = mt5.order_send(request)
        return result

    def get_current_ask(self):
        if self.simulation:
            next_data = self.__get_rates(1, self.sim_index-1)
            open_column = self.get_ohlc_columns()["Open"]
            high_column = self.get_ohlc_columns()["High"]
            value = random.uniform(next_data[open_column].iloc[0], next_data[high_column].iloc[0])
            #value = next_data[open_column].iloc[0] + next_data["spread"].iloc[0]*self.point
            return float(value)
        else:
            return mt5.symbol_info_tick(self.SYMBOL).ask
    
    def get_current_bid(self):
        if self.simulation:
            next_data = self.__get_rates(1, self.sim_index-1)
            open_column = self.get_ohlc_columns()["Open"]
            low_column = self.get_ohlc_columns()["Low"]
            value = random.uniform(next_data[low_column].iloc[0], next_data[open_column].iloc[0])
            #value = next_data[open_column].iloc[0] - next_data["spread"].iloc[0]*self.point
            return float(value)
        else:
            return mt5.symbol_info_tick(self.SYMBOL).bid
    
    def get_current_spread(self):
        if self.simulation:
            return self.data["spread"].iloc[-1]
        else:
            return mt5.symbol_info(self.SYMBOL).spread
    

    def __market_sell(self, price, tp=None, sl=None):
        rate = price
        if self.simulation is False:
            if tp != None:
                if rate <= tp:
                    self.logger.warning("tp should be lower than value")
                else:
                    _tp = tp
                    offset = rate - tp
                    if sl != None:
                        _sl = sl
                    else:
                        _sl = rate + offset
            result = self.__post_market_order(
                _type=mt5.ORDER_TYPE_SELL,
                vol=0.1,
                price=rate,
                dev=20,
                sl=_sl,
                tp=_tp,
            )
            return result
            
    def __buy_for_settlement(self, price, result):
        if self.simulation is False:
            rate = price
            order = result.order
            result = self.__post_market_order(
                _type=mt5.ORDER_TYPE_BUY, 
                vol=0.1, 
                price=rate, 
                dev=20,
                position=order,
            )
            return result
            
    def __market_buy(self, price, tp=None, sl=None):
        if self.simulation is False:
            rate = price
            
            if tp != None:
                if rate >= tp:
                    self.logger.warning("tp should be greater than value")
                else:
                    _tp = tp
                    offset = tp - rate
                    if sl != None:
                        _sl = sl
                    else:
                        _sl = rate - offset
            
            result = self.__post_market_order(
                _type=mt5.ORDER_TYPE_BUY,
                vol=0.1, 
                price=rate,
                dev=20,
                sl=_sl,
                tp=_tp,
            )
            return result
            
    
    def buy_order(self, value, tp=None, sl=None):
        if self.simulation is False:
            result = self.post_order(
                _type=mt5.ORDER_TYPE_BUY_LIMIT,
                vol=0.1,
                price=value,
                dev=20,
                sl=sl,
                tp=tp,
            )
            return result
    
    def sell_order(self, value, tp=None, sl=None):            
        if self.simulation is False:
            result = self.post_order(
                _type=mt5.ORDER_TYPE_SELL_LIMIT,
                vol=0.1,
                price=value,
                dev=20,
                sl=sl,
                tp=tp,
            )
            self.orders["bid"].append(result)
    
    def update_order(self, _type, _id, value, tp, sl):
        print("NOT IMPLEMENTED")

    def __sell_for_settlment(self, price, result):
        if self.simulation is False:
            position = result.order
            result = self.__post_market_order(
                _type=mt5.ORDER_TYPE_SELL,
                vol=0.1,
                price=price,
                dev=20,
                position=position,
            )
            return result
    
    def __get_rates(self, interval, start = 0):
        start_index = 0
        _interval = None
        if start != 0:
            start_index = start
        elif self.simulation:
            start_index = self.sim_index
        if self.auto_index and interval == 1:
            _interval  = interval
            interval += 1
                
        rates = mt5.copy_rates_from_pos(self.SYMBOL, self.mt5_frame, start_index, interval)
        df_rates = pd.DataFrame(rates)
        df_rates['time'] = pd.to_datetime(df_rates['time'], unit='s')
        #df_rates = df_rates.set_index('time')
        
        if self.simulation and self.auto_index:
            if self.__next_time == None:
                self.__next_time = df_rates['time'].iloc[1]
                self.logger.debug(f"auto index: initialized with {df_rates['time'].iloc[0]}")
            else:
                current_time = df_rates['time'].iloc[0]
                if current_time == self.__next_time:
                    self.logger.debug(f"auto index: index is ongoing on {current_time}")
                    self.__next_time = df_rates['time'].iloc[1]
                elif current_time > self.__next_time:
                    self.logger.debug(f"auto index: {current_time} > {self.__next_time}. may time past.")
                    candidate = self.sim_index
                    while current_time != self.__next_time:
                        candidate += 1
                        rates = mt5.copy_rates_from_pos(self.SYMBOL, self.mt5_frame, candidate, interval)
                        df_rates = pd.DataFrame(rates)
                        df_rates['time'] = pd.to_datetime(df_rates['time'], unit='s')
                        current_time = df_rates['time'].iloc[0]
                    self.sim_index = candidate
                    
                    self.logger.debug(f"auto index: fixed to {current_time}")
                    self.__next_time = df_rates['time'].iloc[1]
                    #to avoid infinite loop, don't call oneself
                else:
                    self.logger.debug(f"auto index: {current_time} < {self.__next_time} somehow.")
                    candidate = self.sim_index
                    while current_time != self.__next_time:
                        candidate = candidate - 1
                        rates = mt5.copy_rates_from_pos(self.SYMBOL, self.mt5_frame, candidate, interval)
                        df_rates = pd.DataFrame(rates)
                        df_rates['time'] = pd.to_datetime(df_rates['time'], unit='s')
                        current_time = df_rates['time'].iloc[0]
                    self.sim_index = candidate
                    
                    self.logger.debug(f"auto index: fixed to {current_time}")
                    self.__next_time = df_rates['time'].iloc[1]
                    
        if self.auto_index and _interval:
            return df_rates.iloc[:interval]
        else:
            return df_rates
        
    def get_rates(self, interval, start = 0):
        
        df_rates = self.__get_rates(interval=interval, start=start)
        if self.auto_index:
            self.sim_index = self.sim_index - 1

        return df_rates
    
    def cancel_order(self, order):
        if self.simulation is False:
            position = order.order
            request = {
                'action': mt5.TRADE_ACTION_REMOVE,
                'order':position
            }
            result = mt5.order_send(request)
            return result