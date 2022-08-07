from asyncio.log import logger
from time import sleep
import uuid
import datetime
import json
import os
from logging import getLogger, config
import json

class Position:
    id = uuid.uuid4()
    
    def __init__(self, order_type:str, symbol:str, price:float, amount: float,  tp:float, sl:float, option, result):
        self.order_type = order_type
        self.price = price
        self.amount = amount
        self.option = option
        self.result = result
        self.symbol = symbol
        self.tp = tp
        self.sl = sl
        self.timestamp = datetime.datetime.now()
        
    def __str__(self) -> str:
        return f'(order_type:{self.order_type}, price:{self.price}, amount:{self.amount}, tp: {self.tp}, sl:{self.sl}, symbol:{self.symbol})'
    

class Manager:
    
    positions = {
        "ask": {},
        "bid": {}
    }
    
    pending_orders = {
        "ask": {},
        "bid": {}
    }
    
    listening_positions = {}
    
    __locked = False
    
    def __init__(self, budget, provider="Default", logger = None):
        dir = os.path.dirname(__file__)
        try:
            with open(os.path.join(dir, './settings.json'), 'r') as f:
                settings = json.load(f)
        except Exception as e:
            self.logger.error(f"fail to load settings file: {e}")
            raise e
        
        if logger == None:
            logger_config = settings["log"]
        
            try:
                log_file_base_name = logger_config["handlers"]["fileHandler"]["filename"]
                log_folder = os.path.join(os.path.dirname(__file__), 'logs')
                if os.path.exists(log_folder) == False:
                    os.makedirs(log_folder)
                logger_config["handlers"]["fileHandler"]["filename"] = f'{log_folder}/{log_file_base_name}_{datetime.datetime.utcnow().strftime("%Y%m%d")}.logs'
                config.dictConfig(logger_config)
            except Exception as e:
                self.logger.error(f"fail to set configure file: {e}")
                raise e
            self.logger = getLogger(__name__)
        else:
            self.logger = logger
            
        self.budget = budget
        self.__start_budget = budget
        
        providers = settings["providers"]
        if provider in providers:
            SymbolInfo = providers[provider]
            self.provider = provider
        else:
            error_txt = f"provider {provider} is not defined in settings.json"
            self.logger.error(error_txt)
            raise Exception(error_txt)
        
        self.trade_unit = SymbolInfo["trade_unit"]
        self.logger.info(f"MarketManager is initialized with budget:{budget}, provider:{provider}")
    
    def initialize_budget(self, budget=None):
        if budget == None:
            budget = self.__start_budget
        self.budget = budget
        self.__start_budget = budget
        self.logger.info(f"MarketManager updated budget to {budget}")
        

    def __wait_lock(self):
        while self.__locked:
            sleep(0.3)
        return True
    
    def __check_order_type(self, order_type:str):
        if order_type and type(order_type) == str:
            order_type = str.lower(order_type)
            #if order_type == "ask" or order_type == "buy" or order_type == "long" or order_type == "bid" or order_type == "sell" or order_type == "short":
            if order_type == "ask" or order_type == "bid":
                return order_type
            else:
                raise Exception("unkown order_type: {order_type}")
        else:
            raise Exception(f"order_type should be specified: {order_type}")
    
    def open_position(self, order_type:str, symbol:str, price:float, amount: float, tp=None, sl=None, option = None, result=None):
        order_type = self.__check_order_type(order_type)
        ## check if budget has enough amount
        required_budget = self.trade_unit * amount * price
        ## if enough, add position
        if required_budget <= self.budget:
            position = Position(order_type=order_type, symbol=symbol, price=price, amount=amount, tp=tp, sl=sl, option=option, result=result)
            self.positions[order_type][position.id] = position
            self.logger.debug(f"position is stored: {position}")
            ## then reduce budget
            self.budget = self.budget - required_budget
            ## check if tp/sl exists
            if tp is not None or sl is not None:
                self.listening_positions[position.id] = position
                self.logger.debug(f"position is stored to listening list")
            return position
        else:
            logger.info(f"current budget {self.budget} is less than required {required_budget}")
            
    def get_position(self, id):
        if id in self.positions["ask"]:
            return self.positions["ask"][id]
        elif id in self.positions["bid"]:
            return self.positions["bid"][id]
        return None
            
    
    def get_open_positions(self, order_type:str = None) -> list:
        positions = []
        if order_type:
            order_type = self.__check_order_type(order_type=order_type)
            for id, position in self.positions[order_type].items():
                positions.append(position)
        else:
            for trend, position_type in self.positions.items():
                for id, position in position_type.items():
                    positions.append(position)
        return positions
    
    def close_position(self, position:Position, price: float, amount: float = None):
        if type(position) == Position and type(position.amount) == float or type(position.amount) == int:
            if amount == None:
                amount = position.amount
            if position.amount < amount:
                logger.info(f"specified amount is greater than position. use position value. {position.amount} < {amount}")
                amount = position.amount
            if position.order_type == "ask":
                price_diff = (price - position.price)
            elif position.order_type == "bid":
                price_diff = position.price - price
            
            profit = self.trade_unit * amount * price_diff
            return_budget = self.trade_unit * amount * position.price + profit
            
            if(self.__wait_lock()):
                self.__locked = True
                ## close position
                if position.id in self.positions[position.order_type]:
                    if position.amount == amount:
                        self.positions[position.order_type].pop(position.id)
                    else:
                        position.amount = position.amount - amount
                        self.positions[position.order_type][position.id] = position
                    self.logger.info(f"closed result:: profit {profit}, return_budget: {return_budget}")
                    self.budget += return_budget
                    self.__locked = False
                    return price, position.price, price_diff, profit
                else:
                    self.__locked = False
                    logger.info("info: positionis already removed.")
                    
                ## remove position from listening
                if position.id in self.listening_positions:
                    self.listening_positions.pop(position.id)
            else:
                logger.debug("lock returned false somehow.")
        else:
            logger.warning(f"position amount is invalid: {position.amount}")
                
    def check_order_in_tick(ask:float, bid:float):
        pass