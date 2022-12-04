from time import sleep
from turtle import position
import uuid
import datetime
import json
import os
from logging import getLogger, config
import json
import pickle

class Position:
    
    def __init__(self, order_type:str, symbol:str, price:float, amount: float,  tp:float, sl:float, option, result, id=None):
        if id is None:
            self.id = str(uuid.uuid4())
        else:
            self.id = id
        self.order_type = order_type
        self.price = price
        self.amount = amount
        self.option = option
        if option == "null":
            self.option = None
        self.result = result
        if result == "null":
            self.result = None
        self.symbol = symbol
        self.tp = tp
        if tp == "null":
            self.tp = None
        self.sl = sl
        if sl == "null":
            self.sl = None
        self.timestamp = datetime.datetime.utcnow()
        
    def __str__(self) -> str:
        return f'(order_type:{self.order_type}, price:{self.price}, amount:{self.amount}, tp: {self.tp}, sl:{self.sl}, symbol:{self.symbol})'
    
    def to_dict(self):
        return {"order_type": self.order_type, "price":self.price, "amount":self.amount, "option": json.dumps(self.option), 
             "result": json.dumps(self.result), "symbol":self.symbol,
             "tp":self.tp, "sl":self.sl, "timestamp":self.timestamp.isoformat(),
             "id": self.id
             }

class Manager:
    
    positions = {
        "budget": 0,
        "ask": {},
        "bid": {}
    }
    
    pending_orders = {
        "ask": {},
        "bid": {}
    }
    
    listening_positions = {}

    file_path = f'{os.getcwd()}/positions.json'
    
    def __initialize_positions(self):
        if os.path.exists(self.file_path):
            with open(self.file_path, mode="r") as fp:
                _positions = json.load(fp)
                
            if self.provider in _positions:
                _position = _positions[self.provider]
                if self.__start_budget is None:
                    budget = _position["budget"]
                    self.positions["budget"] = budget
                long_position_list = _position["ask"]
                short_position_list = _position["bid"]
                
                for _position in long_position_list:
                    position = Position(_position["order_type"], _position["symbol"], _position["price"], _position["amount"], _position["tp"], _position["sl"], _position["option"], _position["result"], _position["id"])
                    self.positions["ask"][position.id] = position
                    if position.tp is not None or position.sl is not None:
                        self.listening_positions[position.id] = position
                        
                for _position in short_position_list:
                    position = Position(_position["order_type"], _position["symbol"], _position["price"], _position["amount"], _position["tp"], _position["sl"], _position["option"], _position["result"], _position["id"])
                    self.positions["bid"][position.id] = position
                    if position.tp is not None or position.sl is not None:
                        self.listening_positions[position.id] = position
                
                    
    def __save_positions(self):
        _positions = {}
        if os.path.exists(self.file_path):
            with open(self.file_path, mode="r") as fp:
                _positions = json.load(fp)
        
        _position = {
            "budget": self.positions["budget"],
            "ask": [ self.positions["ask"][position_id].to_dict() for position_id in self.positions["ask"]],
            "bid": [self.positions["bid"][position_id].to_dict() for position_id in self.positions["bid"]]
        }
        
        _positions[self.provider]= _position
        with open(self.file_path, mode="w") as fp:
            json.dump(_positions, fp)
    
    def __init__(self, budget, provider="Default", logger = None):        
        self.__locked = False
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
            
        self.positions["budget"] = budget
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
        self.__initialize_positions()
        self.logger.info(f"MarketManager is initialized with budget:{budget}, provider:{provider}")
    
    def initialize_budget(self, budget=None):
        if budget == None:
            budget = self.__start_budget
        self.positions["budget"] = budget
        self.__start_budget = budget
        self.logger.info(f"MarketManager updated budget to {budget}")
    
    @property
    def budget(self):
        return self.positions["budget"]

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
        
    def __store_position(self, position):
        self.positions[position.order_type][position.id] = position
        self.logger.debug(f"position is stored: {position}")
        #insert position info to file
        self.__save_positions()
            
    def open_position(self, order_type:str, symbol:str, price:float, amount: float, tp=None, sl=None, option = None, result=None):
        order_type = self.__check_order_type(order_type)
        ## check if budget has enough amount
        required_budget = self.trade_unit * amount * price
        ## if enough, add position
        budget = self.positions["budget"]
        if required_budget <= budget:
            position = Position(order_type=order_type, symbol=symbol, price=price, amount=amount, tp=tp, sl=sl, option=option, result=result)
            ## then reduce budget
            self.positions["budget"] = budget - required_budget
            self.__store_position(position)
            ## check if tp/sl exists
            if tp is not None or sl is not None:
                self.listening_positions[position.id] = position
                self.logger.debug(f"position is stored to listening list")
            return position
        else:
            self.logger.info(f"current budget {budget} is less than required {required_budget}")
            
    def get_position(self, id):
        if id in self.positions["ask"]:
            return self.positions["ask"][id]
        elif id in self.positions["bid"]:
            return self.positions["bid"][id]
        return None
    
    def get_open_positions(self, order_type:str = None, symbols=[]) -> list:
        positions = []
        is_all_symbols = False
        if type(symbols) == list:
            is_all_symbols = len(symbols) == 0
        elif type(symbols) == str:
            symbols = [symbols]
        else:
            self.logger.error(f"Unkown type is specified as symbols: {type(symbols)}")
            return []
        if order_type:
            order_type = self.__check_order_type(order_type=order_type)
            for id, position in self.positions[order_type].items():
                if is_all_symbols or position.symbol in symbols:
                    positions.append(position)
        else:
            for order_type in ["ask", "bid"]:
                for id, position in self.positions[order_type].items():
                    if is_all_symbols or position.symbol in symbols:
                        positions.append(position)
        return positions
    
    def close_position(self, position:Position, price: float, amount: float = None):
        if type(position) == Position and type(position.amount) == float or type(position.amount) == int:
            if amount == None:
                amount = position.amount
            if position.amount < amount:
                self.logger.info(f"specified amount is greater than position. use position value. {position.amount} < {amount}")
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
                    self.positions["budget"] += return_budget
                    self.__locked = False
                    self.__save_positions()
                    return price, position.price, price_diff, profit
                else:
                    self.__locked = False
                    self.logger.info("info: positionis already removed.")
                    
                ## remove position from listening
                if position.id in self.listening_positions:
                    self.listening_positions.pop(position.id)
            else:
                self.logger.debug("lock returned false somehow.")
        else:
            self.logger.warning(f"position amount is invalid: {position.amount}")
                
    def check_order_in_tick(ask:float, bid:float):
        pass