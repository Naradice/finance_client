import json
import os
from time import sleep

from finance_client import logger as lg
from finance_client.db import BaseConnector
from finance_client.position import Position


class Manager:
    __singleton = {}

    def __new__(cls, storage: BaseConnector, budget, provider="Default", logger=None):
        if provider not in cls.__singleton:
            singleton = super(Manager, cls).__new__(cls)
            dir = os.path.dirname(__file__)
            try:
                with open(os.path.join(dir, "./settings.json"), "r") as f:
                    settings = json.load(f)
            except Exception as e:
                print(f"fail to load settings file: {e}")
                raise e

            if logger is None:
                singleton.logger = lg
            else:
                singleton.logger = logger

            singleton.budget = budget

            providers = settings["providers"]
            if provider not in providers:
                error_txt = f"provider {provider} is not defined in settings.json. use default settings instead."
                singleton.logger.error(error_txt)
                SymbolInfo = providers[provider]
                singleton.provider = provider
            else:
                SymbolInfo = providers["Default"]
                singleton.provider = provider

            singleton.trade_unit = SymbolInfo["trade_unit"]
            long, short, listening = storage.load_positions()
            singleton.positions = {"long": long, "short": short}
            singleton.listening_positions = listening
            singleton.logger.info(f"MarketManager is initialized with budget:{budget}, provider:{provider}")
            cls.__singleton[provider] = singleton
        return cls.__singleton[provider]

    @property
    def long_positions(self):
        return self.positions["long"]

    @property
    def short_positions(self):
        return self.positions["short"]

    def __check_order_type(self, order_type: str):
        if type(order_type) == str:
            order_type = str.lower(order_type)
            if order_type == "long" or order_type == "short":
                return order_type
            else:
                raise Exception(f"unkown order_type: {order_type}")
        else:
            raise Exception(f"order_type should be specified: {order_type}")

    def __store_position(self, position):
        self.positions[position.order_type][position.id] = position
        self.logger.debug(f"position is stored: {position}")

    def open_position(
        self, order_type: str, symbol: str, price: float, amount: float, tp=None, sl=None, index=None, option=None, result=None
    ):
        order_type = self.__check_order_type(order_type)
        # Market buy without price is ordered during market closed
        if price is None:
            position = Position(
                order_type=order_type,
                symbol=symbol,
                price=price,
                amount=amount,
                tp=tp,
                sl=sl,
                index=index,
                option=option,
                result=result,
            )
            self.positions[order_type][position.id] = position
            return position
        else:
            # check if budget has enough amount
            required_budget = self.trade_unit * amount * price
            # if enough, add position
            if required_budget <= self.budget:
                position = Position(
                    order_type=order_type,
                    symbol=symbol,
                    price=price,
                    amount=amount,
                    tp=tp,
                    sl=sl,
                    index=index,
                    option=option,
                    result=result,
                )
                self.positions[order_type][position.id] = position
                # then reduce budget
                self.budget -= required_budget
                # check if tp/sl exists
                if tp is not None or sl is not None:
                    self.listening_positions[position.id] = position
                    self.logger.debug("position is stored to listening list")
                return position
            else:
                self.logger.info(f"current budget {self.budget} is less than required {required_budget}")

    def get_position(self, id):
        if id in self.positions["long"]:
            return self.positions["long"][id]
        elif id in self.positions["short"]:
            return self.positions["short"][id]
        return None

    def get_open_positions(self, order_type: str = None, symbols=[]) -> list:
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
            for order_type in ["long", "short"]:
                for id, position in self.positions[order_type].items():
                    if is_all_symbols or position.symbol in symbols:
                        positions.append(position)
        return positions

    def close_position(self, position: Position, price: float, amount: float = None):
        if price is None or position is None:
            return None, None, None, None
        if type(position) == Position and type(position.amount) == float or type(position.amount) == int:
            if amount is None:
                amount = position.amount
            if position.amount < amount:
                self.logger.info(f"specified amount is greater than position. use position value. {position.amount} < {amount}")
                amount = position.amount
            if position.order_type == "long":
                price_diff = price - position.price
            elif position.order_type == "short":
                price_diff = position.price - price

            profit = self.trade_unit * amount * price_diff
            return_budget = self.trade_unit * amount * position.price + profit
            # close position
            if position.id in self.positions[position.order_type]:
                if position.amount == amount:
                    self.positions[position.order_type].pop(position.id)
                else:
                    position.amount = position.amount - amount
                    self.positions[position.order_type][position.id] = position
                self.logger.info(f"closed result:: profit {profit}, return_budget: {return_budget}")
                self.budget += return_budget
                return price, position.price, price_diff, profit
            else:
                self.logger.info("info: position is already removed.")

            # remove position from listening
            self.remove_position_from_listening(position)
        else:
            self.logger.warning(f"position amount is invalid: {position.amount}")

    def check_order_in_tick(ask: float, bid: float):
        pass

    def remove_position_from_listening(self, position):
        if position.id in self.listening_positions:
            self.listening_positions.pop(position.id)
