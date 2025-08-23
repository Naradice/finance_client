from abc import ABCMeta, abstractmethod

import pandas as pd


class ProcessBase(metaclass=ABCMeta):
    def __init__(self, key: str):
        self.key = key
        self.initialization_required = False
        self._option = {}

    @property
    def option(self):
        return self._option

    @option.setter
    def option(self, value):
        self._option = value

    @classmethod
    def load(self, key: str, params: dict):
        raise Exception("Need to implement")

    def initialize(self, symbols: list, data: pd.DataFrame, grouped_by_symbol=False):
        print("initialization of base class is called. please create initialize function on your process.")
        pass

    def __call__(self, *args, **kwds):
        if self.initialization_required:
            self.initialize(*args, **kwds)
        return self.run(*args, **kwds)

    @abstractmethod
    def run(self, symbols: list, data: pd.DataFrame, grouped_by_symbol=False) -> dict:
        """process to apply additionally. if an existing key is specified, overwrite existing values

        Args:
            data (pd.DataFrame): row data of dataset

        """
        raise Exception("Need to implement process method")

    def update(self, tick: pd.Series) -> pd.Series:
        """update data using next tick

        Args:
            tick (pd.DataFrame): new data

        Returns:
            dict: appended data
        """
        raise Exception("Need to implement")

    def get_minimum_required_length(self) -> int:
        return 1

    def revert(self, data, columns=None):
        """revert processed data to row data with option value

        Args:
            data (tuple): assume each series or values or processed data is passed

        Returns:
            Boolean, dict: return (True, data: pd.dataFrame) if reverse_process is defined, otherwise (False, None)
        """
        return data

    def __eq__(self, __o: object) -> bool:
        if "key" in dir(__o):
            return self.key == __o.key
        else:
            return False
