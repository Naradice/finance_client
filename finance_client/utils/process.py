import pandas as pd


class ProcessBase:
    kinds = "base"

    def __init__(self, key: str):
        self.key = key
        self.initialization_required = False

    @classmethod
    def load(self, key: str, params: dict):
        raise Exception("Need to implement")

    def initialize(self, symbols: list, data: pd.DataFrame, grouped_by_symbol=False):
        print("initialization of base class is called. please create initialize function on your process.")
        pass

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
        return 0

    def revert(self, data_set: tuple):
        """revert processed data to row data with option value

        Args:
            data (tuple): assume each series or values or processed data is passed

        Returns:
            Boolean, dict: return (True, data: pd.dataFrame) if reverse_process is defined, otherwise (False, None)
        """
        return False, None

    def __eq__(self, __o: object) -> bool:
        if "key" in dir(__o):
            return self.key == __o.key
        else:
            return False
