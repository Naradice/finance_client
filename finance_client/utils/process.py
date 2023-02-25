import pandas as pd

# TODO: create option class so that user can know what parameter can be specified as option


class ProcessBase:
    columns = []
    option = {}
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

    def concat(self, data: pd.DataFrame, new_data: pd.Series):
        if type(data) == pd.DataFrame and type(new_data) == pd.Series:
            return pd.concat([data, pd.DataFrame.from_records([new_data])], ignore_index=True, sort=False)
        elif type(data) == pd.Series and type(new_data) == pd.DataFrame:
            return pd.concat([pd.DataFrame.from_records([data]), new_data], ignore_index=True)
        elif type(data) == pd.DataFrame and type(new_data) == pd.DataFrame:
            return pd.concat([data, new_data], ignore_index=True)
        elif type(data) == pd.Series and type(new_data) == pd.Series:
            return pd.concat([pd.DataFrame.from_records([data]), pd.DataFrame.from_records([new_data])], ignore_index=True)
        else:
            raise Exception("concat accepts dataframe or series")

    def revert(self, data_set: tuple):
        """revert processed data to row data with option value

        Args:
            data (tuple): assume each series or values or processed data is passed

        Returns:
            Boolean, dict: return (True, data: pd.dataFrame) if reverse_process is defined, otherwise (False, None)
        """
        return False, None

    def init_params(self, data: pd.DataFrame):
        pass

    def __eq__(self, __o: object) -> bool:
        if "key" in dir(__o):
            return self.key == __o.key
        else:
            return False
