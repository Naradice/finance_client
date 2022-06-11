import pandas as pd

class ProcessBase:
   
    columns = []
    option = {}
    kinds = "base"
   
    def __init__(self, key:str):
        self.key = key
    
    @classmethod
    def load(self, key:str, params:dict):
        raise Exception("Need to implement")
    
    def run(self, data: pd.DataFrame) -> dict:
        """ process to apply additionally. if an existing key is specified, overwrite existing values

        Args:
            data (pd.DataFrame): row data of dataset

        """
        raise Exception("Need to implement process method")
    
    def update(self, tick:pd.Series) -> pd.Series:
        """ update data using next tick

        Args:
            tick (pd.DataFrame): new data

        Returns:
            dict: appended data
        """
        raise Exception("Need to implement")
    
    def get_minimum_required_length(self) -> int:
        raise Exception("Need to implement")
    
    def concat(self, data:pd.DataFrame, new_data: pd.Series):
        return pd.concat([data, pd.DataFrame.from_records([new_data])], ignore_index=True)
    
    def revert(self, data_set: tuple):
        """ revert processed data to row data with option value

        Args:
            data (tuple): assume each series or values or processed data is passed
            
        Returns:
            Boolean, dict: return (True, data: pd.dataFrame) if reverse_process is defined, otherwise (False, None)
        """
        return False, None
    
    def init_params(self, data: pd.DataFrame):
        pass