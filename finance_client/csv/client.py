from tkinter.ttk import Separator
import numpy
import pandas as pd
import random
import os
import uuid
import datetime
from stocknet.envs.utils import standalization
from stocknet.envs.market_clients.frames import Frame
import json

class CSVClient():
    kinds = 'csv'

    def __read_csv__(self, columns, date_col=None):
        #super().__init__()
        if self.frame in self.files:
            file = self.files[self.frame]
            usecols = columns
            if date_col != None:
                if date_col not in usecols:
                    usecols.append(date_col)
                self.data = pd.read_csv(file,  header=0, parse_dates=[date_col], usecols = usecols)
            else:
                self.data = pd.read_csv(file,  header=0, usecols = usecols)
            self.columns = usecols
        else:
            raise Exception(f"{self.frame} is not available in CSV Client.")

    def __past_date(self, target_date, from_frame, to_frame):
        diff_date = target_date.day % datetime.timedelta(minutes=to_frame).days
        diff_date_as_minutes = diff_date*60*24
        diff_hours_as_minutes = target_date.hour*60 + target_date.minute
        total_diff_minutes = diff_date_as_minutes+diff_hours_as_minutes
        past_index = int(total_diff_minutes/from_frame)
        return past_index

    def __past_hours(self, target_date, from_frame, to_frame) :
        diff_hours = target_date.hour % (to_frame/60)
        diff_hours_as_minutes = diff_hours*60
        total_diff_minutes = diff_hours_as_minutes + target_date.minute
        past_index = int(total_diff_minutes/from_frame)
        return past_index

    def __past_minutes(self, target_date, from_frame, to_frame) :
        target_minute = target_date.minute
        diff_minutes = target_minute % to_frame
        past_index = int(diff_minutes/from_frame)
        return past_index
        
    def __rolling_frame(self, data:pd.DataFrame, from_frame:int, to_frame: int, addNan=False) -> pd.DataFrame:        
        if to_frame >= Frame.MO1:
            raise Exception("not implemented")
        elif to_frame >= Frame.D1:
            get_past_window = self.__past_date
        elif to_frame >= Frame.H1:
            get_past_window = self.__past_hours
        else:
            get_past_window = self.__past_minutes
        
        pre_index = len(data)-1
        past_index = get_past_window(data[self.date_column].iloc[pre_index], from_frame, to_frame)
        window = int(to_frame/from_frame)
        window_ = window
        next_index = pre_index - past_index
        delta = datetime.timedelta(minutes=from_frame * past_index)
        Highs = []
        Lows = []
        Opens = []
        Closes = []
        Timestamp = []
        
        expected_date = data[self.date_column].iloc[pre_index] - delta
        delta = datetime.timedelta(minutes=to_frame)
        
        while next_index >= 0:
            next_date = data[self.date_column].iloc[next_index]
            window_ = window
            while next_date != expected_date:
                if  next_date > expected_date:
                    print(f"{pre_index} to {next_index} has insufficient data")
                    #when next tick doen't start with expected_date, reduce missing count from window
                    NEXT_INDEX_PAD = -1
                    temp_next_date = data[self.date_column].iloc[next_index + NEXT_INDEX_PAD]
                    next_delta = expected_date - temp_next_date
                    exclude = int((next_delta.total_seconds())/60/from_frame)-1
                    if window > exclude:
                        window_ = window - exclude
                    else:#else case the next date has no data
                        if not addNan:
                            past_index = get_past_window(temp_next_date, from_frame, to_frame)
                            window_ = past_index - NEXT_INDEX_PAD
                            expected_date = temp_next_date - datetime.timedelta(minutes=from_frame*past_index)
                    break
                else:
                    # datetime over as there is lack of data
                    next_index = next_index + 1
                    next_date = data[self.date_column].iloc[next_index]
                    
            if pre_index == next_index and addNan:
                print(f"{pre_index} has no data")
                expected_date = expected_date - delta
                next_index = pre_index - window_
                Highs.append(numpy.Nan)
                Lows.append(numpy.Nan)
                Opens.append(numpy.Nan)
                Closes.append(numpy.Nan)
                Timestamp.append(expected_date)
            else:
                Highs.append(data[self.columns[0]].iloc[next_index:pre_index].max())
                Lows.append(data[self.columns[1]].iloc[next_index:pre_index].min())
                Opens.append(data[self.columns[2]].iloc[next_index])
                Closes.append(data[self.columns[3]].iloc[pre_index-1])
                Timestamp.append(data[self.date_column].iloc[next_index])

                expected_date = expected_date - delta
                pre_index = next_index
                next_index = pre_index - window_

        rolled_data = pd.DataFrame({self.date_column:Timestamp, self.columns[0]: Highs, self.columns[1]:Lows, self.columns[2]:Opens, self.columns[3]:Closes})
        return rolled_data
    
    def __get_rolled_file_name(self, org_file_name:str, from_frame, to_frame, addNan=False) -> str:
        separator = '.csv'
        names = org_file_name.split(separator)
        if len(names) > 1:
            rolled_file_name = "".join(names)
            rolled_file_name = f"{rolled_file_name}_{from_frame}_{to_frame}"
            if addNan:
                rolled_file_name = f"{rolled_file_name}_withNaN"
            rolled_file_name += separator
            return rolled_file_name
        raise Exception(f"{org_file_name} isn't csv.")

    def __init__(self, file = None, frame: int= Frame.MIN5, provider="bitflayer", out_frame:int=None, columns = ['High', 'Low','Open','Close'], date_column = "Timestamp", seed=1017):
        """CSV Client for bitcoin, etc. currently bitcoin in available only.
        Need to change codes to use settings file
        
        Args:
            file (str, optional): You can directly specify the file name. Defaults to None.
            file_frame (int, optional): You can specify the frame of data. CSV need to exist. Defaults to 5.
            provider (str, optional): Provider of data to load csv file. Defaults to "bitflayer".
            out_frame (int, optional): output frame. Ex F_5MIN can convert to F_30MIN. Defaults to None.
            columns (list, optional): ohlc columns name. Defaults to ['High', 'Low','Open','Close'].
            date_column (str, optional): If specified, time is parsed. Otherwise ignored. Defaults to Timestamp
        """
        random.seed(seed)
        self.args = (file, frame, provider, out_frame, columns, date_column, seed)
        if type(file) == str:
            file = os.path.abspath(file)
        try:
            self.frame = frame
        except Exception as e:
            print(e)
        self.ask_positions = {}
        if file == None:
            file_name = 'files.json'
            with open(file_name, 'r', encoding='utf-8') as f:
                self.files = json.load(f)
        elif type(file) == str:
            self.files = {
                self.frame: file,
                'provider': provider
            }
        else:
            raise Exception(f"unexpected file type is specified. {type(file)}")
        #if provider == "bitflayer":
        self.base_point = 0.01
        self.__read_csv__(columns, date_column)
        self.date_column = date_column
        
        if out_frame != None and frame != out_frame:
            try:
                to_frame = int(out_frame)
            except Exception as e:
                print(e)
            if frame < to_frame:
                source_file = self.files[self.frame]
                rolled_file_name = self.__get_rolled_file_name(source_file, frame, to_frame)
                if os.path.exists(rolled_file_name):
                    rolled_data = pd.read_csv(rolled_file_name)
                    self.data = rolled_data
                else:
                    rolled_data = self.__rolling_frame(self.data.copy(), from_frame=frame, to_frame=to_frame)
                    rolled_data.to_csv(rolled_file_name)
                    self.data = rolled_data
                    self.frame = to_frame
            elif to_frame == frame:
                print("file_frame and frame are same value. row file_frame is used.")
            else:
                raise Exception("frame should be greater than file_frame as we can't decrease the frame.")
            self.out_frames = to_frame
        else:
            self.out_frames = self.frame
        self.__step_index = random.randint(0, len(self.data))
        self.__high_max = self.get_min_max(self.columns[0])[1]
        self.__low_min = self.get_min_max(self.columns[1])[0]

    def get_rates(self, interval=1):
        if interval > 1:
            rates = None
            if self.__step_index >= interval-1:
                try:
                    #return data which have interval length
                    rates = self.data.iloc[self.__step_index - interval+1:self.__step_index+1].copy()
                    return rates
                except Exception as e:
                    print(e)
            else:
                self.__step_index = random.randint(0, len(self.data))
                return self.get_rates(interval)
        elif interval == -1:
            rates = self.data.copy()
            return rates
        else:
            raise Exception("interval should be greater than 0.")

    def get_future_rates(self,interval=1, back_interval=0):
        if interval > 1:
            rates = None
            if self.__step_index >= interval-1:
                try:
                    #return data which have interval length
                    rates = self.data.iloc[self.__step_index - back_interval:self.__step_index+interval+1].copy()
                    return rates
                except Exception as e:
                    print(e)
            else:
                self.__step_index = random.randint(0, len(self.data))
                return self.get_rates(interval)
        else:
            raise Exception("interval should be greater than 0.")
    
    def get_current_ask(self):
        tick = self.data.iloc[self.__step_index]
        mean = (tick.High + tick.Low)/2
        return random.uniform(mean, tick.High)
        

    def get_current_bid(self):
        tick = self.data.iloc[self.__step_index]
        mean = (tick.High + tick.Low)/2
        return random.uniform(tick.Low, mean)
    
    def buy(self, stop_loss, stop_profit, amount):
        boughtCoinRate = self.get_current_ask()
        id = str(uuid.uuid4())
        position = {"price":boughtCoinRate, "step":self.__step_index, "amount":amount, "id":id, "sl": stop_loss, "sp": stop_profit}
        self.ask_positions[id] = position
        return position

    def market_buy(self, amount) -> dict:
        boughtCoinRate = self.get_current_ask()
        id = str(uuid.uuid4())
        position = {"price":boughtCoinRate, "step":self.__step_index, "amount":amount, "id":id}
        self.ask_positions[id] = position
        return position

    def market_sell(self, order) -> dict:
        print("market_sell is not implemented.")
        return None

    def sell_settlement(self, position:dict, point: int=None):
        if position["id"] in self.ask_positions:
            bid = self.get_current_bid()
            bought_rate = position["price"]
            amount = position["amount"]
            if point == None or point >= amount:##sell all amounts
                rate_diff = (bid - position["price"])
                self.ask_positions.pop(position["id"])
            elif point < amount:
                rate_diff = (bid - position["price"])
                id = position["id"]
                remaining_amount = amount - point
                position["amount"] = remaining_amount
                self.ask_positions[id] = position
            else:
                raise Exception("amount should be int")
            
            pl = rate_diff * point * self.base_point
            
            return bid, bought_rate, rate_diff, pl
        else:
            return None, None, None
        
    def buy_settlement(self, position):
        print("buy_settlement is not implemented.")
        pass

    def sell_all_settlement(self):
        results = []
        bid = self.get_current_bid()
        for key, position in self.ask_positions.items():
            rate_diff = (bid - position["price"])
            pl = rate_diff * position["amount"] * self.base_point
            results.append(
                (bid, position["price"], rate_diff, pl)
                )
        self.ask_positions = {}
        return results
        
    def close(self):
        pass

    def reset(self, mode:str = None, retry=0)-> bool:
        self.ask_positions = {}#ignore if there is position
        self.__step_index = random.randint(0, len(self.data))
        if mode != None:
            if retry <= 10:
                if mode == "day":
                    current_date = self.data[self.date_column].iloc[self.__step_index]
                    day_minutes = 60*24
                    total_minutes = current_date.minute + current_date.hour*60
                    add_minutes = day_minutes - total_minutes
                    add_index = add_minutes/self.frame
                    if self.__step_index + add_index >= len(self.data):
                        self.reset(mode, retry+1)
                    else:
                        candidate_index = self.__step_index + add_index
                        current_date = self.data[self.date_column].iloc[candidate_index]
                        additional_index = (current_date.hour*60 + current_date.minute)/self.frame
                        if additional_index != 0:
                            candidate_index = candidate_index - additional_index
                            current_date = self.data[self.date_column].iloc[candidate_index]
                            total_minutes = current_date.minute + current_date.hour*60
                            if total_minutes != 0:#otherwise, this day may have 1day data
                                self.reset(mode, retry+1)
            else:
                print("Warning: data client reset with {mode} mode retried over 10. index was not correctly reset.")
                return False
        return True
                                

    def get_holding_steps(self, position="ask"):
        steps_diff = []
        for key, ask_position in self.ask_positions.items():
            steps_diff.append(self.__step_index - ask_position["step"])
        return steps_diff

    def get_next_tick(self):
        if self.__step_index < len(self.data)-2:
            self.__step_index += 1
            tick = self.data.iloc[self.__step_index]
            return tick, False
        else:
            self.__step_index = random.randint(0, len(self.data))
            tick = self.data.iloc[self.__step_index]
            return tick, True
        
    def max(self, column):
        if column in self.data.columns:
            return self.data[column].max()
        else:
            raise ValueError(f"{column} is not defined in {self.data.columns}")
            return None
        
    def min(self, column):
        if column in self.data.columns:
            return self.data[column].min()
        else:
            raise ValueError(f"{column} is not defined in {self.data.columns}")
        
    def __getitem__(self, ndx):
        return self.data.iloc[ndx]
        
    def get_min_max(self, column, data_length = 0):
        if column in self.data.columns:
            if data_length == 0:
                return self.data[column].min(), self.data[column].max()
            else:
                if data_length > 0:
                    target_df = self.data[column].iloc[self.__step_index:self.__step_index + data_length]
                else:
                    target_df = self.data[column].iloc[self.__step_index + data_length + 1:self.__step_index +1]
                return target_df.min(), target_df.max()
        else:
            raise ValueError(f"{column} is not defined in {self.data.columns}")
        
    def get_current_index(self):
        return self.__step_index
    
    def get_ohlc_columns(self):
        columns = {}
        for column in self.data.columns.values:
            column_ = str(column).lower()
            if column_ == 'open':
                columns['Open'] = column
            elif column_ == 'high':
                columns['High'] = column
            elif column_ == 'low':
                columns['Low'] = column
            elif column_ == 'close':
                columns['Close'] = column
        return columns
    
    def get_diffs(self, position='ask') -> list:
        if position == 'ask':
            if len(self.ask_positions) > 0:
                amounts = []
                current_bid = self.get_current_bid()
                for key, position in self.ask_positions.items():
                    amounts.append(current_bid - position['price'])
                return amounts
            else:
                return [0.0]
        else:
            print(f'this is not implemented in get_diff_amount with position: {position}')
        
    ##minimux should be done in this class?
    def get_diffs_with_minmax(self, position='ask')-> list:
        if position == 'ask':
            if len(self.ask_positions) > 0:
                amounts = []
                current_bid = self.get_current_bid()
                current_normalized_bid = standalization.mini_max(current_bid, self.__low_min, self.__high_max, (0, 1))
                for key, position in self.ask_positions.items():
                    normalized_price = standalization.mini_max(position['price'], self.__low_min, self.__high_max, (0, 1))
                    amounts.append((current_normalized_bid - normalized_price) * position["amount"])
                return amounts
            else:
                return [0.0]
        else:
            print(f'this is not implemented in get_diff_amount with position: {position}')
            
    def get_positions(self, kinds=None):
        positions = []
        if kinds == None:
            pass
        elif kinds == 'ask':
            #for key, position in self.ask_positions.items():
            #    positions.append(position)
            return self.ask_positions
    
    def get_params(self) -> dict:
        param = {
            'type':self.kinds,
            'provider': self.files,
            'source_frame': self.frame,
            'out_frame': self.out_frames
        }
        
        return param

class MultiFrameClient(CSVClient):
    
    kinds = "multi_csv"
    
    def __init__(self, file=None, frame: int = Frame.MIN5, provider="bitflayer", columns=['High', 'Low', 'Open', 'Close'], out_frames = [Frame.MIN30, Frame.H1], date_column="Timestamp", seed=1017):
        out_frame =None
        super().__init__(file, frame, provider, out_frame, columns, date_column, seed)
        self.args = (file, frame, provider, out_frame, columns, out_frames, date_column, seed)
        self.out_frames = out_frames
        
    def get_ohlc_columns(self):
        columns = {}
        for column in self.data.columns.values:
            column_ = str(column).lower()
            if column_ == 'open':
                columns['Open'] = [f'{str(frame)}_column' for frame in self.out_frames]
            elif column_ == 'high':
                columns['High'] = column
            elif column_ == 'low':
                columns['Low'] = column
            elif column_ == 'close':
                columns['Close'] = column
        return columns