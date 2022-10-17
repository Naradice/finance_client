#from finance_client.render.graph import Rendere
from copy import copy
import matplotlib.pyplot as plt
import math
import numpy
import pandas as pd
from finance_client import utils

class Rendere:
    
    def __init__(self):
        plt.ion()# plots are updated after every plotting command.
        self.plot_num = 1
        self.shape = (1,1)
        self.__data = {}
        self.__is_shown = False
        self.__indicater_process_info = {
            utils.BBANDpreProcess.kinds : {
                    "function": self.overlap_bolinger_band,
                    "columns":("MV", "Width"),
                    "option": ('alpha',)
                }
        }
        
    def add_subplot(self):
        self.plot_num += 1
        if self.plot_num > 2:
            line_num = int(math.sqrt(self.plot_num))
            remain = self.plot_num - line_num**2
            if remain != 0:
                line_num += 1
            self.shape = (line_num, line_num)
        else:
            self.shape = (self.plot_num,1)
    
    def add_subplots(self, num:int):
        for i in range(num):
            self.add_subplot()
    
    def __check_subplot(self, index):
        amount = 0
        if len(self.shape) == 2:
            amount = self.shape[0] * self.shape[1]
        elif len(self.shape) == 1:
            amount = self.shape[0]
        else:
            raise Exception(f"unexpected shape: {self.shape}")
        
        if index < amount:
            return True
        else:
            return False
        
    def __check_subplot_axis(self):
        """check if axis is two or more 

        Raises:
            Exception: shape should be smaller than 2 dim

        Returns:
            boolean: True means multiple axises. False means one axis
        """
        if len(self.shape) == 2:
            if self.shape[0]  == 1 or self.shape[1] == 1:
                return False
            else:
                return True
        elif len(self.shape) == 1:
            return False
        else:
            raise Exception(f"unexpected shape: {self.shape}")
        
    def __get_minmax_index(self):
        indices = numpy.array([])
        if len(self.__data) > 0:
            for index in self.__data:
                indices = numpy.append(indices, index)
            return indices.min(), indices.max()
        return -1, -1
    
    def __get_nextempy_index(self):
        next_empty_index = -1
        if len(self.__data) > 0:
            for i in range(0, self.plot_num):
                if i in self.__data:
                    continue
                next_empty_index = i
                break
        else:
            #return index of first subplot
            next_empty_index = 0
        return next_empty_index
    
    def __register_data(self, symbols:list, type_:str, data, title:str, index:int, options:dict = None):
        if index == -1:
            index_ = self.__get_nextempy_index()
            if index_ == -1:
                self.add_subplot()
                index_ = self.plot_num - 1#index start with 0. plot_num start with 1
        else:
            index_ = index
            #noisy
            #if len(self.__data) > 0 and index_ in self.__data:
            #    print("Warning: specified index will overwrite data in register_{type_}: {index_}")
        self.__data[index_] = {'type': type_, 'data':data, 'title':title, 'symbols': symbols}
        # store additional params
        if options != None:
            for key, content in options.items():
                self.__data[index_][key] = content
        return index_
        
    def register_xy(self, x:list, y:list, title:str=None, index=-1):
        """
        register (x,y) data to plot later

        Args:
            x (list): x-axis data
            y (list): y-axis data
            index (int, optional): index of subplot to plot the data. use greater than 1 to specify subplot index. use -1 to plot on fisrt empty subplot. Defaults to -1.
        """
        self.__register_data('xy', (x,y), title=title, index=index)
    
    def append_x(self, x, index:int):
        """
        add x of (x,y) to plot later

        Args:
            x (int|float): x-axis data. It will be appended
            index (int, optional): index of subplot to plot the data. use greater than 1 to specify subplot index. use -1 to plot on last. Defaults to -1.
        """
        if index in self.__data:
            x_, y_ = self.__data[index]["data"]
            x_.append(x)
            y = y_[-1] + 1
            y_.append(y)
            data = (x_, y_)
        else:
            data = ([x], [0])
            self.__data[index] = {'type': 'xy'}
        self.__data[index]["data"] = data
        
    def __tensor_to_dataframe(self, tensor, columns=None):
        data = copy(tensor)
        if type(data) is not pd.DataFrame:
            if data.is_cuda:
                data = data.cpu().detach().numpy()
            else:
                data = pd.DataFrame(data).astype("float")
                
            data = pd.DataFrame(data)
            if columns is not None:
                if data.shape == (len(columns), 1):
                    data = data.T
                data.columns = columns
                
        return data
            
    def append_ohlc(self, ticks, index:int):
        """
        add ohlc ticks to existing data for plotting it later

        Args:
            ticks (pd.DataFrame): ohlc data. Assume to have same columns with existing data
            index (int): index of subplot to plot the data. use greater than 1 to specify subplot index. use -1 to plot on last. Defaults to -1.
        """
        if index in self.__data:
            ohlc = self.__data[index]["data"]
            if ticks is not pd.DataFrame:
                columns = self.__data[index]["columns"]
                ticks = self.__tensor_to_dataframe(ticks, columns)
            ohlc = pd.concat([ohlc, ticks])
            self.__data[index]["data"] = ohlc
            history = self.__data[index]["trade_history"]
            history.append([])
            self.__data[index]["trade_history"] = history
        else:
            ohlc = ticks
            self.register_ohlc(ohlc, index)
        
    def append_ohlc_predictions(self, ohlc, index:int):
        """add prediction ohlc widh thin color

        Args:
            tick (pd.DataFrame | torch.Tensor): ohlc data. Assume to have same columns with existing data
            index (int): index of subplot to plot the data. use greater than 1 to specify subplot index. use -1 to plot on last. Defaults to -1.
        """
        
        if index in self.__data:
            if ohlc is not pd.DataFrame:
                columns = self.__data[index]["columns"]
                ohlc = self.__tensor_to_dataframe(ohlc, columns)
            self.__data[index]["predictions"] = ohlc
        else:
            print(f"index doesn't exist in plot data.")
    
    def update_ohlc(self, ohlc, index):
        """ update data of the index

        Args:
            ohlc (_type_): _description_
            index (_type_): _description_
        """
        if index in self.__data:
            self.__data[index].update({'data': ohlc})
            history = self.__data[index]["trade_history"]
            history.append([])
            history = history[1:]
            self.__data[index]["trade_history"] = history
        else:
            print("index not in the data")
    
    def register_ohlc(self, symbols:list, ohlc:pd.DataFrame, index=-1, title='OHLC Candle', ohlc_columns=('Open', 'High', 'Low', 'Close')):
        """
        register ohlc to plot later
        Args:
            ohlc (DataFrame): column order should be Open, High, Low, Close
            index (int, optional): index of subplot to plot the data. use greater than 1 to specify subplot index. use -1 to plot on last. Defaults to -1.
            ohlc_columns (tuple|list, optional): ohlc colmun names. Defaults to ('Open', 'High', 'Low', 'Close')
        """
        ohlc_columns = ohlc_columns
        if type(ohlc) == pd.DataFrame:
            consistent = set(ohlc.columns) & set(ohlc_columns)
            if len(consistent) < 4:
                print(f"{ohlc_columns} is not found in the data. try to extruct them from the data.")
                for column in ohlc.columns:
                    c = column.lower()
                    if 'open' in c:
                        open = column
                    elif 'high' in c:
                        high = column
                    elif 'low' in c:
                        low = column
                    elif 'close' in c:
                        close = column
                ohlc_columns = (open, high, low, close)
        else:
            raise TypeError(f"only dataframe is available as ohlc for now.")
        idx = self.__register_data(symbols,'ohlc', ohlc, title, index, {'columns': ohlc_columns})
        self.__data[idx]["trade_history"] = [[] for i in  range(0, len(ohlc))]
        return idx

    def register_ohlc_with_indicaters(self, symbols:list, data:pd.DataFrame, idc_processes:list, index=-1, title='OHLC Candle', ohlc_columns=('Open', 'High', 'Low', 'Close')):
        idx = self.register_ohlc(data, symbols, index=index, title=title, ohlc_columns=ohlc_columns)
        idc_plot_processes = []
        for idc in idc_processes:
            if idc.kinds in self.__indicater_process_info:
                process_info = self.__indicater_process_info[idc.kinds]
                plot_func = process_info["function"]
                column_keys = process_info["columns"]
                columns = tuple(idc.columns[key] for key in column_keys)
                option_keys = process_info["option"]
                options = tuple(idc.option[key] for key in option_keys)
                
                idc_plot_processes.append(
                    (plot_func, columns, options)
                )
            else:
                print(f"{idc.kinds} is not supported for now.")
        if len(idc_plot_processes) > 0:
            self.__data[idx]["indicaters"] = idc_plot_processes
        return idx
    
    def add_trade_histories_to_ohlc(self, positions:list, prices:list, index:int):
        pass
    
    def add_trade_history_to_latest_tick(self, position:int, price:float, index:int):
        if index in self.__data:
            histories = self.__data[index]["trade_history"]
            last_trade = histories[-1]
            last_trade.append([position, price])
            histories[-1] = last_trade
            self.__data[index]["trade_history"] = histories
        else:
            print(f"{index} is not registered.")
    
    def overlap_bolinger_band(self, data, index, mean_column, width_column=None, alpha=2, std_column=None):
        ax = self.__get_ax(index)
        x = numpy.arange(0, len(data))
        std = data[width_column]/alpha
        y1 = data[mean_column] + std
        y2 = data[mean_column] - std
        ax.plot(x, data[mean_column])
        ax.fill_between(x, y1, y2, alpha=0.4)
        y1 = data[mean_column] + std*2
        y2 = data[mean_column] - std*2
        ax.fill_between(x, y1, y2, alpha=0.2)

    def overlap_indicater(self, data, time_column, index, columns:list=None, chart_type:str="line"):
        pass
    
    def register_indicater(self):
        pass
    
    def __get_ax(self, index):
        if index > -1 and self.__check_subplot(index):
            column = int(index/self.shape[0])
            row = index % self.shape[0]
        else:
            column = self.shape[0]
            row = self.shape[1]
        if self.__check_subplot_axis():
            ax = self.axs[column][row]
        else:
            if self.plot_num == 1:
                ax = self.axs
            else:
                ax = self.axs[row]
        return ax
    
    def __plot_candle(self, index, content):
        
        ohlc = content['data']
        open = content['columns'][0]
        high = content['columns'][1]
        low = content['columns'][2]
        close = content['columns'][3]
        title = content['title']
        symbols = content['symbols']
        
        do_plot_prediction = False
        if "predictions" in content:
            prediction = content['predictions']
            do_plot_prediction = True
        
        x = numpy.arange(0, len(ohlc))
        
        ax = self.__get_ax(index)
        ax.clear()
        ax.set_title(title)
        
        if "indicaters" in content:
            for idc_plot_info in content["indicaters"]:
                func, columns, option = idc_plot_info
                func(ohlc, index, *columns, *option)
        index = 0
        for idx, val in ohlc.iterrows():
            color = '#2CA453'
            if val[open] > val[close]: color= '#F04730'
            ax.plot([x[index], x[index]], [val[low], val[high]], color=color)
            ax.plot([x[index], x[index]-0.1], [val[open], val[open]], color=color)
            ax.plot([x[index], x[index]+0.1], [val[close], val[close]], color=color)
            index += 1
        
        index = 0
        if "trade_history" in self.__data[index]:
            histories = self.__data[index]["trade_history"]
            if len(histories) == len(ohlc):
                for history in histories:
                    for trade in history:
                        marker = 'o'
                        color  = '#0000FF'
                        if trade[0] == 1:#buy
                            marker = '^'
                            color  = '#0000FF'
                        elif trade[0] == -1:#buy to close
                            marker = '^'
                            color  = '#7700FF'
                        elif trade[0] == 2:#sell
                            marker = 'v'
                            color = '#FF0077'
                        elif trade[0] == -2:#sell to close
                            marker = 'v'
                            color  = '#7700FF'
                        ax.plot(x[index], trade[1], marker, color=color)
                    index += 1
            else:
                print(f"history length: {len(histories)} is different from data length {len(ohlc)}")
        if do_plot_prediction:
            p_id = 1
            index = index - 1
            for idx, val in prediction.iterrows():
                color = '#b4f4b2'
                if val[open] > val[close]: color= '#ff9395'
                ax.plot([x[index]+p_id, x[index]+p_id], [val[low], val[high]], color=color)
                ax.plot([x[index]+p_id, x[index]+p_id-0.1], [val[open], val[open]], color=color)
                ax.plot([x[index]+p_id, x[index]+p_id+0.1], [val[close], val[close]], color=color)
                p_id += 1
    
    def __plot__xy(self, index, content):
        ax = self.__get_ax(index)
        x, y = content['data']
        ax.clear()
        ax.plot(y,x)
        
    def __plot(self, file_name:str=None):
        if self.__is_shown == False:
            self.__is_shown = True
            self.fig, self.axs = plt.subplots(*self.shape)
            self.fig.show()
        for index, content in self.__data.items():
            data_type = content['type']
            if  data_type == 'ohlc':
                self.__plot_candle(index, content)
            elif data_type == 'xy':
                self.__plot__xy(index, content)
            else:
                raise Exception(f'unexpected type was specified in {index}: {data_type}')
        #self.fig.canvas.draw()
        if file_name is not None and type(file_name) is str:
            plt.savefig(file_name+".png")
        plt.pause(0.01)
    
    def plot_async(self):
        pass
        
    def plot(self, file_name:str=None):
        self.__plot(file_name)
        
    def write_image(self, file_name):
        try:
            plt.savefig(file_name)
        except Exception as e:
            print(e)
            

    def line_plot(data: pd.Series, window = 10, save=False, file_name:str = None):
        if type(data) == list:
            data = pd.Series(data)
        mean = data.rolling(window).mean()
        var = data.rolling(window).var()
        up = mean + var
        down = mean - var
        plt.plot(data)
        plt.plot(mean)
        plt.fill_between(data.index, down, up, alpha=0.5)
        if save:
            if file_name == None:
                file_name = 'line_plot.png'
            try:
                plt.savefig(file_name)
            except Exception as e:
                print('skipped save as ', e)
        plt.show()