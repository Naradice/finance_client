import matplotlib.pyplot as plt
import math
import numpy
import pandas as pd

class Rendere:
    
    def __init__(self):
        plt.ion()# plots are updated after every plotting command.
        self.plot_num = 1
        self.shape = (1,1)
        self.__data = {}
        self.__is_shown = False
        
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
    
    def __register_data(self, type_:str, data, title:str, index:int, options:dict = None):
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
        self.__data[index_] = {'type': type_, 'data':data, 'title':title}
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
            
    def append_ohlc(self, tick, index:int):
        """
        add ohlc tick to existing data for plotting it later

        Args:
            tick (pd.DataFrame): ohlc data. Assume to have same columns with existing data
            index (int): index of subplot to plot the data. use greater than 1 to specify subplot index. use -1 to plot on last. Defaults to -1.
        """
        if index in self.__data:
            ohlc = self.__data[index]["data"]
            ohlc = pd.concat([ohlc, pd.DataFrame.from_records([tick.to_dict()])])
        else:
            ohlc = tick
            self.__data[index] = {'type': 'ohlc'}
        self.__data[index]["data"] = ohlc
    
    def update_ohlc(self, ohlc, index):
        """ update data of the index

        Args:
            ohlc (_type_): _description_
            index (_type_): _description_
        """
        if index in self.__data:
            self.__data[index].update({'data': ohlc})
        else:
            print("index not in the data")
    
    def register_ohlc(self, ohlc, index=-1,title='OHLC Candle',open='Open',high = 'High', low='Low', close='Close'):
        """
        register ohlc to plot later
        Args:
            ohlc (DataFrame): 
            index (int, optional): index of subplot to plot the data. use greater than 1 to specify subplot index. use -1 to plot on last. Defaults to -1.
            open (str, optional): Column name of Open. Defaults to 'Open'.
            high (str, optional): Column name of High. Defaults to 'High'.
            low (str, optional): Column name of Low. Defaults to 'Low'.
            close (str, optional): Column name of Close. Defaults to 'Close'.
            time_column (str, optional): Column name of datetime. Default to None
        """
        return self.__register_data('ohlc', ohlc, title, index, {'columns': (open, high, low, close)})
     
    def register_indicater_to_overlap(self, data, time_column, index=-1, title=None, columns:list=None, chart_type:str="line"):
        pass
    
    def register_indicater(self):
        pass
    
    def __plot_candle(self, index, content):
        
        ohlc = content['data']
        open = content['columns'][0]
        high = content['columns'][1]
        low = content['columns'][2]
        close = content['columns'][3]
        
        x = numpy.arange(0, len(ohlc))
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
        ax.clear()
        index = 0
        for idx, val in ohlc.iterrows():
            color = '#2CA453'
            if val[open] > val[close]: color= '#F04730'
            ax.plot([x[index], x[index]], [val[low], val[high]], color=color)
            ax.plot([x[index], x[index]-0.1], [val[open], val[open]], color=color)
            ax.plot([x[index], x[index]+0.1], [val[close], val[close]], color=color)
            index += 1
    
    def __plot__xy(self, index, content):
        if index > -1 and self.__check_subplot(index):
            column = int(index/self.shape[0])
            row = index % self.shape[0]
        else:
            column = self.shape[0]
            row = self.shape[1]
        x, y = content['data']
        if self.__check_subplot_axis():
            ax = self.axs[column][row]
        else:
            if self.plot_num == 1:
                ax = self.axs
            else:
                ax = self.axs[row]
        ax.clear()
        ax.plot(y,x)
        
    def __plot(self):
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
        plt.pause(0.01)
        #plt.savefig("img.png")
    
    def plot_async(self):
        pass
        
    def plot(self):
        self.__plot()
        
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