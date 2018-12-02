import datetime
import os
import pandas as pd
import requests
import sqlite3
import time


class AlphaVantage:

    cooldown_time = 12
    output_size = 'compact'

    def __init__(self, stock_symbol, alpha_function, api_key):
        self.api_key = api_key
        self.function = alpha_function.upper()
        if type(stock_symbol) is list:
            self.symbol = stock_symbol
        if type(stock_symbol) is str:
            self.symbol = [stock_symbol]

    def get(self):
        # create an empty list
        stock_observation_list = []

        # fill the list with records for each symbol
        for current_symbol in self.symbol:
            if self.function == 'PRIOR_DAY':
                stock_observation_list.append(self.prior_day(current_symbol))
        time.sleep(self.cooldown_time)

    def prior_day(self, lookup_symbol):
        api_function = 'TIME_SERIES_DAILY'

        # define where the previous day stock closing price will be found in the json from Alpha Vantage's API
        observation_date = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime('%Y-%m-%d')
        dict_level_1 = 'Time Series (Daily)'
        dict_level_2 = observation_date

        # call alpha vantage API
        url = f'https://www.alphavantage.co/query?' \
              f'function={api_function}&' \
              f'symbol={lookup_symbol}&' \
              f'outputsize={self.output_size}&' \
              f'apikey={self.api_key}'
        r = requests.get(url)

        # load the result into a tuple
        data = r.json()
        stock_tuple = (
            lookup_symbol,
            observation_date,
            data[dict_level_1][dict_level_2]['5. volume'],
            data[dict_level_1][dict_level_2]['1. open'],
            data[dict_level_1][dict_level_2]['2. high'],
            data[dict_level_1][dict_level_2]['3. low'],
            data[dict_level_1][dict_level_2]['4. close'],
        )

        return stock_tuple



class SqlQuery:

    directory_db = '\\'.join(os.getcwd().split('\\')[0:-1])
    filename_db = 'db.sqlite3'
    path_db = directory_db + '\\' + filename_db

    def __init__(self, sql):
        self.conn = sqlite3.connect(self.path_db)
        self.result = pd.read_sql(sql, self.conn)


class Observation:

    def __init__(self):
        self.stock_symbols = self.get_active_stock_symbols()

    @staticmethod
    def get_active_stock_symbols():

        # generate two lists: active stock symbols and their primary keys
        sql = 'SELECT marketsymbol, id FROM stockindex_app_stock WHERE inactive = \'False\''
        qry = SqlQuery(sql)
        marketsymbol_list = qry.result['marketsymbol'].tolist()
        id_list = qry.result['id'].tolist()

        # load active stock symbols and primary keys into a dictionary
        stock_symbols = {}
        for n in range(0, len(marketsymbol_list)):
            stock_symbols[marketsymbol_list[n]] = id_list[n]
        return stock_symbols

"""
# path to text file for output
directory_output = os.getcwd()
filename_output = 'windex.txt'
path_output = directory_output + '\\' + filename_output

# populate output file with data
with open(path_output, 'w') as f:
  for dict_key in stock_dict.keys():
      f.write(str(stock_dict[dict_key]))
      
      VWXATT8K62KW1GZH
"""