from AlphaVantage import AlphaVantage

import os
import pandas as pd
import sqlite3


class SqlQuery:

    def __init__(self, path):
        self.conn = sqlite3.connect(path)

    def read(self, sql):
        return pd.read_sql(sql, self.conn)


symbol_list = ['TSX:NFI']
a = AlphaVantage(symbol_list, 'prior', 'VWXATT8K62KW1GZH')

directory_db = '\\'.join(os.getcwd().split('\\')[0:-1])
filename_db = 'db.sqlite3'
path_db = directory_db + '\\' + filename_db

sql = 'SELECT marketsymbol, id FROM stockindex_app_stock WHERE inactive = \'False\''
q = SqlQuery(path_db)
print(q.read(sql))


"""
unused code 



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

# path to text file for output
directory_output = os.getcwd()
filename_output = 'windex.txt'
path_output = directory_output + '\\' + filename_output

# populate output file with data
with open(path_output, 'w') as f:
  for dict_key in stock_dict.keys():
      f.write(str(stock_dict[dict_key]))

"""
