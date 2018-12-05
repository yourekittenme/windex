from AlphaVantage import AlphaVantage

import os
import pandas as pd
import sqlite3


class SqlQuery:

    def __init__(self, path):
        self.conn = sqlite3.connect(path)

    def read(self, sql):
        return pd.read_sql(sql, self.conn)

    def write(self, df, db_table):
        pd.DataFrame.to_sql(df, db_table, self.conn, if_exists='append', index=False)


directory_db = '\\'.join(os.getcwd().split('\\')[0:-1])
filename_db = 'db.sqlite3'
path_db = directory_db + '\\' + filename_db

sql = 'SELECT marketsymbol, id FROM stockindex_app_stock WHERE inactive = \'False\''
q = SqlQuery(path_db)
df_symbol = q.read(sql)

"""

a = AlphaVantage(symbol_list, 'prior', 'VWXATT8K62KW1GZH')
"""


test_records = [('TSX:BUI', '2018-12-03', '0', '3.6900', '3.6900', '3.6900', '3.6900'),
                ('TSX:BYD-UN', '2018-12-03', '83073', '115.9700', '121.2100', '114.1700', '118.7800'),
                ('TSX:GWO', '2018-12-03', '725300', '30.5500', '30.5900', '29.8400', '30.0000'),
                ('TSX:IGM', '2018-12-03', '328800', '34.2600', '34.8400', '34.1900', '34.8400'),
                ('TSX:PBL', '2018-12-03', '6200', '22.1000', '22.3700', '22.0100', '22.3700'),
                ('TSX:NFI', '2018-12-03', '448300', '38.2100', '38.4200', '37.1800', '37.3600'),
                ('TSX:NWC', '2018-12-03', '76200', '29.2900', '29.4400', '28.8100', '29.3000'),
                ('TSX:AFN', '2018-12-03', '18400', '54.4400', '54.4400', '53.3400', '53.6300'),
                ('TSX:EIF', '2018-12-03', '67300', '31.3000', '31.4800', '30.4500', '30.9900')]

columns_list = ['marketsymbol','observation_date','volume','high_price','low_price','open_price','close_price']

df_alpha = pd.DataFrame.from_records(test_records, columns=columns_list)
df_merge = pd.merge(df_symbol, df_alpha, on='marketsymbol')
df_merge.drop(columns='marketsymbol', inplace=True)
df_merge.rename(columns={'id': 'stock_id'}, inplace=True)


q.write(df_merge, 'stockindex_app_observations')

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
