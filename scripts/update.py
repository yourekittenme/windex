from AlphaVantage import AlphaVantage
from SqlQuery import SqlQuery
import pandas as pd


class Observation:

    columns_list = ['marketsymbol', 'observation_date', 'volume', 'high_price',
                    'low_price', 'open_price', 'close_price']

    def __init__(self, obs_records):
        self.df = pd.DataFrame.from_records(obs_records, columns=self.columns_list)

    def get_symbol_fk(self):

        sql = 'SELECT marketsymbol, id FROM stockindex_app_stock WHERE inactive = \'False\''
        q = SqlQuery()
        df_symbol = q.read(sql)

        self.df = pd.merge(df_symbol, self.df, on='marketsymbol')
        self.df.drop(columns='marketsymbol', inplace=True)
        self.df.rename(columns={'id': 'stock_id'}, inplace=True)

    def write(self):
        db_table = 'stockindex_app_observations'
        q = SqlQuery()
        q.write(self.df, db_table)


test_records = [('TSX:BUI', '2018-12-03', '0', '3.6900', '3.6900', '3.6900', '3.6900'),
                ('TSX:BYD-UN', '2018-12-03', '83073', '115.9700', '121.2100', '114.1700', '118.7800'),
                ('TSX:GWO', '2018-12-03', '725300', '30.5500', '30.5900', '29.8400', '30.0000'),
                ('TSX:IGM', '2018-12-03', '328800', '34.2600', '34.8400', '34.1900', '34.8400'),
                ('TSX:PBL', '2018-12-03', '6200', '22.1000', '22.3700', '22.0100', '22.3700'),
                ('TSX:NFI', '2018-12-03', '448300', '38.2100', '38.4200', '37.1800', '37.3600'),
                ('TSX:NWC', '2018-12-03', '76200', '29.2900', '29.4400', '28.8100', '29.3000'),
                ('TSX:AFN', '2018-12-03', '18400', '54.4400', '54.4400', '53.3400', '53.6300'),
                ('TSX:EIF', '2018-12-03', '67300', '31.3000', '31.4800', '30.4500', '30.9900')]

if __name__ == "__main__":

    o = Observation(test_records)
    o.get_symbol_fk()
    o.write()
    print(o.df.head())
"""

a = AlphaVantage(symbol_list, 'prior', 'VWXATT8K62KW1GZH')

unused code 

# path to text file for output
directory_output = os.getcwd()
filename_output = 'windex.txt'
path_output = directory_output + '\\' + filename_output

# populate output file with data
with open(path_output, 'w') as f:
  for dict_key in stock_dict.keys():
      f.write(str(stock_dict[dict_key]))

"""
