import datetime
from datetime import timedelta
from AlphaVantage import AlphaVantage
from SqlQuery import SqlQuery
from TmxMoney import TmxMoney
import pandas as pd


class Observation:

    columns_list = ['marketsymbol', 'observation_date', 'volume', 'high_price',
                    'low_price', 'open_price', 'close_price']

    def __init__(self, obs_records):
        self.df = pd.DataFrame.from_records(obs_records, columns=self.columns_list)

    def get_stock_fk(self):

        sql = 'SELECT marketsymbol, id FROM stockindex_app_stock WHERE inactive = 0'
        q = SqlQuery()
        df_symbol = q.read(sql)

        self.df = pd.merge(df_symbol, self.df, on='marketsymbol')
        self.df.drop(columns='marketsymbol', inplace=True)
        self.df.rename(columns={'id': 'stock_id'}, inplace=True)

    def write(self):
        db_table = 'stockindex_app_observations'
        q = SqlQuery()
        q.write(self.df, db_table)


class Stock:

    def __init__(self):
        sql = 'SELECT * FROM stockindex_app_stock WHERE inactive = 0'
        q = SqlQuery()
        self.df = q.read(sql)

    def update_price(self, df_obs):
        self.df['prior_close_price'] = self.df['current_price']
        self.df.drop(['current_price', 'high_price', 'low_price'], axis=1, inplace=True)
        df_obs = df_obs[['stock_id', 'close_price', 'high_price', 'low_price']]
        self.df = pd.merge(self.df, df_obs, how='inner', left_on='id', right_on='stock_id')
        self.df.rename(columns={'close_price': 'current_price'}, inplace=True)
        self.df['change_price'] = self.df['current_price'] - self.df['prior_close_price']
        self.df.drop('stock_id', axis=1, inplace=True)

    def update_52_week_highlow(self):
        year_ago = datetime.datetime.now() - datetime.timedelta(weeks=52)
        sql = 'SELECT stock_id, max(high_price) high_price_52_weeks, min(low_price) low_price_52_weeks ' \
              'FROM stockindex_app_observations GROUP BY stock_id'
        q = SqlQuery()
        df_highlow = q.read(sql)
        self.df.drop(['high_price_52_weeks', 'low_price_52_weeks'], axis=1, inplace=True)
        self.df = pd.merge(self.df, df_highlow, how='inner', left_on='id', right_on='stock_id')
        self.df.drop('stock_id', axis=1, inplace=True)

    def calculate_mktcap(self):
        shares_out_list = [x.replace('-', '.') for x in self.df['symbol'].tolist()]
        tmx = TmxMoney(shares_out_list)
        df_shares_out = tmx.get_shares_outstanding()
        self.df.drop(['shares_outstanding'], axis=1, inplace=True)
        self.df = pd.merge(self.df, df_shares_out, how='inner', left_on='symbol', right_on='symbol')
        self.df['market_cap'] = self.df['current_price'] * self.df['shares_outstanding']
        print(self.df.to_records())

    def write(self):
        db_table = 'stockindex_app_stock'
        q = SqlQuery()
        q.write(self.df, db_table)


def get_mktsymbol_list():
    sql = 'SELECT marketsymbol FROM stockindex_app_stock WHERE inactive = 0'
    q = SqlQuery()
    symbol_list = q.read(sql)['marketsymbol'].tolist()
    return symbol_list


if __name__ == "__main__":

    test_records = [('TSX:BUI', '2018-12-03', 0, 3.6900, 3.6900, 3.6900, 3.6900),
                    ('TSX:BYD-UN', '2018-12-03', 83073, 115.9700, 121.2100, 114.1700, 118.7800),
                    ('TSX:GWO', '2018-12-03', 725300, 30.5500, 30.5900, 29.8400, 30.0000),
                    ('TSX:IGM', '2018-12-03', 328800, 34.2600, 34.8400, 34.1900, 34.8400),
                    ('TSX:PBL', '2018-12-03', 6200, 22.1000, 22.3700, 22.0100, 22.3700),
                    ('TSX:NFI', '2018-12-03', 448300, 38.2100, 38.4200, 37.1800, 37.3600),
                    ('TSX:NWC', '2018-12-03', 76200, 29.2900, 29.4400, 28.8100, 29.3000),
                    ('TSX:AFN', '2018-12-03', 18400, 54.4400, 54.4400, 53.3400, 53.6300),
                    ('TSX:EIF', '2018-12-03', 67300, 31.3000, 31.4800, 30.4500, 30.9900)]

    o = Observation(test_records)
    o.get_stock_fk()
    s = Stock()
    s.update_price(o.df)
    s.update_52_week_highlow()
    s.calculate_mktcap()

test_records_update = [(0, 1, 'BUI', 'Buhler Industries',   3.69,  0, 0, 9.22500000e+07, 'TSX', 'TSX:BUI', '',
                        3.69,  3.69,  3.69, 3.94, 3.53, 25000000),
                     (1, 2, 'BYD-UN', 'The Boyd Group', 111.1 ,  7.68, 0, 2.33681536e+09, 'TSX', 'TSX:BYD-UN', '',
                      118.78, 115.97, 121.21, 133, 102.59,  19673475),
                     (2, 3, 'GWO', 'Great West Life Company',  27.87,  2.13, 0, 2.96515036e+10, 'TSX', 'TSX:GWO', '',
                      30,  30.55,  30.59, 33.1,  28.37, 988383452),
                     (3, 4, 'IGM', 'IGM Financial',  22.37, 12.47, 0, 8.39154568e+09, 'TSX', 'TSX:IGM', '',
                      34.84,  34.26,  34.84, 39.95, 31.54, 240859520),
                     (4, 5, 'PBL', 'Pollard Banknote',  19.53,  2.84, 0, 5.73245969e+08, 'TSX', 'TSX:PBL', '',
                      22.37,  22.1,  22.37, 27.75,  19.91,  25625658),
                     (5, 6, 'NFI', 'New Flyer Industries',  34.31,  3.05, 0, 2.32951738e+09, 'TSX', 'TSX:NFI', '',
                      37.36,  38.21,  38.42, 52.48,  32.95,  62353249),
                     (6, 7, 'NWC', 'Northwest Company',  28,  1.3, 0, 1.42685108e+09, 'TSX', 'TSX:NWC', '',
                      29.3 ,  29.29,  29.44, 30.6 ,  27.03,  48697989),
                     (7, 8, 'EIF', 'Exchange Income Corp',  30.23,  0.76, 0, 9.72296840e+08, 'TSX', 'TSX:EIF', '',
                      30.99,  31.3,  31.48, 35.34,  26.87,  31374535),
                     (8, 9, 'AFN', 'Ag Growth International',  50,  3.63, 0, 9.84849521e+08, 'TSX', 'TSX:AFN', '',
                      53.63,  54.44,  54.44, 64.72,  47.84,  18363780)]

"""
print(s.df.to_string())

    a = AlphaVantage(get_mktsymbol_list(), 'prior', 'VWXATT8K62KW1GZH')
    o = Observation(a.get())
    o.get_symbol_fk()
    o.write()

"""
