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

    def get_shares_outstanding(self):
        shares_out_list = [x.replace('-', '.') for x in self.df['symbol'].tolist()]
        tmx = TmxMoney(shares_out_list)
        df_shares_out = tmx.get_shares_outstanding()
        self.df.drop(['shares_outstanding'], axis=1, inplace=True)
        self.df = pd.merge(self.df, df_shares_out, how='inner', left_on='symbol', right_on='symbol')

    def calculate_mktcap(self):
        self.df['market_cap'] = self.df['current_price'] * self.df['shares_outstanding']
        print(self.df.to_string())

    def write(self):
        db_table = 'stockindex_app_stcok'
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
    s.get_shares_outstanding()
    s.calculate_mktcap()

"""
print(s.df.to_string())


    a = AlphaVantage(get_mktsymbol_list(), 'prior', 'VWXATT8K62KW1GZH')
    o = Observation(a.get())
    o.get_symbol_fk()
    o.write()

    test_records = [('TSX:BUI', '2018-12-03', 0, 3.6900, 3.6900, 3.6900, 3.6900),
                    ('TSX:BYD-UN', '2018-12-03', 83073, 115.9700, 121.2100, 114.1700, 118.7800),
                    ('TSX:GWO', '2018-12-03', 725300, 30.5500, 30.5900, 29.8400, 30.0000),
                    ('TSX:IGM', '2018-12-03', 328800, 34.2600, 34.8400, 34.1900, 34.8400),
                    ('TSX:PBL', '2018-12-03', 6200, 22.1000, 22.3700, 22.0100, 22.3700),
                    ('TSX:NFI', '2018-12-03', 448300, 38.2100, 38.4200, 37.1800, 37.3600),
                    ('TSX:NWC', '2018-12-03', 76200, 29.2900, 29.4400, 28.8100, 29.3000),
                    ('TSX:AFN', '2018-12-03', 18400, 54.4400, 54.4400, 53.3400, 53.6300),
                    ('TSX:EIF', '2018-12-03', 67300, 31.3000, 31.4800, 30.4500, 30.9900)]

"""
