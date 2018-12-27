from AlphaVantage import AlphaVantage
import datetime
from SqlQuery import SqlConnection
from sqlalchemy import select, insert, func, update
from TmxMoney import TmxMoney
import pandas as pd

import logging
logging.basicConfig(filename='log.txt', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


class Observation:

    columns_list = ['marketsymbol', 'observation_date', 'volume', 'open_price',
                    'high_price', 'low_price', 'close_price']

    def __init__(self, obs_records):
        self.df = pd.DataFrame.from_records(obs_records, columns=self.columns_list)
        self.df['observation_date'] = pd.to_datetime(self.df['observation_date'], format='%Y-%m-%d %H:%M:%S')

    def get_stock_fk(self):
        tbl = 'stockindex_app_stock'
        stock = SqlConnection(tbl)
        stmt = select([stock.table.c.marketsymbol, stock.table.c.id]).where(stock.table.c.inactive == 0)
        df_symbol = stock.select_query(stmt)

        self.df = pd.merge(df_symbol, self.df, on='marketsymbol')
        self.df.drop(columns='marketsymbol', inplace=True)
        self.df.rename(columns={'id': 'stock_id'}, inplace=True)

    def write(self):
        tbl = 'stockindex_app_observations'
        observation = SqlConnection(tbl)
        stmt = insert(observation.table)
        observation.insert_query(stmt, self.df)


class Stock:

    def __init__(self):
        tbl = 'stockindex_app_stock'
        stock = SqlConnection(tbl)
        stmt = select([stock.table]).where(stock.table.c.inactive == 0)
        self.df = stock.select_query(stmt)

    def update_price(self, df_obs):
        self.df['prior_close_price'] = self.df['current_price']
        self.df.drop(['current_price', 'high_price', 'low_price'], axis=1, inplace=True)
        df_obs = df_obs[['stock_id', 'close_price', 'high_price', 'low_price']]
        self.df = pd.merge(self.df, df_obs, how='inner', left_on='id', right_on='stock_id')
        self.df.rename(columns={'close_price': 'current_price'}, inplace=True)
        self.df['change_price'] = pd.to_numeric(self.df['current_price']) - pd.to_numeric(self.df['prior_close_price'])
        self.df['change_price'] = self.df['change_price'].round(decimals=2)
        self.df.drop('stock_id', axis=1, inplace=True)

    def update_52_week_highlow(self):
        tbl = 'stockindex_app_observations'
        observation = SqlConnection(tbl)
        highest_price = func.max(observation.table.c.high_price).label('high_price_52_weeks')
        lowest_price = func.min(observation.table.c.low_price).label('low_price_52_weeks')
        stmt = select([observation.table.c.stock_id, highest_price, lowest_price])
        stmt = stmt.where(observation.table.c.observation_date >= datetime.datetime.now()+datetime.timedelta(weeks=-52))
        stmt = stmt.group_by(observation.table.c.stock_id)
        df_highlow = observation.select_query(stmt)

        self.df.drop(['high_price_52_weeks', 'low_price_52_weeks'], axis=1, inplace=True)
        self.df = pd.merge(self.df, df_highlow, how='inner', left_on='id', right_on='stock_id')
        self.df.drop('stock_id', axis=1, inplace=True)

    def calculate_mktcap(self):
        shares_out_list = [x.replace('-', '.') for x in self.df['symbol'].tolist()]
        tmx = TmxMoney(shares_out_list)
        df_shares_out = tmx.get_shares_outstanding()
        self.df.drop(['shares_outstanding'], axis=1, inplace=True)
        self.df = pd.merge(self.df, df_shares_out, how='inner', left_on='symbol', right_on='symbol')
        self.df['market_cap'] = \
            (pd.to_numeric(self.df['current_price']) * pd.to_numeric(self.df['shares_outstanding'])).astype('int64')
        self.df['high_market_cap'] = \
            (pd.to_numeric(self.df['high_price']) * pd.to_numeric(self.df['shares_outstanding'])).astype('int64')
        self.df['low_market_cap'] = \
            (pd.to_numeric(self.df['low_price']) * pd.to_numeric(self.df['shares_outstanding'])).astype('int64')

    def write(self):
        tbl = 'stockindex_app_stock'
        stock = SqlConnection(tbl)
        values_list = [x for x in self.df.T.to_dict().values()]

        for value in values_list:
            stmt = update(stock.table).values(
                current_price=value['current_price'],
                prior_close_price=value['prior_close_price'],
                change_price=value['change_price'],
                market_cap=value['market_cap'],
                high_market_cap=value['high_market_cap'],
                low_market_cap=value['low_market_cap'],
                high_price=value['high_price'],
                low_price=value['low_price'],
                high_price_52_weeks=value['high_price_52_weeks'],
                low_price_52_weeks=value['low_price_52_weeks'],
                shares_outstanding=value['shares_outstanding']
            ).where(
                stock.table.c.id == value['id']
            )
            stock.update_query(stmt)


class IndexObservations:

    def __init__(self):
        tbl = 'stockindex_app_index'
        index = SqlConnection(tbl)
        stmt = select([index.table]).where(index.table.c.inactive == 0)
        self.df = index.select_query(stmt)

    def get_observation(self):
        tbl = 'stockindex_app_stocksindexed'
        stocksindexed = SqlConnection(tbl)
        stmt = select([stocksindexed.table])
        df_stocksindexed = stocksindexed.select_query(stmt)

        df_update = pd.merge(self.df, df_stocksindexed, how='inner', left_on='id', right_on='index_id')
        df_update = df_update[['index_id', 'stock_id']]

        tbl = 'stockindex_app_stock'
        stock = SqlConnection(tbl)
        stmt = select([stock.table.c.id,
                       stock.table.c.marketsymbol,
                       stock.table.c.market_cap,
                       stock.table.c.high_market_cap,
                       stock.table.c.low_market_cap,
                       ]).where(stock.table.c.inactive == 0)
        df_stock = stock.select_query(stmt)

        df_update = pd.merge(df_update, df_stock, how='inner', left_on='stock_id', right_on='id')
        df_update.drop(['stock_id', 'id'], axis=1, inplace=True)
        df_update['market_cap'] = (df_update['market_cap']/1000000).astype('int64')
        df_update['high_market_cap'] = (df_update['high_market_cap'] / 1000000).astype('int64')
        df_update['low_market_cap'] = (df_update['low_market_cap'] / 1000000).astype('int64')
        df_update = df_update.groupby('index_id')['market_cap', 'high_market_cap', 'low_market_cap'].sum()
        df_update.rename(columns={'market_cap': 'current_value', 'high_market_cap': 'high_value',
                                  'low_market_cap': 'low_value'}, inplace=True)

        self.df['open_value'] = self.df['current_value']
        self.df.drop(['current_value', 'high_value', 'low_value'], axis=1, inplace=True)
        self.df = pd.merge(self.df, df_update, how='inner', left_on='id', right_on='index_id')
        self.df['observation_date'] = datetime.datetime.strptime(
            (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime('%Y-%m-%d'), '%Y-%m-%d')
        self.df.rename(columns={'id': 'index_id', 'current_value': 'close_value'}, inplace=True)
        self.df = self.df[['observation_date', 'open_value', 'high_value', 'low_value', 'close_value', 'index_id']]

    def write(self):
        tbl = 'stockindex_app_indexobservations'
        observation = SqlConnection(tbl)
        stmt = insert(observation.table)
        observation.insert_query(stmt, self.df)


class Index:

    def __init__(self):
        tbl = 'stockindex_app_index'
        index = SqlConnection(tbl)
        stmt = select([index.table]).where(index.table.c.inactive == 0)
        self.df = index.select_query(stmt)

    def update_price(self, df_index_obs):
        self.df['prior_close_value'] = self.df['current_value']
        self.df.drop(['current_value', 'high_value', 'low_value'], axis=1, inplace=True)
        self.df = pd.merge(self.df, df_index_obs, how='inner', left_on='id', right_on='index_id')
        self.df['change_value'] = pd.to_numeric(self.df['close_value']) - pd.to_numeric(self.df['prior_close_value'])
        self.df['change_value'] = self.df['change_value'].round(decimals=2)
        self.df.rename(columns={'close_value': 'current_value'})
        self.df.drop(['open_value'], axis=1, inplace=True)

    def update_52_week_highlow(self):
        tbl = 'stockindex_app_indexobservations'
        index_obs = SqlConnection(tbl)
        highest_value = func.max(index_obs.table.c.high_value).label('high_value_52_weeks')
        lowest_value = func.min(index_obs.table.c.low_value).label('low_value_52_weeks')
        stmt = select([index_obs.table.c.index_id, highest_value, lowest_value])
        stmt = stmt.where(index_obs.table.c.observation_date >= datetime.datetime.now()+datetime.timedelta(weeks=-52))
        df_index_obs = index_obs.select_query(stmt)
        self.df.drop(['high_value_52_weeks', 'low_value_52_weeks'], axis=1, inplace=True)
        self.df = pd.merge(self.df, df_index_obs, how='inner', left_on='id', right_on='index_id')
        self.df.rename(columns={'close_value': 'current_value'}, inplace=True)
        self.df = self.df[['id', 'current_value', 'prior_close_value', 'change_value', 'high_value', 'low_value',
                           'high_value_52_weeks', 'low_value_52_weeks']]

    def write(self):
        tbl = 'stockindex_app_index'
        index = SqlConnection(tbl)
        values_list = [x for x in self.df.T.to_dict().values()]

        for value in values_list:
            stmt = update(index.table).values(
                current_value=value['current_value'],
                prior_close_value=value['prior_close_value'],
                change_value=value['change_value'],
                high_value=value['high_value'],
                low_value=value['low_value'],
                high_value_52_weeks=value['high_value_52_weeks'],
                low_value_52_weeks=value['low_value_52_weeks'],
            ).where(
                index.table.c.id == value['id']
            )
            index.update_query(stmt)


def get_mktsymbol_list():
    tbl = 'stockindex_app_stock'
    stock = SqlConnection(tbl)
    stmt = select([stock.table.c.marketsymbol]).where(stock.table.c.inactive == 0)
    df = stock.select_query(stmt)
    symbol_list = df['marketsymbol'].tolist()
    return symbol_list


if __name__ == "__main__":
    logging.debug('Stock updates loaded into database')
    io = IndexObservations()
    logging.debug('Index observations object created')
    io.get_observation()
    logging.debug('Index observations object updated')
    io.write()
    logging.debug('Index observations written to database')
    i = Index()
    logging.debug('Index object created')
    i.update_price(io.df)
    logging.debug('Index prices updated')
    i.update_52_week_highlow()
    i.write()

    """ 
    logging.debug('Start WINDEX update script')
    a = AlphaVantage(get_mktsymbol_list(), 'prior', 'VWXATT8K62KW1GZH')
    logging.debug('AlphaVantage API object created')
    o = Observation(a.get())
    logging.debug('Downloaded daily stock info from AlphaVantage')
    o.get_stock_fk()
    logging.debug('FKs obtained from database')
    o.write()
    s = Stock()
    logging.debug('Queried active stocks in database')
    s.update_price(o.df)
    logging.debug('Updated prices of active stocks')
    s.update_52_week_highlow()
    logging.debug('Calculated 52 week high & low stock prices')
    s.calculate_mktcap()
    logging.debug('Calculated market capitalization')
    s.write()
    
    test_records = [('TSX:BUI', '2018-12-03 00:00:00', 0, 3.6900, 3.6900, 3.6900, 3.6900),
                    ('TSX:BYD-UN', '2018-12-03 00:00:00', 83073, 115.9700, 121.2100, 114.1700, 118.7800),
                    ('TSX:GWO', '2018-12-03 00:00:00', 725300, 30.5500, 30.5900, 29.8400, 30.0000),
                    ('TSX:IGM', '2018-12-03 00:00:00', 328800, 34.2600, 34.8400, 34.1900, 34.8400),
                    ('TSX:PBL', '2018-12-03 00:00:00', 6200, 22.1000, 22.3700, 22.0100, 22.3700),
                    ('TSX:NFI', '2018-12-03 00:00:00', 448300, 38.2100, 38.4200, 37.1800, 37.3600),
                    ('TSX:NWC', '2018-12-03 00:00:00', 76200, 29.2900, 29.4400, 28.8100, 29.3000),
                    ('TSX:AFN', '2018-12-03 00:00:00', 18400, 54.4400, 54.4400, 53.3400, 53.6300),
                    ('TSX:EIF', '2018-12-03 00:00:00', 67300, 31.3000, 31.4800, 30.4500, 30.9900)]

    test_records_update = [(1, 'BUI', 'Buhler Industries',   3.69,  0, 0, 9.22500000e+07, 'TSX', 'TSX:BUI', '',
                        3.69,  3.69,  3.69, 3.94, 3.53, 25000000),
                     (2, 'BYD-UN', 'The Boyd Group', 111.1,  7.68, 0, 2.33681536e+09, 'TSX', 'TSX:BYD-UN', '',
                      118.78, 115.97, 121.21, 133, 102.59,  19673475),
                     (3, 'GWO', 'Great West Life Company',  27.87,  2.13, 0, 2.96515036e+10, 'TSX', 'TSX:GWO', '',
                      30,  30.55,  30.59, 33.1,  28.37, 988383452),
                     (4, 'IGM', 'IGM Financial',  22.37, 12.47, 0, 8.39154568e+09, 'TSX', 'TSX:IGM', '',
                      34.84,  34.26,  34.84, 39.95, 31.54, 240859520),
                     (5, 'PBL', 'Pollard Banknote',  19.53,  2.84, 0, 5.73245969e+08, 'TSX', 'TSX:PBL', '',
                      22.37,  22.1,  22.37, 27.75,  19.91,  25625658),
                     (6, 'NFI', 'New Flyer Industries',  34.31,  3.05, 0, 2.32951738e+09, 'TSX', 'TSX:NFI', '',
                      37.36,  38.21,  38.42, 52.48,  32.95,  62353249),
                     (7, 'NWC', 'Northwest Company',  28,  1.3, 0, 1.42685108e+09, 'TSX', 'TSX:NWC', '',
                      29.3 ,  29.29,  29.44, 30.6 ,  27.03,  48697989),
                     (8, 'EIF', 'Exchange Income Corp',  30.23,  0.76, 0, 9.72296840e+08, 'TSX', 'TSX:EIF', '',
                      30.99,  31.3,  31.48, 35.34,  26.87,  31374535),
                     (9, 'AFN', 'Ag Growth International',  50,  3.63, 0, 9.84849521e+08, 'TSX', 'TSX:AFN', '',
                      53.63,  54.44,  54.44, 64.72,  47.84,  18363780)]
"""
