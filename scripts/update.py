# custom packages
from AlphaVantage import AlphaVantage
from SqlQuery import SqlConnection
from TmxMoney import TmxMoney

# python packages
import datetime
import pandas as pd
from sqlalchemy import select, insert, func, update

# init logging
import logging
logging.basicConfig(filename='log.txt', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


class Observation:
    """Daily statistics about how a stock is trading"""

    def __init__(self, obs_records):
        columns_list = ['marketsymbol', 'observation_date', 'volume', 'open_price',
                        'high_price', 'low_price', 'close_price']
        self.df = pd.DataFrame.from_records(obs_records, columns=self.columns_list)
        self.df['observation_date'] = pd.to_datetime(self.df['observation_date'], format='%Y-%m-%d %H:%M:%S')

    def get_stock_fk(self):
        """get the foreign keys for each symbol in the observations DataFrame"""
        tbl = 'stockindex_app_stock'
        stock = SqlConnection(tbl)
        stmt = select([stock.table.c.marketsymbol, stock.table.c.id]).where(stock.table.c.inactive == 0)
        df_symbol = stock.select_query(stmt)

        self.df = pd.merge(df_symbol, self.df, on='marketsymbol')
        self.df.drop(columns='marketsymbol', inplace=True)
        self.df.rename(columns={'id': 'stock_id'}, inplace=True)

    def write(self):
        """insert new observations into the SQL database"""
        tbl = 'stockindex_app_observations'
        observation = SqlConnection(tbl)
        stmt = insert(observation.table)
        observation.insert_query(stmt, self.df)


class Stock:
    """A single publicly traded stock"""
    def __init__(self):
        tbl = 'stockindex_app_stock'
        stock = SqlConnection(tbl)
        stmt = select([stock.table]).where(stock.table.c.inactive == 0)
        self.df = stock.select_query(stmt)

    def update_price(self, df_obs):
        """update the current, prior, high, low and change price of a stock"""
        self.df['prior_close_price'] = self.df['current_price']
        self.df.drop(['current_price', 'high_price', 'low_price'], axis=1, inplace=True)
        df_obs = df_obs[['stock_id', 'close_price', 'high_price', 'low_price']]
        self.df = pd.merge(self.df, df_obs, how='inner', left_on='id', right_on='stock_id')
        self.df.rename(columns={'close_price': 'current_price'}, inplace=True)
        self.df['change_price'] = pd.to_numeric(self.df['current_price']) - pd.to_numeric(self.df['prior_close_price'])
        self.df['change_price'] = self.df['change_price'].round(decimals=2)
        self.df.drop('stock_id', axis=1, inplace=True)

    def update_52_week_highlow(self):
        """compute the 52 week high/low price of a stock"""
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
        """find the market capitalization of a stock: shares outstanding * stock price"""
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
        """update the stock information in the SQL database"""
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
    """Daily statistics about how an index is trading"""

    def __init__(self):
        tbl = 'stockindex_app_index'
        index = SqlConnection(tbl)
        stmt = select([index.table]).where(index.table.c.inactive == 0)
        self.df = index.select_query(stmt)

    def get_observation(self):
        """update the current, prior, high, low and change value of the index"""
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
        """insert index observations into the SQL database"""
        tbl = 'stockindex_app_indexobservations'
        observation = SqlConnection(tbl)
        stmt = insert(observation.table)
        observation.insert_query(stmt, self.df)


class Index:
    """An indicator of stock market performance based on information about selected stocks"""

    def __init__(self):
        tbl = 'stockindex_app_index'
        index = SqlConnection(tbl)
        stmt = select([index.table]).where(index.table.c.inactive == 0)
        self.df = index.select_query(stmt)

    def update_price(self, df_index_obs):
        """update the current, prior, high, low and change value of the index"""
        self.df['prior_close_value'] = self.df['current_value']
        self.df.drop(['current_value', 'high_value', 'low_value'], axis=1, inplace=True)
        self.df = pd.merge(self.df, df_index_obs, how='inner', left_on='id', right_on='index_id')
        self.df['change_value'] = pd.to_numeric(self.df['close_value']) - pd.to_numeric(self.df['prior_close_value'])
        self.df['change_value'] = self.df['change_value'].round(decimals=2)
        self.df.rename(columns={'close_value': 'current_value'})
        self.df.drop(['open_value'], axis=1, inplace=True)

    def update_52_week_highlow(self):
        """compute the 52 week high/low value of an index"""
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
        """update the index information in the SQL database"""
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
    """get a list of symbols to lookup on AlphaVantage"""
    tbl = 'stockindex_app_stock'
    stock = SqlConnection(tbl)
    stmt = select([stock.table.c.marketsymbol]).where(stock.table.c.inactive == 0)
    df = stock.select_query(stmt)
    symbol_list = df['marketsymbol'].tolist()
    return symbol_list


if __name__ == "__main__":
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
