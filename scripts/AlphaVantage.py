import datetime
import requests
import time


class AlphaVantage:

    cooldown_time = 12
    output_size = 'compact'

    def __init__(self, stock_symbol, alpha_function, api_key):
        self.api_key = api_key
        self.function = alpha_function.lower()
        if type(stock_symbol) is list:
            self.symbol = stock_symbol
        if type(stock_symbol) is str:
            self.symbol = [stock_symbol]

    def get(self):
        # create an empty list
        stock_observation_list = []

        # fill the list with records for each symbol
        for current_symbol in self.symbol:
            if self.function == 'prior':
                stock_observation_list.append(self.prior_day(current_symbol))
            time.sleep(self.cooldown_time)

        return stock_observation_list

    def prior_day(self, lookup_symbol, days_ago=6):
        api_function = 'TIME_SERIES_DAILY'

        # define where the previous day stock closing price will be found in the json from Alpha Vantage's API
        observation_date = (datetime.datetime.now() + datetime.timedelta(days=-days_ago)).strftime('%Y-%m-%d')
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
