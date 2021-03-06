import datetime
import os
import pandas as pd
import requests
import sqlite3
import time

# create a connection to the database
directory_db = '\\'.join(os.getcwd().split('\\')[0:-1])
filename_db = 'db.sqlite3'
path_db = directory_db + '\\' + filename_db
con = sqlite3.connect(path_db)

# get the list of stock symbols to lookup
sql_query = 'SELECT marketsymbol, id FROM stockindex_app_stock'
df = pd.read_sql(sql_query, con)
marketsymbol_list = df['marketsymbol'].tolist()
id_list = df['id'].tolist()
symbol_dict = {}
for n in range(0, len(marketsymbol_list)):
    symbol_dict[marketsymbol_list[n]] = id_list[n]

# define alpha vantage API parameters
alpha_function = 'TIME_SERIES_DAILY'
alpha_apikey = 'VWXATT8K62KW1GZH'
alpha_output_size = 'full'

# define where the previous day stock closing price will be found in the json from Alpha Vantage's API
observation_date = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime('%Y-%m-%d')
dict_level_1 = 'Time Series (Daily)'
dict_level_2 = observation_date

# call alpha vantage API and load the results into a list of records
stock_observation_list = []

print(list(symbol_dict.keys()))

for current_symbol in list(symbol_dict.keys()):

    # call alpha vantage API and load the results into a dictionary
    url = f'https://www.alphavantage.co/query?function={alpha_function}&symbol={current_symbol}&outputsize={alpha_output_size}&apikey={alpha_apikey}'
    r = requests.get(url)
    data = r.json()

    stock_observation = (
        symbol_dict[current_symbol],
        observation_date,
        data[dict_level_1][dict_level_2]['5. volume'],
        data[dict_level_1][dict_level_2]['1. open'],
        data[dict_level_1][dict_level_2]['2. high'],
        data[dict_level_1][dict_level_2]['3. low'],
        data[dict_level_1][dict_level_2]['4. close'],
    )
    stock_observation_list.append(stock_observation)
    print(stock_observation)

    time.sleep(30)



"""
# path to text file for output
directory_output = os.getcwd()
filename_output = 'windex.txt'
path_output = directory_output + '\\' + filename_output

# populate output file with data
with open(path_output, 'w') as f:
  for dict_key in stock_dict.keys():
      f.write(str(stock_dict[dict_key]))
"""