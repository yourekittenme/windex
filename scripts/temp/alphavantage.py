import datetime
import os
import requests
import time


# alpha vantage API parameters
alpha_function = 'TIME_SERIES_DAILY'
alpha_apikey = 'VWXATT8K62KW1GZH'
alpha_output_size = 'compact'

# path to text file containing list of stocks
directory_stock_list = os.getcwd()
filename_stock_list = 'windex.txt'
path_stock_list = directory_stock_list + '\\' + filename_stock_list

# create a list of stocks from text file
stock_list = []
with open(path_stock_list, 'r') as f:
    for stock_symbol in f.readlines():
        stock_list.append('{}'.format(stock_symbol).rstrip())

print(stock_list)

# define where the previous day stock closing price will be found in the json from Alpha Vantage's API
# observation_date = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime('%Y-%m-%d')
observation_date = (datetime.datetime.now() + datetime.timedelta(days=-2)).strftime('%Y-%m-%d')
dict_level_1 = 'Time Series (Daily)'
dict_level_2 = observation_date
dict_level_3 = '4. close'

# call alpha vantage API and load the results into a dictionary
stock_dict = {}

for current_stock in stock_list:

    # call alpha vantage API and load the results into a dictionary
    url = 'https://www.alphavantage.co/query?function={}&symbol={}&outputsize={}&apikey={}'\
        .format(alpha_function, current_stock,  alpha_output_size, alpha_apikey)
    r = requests.get(url)
    data = r.json()
    stock_dict[current_stock, observation_date] = round(float(data[dict_level_1][dict_level_2][dict_level_3]), 2)
    time.sleep(12)

# path to text file for output
directory_output = os.getcwd()
filename_output = 'windex.txt'
path_output = directory_output + '\\' + filename_output

# populate output file with data
with open(path_output, 'w') as f:
    for dict_key in stock_dict.keys():
        f.write(str(stock_dict[dict_key]))
