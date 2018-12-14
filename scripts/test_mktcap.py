from bs4 import BeautifulSoup
import requests
import pandas as pd
from SqlQuery import SqlQuery
import time

def get_symbol_df():
    sql = 'SELECT symbol, id FROM stockindex_app_stock WHERE inactive = 0'
    q = SqlQuery()
    symbol_df = q.read(sql)
    return symbol_df

symbol_df = get_symbol_df()
symbol_list = [x.replace('-', '.') for x in symbol_df['symbol'].tolist()]
print(symbol_list)

for symbol in symbol_list:
    r = requests.get(f'https://web.tmxmoney.com/quote.php?qm_symbol={symbol}')
    html_doc = r.text
    soup = BeautifulSoup(html_doc, 'html.parser')
    soup_td = soup.find_all('td')
    list_pos = 0
    for x in soup_td:
        list_pos = list_pos + 1
        if 'Shares Out.:' in x:
            break
    shares_outstanding = int(soup_td[list_pos].text.replace(',', ''))
    print(shares_outstanding)
    time.sleep(10)
