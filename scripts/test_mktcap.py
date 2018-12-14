from bs4 import BeautifulSoup
import requests
import pandas as pd
from SqlQuery import SqlQuery


def get_symbol_list():
    sql = 'SELECT symbol FROM stockindex_app_stock WHERE inactive = 0'
    q = SqlQuery()
    symbol_list = q.read(sql)['symbol'].tolist()
    return symbol_list

print(get_symbol_list())

r = requests.get(f'https://web.tmxmoney.com/quote.php?qm_symbol={get_symbol_list()[2]}')
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
