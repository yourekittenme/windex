from bs4 import BeautifulSoup
import requests
import pandas as pd
import time


class TmxMoney:

    def __init__(self, stock_symbol):
        if type(stock_symbol) is list:
            self.symbol = [x.replace('-', '.') for x in stock_symbol]
        if type(stock_symbol) is str:
            self.symbol = [stock_symbol.replace('-', '.')]

    def get_shares_outstanding(self):

        shares_out_records = []

        for symbol in self.symbol:
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
            shares_out_tuple = (
                symbol.replace('.', '-'),
                shares_outstanding
            )
            shares_out_records.append(shares_out_tuple)
            time.sleep(8)

        df_shares_out = pd.DataFrame.from_records(shares_out_records, columns=['symbol', 'shares_outstanding'])

        return df_shares_out
