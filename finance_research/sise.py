from bs4 import BeautifulSoup
from collections import namedtuple
from typing import List
import logging
import time
import requests
import ray

from finance_research import utils

utils.setup_logging()
StockData = namedtuple('StockData', ['date', 'open', 'high', 'low', 'close', 'volume'])

@ray.remote
class SiseParser:
    def __init__(self):
        self.sise_url = "https://finance.naver.com/item/sise_day.nhn?code="
        self.headers = {"User-agent": "Mozilla/5.0"}

        self.html = None
        self.bs = None

    def last_page(self, ticker: str)-> str:
        logging.info("Get last page")
        tt = time.time()

        self.html = requests.get(self.sise_url + ticker + '&page=1', headers=self.headers).text
        self.bs = BeautifulSoup(self.html, 'lxml')

        pgRR = self.bs.find('td', class_='pgRR')

        last_page = str(pgRR.a['href']).split('=')
        
        logging.debug(f"Get last page execution time: {time.time() - tt}")
        return last_page[-1]

    def get_stock_data(self, ticker: str)-> StockData:
        logging.info("Get Stock Data")
        tt = time.time()

        date, open_, high, low, close, volume = [], [], [], [], [], []
        for i in range(int(self.last_page(ticker))):
            self.html = requests.get(self.sise_url + ticker + '&page={}'.format(i), headers=self.headers).text
            self.bs = BeautifulSoup(self.html, 'lxml')
            date.append(self.stock_date(ticker, i, self.html, self.bs))
            
            res = self.bs.find_all('span', class_='tah p11')
            open_.append(self.stock_open(ticker, res))
            high.append(self.stock_high(ticker, res))
            low.append(self.stock_low(ticker, res))
            close.append(self.stock_close(ticker, res))
            volume.append(self.stock_volume(ticker, res))

        logging.debug(f"Get Stock Data execution time: {time.time() - tt}")
        return StockData(date=date, open=open_, high=high,
                        low=low, close=close, volume=volume)

    def stock_date(self, ticker: str, page: str, html, bs)-> List:
        _date_list = self.bs.find_all('span', class_='tah p10 gray03')

        date = [_date.text for _date in _date_list]

        return date

    def stock_close(self, ticker: str, bs4_res):
        close = [_close.text for _close in bs4_res[0::5]]

        return close

    def stock_open(self, ticker: str, bs4_res):
        open_ = [_open.text for _open in bs4_res[1::5]]

        return open_

    def stock_high(self, ticker: str, bs4_res):
        high = [_high.text for _high in bs4_res[2::5]]

        return high

    def stock_low(self, ticker: str, bs4_res):
        low = [_low.text for _low in bs4_res[3::5]]

        return low

    def stock_volume(self, ticker: str, bs4_res):
        volume = [_volume.text for _volume in bs4_res[4::5]]

        return volume
