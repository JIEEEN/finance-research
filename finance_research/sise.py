from bs4 import BeautifulSoup
from collections import namedtuple
from typing import List
import logging
import time
import requests
import ray
import numpy as np

from finance_research import utils

utils.setup_logging()
StockData = namedtuple('StockData', ['date', 'open', 'high', 'low', 'close', 'volume'])

@ray.remote
class SiseParser:
    def __init__(self, ticker):
        self.sise_url = "https://finance.naver.com/item/sise_day.nhn?code="
        self.headers = {"User-agent": "Mozilla/5.0"}

        self.ticker = ticker
        self.html = requests.get(self.sise_url + ticker + '&page=1', headers=self.headers).text
        self.bs = BeautifulSoup(self.html, 'lxml')

    @property
    def last_page(self)-> str:
        logging.info("Get last page")
        tt = time.time()

        pgRR = self.bs.find('td', class_='pgRR')

        last_page = str(pgRR.a['href']).split('=')
        
        logging.debug(f"Get last page execution time: {time.time() - tt}")
        return last_page[-1]

    def get_stock_data(self)-> StockData:
        logging.info("Get Stock Data")
        tt = time.time()

        date, open_, high, low, close, volume = np.array([]), np.array([]), np.array([]), np.array([]), np.array([]), np.array([])
        for i in range(1, int(self.last_page)+1):
            self.html = requests.get(self.sise_url + self.ticker + '&page={}'.format(i), headers=self.headers).text
            self.bs = BeautifulSoup(self.html, 'lxml')
            date = np.append(date, self.stock_date(i, self.html, self.bs))
            
            res = self.bs.find_all('span', class_='tah p11')
            open_ = np.append(open_, self.stock_open(res))
            high = np.append(high, self.stock_high(res))
            low = np.append(low, self.stock_low(res))
            close = np.append(close, self.stock_close(res))
            volume = np.append(volume, self.stock_volume(res))

        logging.debug(f"Get Stock Data execution time: {time.time() - tt}")
        return StockData(date=np.array(date, dtype=np.str_), open=np.array(open_, dtype=np.str_), high=np.array(high, dtype=np.str_),
                        low=np.array(low, dtype=np.str_), close=np.array(close, dtype=np.str_), volume=np.array(volume, dtype=np.str_))


    def stock_date(self, page: str, html, bs)-> np.array:
        _date_list = self.bs.find_all('span', class_='tah p10 gray03')

        date = [_date.text for _date in _date_list]

        return np.array(date)

    def stock_close(self, bs4_res)-> np.array:
        close = [_close.text for _close in bs4_res[0::5]]

        return np.array(close)

    def stock_open(self, bs4_res)-> np.array:
        open_ = [_open.text for _open in bs4_res[1::5]]

        return np.array(open_)

    def stock_high(self, bs4_res)-> np.array:
        high = [_high.text for _high in bs4_res[2::5]]

        return np.array(high)

    def stock_low(self, bs4_res)-> np.array:
        low = [_low.text for _low in bs4_res[3::5]]

        return np.array(low)

    def stock_volume(self, bs4_res)-> np.array:
        volume = [_volume.text for _volume in bs4_res[4::5]]

        return np.array(volume)
