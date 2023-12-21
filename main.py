import logging
import pandas as pd
import requests
import ray
import time
from bs4 import BeautifulSoup

from finance_research import utils, sise

ray.init()
logging.basicConfig(format='{%(asctime)s} [%(levelname)-8s] %(message)s', 
                    datefmt='%m/%d %I:%M:%S %p', level=logging.DEBUG)

df = utils.read_excel('data/data.xlsx')
ticker_list = utils.get_ticker_list(df)



logging.info("Get last page")
tt = time.time()
parser = sise.SiseParser.remote()

last_page_list = ray.get([parser.get_last_page.remote(ticker) for ticker in ticker_list])
logging.debug(f"Get last page execution time : {time.time() - tt}")