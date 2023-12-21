import logging
import pandas as pd
import requests
import ray
import time
import argparse
import warnings

from bs4 import BeautifulSoup

from finance_research import utils, sise

warnings.simplefilter('ignore')

parser = argparse.ArgumentParser()
parser.add_argument('-m', '--mode', help='init_mode or update_mode')


args = parser.parse_args()

ray.init()
utils.setup_logging()

if __name__ == "__main__":
    if args.mode == "init":
        df = utils.read_excel('data/data.xlsx')
        ticker_list = utils.get_ticker_list(df)

        sise_parser = sise.SiseParser.remote()

        res = ray.get([sise_parser.get_stock_data.remote(ticker) for ticker in ticker_list])
    elif args.mode == "update":
        pass
    elif args.mode == "debug":
        df = utils.read_excel('data/data.xlsx')
        ticker_list = utils.get_ticker_list(df)

        sise_parser = sise.SiseParser.remote()

        res = ray.get([sise_parser.get_stock_data.remote(ticker) for ticker in ticker_list[:3]])
