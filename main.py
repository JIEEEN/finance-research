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

ray.init(object_store_memory=10**9)
utils.setup_logging()


if __name__ == "__main__":
    if args.mode == "init":
        logging.info("Start to get All stock Data for initiate")
        tt = time.time()
        
        df = utils.read_excel('data/data.xlsx')
        ticker_list = utils.get_ticker_list(df)

        batch_size = 10
        res, sise_parsers = [], []
        for i in range(0, len(ticker_list), batch_size):
            sise_parsers = [sise.SiseParser.remote(ticker) for ticker in ticker_list[i:i+batch_size]]
            batch_res = ray.get([s.get_stock_data.remote() for s in sise_parsers])
            res.extend(batch_res)

        ray.shutdown()
        logging.debug(f"Get all stock data execution time: {time.time() - tt}")
    elif args.mode == "update":
        pass
    elif args.mode == "debug":
        logging.info("Start to get All stock Data for initiate")
        tt = time.time()
        
        df = utils.read_excel('data/data.xlsx')
        ticker_list = utils.get_ticker_list(df)
        
        batch_size = 10
        res, sise_parsers = [], []
        for i in range(0, len(ticker_list), batch_size):
            sise_parsers = [sise.SiseParser.remote(ticker) for ticker in ticker_list[i:i+batch_size]]
            batch_res = ray.get([s.get_stock_data.remote() for s in sise_parsers])
            res.extend(batch_res)

        ray.shutdown()
        logging.debug(f"Get all stock data execution time: {time.time() - tt}")
