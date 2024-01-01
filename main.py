import logging
import pandas as pd
import ray
import time
import argparse
import warnings

import finance_research.utils as utils
import finance_research.sise as sise
import finance_research.stockDB as stockDB
import finance_research.stockDF as stockDF
import finance_research.stockGUI as stockGUI

warnings.simplefilter('ignore')

parser = argparse.ArgumentParser()
parser.add_argument('-m', '--mode', default='debug', help='init_mode or update_mode')
parser.add_argument('--batch_size', default=10, help='set batch_size for get data parallel')

args = parser.parse_args()

ray.init(object_store_memory=10**9)
utils.setup_logging()


if __name__ == "__main__":
    _stock_db = stockDB.StockDB()

    if args.mode == "init":
        logging.info("Start to get All stock Data for initiate")
        tt = time.time()
        
        df = utils.read_excel('data/data.xlsx')
        ticker_list = utils.get_ticker_list(df)

        batch_size = int(args.batch_size)
        res, sise_parsers = [], []
        for i in range(0, len(ticker_list), batch_size):
            sise_parsers = [sise.SiseParser.remote(ticker) for ticker in ticker_list[i:i+batch_size]]
            batch_res = ray.get([s.get_stock_data.remote() for s in sise_parsers])
            res.extend(batch_res)

        for i in range(len(ticker_list)):
            _stock_db.create_table(ticker_list[i])
            _stock_db.insert_data(ticker_list[i], res[i])

        _gui = stockGUI.StockGUI(ticker_list, res)
        _gui.run()

        ray.shutdown()
        logging.debug(f"Get all stock data execution time: {time.time() - tt}")
    elif args.mode == "update":
        pass
    elif args.mode == "debug":
        logging.info("Start to get All stock Data for initiate")
        tt = time.time()
        DEBUG_TEST_NUM = 1
        
        df = utils.read_excel('data/data.xlsx')
        ticker_list = utils.get_ticker_list(df)
        ticker_names = utils.get_ticker_names(df)

        with open("ticker.txt", 'w') as f:
            for data in ticker_list:
                f.write(str(data) + '\n')
        
        res = []
        sise_parsers = [sise.SiseParser.remote(ticker) for ticker in ticker_list[:DEBUG_TEST_NUM]] # only use three stock data for debug mode
        batch_res = ray.get([s.get_stock_data.remote() for s in sise_parsers])
        res.extend(batch_res)

        res = [utils.preprocess_data(data) for data in res]

        for i in range(DEBUG_TEST_NUM):
            _stock_db.create_table(ticker_list[i])
            # TODO: if data already exist in DB, skip insert_data
            # TODO: split mode for db and debug
            _stock_db.insert_data(ticker_list[i], res[i])
            
        db_res = []
        for i in range(DEBUG_TEST_NUM):
            db_res.append(_stock_db.get_stock_data_from_db(ticker_list[i]))
            
        _gui = stockGUI.StockGUI(_stock_db, ticker_list[:DEBUG_TEST_NUM], ticker_names[:DEBUG_TEST_NUM], db_res)
        _gui.run()

        ray.shutdown()
        logging.debug(f"Get all stock data execution time: {time.time() - tt}")
