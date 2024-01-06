import logging
import time
import ray

import finance_research.utils as utils
import finance_research.sise as sise
import finance_research.stockDB as stockDB
import finance_research.stockGUI as stockGUI

utils.setup_logging()

def run():
    logging.info("Start to get all stock data for initiate")
    tt = time.time()
    DEBUG_TEST_NUM = 1
    
    _stock_db = stockDB.StockDB()
    
    df = utils.read_excel('data/data.xlsx')
    ticker_list = utils.get_ticker_list(df)
    ticker_names = utils.get_ticker_names(df)
    
    with open("ticker.txt", "w") as f:
        for data in ticker_list:
            f.write(str(data) + '\n')
    
    res = []
    sise_parsers = [sise.SiseParser.remote(ticker) for ticker in ticker_list[:DEBUG_TEST_NUM]]
    batch_res = ray.get([s.get_stock_data.remote() for s in sise_parsers])
        
    res.extend(batch_res)
    
    res = [utils.preprocess_data(data) for data in res]
        
    for i in range(DEBUG_TEST_NUM):
        _stock_db.create_table(ticker_list[i])
        # TODO: if data already exist in DB, skip insert_data
        # TODO: split mode for db and debug
        _stock_db.insert_data(ticker_list[i], res[i])
        
    _gui = stockGUI.StockGUI(_stock_db, ticker_list[:DEBUG_TEST_NUM], ticker_names[:DEBUG_TEST_NUM], res)
    _gui.run()
    
    ray.shutdown()
    logging.debug(f"Get all stock data execution time: {time.time() - tt}")
    