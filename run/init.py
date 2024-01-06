import logging
import time
import ray

import finance_research.utils as utils
import finance_research.sise as sise
import finance_research.stockDB as stockDB
import finance_research.stockGUI as stockGUI

utils.setup_logging()

def run(batch_size):
    logging.info("Start to get all stock data for initiate")
    tt = time.time()
    
    _stock_db = stockDB.StockDB()
    
    df = utils.read_excel('data/data.xlsx')
    ticker_list = utils.get_ticker_list(df)
    ticker_names = utils.get_ticker_names(df)
    
    batch_size = int(batch_size)
    res, sise_parsers = [], []
    for i in range(0, len(ticker_list), batch_size):
        sise_parsers = [sise.SiseParser.remote(ticker) for ticker in ticker_list[i:i+batch_size]]
        batch_res = ray.get([s.get_stock_data.remote() for s in sise_parsers])
        
        res.extend(batch_res)
        
    res = [utils.preprocess_data(data) for data in res]
        
    for i in range(len(ticker_list)):
        _stock_db.create_table(ticker_list[i])
        _stock_db.insert_data(ticker_list[i], res[i])
        
    _gui = stockGUI.StockGUI(_stock_db, ticker_list, ticker_names, res)
    _gui.run()
    
    ray.shutdown()
    logging.debug(f"Get all stock data execution time: {time.time() - tt}")
    