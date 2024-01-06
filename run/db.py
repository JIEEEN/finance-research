import logging
import time
import ray

import finance_research.utils as utils
import finance_research.stockDB as stockDB
import finance_research.stockGUI as stockGUI

utils.setup_logging()

def run():
    logging.info("Start to get all stock data for initiate")
    tt = time.time()
    
    _stock_db = stockDB.StockDB()
    
    df = utils.read_excel('data/data.xlsx')
    ticker_list = _stock_db.get_ticker_list_from_db()
    ticker_names = utils.get_ticker_names(df)
    
    db_res = []
    for ticker in ticker_list:
        db_res.append(_stock_db.get_stock_data_from_db(ticker))
    
    _gui = stockGUI.StockGUI(_stock_db, ticker_list, ticker_names[:len(ticker_list)], db_res)
    _gui.run()
    
    ray.shutdown()
    logging.debug(f"Get all stock data execution time: {time.time() - tt}")
    