import pandas as pd
from typing import List
import logging
from datetime import datetime
import numpy as np

def setup_logging():
    logging.basicConfig(format='{%(asctime)s} [%(levelname)-8s] %(message)s', 
                        datefmt='%m/%d %I:%M:%S %p', level=logging.DEBUG)

def read_excel(excel_file_path: str)-> pd.DataFrame:
    logging.info("read excel file from {}".format(excel_file_path))
    df = pd.read_excel(excel_file_path, header=0)

    return df

def get_ticker_list(df: pd.DataFrame)-> List:
    ticker_list = df["종목코드"]

    return list(ticker_list)

def preprocess_data(stock_data: tuple):
    date, open_, high, low, close, volume = stock_data

    p_date, p_open_, p_high, p_low, p_close, p_volume = np.array([]), np.array([]), np.array([]), np.array([]), np.array([]), np.array([])
    for i in range(len(date)):
        p_date = np.append(p_date, np.array(datetime.strptime(date[i], '%Y.%m.%d').strftime('%Y-%m-%d'), dtype=np.str_))

        p_open_ = np.append(p_open_, np.array(open_[i].replace(',', ''), dtype=np.float32))
        p_high = np.append(p_high, np.array(high[i].replace(',', ''), dtype=np.float32))
        p_low = np.append(p_low, np.array(low[i].replace(',', ''), dtype=np.float32))
        p_close = np.append(p_close, np.array(close[i].replace(',', ''), dtype=np.float32))
        p_volume = np.append(p_volume, np.array(volume[i].replace(',', ''), dtype=np.float32))

    stock_data = p_date, p_open_, p_high, p_low, p_close, p_volume

    return stock_data