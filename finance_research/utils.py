import pandas as pd
from typing import List
import logging


def read_excel(excel_file_path: str)-> pd.DataFrame:
    logging.info("read excel file from {}".format(excel_file_path))
    df = pd.read_excel(excel_file_path, header=0)

    return df

def get_ticker_list(df: pd.DataFrame)-> List:
    ticker_list = df["종목코드"]

    return list(ticker_list)