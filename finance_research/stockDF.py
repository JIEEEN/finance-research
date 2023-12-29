import pandas as pd
import logging
from datetime import datetime, timedelta

import finance_research.utils as utils

utils.setup_logging()


class StockDF:
    def __init__(self, stock_data: tuple):
        self.stock_data = stock_data

        self.stock_df = None
        self.convert_to_df()

    def convert_to_df(self):
        logging.info("Convert Stock data to DataFrame")
        transposed_tuple = list(zip(*self.stock_data))

        self.stock_df = pd.DataFrame(data=transposed_tuple, 
            columns=['date', 'open', 'high', 'low', 'close', 'volume']
        )
        self.stock_df.set_index('date', inplace=True)

    def stock_duration(self, start_date: str, end_date: str):
        logging.info(f"Get Stock Data from {start_date} to {end_date}")

        assert start_date <= end_date, "Start date must be earlier than the end date"
        assert utils.check_date_format(start_date) and utils.check_date_format(end_date), "Date format mismatch error"

        utils.convert_date_format(start_date)
        utils.convert_date_format(end_date)

        return self.stock_df.loc[end_date: start_date]

    def stock_one_month(self):
        recent_date = self.stock_df.index[0]

        dt_recent_date = datetime.strptime(recent_date, "%Y.%m.%d")

        before_one_month = dt_recent_date - timedelta(days=28)

        return self.stock_duration(before_one_month.strftime("%Y.%m.%d"), recent_date)

    def stock_one_week(self):
        recent_date = self.stock_df.index[0]

        dt_recent_date = datetime.strptime(recent_date, "%Y.%m.%d")

        before_one_week = dt_recent_date - timedelta(days=7)

        return self.stock_duration(before_one_week.strftime("%Y.%m.%d"), recent_date)
        
    def stock_one_day(self):
        recent_date = self.stock_df.index[0]

        return self.stock_df.loc[recent_date]

    def reverse_order_df(self):
        self.stock_df.sort_index(ascending=False, inplace=True)

    
