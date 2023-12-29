import pymysql
import os
import sys
import dotenv
import logging
import ray

dotenv.load_dotenv()

db_config = {
    'host': os.getenv("DB_HOST"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWORD"),
    'database': os.getenv("DB_DATABASE"),
    'charset': os.getenv("DB_CHARSET")
}

class StockDB:
    def __init__(self):
        self.conn = pymysql.connect(**db_config)

    def create_table(self, ticker: str):
        try:
            with self.conn.cursor() as cursor:    
                create_table_query = f"""
                    create table if not exists c{ticker} (
                        date DATE, open FLOAT, high FLOAT, 
                        low FLOAT, close FLOAT, volume FLOAT
                    );
                """
                cursor.execute(create_table_query)

            self.conn.commit()
        except:
            logging.error("Error occurred while create table")
            self.conn.close()
            sys.exit(-1)
        finally:
            self.conn.close()

    def insert_data(self, ticker: str, stock_data: tuple):
        date, open_, high, low, close, volume = stock_data
        print(len(date), len(open_), len(high), len(low), len(close), len(volume))

        assert len(date) == len(open_) == len(high) == len(low) == len(close) == len(volume), "List Length does not match"

        try:
            with self.conn.cursor() as cursor:
                for i in range(len(date)):
                    insert_data_query = f"""
                        insert into c{ticker} values (STR_TO_DATE(%s, '%Y-%m-%d'), %s, %s, %s, %s, %s);
                    """
                    cursor.execute(insert_data_query, (date[i], open_[i], high[i], low[i], close[i], volume[i]))
            
            self.conn.commit()
        except:
            logging.error("Error occurred while insert data into table")
            self.conn.close()
            sys.exit(-1)
        finally:
            self.conn.close()