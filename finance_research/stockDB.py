import pymysql
import os
import sys
import dotenv
import logging
import numpy as np
from datetime import datetime

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
        self.create_database()
        
    def create_database(self):
        try:
            conn = pymysql.connect(**db_config)
            with conn.cursor() as cursor:
                create_db_query = """
                    create database if not exists stock_db;
                """
                cursor.execute(create_db_query)
            
            conn.commit()
        except Exception as e:
            logging.error(f"Error occurred while create database: {e}")
            conn.close()
            sys.exit(-1)
        finally:
            conn.close()

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
        except Exception as e:
            logging.error(f"Error occurred while create table: {e}")
            self.conn.close()
            sys.exit(-1)

    def insert_data(self, ticker: str, stock_data: tuple):
        date, open_, high, low, close, volume = stock_data
        assert len(date) == len(open_) == len(high) == len(low) == len(close) == len(volume), "List Length does not match"

        try:
            with self.conn.cursor() as cursor:
                for i in range(len(date)):
                    insert_data_query = f"""
                        insert into c{ticker} values (%s, %s, %s, %s, %s, %s);
                    """
                    cursor.execute(insert_data_query, (date[i], open_[i], high[i], low[i], close[i], volume[i]))
            
            self.conn.commit()
        except Exception as e:
            logging.error(f"Error occurred while insert data into table: {e}")
            self.conn.close()
            sys.exit(-1)
            
    def _get_stock_data_from_db(self, data: str, ticker: str):
        get_data_query = f"""
            select {data} from c{ticker};
        """
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(get_data_query)
                
                res = cursor.fetchall()
                return res
        except Exception as e:
            logging.error(f"Error occurred while get data from db: {e}")
            self.conn.close()
            sys.exit(-1)
        
        
    def get_stock_data_from_db(self, ticker: str):
        date, open_, high, low, close, volume = np.array([]), np.array([]), np.array([]), np.array([]), np.array([]), np.array([])
        
        date = np.append(date, np.array(self._get_stock_data_from_db('date', ticker), dtype=np.str_))
        open_ = np.append(open_, np.array(self._get_stock_data_from_db('open', ticker), dtype=np.float32))
        high = np.append(high, np.array(self._get_stock_data_from_db('high', ticker), dtype=np.float32))
        low = np.append(low, np.array(self._get_stock_data_from_db('low', ticker), dtype=np.float32))
        close = np.append(close, np.array(self._get_stock_data_from_db('close', ticker), dtype=np.float32))
        volume = np.append(volume, np.array(self._get_stock_data_from_db('volume', ticker), dtype=np.float32))
        
        stock_data = (date, open_, high, low, close, volume)
            
        return stock_data