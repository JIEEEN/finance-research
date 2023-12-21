import pymysql
import os
import dotenv

load_dotenv()

db_config = {
    'host': os.getenv("DB_HOST")
    
}

class StockDB:
    def __init__(self):
