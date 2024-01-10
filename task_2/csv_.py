import pandas as pd
import logging
from database import Database
from dotenv import load_dotenv
load_dotenv()
import os

class CSV:
    def __init__(self,file_name):
        self.file_name = file_name
        self.database = Database(os.getenv('DB_HOST'), os.getenv('DB_PORT'), os.getenv('DB_NAME'), os.getenv('DB_USER'), os.getenv('DB_PASSWORD'))

    def handle_missing_data(self,data):
        for x in data:
            if str(x) == '':
                return False
        return True


    def insert_data(self):
        df = pd.read_csv(self.file_name, delimiter='\t')
        for row in df.values:
            data = row[0]
            data = data.split(',')
            if self.handle_missing_data(data):
                if self.file_name == "Orders.csv":
                    self.database.insert_orders(data[0],data[1],data[2],data[3],data[4])
                    logging.info("Data inserted : " + str(data))
                elif self.file_name == 'Products.csv':
                    self.database.insert_products(data[0],data[1],data[2])
                    logging.info("Data inserted : " + str(data))
                elif self.file_name == "Users.csv":
                    self.database.insert_users(data[0],data[1],data[2])
                    logging.info("Data inserted : " + str(data))
            else:
                logging.error("Invalid data : " +str(data))

