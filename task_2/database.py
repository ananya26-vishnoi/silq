

import psycopg2
import logging

class Database:
    def __init__(self, host, port, database, user, password):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connection = None
        self.cursor = None

        # Configure logging
        logging.basicConfig(filename='database_operations.log', level=logging.INFO)
        self.connect()

    def connect(self):
        try:
            # Establish a connection to the PostgreSQL database
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            # Create a cursor object to interact with the database
            self.cursor = self.connection.cursor()

            logging.info("Connected to the database!")

        except Exception as e:
            logging.error(f"Error: Unable to connect to the database. {str(e)}")

    def create_table(self, table_name, fields):
        try:
            if self.table_exists(table_name):
                logging.info("Table already exists")
                return True
            self.cursor.execute("CREATE TABLE public." +
                                table_name + " (" + fields + ")")
            self.connection.commit()
            logging.info("Table created successfully")
            return True
        except Exception as e:
            logging.error(f"Error: Unable to create table. {str(e)}")
            return False

    def table_exists(self, table_name):
        try:
            self.cursor.execute(
                "SELECT to_regclass('public." + table_name + "')")
            table_exists = self.cursor.fetchone()[0]
            return table_exists
        except Exception as e:
            logging.error(f"Error: Unable to check table. {str(e)}")
            return False
    def already_exists(self,table_name,fields):
        try:
            query = f'SELECT * FROM {table_name} WHERE '
            for key, value in fields.items():
                temp = str(key) + " = " + str(value) + " AND "
                query+=temp
            query = query.split()
            query = ' '.join(query[:-1])
            query = query + ";"
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            if result:
                return True
            return False
        except Exception as e:
            logging.error(f"Error: Unable to search records. {str(e)}")
            return False

    def insert_orders(self, order_id, user_id, product_id, order_date, amount):
        if not self.table_exists('orders'):
            self.create_table(
                "orders", "order_id int, user_id int, product_id int, order_date date, amount float")

        try:
            fields = {
                "order_id" : order_id
            }
            if self.already_exists('public.orders',fields):
                logging.info("Record already present")
                return False
            self.cursor.execute(
                "INSERT INTO public.orders (order_id,user_id,product_id,order_date,amount) VALUES (%s,%s,%s,%s,%s)",
                (order_id, user_id, product_id, order_date, amount))
            self.connection.commit()
            logging.info("Records inserted successfully")
            return True
        except Exception as e:
            logging.error(f"Error: Unable to insert records. {str(e)}")
            return False

    def insert_products(self, product_id, product_name, price):
        if not self.table_exists(table_name='products'):
            self.create_table(
                "products", "product_id int, product_name varchar(100), price float")

        try:
            fields={
                'product_id' : product_id
            }
            if self.already_exists('public.products',fields):
                logging.info("Record already present")
                return False
            self.cursor.execute(
                "INSERT INTO public.products (product_id,product_name,price) VALUES (%s,%s,%s)",
                (product_id, product_name, price))
            self.connection.commit()
            logging.info("Records inserted successfully")
            return True
        except Exception as e:
            logging.error(f"Error: Unable to insert records. {str(e)}")
            return False

    def insert_users(self, user_id, signup_date, location):
        if not self.table_exists('users'):
            self.create_table(
                "users", "user_id int, signup_date date, location varchar(100)")

        try:
            fields={
                'user_id' : user_id
            }
            if self.already_exists('public.users',fields):
                logging.info("Record already present")
                return False
            self.cursor.execute(
                "INSERT INTO public.users (user_id,signup_date,location) VALUES (%s,%s,%s)",
                (user_id, signup_date, location))
            self.connection.commit()
            logging.info("Records inserted successfully")
            return True
        except Exception as e:
            logging.error(f"Error: Unable to insert records. {str(e)}")
            return False

    def disconnect(self):
        try:
            # Close the cursor and connection
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()

            logging.info("Disconnected from the database!")

        except Exception as e:
            logging.error(f"Error: Unable to disconnect from the database. {str(e)}")
