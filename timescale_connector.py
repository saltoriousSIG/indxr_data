import psycopg2
from psycopg2 import sql

class TimescaleConnection: 
    def __init__(self, host, port, dbname, user, pw): 
        self.connection_string = f"host={host} port={port} dbname={dbname} user={user} password={pw}"
        self.conn = None
        self.cursor = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(self.connection_string)
            self.cursor = self.conn.cursor()
            print("DB Connected")
        except e: 
            print(e)
            print("There was an error establishing a connection")

    def close(self):
        if self.cursor: 
            self.cursor.close()
        if self.conn: 
            self.conn.close()
        print("Connection closed successfully")

    # data is a dict
    def insert(self, table_name, data): 
        try: 
            cols = ', '.join(data.keys())
            vals = ', '.join(['%s'] * len(data))
            query = f"INSERT INTO {table_name} ({cols}) VALUES ({vals})"
            self.cursor.execute(query, list(data.values()))
            self.conn.commit()
            print("Data inserted")
        except Exception as err: 
            print(err)
            print("There was an error inserting data")

    def exec(self, query): 
        try: 
            self.cursor.execute(query)
            return self.cursor.fetchall()
            print("Query executed")
        except:
            print("There was a read error")


    
