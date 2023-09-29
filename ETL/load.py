from sqlalchemy import create_engine


class DataLoader:
    def __init__(self, host, port, database, user, password):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.engine = None
   
    def connect(self):
        try:
            self.engine = create_engine(f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}')
            print("Connected to Redshift")
        except Exception as e:
            print(f"Error connecting to Redshift: {e}")


    def load_data_from_dataframe(self, df, table_name):
        try:
            df.to_sql(table_name, self.engine, index=False, if_exists='replace')
            print(f"Data loaded into {table_name}")

        except Exception as e:
            print(f"Error loading data into {table_name}: {e}")
   

