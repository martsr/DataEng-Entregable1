import psycopg2
from io import StringIO
from sqlalchemy import create_engine


class DataLoader:
    def __init__(self, host, port, database, user, password):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connection = None
   
    def connect(self):
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            self.connection.autocommit = True
            print("Connected to Redshift")
        except Exception as e:
            print(f"Error connecting to Redshift: {e}")


    def load_data_from_dataframe(self, df, table_name, delimiter=','):
        try:
            # Create a CSV in-memory buffer
            # csv_buffer = StringIO()
            # df.to_csv(csv_buffer, sep=delimiter, index=False, header=False)
            # csv_buffer.seek(0)
            csv_data=df.to_csv(index=False, header=False)
            csv_data_io = StringIO(csv_data)
            # Execute the COPY command to load data from the buffer
            cursor = self.connection.cursor()
            copy_command = f"COPY {table_name} FROM stdin CSV DELIMITER '{delimiter}' "
            cursor.copy_expert(sql=copy_command, file=csv_data_io)
            cursor.close()
            print(f"Data loaded into {table_name}")
        except Exception as e:
            print(f"Error loading data into {table_name}: {e}")
   

    def disconnect(self):
        if self.connection:
            self.connection.close()
            print("Disconnected from Redshift")
