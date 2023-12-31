import psycopg2
from io import StringIO


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
            
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, sep=delimiter, index=False, header=False)
            csv_buffer.seek(0)
            

            cursor = self.connection.cursor()
            copy_command = f"COPY {table_name} FROM stdin CSV DELIMITER '{delimiter}' "
            cursor.copy_expert(sql=copy_command, file=csv_buffer)
            cursor.close()
            print(f"Data loaded into {table_name}")
        except Exception as e:
            print(f"Error loading data into {table_name}: {e}")
   

    def disconnect(self):
        if self.connection:
            self.connection.close()
            print("Disconnected from Redshift")
