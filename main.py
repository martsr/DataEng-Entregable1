from dotenv import load_dotenv
import os 
from ETL.extract import DataExtractor
from ETL.transform import DataTransformer
from ETL.load import DataLoader

def fetch_redshift_credentials():
    load_dotenv()
    config = {
        'host': os.getenv("HOST"),
        'port': os.getenv("PORT"),
        'database': os.getenv("DATABASE"),
        'user': os.getenv("USER"),
        'password': os.getenv("PASSWORD")
        }
    return config


def upload_dataframes_to_redshift(connector, dataframes_and_table_names):
    connector.connect()
    
    for df, table_name in dataframes_and_table_names:
        connector.load_data_from_dataframe(df, table_name)
    

def main():
    print("Extracting Data ")
    events_data =DataExtractor.extract_data()
    if(events_data is None):
        return 
    
    print("Creating Data Frames")
    dataframes_and_table_names = [
    (DataTransformer.events_df_creation(events_data), 'events'),
    (DataTransformer.category_df_creation(events_data), 'categories'),
    (DataTransformer.geo_df_creation(events_data), 'geometries')
]
    print("Configuring Redshift")
    redshift_config=fetch_redshift_credentials()
    
    print("Connecting to redshift")
    connector = DataLoader(**redshift_config)
    print("Uploading data ")
    upload_dataframes_to_redshift(connector, dataframes_and_table_names)
    print("Finished uploading!")







if __name__ == "__main__":
    main()