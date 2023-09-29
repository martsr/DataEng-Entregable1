import json
import requests


class DataExtractor:
    
    @staticmethod
    def extract_data():
        api_url = "https://eonet.gsfc.nasa.gov/api/v2.1/events"

        response = requests.get(api_url, timeout=150)
        if response.status_code == 200:
            data = json.loads(response.text)
            if data["events"]:
                return data["events"]
            
            else:
                print("Failed to retrieve data from the API")
                return None
                


        else:
            print("No data available to create DataFrames.")

        
        



