import json
import requests
import pandas as pd
BASE_API_URL = 'https://rickandmortyapi.com/api/character'
response_API = requests.get(BASE_API_URL, timeout=150)
data = response_API.text
get_characters = json.loads(data)['results']
characters_to_json = json.dumps(get_characters)
file = pd.read_json(characters_to_json)
DF = pd.DataFrame(file)
DF = DF.drop(['origin', 'location', 'url', 'image',
             'episode', 'created'], axis=1)
print(DF)
