import pandas as pd
from pandas import json_normalize
import json
import requests


def events_df_creation(events):
    df = json_normalize(
        events,
        "categories",
        ["id", "title", "description", "link"],
        record_prefix="category_",
    )
    df = df.drop(["category_title"], axis=1)
    df["id"] = df["id"].str.replace(r"^EONET_", "", regex=True)
    columns_order = ["id", "title", "description", "link", "category_id"]
    return df[columns_order]


def category_df_creation(events):
    df = json_normalize(events, "categories").drop_duplicates()
    return df


def geo_df_creation(events):
    df = json_normalize(events, "geometries", ["id"], record_prefix="geo_")
    df = df.rename(columns={"id": "event_id", "geo_date": "date", "geo_type": "type"})
    df["longitude"] = df["geo_coordinates"].apply(lambda x: x[0])
    df["latitude"] = df["geo_coordinates"].apply(lambda x: x[1])
    df = df.drop(["geo_coordinates"], axis=1)
    df["event_id"] = df["event_id"].str.replace(r"^EONET_", "", regex=True)
    df.reset_index(inplace=True)
    df.rename(columns={"index": "id"}, inplace=True)
    column_order = ["id", "event_id", "date", "type", "longitude", "latitude"]
    return df[column_order]


api_url = "https://eonet.gsfc.nasa.gov/api/v2.1/events"

response = requests.get(api_url, timeout=150)
if response.status_code == 200:
    # Paso de un string (.text) a un dict (json.loads)
    data = json.loads(response.text)

else:
    print("Failed to retrieve data from the API")
    data = None

if data:
    events = data["events"]
    events_df = events_df_creation(events)
    cat_df = category_df_creation(events)
    geo_df = geo_df_creation(events)
    print(events_df)
    print(cat_df)
    print(geo_df)

else:
    print("No data available to create DataFrames.")
