from pandas import json_normalize

class DataTransformer:
    
    @staticmethod
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

    @staticmethod
    def category_df_creation(events):
        df = json_normalize(events, "categories").drop_duplicates()
        return df

    @staticmethod
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