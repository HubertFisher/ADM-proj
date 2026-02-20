from pymongo import MongoClient
import pandas as pd
from tqdm import tqdm

client = MongoClient("mongodb://mongos:27020")
db = client["metropt"]

# Удаляем старую коллекцию (если была)
if "sensor_data" in db.list_collection_names():
    db.drop_collection("sensor_data")

# Создаём Time Series Collection
db.create_collection(
    "sensor_data",
    timeseries={
        "timeField": "timestamp",
        "metaField": "COMP",
        "granularity": "seconds"
    }
)

# Шардируем коллекцию до вставки данных
db.command("shardCollection", "metropt.sensor_data", key={"COMP": 1, "timestamp": 1})

collection = db["sensor_data"]

# Чтение CSV чанками
csv_file = "/data/metropt3.csv"
chunk_size = 50000

for chunk in tqdm(pd.read_csv(csv_file, chunksize=chunk_size, parse_dates=["timestamp"])):
    records = chunk.to_dict(orient="records")
    collection.insert_many(records)

print("Import completed!")
