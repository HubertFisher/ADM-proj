from pymongo import MongoClient
import pandas as pd
from tqdm import tqdm

client = MongoClient("mongodb://mongos:27020")
db = client["metropt"]
admin_db = client["admin"]

if "sensor_data" in db.list_collection_names():
    db.drop_collection("sensor_data")

try:
    admin_db.command("enableSharding", "metropt")
except Exception as e:
    print(f"error: {e}")

db.create_collection(
    "sensor_data",
    timeseries={
        "timeField": "timestamp",
        "metaField": "COMP",
        "granularity": "seconds"
    }
)

admin_db.command(
    "shardCollection",
    "metropt.sensor_data",
    key={"COMP": 1, "timestamp": 1}
)

collection = db["sensor_data"]

csv_file = "/data/metropt3.csv"
chunk_size = 50000

# Явное разделение типов
analog_columns = [
    "TP2", "TP3", "H1", "DV_pressure",
    "Reservoirs", "Motor_current",
    "Oil_temperature"
]

digital_columns = [
    "COMP", "DV_eletric", "Towers", "MPG",
    "LPS", "Pressure_switch", "Oil_level",
    "Caudal_impulses"
]

print("Starting import...")

for chunk in tqdm(
    pd.read_csv(
        csv_file,
        chunksize=chunk_size,
        parse_dates=["timestamp"]
    )
):
    # Удаляем лишнюю колонку если есть
    if "" in chunk.columns:
        chunk = chunk.drop(columns=[""])

    # Приведение типов
    for col in analog_columns:
        chunk[col] = pd.to_numeric(chunk[col], errors="coerce")

    for col in digital_columns:
        chunk[col] = pd.to_numeric(chunk[col], errors="coerce").astype("Int8")

    # timestamp уже datetime благодаря parse_dates

    records = chunk.to_dict(orient="records")
    collection.insert_many(records, ordered=False)

print("Import completed!")
