# scripts/setup_import.py
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import CollectionInvalid
from tqdm import tqdm

# Настройки подключения
MONGOS_URI = "mongodb://localhost:27020"  # mongos
DB_NAME = "metropt"
COLLECTION_NAME = "sensor_data"
CSV_PATH = "./data/metropt3.csv"
BATCH_SIZE = 50000

client = MongoClient(MONGOS_URI)
db = client[DB_NAME]

# --- 1. Создаем Time Series коллекцию ---
try:
    db.create_collection(
        COLLECTION_NAME,
        timeseries={
            "timeField": "timestamp",
            "metaField": "sensor_id",
            "granularity": "seconds"
        }
    )
    print(f"Collection '{COLLECTION_NAME}' created as Time Series collection.")
except CollectionInvalid:
    print(f"Collection '{COLLECTION_NAME}' already exists.")

# --- 2. Включаем шардирование для базы и коллекции ---
admin_db = client["admin"]

# Включаем шардирование базы
admin_db.command("enableSharding", DB_NAME)
print(f"Sharding enabled for database '{DB_NAME}'.")

# Шардируем коллекцию по sensor_id
shard_key = {"sensor_id": "hashed"}
try:
    admin_db.command(
        "shardCollection",
        f"{DB_NAME}.{COLLECTION_NAME}",
        key=shard_key
    )
    print(f"Collection '{COLLECTION_NAME}' sharded on key: {shard_key}")
except Exception as e:
    print(f"ShardCollection error: {e}")

# --- 3. Импорт CSV в коллекцию пакетами ---
print("Starting data import...")
for chunk in tqdm(pd.read_csv(CSV_PATH, chunksize=BATCH_SIZE)):
    docs = []
    for _, row in chunk.iterrows():
        doc = {
            "timestamp": pd.to_datetime(row["timestamp"]),
            "sensor_id": row["sensor_id"],
            "motor_current": float(row["motor_current"]),
            "pressure": float(row["pressure"]),
            "temperature": float(row["temperature"]),
            "air_intake": float(row["air_intake"])
        }
        docs.append(doc)
    collection = db[COLLECTION_NAME]
    collection.insert_many(docs)

print("Data import complete.")
