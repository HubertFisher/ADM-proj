# scripts/import.py
import pandas as pd
from pymongo import MongoClient
from tqdm import tqdm

# Параметры подключения
MONGOS_URI = "mongodb://localhost:27020"  # mongos контейнер
DB_NAME = "metropt"
COLLECTION_NAME = "sensor_data"
CSV_PATH = "./data/metropt3.csv"  # путь на хосте, смонтированный в контейнер

# Пакетная вставка
BATCH_SIZE = 50000

# Подключаемся
client = MongoClient(MONGOS_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# Чтение CSV по кускам
for chunk in tqdm(pd.read_csv(CSV_PATH, chunksize=BATCH_SIZE)):
    # Преобразуем строки в словари для MongoDB
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
    
    # Вставляем пакет
    collection.insert_many(docs)

print("Import complete.")
