import os
import time
from typing import Dict, List

import pandas as pd
from pymongo import MongoClient
from pymongo.errors import OperationFailure
from tqdm import tqdm


MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongos:27020")
DB_NAME = os.getenv("DB_NAME", "metropt")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "sensor_data")
CSV_PATH = os.getenv("CSV_PATH", "/data/metropt3.csv")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50000"))
DROP_COLLECTION = os.getenv("DROP_COLLECTION", "true").lower() == "true"
WAIT_TIMEOUT_SEC = int(os.getenv("WAIT_TIMEOUT_SEC", "180"))

TIME_FIELD = "timestamp"
META_FIELD = "COMP"

REQUIRED_COLUMNS = {
    "timestamp",
    "TP2",
    "TP3",
    "H1",
    "DV_pressure",
    "Reservoirs",
    "Oil_temperature",
    "Motor_current",
    "COMP",
    "DV_eletric",
    "Towers",
    "MPG",
    "LPS",
    "Pressure_switch",
    "Oil_level",
    "Caudal_impulses",
}


def wait_for_mongo(client: MongoClient, timeout_sec: int) -> None:
    started = time.time()
    while True:
        try:
            if client.admin.command("ping").get("ok") == 1:
                return
        except Exception:
            pass

        if time.time() - started > timeout_sec:
            raise TimeoutError(f"MongoDB at {MONGO_URI} is not ready after {timeout_sec} seconds")
        time.sleep(2)


def ensure_timeseries_collection(db) -> None:
    existing = {c["name"]: c for c in db.list_collections()}

    if COLLECTION_NAME in existing:
        if DROP_COLLECTION:
            db.drop_collection(COLLECTION_NAME)
        else:
            return

    db.create_collection(
        COLLECTION_NAME,
        timeseries={
            "timeField": TIME_FIELD,
            "metaField": META_FIELD,
            "granularity": "seconds",
        },
    )


def ensure_sharding(db) -> None:
    try:
        db.client.admin.command({"enableSharding": DB_NAME})
    except OperationFailure as exc:
        if "already enabled" not in str(exc).lower():
            raise
    try:
        db.command(
            {
                "shardCollection": f"{DB_NAME}.{COLLECTION_NAME}",
                "key": {META_FIELD: 1, TIME_FIELD: 1},
            }
        )
    except OperationFailure as exc:
        # 20 = IllegalOperation / already sharded variations
        if "already" not in str(exc).lower():
            raise


def validate_columns(chunk: pd.DataFrame) -> None:
    missing = REQUIRED_COLUMNS.difference(chunk.columns)
    if missing:
        raise ValueError(f"CSV misses required columns: {sorted(missing)}")


def normalize_chunk(chunk: pd.DataFrame) -> List[Dict]:
    chunk = chunk.where(pd.notnull(chunk), None)
    return chunk.to_dict(orient="records")


def main() -> None:
    client = MongoClient(MONGO_URI)
    wait_for_mongo(client, WAIT_TIMEOUT_SEC)

    db = client[DB_NAME]
    ensure_timeseries_collection(db)
    ensure_sharding(db)

    collection = db[COLLECTION_NAME]

    total_inserted = 0
    started = time.time()

    reader = pd.read_csv(CSV_PATH, chunksize=BATCH_SIZE, parse_dates=[TIME_FIELD])
    for chunk in tqdm(reader, desc="Importing batches"):
        validate_columns(chunk)
        records = normalize_chunk(chunk)
        if not records:
            continue
        collection.insert_many(records, ordered=False)
        total_inserted += len(records)

    elapsed = max(time.time() - started, 1e-6)
    print(f"Import completed. Inserted={total_inserted} docs, elapsed={elapsed:.2f}s, throughput={total_inserted/elapsed:.2f} docs/s")


if __name__ == "__main__":
    main()
