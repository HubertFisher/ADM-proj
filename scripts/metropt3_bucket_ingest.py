"""
metropt3_mongo_ingest.py
────────────────────────
Reads the entire MetroPT-3 predictive-maintenance CSV, applies the MongoDB Bucket
Pattern (60-second windows per unit), and bulk-inserts the resulting
documents with majority write-concern.

Requirements:
    pip install pandas pymongo

Usage:
    python metropt3_mongo_ingest.py
    python metropt3_mongo_ingest.py --csv path/to/file.csv --uri mongodb://host:27017
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from typing import Generator

import pandas as pd
from pymongo import MongoClient, WriteConcern
from pymongo.collection import Collection
from pymongo.errors import BulkWriteError, ConnectionFailure

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("metropt3_ingest")


# ── Constants ──────────────────────────────────────────────────────────────────
DEFAULT_CSV_PATH   = "data/MetroPT3.csv"
DEFAULT_MONGO_URI  = "mongodb://localhost:27017"
DEFAULT_DB         = "metropt3"
DEFAULT_COLLECTION = "sensor_buckets"
DEFAULT_UNIT_ID    = "compressor_unit_01"

BATCH_SIZE     = 5000          # documents per insert_many call
BUCKET_SECONDS = 60

# Sensor column groups (graceful fallback if a column is absent from the file)
ANALOGUE_COLS: list[str] = [
    "TP2", "TP3", "H1", "DV_pressure",
    "Reservoirs", "Motor_current", "Oil_temperature",
]
DIGITAL_COLS: list[str] = [
    "COMP", "DV_electric", "Towers", "MPG",
    "LPS", "Pressure_switch", "Oil_level", "Caudal_impulses",
]

# Stats derived per bucket  {output_key: source_column}
STAT_DEFINITIONS: dict[str, str] = {
    "avg_motor_current": "Motor_current",
    "max_motor_current": "Motor_current",
    "min_motor_current": "Motor_current",
    "avg_oil_temp":      "Oil_temperature",
    "max_oil_temp":      "Oil_temperature",
    "avg_TP2":           "TP2",
    "avg_TP3":           "TP3",
    "avg_DV_pressure":   "DV_pressure",
}


# ── Step 1 – Load CSV ──────────────────────────────────────────────────────────
def load_csv(path: str) -> pd.DataFrame:
    """Read the complete CSV file, parse timestamps rapidly, sort chronologically."""
    logger.info("Loading entire dataset from '%s' …", path)

    # Загружаем файл целиком без использования nrows
    df = pd.read_csv(
        path,
        low_memory=False,
    )

    if "timestamp" not in df.columns:
        raise ValueError("CSV must contain a 'timestamp' column.")

    # Быстрый парсинг даты с явным указанием формата для ускорения работы с полным файлом
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y-%m-%d %H:%M:%S", errors="raise")
    df.sort_values("timestamp", inplace=True, ignore_index=True)

    logger.info("Successfully loaded %s rows | columns: %s", f"{len(df):,}", list(df.columns))
    return df


# ── Step 2 – Bucket assignment ─────────────────────────────────────────────────
def assign_buckets(df: pd.DataFrame, unit_id: str, bucket_seconds: int) -> pd.DataFrame:
    """
    Add two synthetic columns:
        unit_id      – constant identifier (single compressor prototype)
        bucket_start – floor of the timestamp to the nearest *bucket_seconds* epoch
    """
    epoch_s = df["timestamp"].astype("int64") // 10**9
    df["bucket_start"] = pd.to_datetime(
        (epoch_s // bucket_seconds) * bucket_seconds,
        unit="s",
        utc=False,
    )
    df["unit_id"] = unit_id
    logger.info(
        "Assigned %s unique 60-second buckets across unit '%s'.",
        f"{df[['unit_id', 'bucket_start']].drop_duplicates().shape[0]:,}",
        unit_id,
    )
    return df


# ── Step 3+4 – Build bucket documents ─────────────────────────────────────────
def _compute_stat(series: pd.Series, stat_key: str) -> float:
    """Dispatch a single aggregation by key prefix."""
    if stat_key.startswith("avg_"):
        return round(float(series.mean()), 6)
    if stat_key.startswith("max_"):
        return round(float(series.max()), 6)
    if stat_key.startswith("min_"):
        return round(float(series.min()), 6)
    return round(float(series.mean()), 6)


def _build_readings(
    group: pd.DataFrame,
    analogue_cols: list[str],
    digital_cols: list[str],
) -> list[dict]:
    """Vectorised reading construction."""
    keep = ["timestamp"] + analogue_cols + digital_cols
    present = [c for c in keep if c in group.columns]
    records = group[present].to_dict("records")

    readings: list[dict] = []
    for rec in records:
        ts = rec["timestamp"]
        analogue = {
            col: round(float(rec[col]), 6)
            for col in analogue_cols
            if col in rec and pd.notna(rec[col])
        }
        digital = {
            col: int(rec[col])
            for col in digital_cols
            if col in rec and pd.notna(rec[col])
        }
        readings.append(
            {
                "ts": ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts,
                "analogue": analogue,
                "digital": digital,
            }
        )
    return readings


def _build_stats(
    group: pd.DataFrame,
    stat_definitions: dict[str, str],
) -> dict[str, float]:
    stats: dict[str, float] = {}
    for stat_key, src_col in stat_definitions.items():
        if src_col in group.columns:
            stats[stat_key] = _compute_stat(group[src_col], stat_key)
    return stats


def document_generator(
    df: pd.DataFrame,
    analogue_cols: list[str],
    digital_cols: list[str],
    stat_definitions: dict[str, str],
) -> Generator[dict, None, None]:
    """Yields one MongoDB document per (unit_id, bucket_start) group."""
    now_utc = datetime.now(timezone.utc)
    grouped = df.groupby(["unit_id", "bucket_start"], sort=True)
    total = len(grouped)

    for i, ((unit_id, bucket_start), group) in enumerate(grouped, 1):
        readings = _build_readings(group, analogue_cols, digital_cols)
        stats    = _build_stats(group, stat_definitions)

        yield {
            "unit_id":      unit_id,
            "bucket_start": bucket_start.to_pydatetime(),
            "bucket_end":   group["timestamp"].max().to_pydatetime(),
            "nsamples":     len(readings),
            "bucket_stats": stats,
            "readings":     readings,
            "schema_version": 1,
            "created_at":   now_utc,
        }

        if i % 500 == 0 or i == total:
            logger.info("  Built %s / %s bucket documents …", f"{i:,}", f"{total:,}")


# ── Step 5 – MongoDB insertion ─────────────────────────────────────────────────
def _ensure_indexes(collection: Collection) -> None:
    """Compound unique index on (unit_id, bucket_start)."""
    collection.create_index(
        [("unit_id", 1), ("bucket_start", 1)],
        unique=True,
        name="uq_unit_bucket",
        background=True,
    )
    collection.create_index(
        [("bucket_start", 1)],
        name="idx_bucket_start",
        background=True,
    )
    logger.info("Indexes verified / created.")


def _batched(
    generator: Generator[dict, None, None],
    batch_size: int,
) -> Generator[list[dict], None, None]:
    batch: list[dict] = []
    for doc in generator:
        batch.append(doc)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def bulk_insert(
    doc_generator: Generator[dict, None, None],
    uri: str,
    db_name: str,
    collection_name: str,
    batch_size: int,
) -> int:
    """Streams documents from generator into MongoDB with majority write-concern."""
    client = MongoClient(uri, serverSelectionTimeoutMS=5_000)
    try:
        client.admin.command("ping")
    except ConnectionFailure as exc:
        logger.error("MongoDB unreachable at '%s': %s", uri, exc)
        raise

    logger.info("Connected to MongoDB at '%s'.", uri)

    wc = WriteConcern(w="majority", j=True)
    collection = client[db_name].get_collection(collection_name, write_concern=wc)
    _ensure_indexes(collection)

    total_inserted = 0
    total_skipped  = 0

    for batch_num, batch in enumerate(_batched(doc_generator, batch_size), 1):
        try:
            result = collection.insert_many(batch, ordered=False)
            inserted = len(result.inserted_ids)
            total_inserted += inserted
            logger.info(
                "Batch %s → inserted %s | running total: %s",
                batch_num, inserted, f"{total_inserted:,}",
            )
        except BulkWriteError as bwe:
            n_ok  = bwe.details.get("nInserted", 0)
            errs  = bwe.details.get("writeErrors", [])
            dups  = sum(1 for e in errs if e.get("code") == 11000)
            other = len(errs) - dups
            total_inserted += n_ok
            total_skipped  += dups
            if dups:
                logger.warning("Batch %s → %s inserted, %s duplicate(s) skipped.", batch_num, n_ok, dups)
            if other:
                logger.error(
                    "Batch %s → %s non-duplicate error(s): %s",
                    batch_num, other,
                    [e for e in errs if e.get("code") != 11000][:3],
                )

    client.close()
    logger.info(
        "Insertion complete. Inserted: %s | Skipped (duplicates): %s",
        f"{total_inserted:,}", f"{total_skipped:,}",
    )
    return total_inserted


# ── CLI + main ─────────────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="MetroPT-3 → MongoDB Bucket Pattern ingestion.")
    p.add_argument("--csv",        default=DEFAULT_CSV_PATH,   help="Path to the MetroPT-3 CSV file.")
    p.add_argument("--uri",        default=DEFAULT_MONGO_URI,  help="MongoDB connection URI.")
    p.add_argument("--db",         default=DEFAULT_DB,         help="Target database name.")
    p.add_argument("--collection", default=DEFAULT_COLLECTION, help="Target collection name.")
    p.add_argument("--unit-id",    default=DEFAULT_UNIT_ID,    help="Logical unit identifier.")
    p.add_argument("--batch-size", default=BATCH_SIZE, type=int, help="Insert batch size.")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    # Загрузка полной базы без ограничений
    df = load_csv(args.csv)

    analogue_cols = [c for c in ANALOGUE_COLS if c in df.columns]
    digital_cols  = [c for c in DIGITAL_COLS  if c in df.columns]
    missing       = set(ANALOGUE_COLS) - set(analogue_cols)
    if missing:
        logger.warning("Analogue columns not found in CSV (will be omitted): %s", missing)

    stat_defs = {k: v for k, v in STAT_DEFINITIONS.items() if v in analogue_cols}

    df = assign_buckets(df, args.unit_id, BUCKET_SECONDS)
    doc_gen = document_generator(df, analogue_cols, digital_cols, stat_defs)
    bulk_insert(doc_gen, args.uri, args.db, args.collection, args.batch_size)

    logger.info("Pipeline finished successfully.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        logger.exception("Fatal error: %s", exc)
        sys.exit(1)