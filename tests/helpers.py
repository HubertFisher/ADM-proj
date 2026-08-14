from __future__ import annotations

import csv
import zipfile
from datetime import UTC, datetime
from pathlib import Path

from metropt_ingest.models import (
    ANALOGUE_SENSOR_FIELDS,
    DIGITAL_SENSOR_FIELDS,
    REQUIRED_CSV_FIELDS,
    SOURCE_SEQUENCE_FIELD,
    SensorReading,
)


def sensor_reading(
    timestamp: datetime,
    value: float = 1.0,
    *,
    source_sequence: int = 0,
    digital_value: int = 1,
) -> SensorReading:
    return SensorReading(
        source_sequence=source_sequence,
        timestamp=timestamp.astimezone(UTC),
        analogue={field: value for field in ANALOGUE_SENSOR_FIELDS},
        digital={field: digital_value for field in DIGITAL_SENSOR_FIELDS},
    )


def source_row(
    *, source_sequence: int = 0, timestamp: str = "2020-02-01 00:00:00"
) -> dict[str, str]:
    row = {field: "1.0" for field in REQUIRED_CSV_FIELDS}
    row[SOURCE_SEQUENCE_FIELD] = str(source_sequence)
    row["timestamp"] = timestamp
    row["TP2"] = "-0.0120000000000004"
    row["Motor_current"] = "0.04"
    row["Oil_temperature"] = "53.60000000000001"
    return row


def write_csv(
    path: Path,
    rows: list[dict[str, str]],
    fields: list[str] | None = None,
) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields or list(REQUIRED_CSV_FIELDS))
        writer.writeheader()
        writer.writerows(rows)


def write_zip(path: Path, csv_path: Path, member: str = "MetroPT3(AirCompressor).csv") -> None:
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.write(csv_path, member)
        archive.writestr("README.txt", "not a data member")
