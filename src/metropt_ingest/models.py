"""Typed domain models for sensor readings and persisted time buckets."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from hashlib import sha256
from types import MappingProxyType
from typing import Any

# The first column in the distributed CSV has a deliberately blank header.
SOURCE_SEQUENCE_FIELD = ""

ANALOGUE_SENSOR_FIELDS: tuple[str, ...] = (
    "TP2",
    "TP3",
    "H1",
    "DV_pressure",
    "Reservoirs",
    "Motor_current",
    "Oil_temperature",
)

DIGITAL_SOURCE_FIELDS: Mapping[str, str] = MappingProxyType(
    {
        "COMP": "COMP",
        # The source dataset consistently uses this misspelling. Keep it at the
        # boundary and expose the correctly spelled name in the domain model.
        "DV_eletric": "DV_electric",
        "Towers": "Towers",
        "MPG": "MPG",
        "LPS": "LPS",
        "Pressure_switch": "Pressure_switch",
        "Oil_level": "Oil_level",
        "Caudal_impulses": "Caudal_impulses",
    }
)
DIGITAL_SENSOR_FIELDS: tuple[str, ...] = tuple(DIGITAL_SOURCE_FIELDS.values())

REQUIRED_CSV_FIELDS: tuple[str, ...] = (
    SOURCE_SEQUENCE_FIELD,
    "timestamp",
    *ANALOGUE_SENSOR_FIELDS,
    *DIGITAL_SOURCE_FIELDS,
)


@dataclass(frozen=True, slots=True)
class SensorReading:
    """One validated sensor sample, normalized to an aware UTC timestamp."""

    source_sequence: int
    timestamp: datetime
    analogue: Mapping[str, float]
    digital: Mapping[str, int]

    def __post_init__(self) -> None:
        if self.source_sequence < 0:
            raise ValueError("SensorReading.source_sequence must not be negative")
        if self.timestamp.tzinfo is None:
            raise ValueError("SensorReading.timestamp must be timezone-aware")
        if set(self.analogue) != set(ANALOGUE_SENSOR_FIELDS):
            raise ValueError("SensorReading.analogue does not match the MetroPT-3 schema")
        if set(self.digital) != set(DIGITAL_SENSOR_FIELDS):
            raise ValueError("SensorReading.digital does not match the MetroPT-3 schema")
        if not all(math.isfinite(value) for value in self.analogue.values()):
            raise ValueError("SensorReading analogue values must be finite")
        if not all(value in (0, 1) for value in self.digital.values()):
            raise ValueError("SensorReading digital values must be 0 or 1")
        object.__setattr__(self, "analogue", MappingProxyType(dict(self.analogue)))
        object.__setattr__(self, "digital", MappingProxyType(dict(self.digital)))


@dataclass(frozen=True, slots=True)
class MeasurementStats:
    minimum: float
    maximum: float
    average: float

    def to_mongo(self) -> dict[str, float]:
        return {
            "min": self.minimum,
            "max": self.maximum,
            "avg": self.average,
        }


@dataclass(frozen=True, slots=True)
class BucketStats:
    """Uniform aggregates for every measured field."""

    analogue: Mapping[str, MeasurementStats]
    digital_on_ratio: Mapping[str, float]

    def to_mongo(self) -> dict[str, Any]:
        return {
            "analogue": {field: summary.to_mongo() for field, summary in self.analogue.items()},
            "digital_on_ratio": dict(self.digital_on_ratio),
        }


@dataclass(frozen=True, slots=True)
class SensorBucket:
    """A fixed-duration set of readings for one compressor unit."""

    unit_id: str
    source_timezone: str
    bucket_start: datetime
    bucket_seconds: int
    readings: tuple[SensorReading, ...]
    stats: BucketStats

    @property
    def bucket_end(self) -> datetime:
        """Exclusive end of the fixed time window."""
        return self.bucket_start + timedelta(seconds=self.bucket_seconds)

    @property
    def document_id(self) -> str:
        """Stable identity makes reprocessing the same bucket idempotent."""
        identity = f"{self.unit_id}\0{self.bucket_seconds}\0{self.bucket_start.isoformat()}"
        return sha256(identity.encode("utf-8")).hexdigest()

    def to_mongo(self) -> dict[str, Any]:
        return {
            "_id": self.document_id,
            "unit_id": self.unit_id,
            "bucket_seconds": self.bucket_seconds,
            "bucket_start": self.bucket_start,
            "bucket_end": self.bucket_end,
            "source_timezone": self.source_timezone,
            "reading_count": len(self.readings),
            "observed_span_seconds": round(
                (self.readings[-1].timestamp - self.readings[0].timestamp).total_seconds()
            ),
            "source_sequence": {
                "first": self.readings[0].source_sequence,
                "last": self.readings[-1].source_sequence,
            },
            "readings": [
                {
                    "seq": sequence,
                    "source_seq": reading.source_sequence,
                    "ts": reading.timestamp,
                    "analogue": dict(reading.analogue),
                    "digital": dict(reading.digital),
                }
                for sequence, reading in enumerate(self.readings, start=1)
            ],
            "bucket_stats": self.stats.to_mongo(),
            "schema_version": 2,
        }
