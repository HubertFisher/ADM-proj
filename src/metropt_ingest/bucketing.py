"""Pure bucketing logic, independent of storage and file formats."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from datetime import UTC, datetime

from metropt_ingest.errors import ConfigurationError, DataValidationError
from metropt_ingest.models import (
    ANALOGUE_SENSOR_FIELDS,
    DIGITAL_SENSOR_FIELDS,
    BucketStats,
    MeasurementStats,
    SensorBucket,
    SensorReading,
)


def bucket_start_for(timestamp: datetime, bucket_seconds: int) -> datetime:
    if bucket_seconds <= 0:
        raise ConfigurationError("bucket_seconds must be greater than zero")
    epoch_seconds = int(timestamp.astimezone(UTC).timestamp())
    floored_seconds = epoch_seconds - (epoch_seconds % bucket_seconds)
    return datetime.fromtimestamp(floored_seconds, tz=UTC)


def iter_buckets(
    readings: Iterable[SensorReading],
    *,
    unit_id: str,
    source_timezone: str,
    bucket_seconds: int = 300,
) -> Iterator[SensorBucket]:
    """Group chronologically ordered readings into fixed time windows."""
    if not unit_id.strip():
        raise ConfigurationError("unit_id must not be empty")
    if not source_timezone.strip():
        raise ConfigurationError("source_timezone must not be empty")
    if bucket_seconds <= 0:
        raise ConfigurationError("bucket_seconds must be greater than zero")

    active_start: datetime | None = None
    active_readings: list[SensorReading] = []
    previous_timestamp: datetime | None = None

    for reading in readings:
        if previous_timestamp is not None and reading.timestamp <= previous_timestamp:
            raise DataValidationError(
                "CSV readings must have unique, increasing timestamps; "
                f"{reading.timestamp.isoformat()} follows {previous_timestamp.isoformat()}"
            )
        previous_timestamp = reading.timestamp
        reading_bucket_start = bucket_start_for(reading.timestamp, bucket_seconds)

        if active_start is not None and reading_bucket_start != active_start:
            yield _build_bucket(
                unit_id, source_timezone, active_start, bucket_seconds, active_readings
            )
            active_readings = []

        active_start = reading_bucket_start
        active_readings.append(reading)

    if active_start is not None:
        yield _build_bucket(unit_id, source_timezone, active_start, bucket_seconds, active_readings)


def _build_bucket(
    unit_id: str,
    source_timezone: str,
    bucket_start: datetime,
    bucket_seconds: int,
    readings: list[SensorReading],
) -> SensorBucket:
    def average(samples: list[float]) -> float:
        return round(sum(samples) / len(samples), 6)

    analogue_stats: dict[str, MeasurementStats] = {}
    for field in ANALOGUE_SENSOR_FIELDS:
        values = [reading.analogue[field] for reading in readings]
        analogue_stats[field] = MeasurementStats(
            minimum=round(min(values), 6),
            maximum=round(max(values), 6),
            average=average(values),
        )

    stats = BucketStats(
        analogue=analogue_stats,
        digital_on_ratio={
            field: average([float(reading.digital[field]) for reading in readings])
            for field in DIGITAL_SENSOR_FIELDS
        },
    )
    return SensorBucket(
        unit_id=unit_id,
        source_timezone=source_timezone,
        bucket_start=bucket_start,
        bucket_seconds=bucket_seconds,
        readings=tuple(readings),
        stats=stats,
    )
