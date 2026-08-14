"""Runtime configuration and validation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from metropt_ingest.errors import ConfigurationError


@dataclass(frozen=True, slots=True)
class IngestionConfig:
    input_path: Path
    mongo_uri: str
    source_timezone: str
    database: str = "metropt3"
    collection: str = "sensor_buckets"
    unit_id: str = "APU_METRO_01"
    archive_member: str | None = None
    expected_source_step: int = 10
    bucket_seconds: int = 300
    batch_size: int = 1_000
    max_rows: int | None = None
    dry_run: bool = False

    def validate(self) -> None:
        if not self.mongo_uri.startswith(("mongodb://", "mongodb+srv://")):
            raise ConfigurationError("mongo_uri must use mongodb:// or mongodb+srv://")
        for name, value in (
            ("database", self.database),
            ("collection", self.collection),
            ("unit_id", self.unit_id),
            ("source_timezone", self.source_timezone),
        ):
            if not value.strip():
                raise ConfigurationError(f"{name} must not be empty")
        if self.bucket_seconds <= 0:
            raise ConfigurationError("bucket_seconds must be greater than zero")
        if self.expected_source_step <= 0:
            raise ConfigurationError("expected_source_step must be greater than zero")
        if self.batch_size <= 0:
            raise ConfigurationError("batch_size must be greater than zero")
        if self.max_rows is not None and self.max_rows <= 0:
            raise ConfigurationError("max_rows must be greater than zero")
        if self.max_rows is not None and not self.dry_run:
            raise ConfigurationError("max_rows is only safe with dry_run")
