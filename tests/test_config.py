from pathlib import Path

import pytest

from metropt_ingest.config import IngestionConfig
from metropt_ingest.errors import ConfigurationError


def valid_config() -> dict[str, object]:
    return {
        "input_path": Path("data/metropt+3+dataset.zip"),
        "mongo_uri": "mongodb://localhost:27017",
        "source_timezone": "UTC",
    }


def test_accepts_measured_dataset_defaults() -> None:
    config = IngestionConfig(**valid_config())  # type: ignore[arg-type]
    config.validate()
    assert config.bucket_seconds == 300
    assert config.expected_source_step == 10


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("mongo_uri", "http://localhost", "mongo_uri"),
        ("database", " ", "database"),
        ("collection", "", "collection"),
        ("unit_id", "", "unit_id"),
        ("source_timezone", "", "source_timezone"),
        ("bucket_seconds", 0, "bucket_seconds"),
        ("expected_source_step", 0, "expected_source_step"),
        ("batch_size", 0, "batch_size"),
        ("max_rows", 0, "max_rows"),
    ],
)
def test_rejects_invalid_configuration(field: str, value: object, message: str) -> None:
    values = valid_config()
    values[field] = value

    with pytest.raises(ConfigurationError, match=message):
        IngestionConfig(**values).validate()  # type: ignore[arg-type]


def test_rejects_partial_database_ingestion() -> None:
    values = valid_config() | {"max_rows": 1000}

    with pytest.raises(ConfigurationError, match="only safe with dry_run"):
        IngestionConfig(**values).validate()  # type: ignore[arg-type]
