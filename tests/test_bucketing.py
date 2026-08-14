from datetime import UTC, datetime, timedelta

import pytest

from metropt_ingest.bucketing import iter_buckets
from metropt_ingest.errors import ConfigurationError, DataValidationError
from tests.helpers import sensor_reading


def test_groups_actual_cadence_into_five_minute_buckets() -> None:
    start = datetime(2020, 2, 1, tzinfo=UTC)
    offsets = (0, 10, 19, 29, 41, 299, 309)
    readings = [
        sensor_reading(
            start + timedelta(seconds=offset), float(index + 1), source_sequence=index * 10
        )
        for index, offset in enumerate(offsets)
    ]

    buckets = list(
        iter_buckets(
            readings,
            unit_id="APU_01",
            source_timezone="UTC",
            bucket_seconds=300,
        )
    )

    assert [len(bucket.readings) for bucket in buckets] == [6, 1]
    assert buckets[0].bucket_end == start + timedelta(minutes=5)
    assert buckets[0].stats.analogue["Motor_current"].average == 3.5
    assert buckets[0].stats.analogue["TP2"].minimum == 1.0
    assert buckets[0].stats.digital_on_ratio["COMP"] == 1.0


def test_document_contains_source_lineage_and_uniform_stats() -> None:
    start = datetime(2020, 2, 1, tzinfo=UTC)
    bucket = next(
        iter_buckets(
            [sensor_reading(start, source_sequence=100)],
            unit_id="APU_01",
            source_timezone="Etc/UTC",
        )
    )

    document = bucket.to_mongo()
    assert document["bucket_seconds"] == 300
    assert document["source_timezone"] == "Etc/UTC"
    assert document["source_sequence"] == {"first": 100, "last": 100}
    assert document["readings"][0]["source_seq"] == 100
    assert document["schema_version"] == 2
    assert set(document["bucket_stats"]["analogue"]) == {
        "TP2",
        "TP3",
        "H1",
        "DV_pressure",
        "Reservoirs",
        "Motor_current",
        "Oil_temperature",
    }


def test_document_identity_includes_bucket_resolution() -> None:
    start = datetime(2020, 2, 1, tzinfo=UTC)
    reading = sensor_reading(start)
    minute = next(
        iter_buckets([reading], unit_id="APU_01", source_timezone="UTC", bucket_seconds=60)
    )
    five_minutes = next(
        iter_buckets([reading], unit_id="APU_01", source_timezone="UTC", bucket_seconds=300)
    )

    assert minute.document_id != five_minutes.document_id


@pytest.mark.parametrize("offsets", [(10, 0), (0, 0)])
def test_rejects_nonincreasing_timestamps(offsets: tuple[int, int]) -> None:
    start = datetime(2020, 2, 1, tzinfo=UTC)
    readings = [sensor_reading(start + timedelta(seconds=offset)) for offset in offsets]

    with pytest.raises(DataValidationError, match="unique, increasing"):
        list(iter_buckets(readings, unit_id="APU_01", source_timezone="UTC"))


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"unit_id": "", "source_timezone": "UTC"}, "unit_id"),
        ({"unit_id": "APU", "source_timezone": ""}, "source_timezone"),
        ({"unit_id": "APU", "source_timezone": "UTC", "bucket_seconds": 0}, "bucket_seconds"),
    ],
)
def test_rejects_invalid_bucket_configuration(kwargs: dict[str, object], message: str) -> None:
    with pytest.raises(ConfigurationError, match=message):
        list(iter_buckets([], **kwargs))  # type: ignore[arg-type]
