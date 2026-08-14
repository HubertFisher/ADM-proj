from datetime import UTC, datetime
from pathlib import Path
from zipfile import ZipFile

import pytest

from metropt_ingest.csv_source import MetroPTCsvSource
from metropt_ingest.errors import ConfigurationError, DataValidationError
from metropt_ingest.models import REQUIRED_CSV_FIELDS
from tests.helpers import source_row, write_csv, write_zip


def test_parses_real_schema_and_canonicalizes_misspelled_field(tmp_path: Path) -> None:
    path = tmp_path / "readings.csv"
    write_csv(path, [source_row()])

    parsed = list(MetroPTCsvSource(path, source_timezone="Asia/Almaty"))

    assert parsed[0].source_sequence == 0
    assert parsed[0].timestamp == datetime(2020, 1, 31, 18, 0, tzinfo=UTC)
    assert parsed[0].analogue["TP2"] == pytest.approx(-0.012)
    assert parsed[0].digital["DV_electric"] == 1
    assert "DV_eletric" not in parsed[0].digital


def test_streams_csv_directly_from_official_style_zip(tmp_path: Path) -> None:
    csv_path = tmp_path / "source.csv"
    archive_path = tmp_path / "dataset.zip"
    write_csv(
        csv_path,
        [
            source_row(source_sequence=0, timestamp="2020-02-01 00:00:00"),
            source_row(source_sequence=10, timestamp="2020-02-01 00:00:10"),
        ],
    )
    write_zip(archive_path, csv_path)

    readings = list(MetroPTCsvSource(archive_path, source_timezone="UTC"))

    assert [reading.source_sequence for reading in readings] == [0, 10]


def test_rejects_source_sequence_gap_that_real_dataset_does_not_have(tmp_path: Path) -> None:
    path = tmp_path / "readings.csv"
    write_csv(path, [source_row(source_sequence=0), source_row(source_sequence=20)])

    with pytest.raises(DataValidationError, match="source sequence step is 20"):
        list(MetroPTCsvSource(path, source_timezone="UTC"))


def test_rejects_missing_real_source_column(tmp_path: Path) -> None:
    path = tmp_path / "readings.csv"
    fields = [field for field in REQUIRED_CSV_FIELDS if field != "DV_eletric"]
    row = {key: value for key, value in source_row().items() if key in fields}
    write_csv(path, [row], fields)

    with pytest.raises(DataValidationError, match="DV_eletric"):
        MetroPTCsvSource(path, source_timezone="UTC").validate()


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("TP2", "nan", "finite number"),
        ("COMP", "2.0", "must be 0 or 1"),
        ("timestamp", "2020-02-01T00:00:00", "must match"),
        ("", "-10", "must not be negative"),
    ],
)
def test_reports_invalid_values(tmp_path: Path, field: str, value: str, message: str) -> None:
    path = tmp_path / "readings.csv"
    row = source_row()
    row[field] = value
    write_csv(path, [row])

    with pytest.raises(DataValidationError, match=message):
        list(MetroPTCsvSource(path, source_timezone="UTC"))


def test_requires_member_selection_for_multiple_csvs(tmp_path: Path) -> None:
    csv_path = tmp_path / "source.csv"
    archive_path = tmp_path / "dataset.zip"
    write_csv(csv_path, [source_row()])
    with ZipFile(archive_path, "w") as archive:
        archive.write(csv_path, "first.csv")
        archive.write(csv_path, "second.csv")

    with pytest.raises(ConfigurationError, match="exactly one CSV"):
        MetroPTCsvSource(archive_path, source_timezone="UTC").validate()

    selected = list(
        MetroPTCsvSource(archive_path, source_timezone="UTC", archive_member="second.csv")
    )
    assert len(selected) == 1


def test_honors_row_limit(tmp_path: Path) -> None:
    path = tmp_path / "readings.csv"
    write_csv(path, [source_row(), source_row(source_sequence=10)])

    assert len(list(MetroPTCsvSource(path, source_timezone="UTC", max_rows=1))) == 1
