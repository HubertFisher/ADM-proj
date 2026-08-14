from pathlib import Path

from metropt_ingest.cli import main
from tests.helpers import source_row, write_csv, write_zip


def test_successful_dry_run_reads_zip_without_mongodb(tmp_path: Path) -> None:
    csv_path = tmp_path / "source.csv"
    archive_path = tmp_path / "dataset.zip"
    write_csv(
        csv_path,
        [
            source_row(),
            source_row(source_sequence=10, timestamp="2020-02-01 00:00:10"),
        ],
    )
    write_zip(archive_path, csv_path)

    exit_code = main(
        [
            "--input",
            str(archive_path),
            "--source-timezone",
            "UTC",
            "--dry-run",
            "--log-level",
            "ERROR",
        ]
    )

    assert exit_code == 0


def test_invalid_configuration_returns_nonzero(tmp_path: Path) -> None:
    exit_code = main(
        [
            "--input",
            str(tmp_path / "missing.zip"),
            "--source-timezone",
            "UTC",
            "--mongo-uri",
            "https://not-mongodb",
            "--log-level",
            "ERROR",
        ]
    )
    assert exit_code == 2


def test_row_limit_requires_dry_run(tmp_path: Path) -> None:
    exit_code = main(
        [
            "--input",
            str(tmp_path / "source.csv"),
            "--source-timezone",
            "UTC",
            "--max-rows",
            "1000",
            "--log-level",
            "ERROR",
        ]
    )

    assert exit_code == 2
