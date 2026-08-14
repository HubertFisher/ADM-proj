from datetime import UTC, datetime
from pathlib import Path

import pytest

from metropt_ingest.csv_source import MetroPTCsvSource

DATASET = Path("data/metropt+3+dataset.zip")


@pytest.mark.dataset
@pytest.mark.skipif(not DATASET.exists(), reason="official dataset is not checked into Git")
def test_official_archive_contract() -> None:
    readings = list(
        MetroPTCsvSource(
            DATASET,
            source_timezone="UTC",
            max_rows=4,
        )
    )

    assert [reading.source_sequence for reading in readings] == [0, 10, 20, 30]
    assert [reading.timestamp for reading in readings] == [
        datetime(2020, 2, 1, 0, 0, 0, tzinfo=UTC),
        datetime(2020, 2, 1, 0, 0, 10, tzinfo=UTC),
        datetime(2020, 2, 1, 0, 0, 19, tzinfo=UTC),
        datetime(2020, 2, 1, 0, 0, 29, tzinfo=UTC),
    ]
    assert readings[0].analogue["TP2"] == pytest.approx(-0.012)
    assert readings[0].digital["DV_electric"] == 0
