from datetime import UTC, datetime, timedelta

from metropt_ingest.bucketing import iter_buckets
from metropt_ingest.repository import WriteResult
from metropt_ingest.service import IngestionService
from tests.helpers import sensor_reading


class RecordingRepository:
    def __init__(self) -> None:
        self.prepared = False
        self.batch_sizes: list[int] = []

    def prepare(self) -> None:
        self.prepared = True

    def write(self, buckets: list[object]) -> WriteResult:
        self.batch_sizes.append(len(buckets))
        return WriteResult(inserted=len(buckets))


def test_batches_repository_writes_and_counts_readings() -> None:
    start = datetime(2020, 2, 1, tzinfo=UTC)
    readings = [
        sensor_reading(start + timedelta(minutes=minute * 5), source_sequence=minute * 10)
        for minute in range(5)
    ]
    buckets = iter_buckets(readings, unit_id="APU_01", source_timezone="UTC")
    repository = RecordingRepository()

    summary = IngestionService(repository, batch_size=2).run(buckets)

    assert repository.prepared
    assert repository.batch_sizes == [2, 2, 1]
    assert summary.readings_processed == 5
    assert summary.buckets_processed == 5
    assert summary.buckets_inserted == 5
