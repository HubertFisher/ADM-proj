"""Application orchestration for the ingestion use case."""

from __future__ import annotations

import logging
from collections.abc import Iterable, Iterator
from dataclasses import dataclass

from metropt_ingest.models import SensorBucket
from metropt_ingest.repository import BucketRepository, WriteResult

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class IngestionSummary:
    readings_processed: int
    buckets_processed: int
    buckets_inserted: int
    buckets_matched: int
    buckets_modified: int


class IngestionService:
    def __init__(self, repository: BucketRepository, *, batch_size: int = 1_000) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size must be greater than zero")
        self._repository = repository
        self._batch_size = batch_size

    def run(self, buckets: Iterable[SensorBucket]) -> IngestionSummary:
        self._repository.prepare()
        readings_processed = 0
        processed = 0
        aggregate = WriteResult()

        for batch_number, batch in enumerate(self._batched(buckets), start=1):
            result = self._repository.write(batch)
            readings_processed += sum(len(bucket.readings) for bucket in batch)
            processed += len(batch)
            aggregate = WriteResult(
                matched=aggregate.matched + result.matched,
                modified=aggregate.modified + result.modified,
                inserted=aggregate.inserted + result.inserted,
            )
            LOGGER.info(
                "Processed batch %d: buckets=%d inserted=%d matched=%d modified=%d",
                batch_number,
                len(batch),
                result.inserted,
                result.matched,
                result.modified,
            )

        return IngestionSummary(
            readings_processed=readings_processed,
            buckets_processed=processed,
            buckets_inserted=aggregate.inserted,
            buckets_matched=aggregate.matched,
            buckets_modified=aggregate.modified,
        )

    def _batched(self, buckets: Iterable[SensorBucket]) -> Iterator[list[SensorBucket]]:
        batch: list[SensorBucket] = []
        for bucket in buckets:
            batch.append(bucket)
            if len(batch) == self._batch_size:
                yield batch
                batch = []
        if batch:
            yield batch
