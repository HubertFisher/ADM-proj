"""Persistence ports and the MongoDB adapter."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol, Self

from pymongo import ASCENDING, MongoClient, ReplaceOne, WriteConcern
from pymongo.collection import Collection
from pymongo.errors import PyMongoError

from metropt_ingest.errors import RepositoryError
from metropt_ingest.models import SensorBucket


@dataclass(frozen=True, slots=True)
class WriteResult:
    matched: int = 0
    modified: int = 0
    inserted: int = 0


class BucketRepository(Protocol):
    """Storage contract consumed by the application service."""

    def prepare(self) -> None: ...

    def write(self, buckets: Sequence[SensorBucket]) -> WriteResult: ...


class MongoBucketRepository:
    """Store buckets through ``mongos`` using targeted, majority-durable upserts."""

    def __init__(
        self,
        *,
        uri: str,
        database: str,
        collection: str,
        connect_timeout_ms: int = 5_000,
    ) -> None:
        self._client: MongoClient[dict[str, object]] = MongoClient(
            uri,
            appname="metropt-ingest",
            connectTimeoutMS=connect_timeout_ms,
            serverSelectionTimeoutMS=connect_timeout_ms,
            retryWrites=True,
        )
        write_concern = WriteConcern(w="majority", j=True, wtimeout=10_000)
        self._collection: Collection[dict[str, object]] = self._client[database].get_collection(
            collection, write_concern=write_concern
        )

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def prepare(self) -> None:
        try:
            topology = self._client.admin.command("hello")
            if topology.get("msg") != "isdbgrid":
                raise RepositoryError(
                    "MongoDB endpoint is not a mongos router; ingestion must use the sharded "
                    "cluster endpoint"
                )
            self._collection.create_index(
                [
                    ("unit_id", ASCENDING),
                    ("bucket_seconds", ASCENDING),
                    ("bucket_start", ASCENDING),
                ],
                name="unit_resolution_time",
            )
        except PyMongoError as exc:
            raise RepositoryError(f"Unable to prepare MongoDB collection: {exc}") from exc

    def write(self, buckets: Sequence[SensorBucket]) -> WriteResult:
        if not buckets:
            return WriteResult()

        operations = [
            ReplaceOne({"_id": bucket.document_id}, bucket.to_mongo(), upsert=True)
            for bucket in buckets
        ]
        try:
            result = self._collection.bulk_write(operations, ordered=False)
        except PyMongoError as exc:
            raise RepositoryError(f"MongoDB bulk write failed: {exc}") from exc
        return WriteResult(
            matched=result.matched_count,
            modified=result.modified_count,
            inserted=result.upserted_count,
        )

    def close(self) -> None:
        self._client.close()


class DryRunBucketRepository:
    """Exercise the complete transformation without connecting to MongoDB."""

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def prepare(self) -> None:
        return None

    def write(self, buckets: Sequence[SensorBucket]) -> WriteResult:
        return WriteResult()
