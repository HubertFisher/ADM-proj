from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from pymongo.errors import ConnectionFailure, OperationFailure

from metropt_ingest.bucketing import iter_buckets
from metropt_ingest.errors import RepositoryError
from metropt_ingest.repository import (
    DryRunBucketRepository,
    MongoBucketRepository,
    WriteResult,
)
from tests.helpers import sensor_reading


def one_bucket():
    return next(
        iter_buckets(
            [sensor_reading(datetime(2020, 2, 1, tzinfo=UTC))],
            unit_id="APU_01",
            source_timezone="UTC",
        )
    )


@patch("metropt_ingest.repository.MongoClient")
def test_requires_router_and_prepares_time_query_index(mongo_client: MagicMock) -> None:
    client = mongo_client.return_value
    client.admin.command.return_value = {"msg": "isdbgrid"}
    collection = client.__getitem__.return_value.get_collection.return_value
    repository = MongoBucketRepository(
        uri="mongodb://localhost", database="metropt3", collection="sensor_buckets"
    )

    repository.prepare()

    client.admin.command.assert_called_once_with("hello")
    _, kwargs = collection.create_index.call_args
    assert kwargs == {"name": "unit_resolution_time"}


@patch("metropt_ingest.repository.MongoClient")
def test_rejects_direct_connection_to_non_router(mongo_client: MagicMock) -> None:
    mongo_client.return_value.admin.command.return_value = {"isWritablePrimary": True}
    repository = MongoBucketRepository(
        uri="mongodb://localhost", database="metropt3", collection="sensor_buckets"
    )

    with pytest.raises(RepositoryError, match="not a mongos router"):
        repository.prepare()


@patch("metropt_ingest.repository.MongoClient")
def test_wraps_prepare_failure(mongo_client: MagicMock) -> None:
    mongo_client.return_value.admin.command.side_effect = ConnectionFailure("offline")
    repository = MongoBucketRepository(
        uri="mongodb://localhost", database="metropt3", collection="sensor_buckets"
    )
    with pytest.raises(RepositoryError, match="prepare"):
        repository.prepare()


@patch("metropt_ingest.repository.MongoClient")
def test_replaces_document_by_deterministic_identity(mongo_client: MagicMock) -> None:
    client = mongo_client.return_value
    collection = client.__getitem__.return_value.get_collection.return_value
    collection.bulk_write.return_value = MagicMock(
        matched_count=2, modified_count=1, upserted_count=3
    )
    repository = MongoBucketRepository(
        uri="mongodb://localhost", database="metropt3", collection="sensor_buckets"
    )
    bucket = one_bucket()

    result = repository.write([bucket])

    assert result == WriteResult(matched=2, modified=1, inserted=3)
    operation = collection.bulk_write.call_args.args[0][0]
    assert operation._filter == {"_id": bucket.document_id}
    assert operation._doc["bucket_seconds"] == 300
    assert repository.write([]) == WriteResult()


@patch("metropt_ingest.repository.MongoClient")
def test_wraps_bulk_write_failure(mongo_client: MagicMock) -> None:
    client = mongo_client.return_value
    collection = client.__getitem__.return_value.get_collection.return_value
    collection.bulk_write.side_effect = OperationFailure("rejected")
    repository = MongoBucketRepository(
        uri="mongodb://localhost", database="metropt3", collection="sensor_buckets"
    )
    with pytest.raises(RepositoryError, match="bulk write"):
        repository.write([one_bucket()])


@patch("metropt_ingest.repository.MongoClient")
def test_context_manager_closes_client(mongo_client: MagicMock) -> None:
    client = mongo_client.return_value
    with MongoBucketRepository(
        uri="mongodb://localhost", database="metropt3", collection="sensor_buckets"
    ):
        pass
    client.close.assert_called_once()


def test_dry_run_repository_has_no_side_effects() -> None:
    with DryRunBucketRepository() as repository:
        repository.prepare()
        assert repository.write([one_bucket()]) == WriteResult()
