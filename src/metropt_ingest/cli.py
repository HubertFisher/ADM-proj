"""Command-line interface for validated MetroPT-3 ingestion."""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from collections.abc import Sequence
from pathlib import Path

from metropt_ingest import __version__
from metropt_ingest.bucketing import iter_buckets
from metropt_ingest.config import IngestionConfig
from metropt_ingest.csv_source import MetroPTCsvSource
from metropt_ingest.errors import MetroPTError
from metropt_ingest.repository import DryRunBucketRepository, MongoBucketRepository
from metropt_ingest.service import IngestionService

LOGGER = logging.getLogger(__name__)
DEFAULT_INPUT = "data/metropt+3+dataset.zip"


def build_parser() -> argparse.ArgumentParser:
    source_timezone = os.getenv("METROPT_SOURCE_TIMEZONE")
    parser = argparse.ArgumentParser(
        prog="metropt-ingest",
        description="Stream the MetroPT-3 CSV/ZIP into fixed-window MongoDB bucket documents.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument(
        "--input",
        type=Path,
        default=Path(os.getenv("METROPT_INPUT_PATH", DEFAULT_INPUT)),
        help="Input CSV or ZIP path (env: METROPT_INPUT_PATH).",
    )
    parser.add_argument(
        "--archive-member",
        default=os.getenv("METROPT_ARCHIVE_MEMBER"),
        help="CSV member name when a ZIP contains more than one CSV.",
    )
    parser.add_argument(
        "--mongo-uri",
        default=os.getenv("METROPT_MONGO_URI", "mongodb://localhost:27017"),
        help="MongoDB URI (env: METROPT_MONGO_URI).",
    )
    parser.add_argument("--database", default=os.getenv("METROPT_DATABASE", "metropt3"))
    parser.add_argument("--collection", default=os.getenv("METROPT_COLLECTION", "sensor_buckets"))
    parser.add_argument("--unit-id", default=os.getenv("METROPT_UNIT_ID", "APU_METRO_01"))
    parser.add_argument(
        "--source-timezone",
        default=source_timezone,
        required=source_timezone is None,
        help="Required IANA zone assigned to the dataset's naive timestamps.",
    )
    parser.add_argument("--expected-source-step", type=int, default=10)
    parser.add_argument("--bucket-seconds", type=int, default=300)
    parser.add_argument("--batch-size", type=int, default=1_000)
    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Optional row limit for smoke tests; omitted for full ingestion.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and transform all input without connecting to MongoDB.",
    )
    parser.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        default=os.getenv("METROPT_LOG_LEVEL", "INFO"),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    config = IngestionConfig(
        input_path=args.input,
        mongo_uri=args.mongo_uri,
        source_timezone=args.source_timezone,
        database=args.database,
        collection=args.collection,
        unit_id=args.unit_id,
        archive_member=args.archive_member,
        expected_source_step=args.expected_source_step,
        bucket_seconds=args.bucket_seconds,
        batch_size=args.batch_size,
        max_rows=args.max_rows,
        dry_run=args.dry_run,
    )

    started = time.perf_counter()
    try:
        config.validate()
        source = MetroPTCsvSource(
            config.input_path,
            source_timezone=config.source_timezone,
            archive_member=config.archive_member,
            expected_source_step=config.expected_source_step,
            max_rows=config.max_rows,
        )
        source.validate()
        buckets = iter_buckets(
            source,
            unit_id=config.unit_id,
            source_timezone=source.source_timezone,
            bucket_seconds=config.bucket_seconds,
        )
        repository_context = (
            DryRunBucketRepository()
            if config.dry_run
            else MongoBucketRepository(
                uri=config.mongo_uri,
                database=config.database,
                collection=config.collection,
            )
        )
        with repository_context as repository:
            summary = IngestionService(repository, batch_size=config.batch_size).run(buckets)
    except KeyboardInterrupt:
        LOGGER.warning("Ingestion interrupted")
        return 130
    except (MetroPTError, ValueError) as exc:
        LOGGER.error("Ingestion failed: %s", exc)
        return 2

    elapsed = time.perf_counter() - started
    throughput = summary.readings_processed / elapsed if elapsed else 0.0
    LOGGER.info(
        "Ingestion complete: readings=%d buckets=%d inserted=%d matched=%d modified=%d "
        "elapsed=%.2fs throughput=%.0f readings/s%s",
        summary.readings_processed,
        summary.buckets_processed,
        summary.buckets_inserted,
        summary.buckets_matched,
        summary.buckets_modified,
        elapsed,
        throughput,
        " (dry run)" if config.dry_run else "",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
