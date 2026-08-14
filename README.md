# MetroPT-3 MongoDB Ingestion

A data-validated MongoDB sharding project built around the actual MetroPT-3 distribution in this
repository. It combines a bounded-memory ingestion service with an observable local sharded
cluster: a three-member config-server replica set, two three-member shard replica sets, and a
`mongos` query router. The importer reads directly from the official ZIP, normalizes explicitly
configured source timestamps to UTC, creates five-minute bucket documents, and performs targeted,
idempotent upserts across both shards.

## Measured dataset facts

The implementation and tests are based on `data/metropt+3+dataset.zip`, not on historical planning
documents or nominal descriptions.

| Property | Measured value |
|---|---:|
| ZIP size | 218,381,995 bytes |
| CSV member | `MetroPT3(AirCompressor).csv` |
| CSV size | 218,300,507 bytes |
| Data rows | 1,516,948 |
| Time range | `2020-02-01 00:00:00` to `2020-09-01 03:59:50` |
| Source timezone | Not encoded and not inferable from the CSV |
| Null or non-finite values | 0 |
| Duplicate/non-increasing timestamps | 0 |
| Source sequence | `0` through `15,169,470`, always increasing by `10` |
| Timestamp gaps over 60 seconds | 331 |

The first CSV header is blank and contains the source sequence. The archive then contains
`timestamp`, seven analogue fields, and eight digital fields. The raw electrical-valve column is
consistently spelled `DV_eletric`; it is mapped to canonical `DV_electric` at the input boundary.

The data is not 1 Hz. Of the 1,516,947 adjacent intervals, 1,337,521 are 10 seconds, 128,277 are 9
seconds, 38,321 are 12 seconds, and 7,988 are 13 seconds. In total, 99.976% are at most 13 seconds,
but long operational gaps raise the overall mean interval to 12.14 seconds. The longest gap is
172,918 seconds. No timezone offset or daylight-saving transition is encoded, so callers must make
the timezone choice explicitly.

Observed analogue ranges are preserved rather than rejected as presumed anomalies:

| Field | Minimum | Mean | Maximum |
|---|---:|---:|---:|
| `TP2` | -0.032 | 1.368 | 10.676 |
| `TP3` | 0.730 | 8.985 | 10.302 |
| `H1` | -0.036 | 7.568 | 10.288 |
| `DV_pressure` | -0.032 | 0.056 | 9.844 |
| `Reservoirs` | 0.712 | 8.985 | 10.300 |
| `Oil_temperature` | 15.400 | 62.644 | 89.050 |
| `Motor_current` | 0.020 | 2.050 | 9.295 |

Every digital field contains only floating-point representations of `0` and `1`. Their measured
on-rates range from 0.342% (`LPS`) to 99.144% (`Pressure_switch`). The parser accepts `0`, `1`,
`0.0`, and `1.0`, then stores canonical integers.

## Architecture

```text
Official ZIP or extracted CSV
    -> positional streaming parser + schema validation
    -> typed UTC readings with source lineage
    -> pure five-minute bucketing and statistics
    -> batches of 1,000 bucket documents
    -> deterministic replacement upserts through mongos
    -> hashed chunks distributed across two replicated shards
```

- `csv_source.py` reads either `.zip` or `.csv` without extraction or whole-file loading. It owns
  raw field names, archive-member selection, types, numeric validation, and source-sequence checks.
- `models.py` defines immutable domain values and MongoDB schema version 2.
- `bucketing.py` enforces unique increasing timestamps and computes min/max/average for every
  analogue field plus the on-ratio for every digital field.
- `service.py` performs bounded batching and reports both reading and bucket counts.
- `repository.py` requires a `mongos` endpoint and owns durability, indexes, and targeted writes.
- `cli.py` is the composition root and provides a database-free full dry-run mode.
- `docker-compose.yml` and `scripts/` create replica sets, register shards, shard the collection,
  enforce a server-side document validator, and expose routing/distribution diagnostics.

See [Architecture](docs/architecture.md) for the data-driven design decisions.

## Bucketing and sharding are separate decisions

Measured bucket density is:

| Window | Documents | Mean readings/document |
|---|---:|---:|
| 60 seconds | 252,720 | 6.00 |
| 300 seconds | 50,782 | 29.87 |
| 900 seconds | 17,090 | 88.76 |

Five minutes gives a useful ~30x document reduction while keeping documents small and time-range
reads reasonably granular. In a 10,000-reading BSON sample, a typical five-minute document was
about 10.3 KB and the largest was 10.7 KB. A 1,000-document batch was approximately 9.8 MiB.

The measured data represents one fixed unit and only about 500 MiB of logical BSON, so sharding is
not required for capacity. It is retained because horizontal partitioning, routing, balancing, and
replication are the central learning goals of this project. The topology is production-shaped but
runs on one Docker host, so it demonstrates MongoDB behavior rather than real fault-domain
isolation.

The collection uses `{_id: "hashed"}` as its shard key. Each bucket has a deterministic SHA-256 ID
derived from unit, resolution, and UTC window. Hashing this high-cardinality value distributes both
initial ingestion and retries across the shards. The replacement-upsert filter contains the exact
`_id`, so `mongos` can route every write to one shard. Two tempting alternatives are worse for this
dataset:

- `{unit_id: "hashed"}` cannot distribute anything because all rows have the same unit ID.
- `{bucket_start: 1}` supports targeted time ranges but makes the append-only workload monotonic,
  concentrating new writes in the current high-end chunk.

The tradeoff is explicit: unit/time range queries do not contain the hashed shard key, so `mongos`
broadcasts them to both shards and merges the results. The non-unique
`{unit_id: 1, bucket_seconds: 1, bucket_start: 1}` index makes each shard's local portion efficient.
Run `make cluster-status` to see `SINGLE_SHARD` for exact-ID routing and `SHARD_MERGE` for the time
range plan.

## Requirements

- Python 3.11 or newer
- Docker Engine with Compose for the local MongoDB 8 sharded cluster
- `data/metropt+3+dataset.zip` for the full ingestion

The 218 MB archive is deliberately ignored by Git and excluded from Docker builds.

## Docker quick start

```bash
cp .env.example .env
# Review METROPT_SOURCE_TIMEZONE in .env; UTC is an example, not a dataset fact.
make cluster-up
make cluster-status
make ingest
make cluster-verify
```

`cluster-up` starts ten long-running MongoDB processes plus idempotent one-shot initialization:

```text
application -> mongos:27017 -> configRS (config1, config2, config3)
                           -> shard1RS (shard1a, shard1b, shard1c)
                           -> shard2RS (shard2a, shard2b, shard2c)
```

Only the router is published, at `mongodb://127.0.0.1:27017`. Applications must not connect to an
individual shard. `make cluster-down` stops containers but preserves database volumes.

## Host development and dry run

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'

# Exercises the complete real-data transformation without MongoDB.
metropt-ingest \
  --input data/metropt+3+dataset.zip \
  --source-timezone UTC \
  --dry-run
```

On the current development machine, the complete dry run processed 1,516,948 readings into 50,782
buckets in 16.1 seconds (about 94,300 readings/second) with 108.8 MB peak RSS. These numbers are a
local baseline, not a cross-platform performance guarantee.

Run a bounded transformation smoke test with:

```bash
metropt-ingest \
  --input data/metropt+3+dataset.zip \
  --source-timezone UTC \
  --max-rows 1000 \
  --dry-run
```

`--max-rows` is deliberately limited to dry runs. Ending mid-window and upserting that partial
bucket could otherwise replace a complete document. Database ingestion always processes the full
input.

With MongoDB 8.0.29 on the local sharded topology, the first complete ingestion took 46.78 seconds
(~32,427 readings/second). An unchanged full rerun took 29.85 seconds
(~50,823 readings/second), matched all 50,782 buckets, and inserted/modified none. The final
distribution was 25,292 documents (49.8%, 261.3 MB logical) on shard 1 and 25,490 (50.2%, 263.2 MB)
on shard 2. A representative one-day query returned 239 buckets, examined exactly 239 keys and 239
documents across both shards, and completed in 1 ms locally. These are local measurements, not a
cross-platform performance guarantee.

The source timezone is required. Replace `UTC` with the known operational timezone when available.
The input path, timezone, MongoDB URI, unit ID, database, collection, and archive member can also be
set with the `METROPT_*` variables shown in `.env.example` or CLI help.

## MongoDB document and indexes

```javascript
{
  _id: "<SHA-256(unit, resolution, UTC bucket start)>",
  unit_id: "APU_METRO_01",
  bucket_seconds: 300,
  bucket_start: ISODate("2020-02-01T00:00:00Z"),
  bucket_end: ISODate("2020-02-01T00:05:00Z"), // exclusive
  source_timezone: "UTC",
  reading_count: 30,
  observed_span_seconds: 290,
  source_sequence: { first: 0, last: 290 },
  readings: [
    {
      seq: 1,
      source_seq: 0,
      ts: ISODate("2020-02-01T00:00:00Z"),
      analogue: { /* seven measured values */ },
      digital: { /* eight canonical 0/1 values */ }
    }
  ],
  bucket_stats: {
    analogue: { TP2: { min: -0.014, max: -0.010, avg: -0.012 }, /* ... */ },
    digital_on_ratio: { COMP: 1.0, DV_electric: 0.0, /* ... */ }
  },
  schema_version: 2
}
```

The sharded collection has these indexes:

- `{_id: 1}`, MongoDB's mandatory unique identity index;
- `{_id: "hashed"}`, the shard-key index used for data placement and equality routing;
- `{unit_id: 1, bucket_seconds: 1, bucket_start: 1}`, a non-unique local time-query index.

The last index cannot be unique on this sharded collection because it does not start with the shard
key, and hashed indexes themselves cannot be unique. Logical uniqueness instead comes from the
deterministic `_id`, which is also the complete shard key. Reprocessing the same configuration
therefore routes to and replaces the same document. Initialization also installs a strict MongoDB
JSON Schema validator for the full canonical sensor shape, BSON types, binary values, aggregate
shape, and required fields.

## Inspect the DBMS behavior

```bash
make cluster-status
```

The status report shows registered shards, every replica-set member and state, the shard key, chunk
count, validation mode, indexes, per-shard document/storage counts, and query-plan execution metrics.
After full ingestion, `make cluster-verify` turns these expectations into executable integration
checks, including exact dataset totals and rejection of an intentionally invalid probe. Useful
follow-up experiments are:

```bash
# If cluster-status currently identifies shard1a as PRIMARY:
docker compose stop shard1a
make cluster-status
docker compose start shard1a

# Initialization is idempotent and safe to rerun.
docker compose run --rm cluster-init
```

Replica-set elections may choose a different member than `shard1a`; inspect the report before a
failover exercise and stop whichever member currently reports `PRIMARY`.

## Quality checks

```bash
make check
```

This runs Ruff, strict mypy, pytest with branch coverage, and Compose validation. When the official
archive is present, the test suite also checks its leading records and exact raw schema. CI skips
that optional contract test because the large archive is not committed.

`make cluster-verify` is intentionally separate because it requires the running, fully ingested
Docker cluster.

## Security and production boundary

Local Compose has no authentication or TLS, so its only published port is the loopback-bound router.
Do not expose it publicly. The three-member sets share one Docker host and therefore do not provide
real infrastructure availability. A production deployment needs separate fault domains, internal
member authentication, client TLS/authentication, multiple routers behind service discovery,
secret management, backups and restore testing, resource limits, metrics, and alerting. Supply the
router URI through `METROPT_MONGO_URI`; never commit credentials or `.env` files.
