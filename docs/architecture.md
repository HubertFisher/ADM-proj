# Architecture

## Source-of-truth dataset

The only behavior-defining input is the current archive at `data/metropt+3+dataset.zip`. Its CSV
contains 1,516,948 readings and 17 columns: a blank-header integer source sequence, a naive timestamp,
seven analogue measurements, and eight binary signals. The ZIP also contains a PDF, but ingestion
does not use descriptive documents to infer schema or runtime behavior.

A chunked audit of every row established:

- no missing, null, non-finite, malformed, duplicate-timestamp, or out-of-order records;
- source sequence `0..15,169,470` with an invariant step of 10;
- timestamp format `YYYY-MM-DD HH:MM:SS`, with no offset or zone metadata;
- 99.976% of timestamp deltas at most 13 seconds, plus 331 gaps longer than one minute;
- all eight digital columns containing only `0.0` and `1.0`;
- one raw typo, `DV_eletric`, that must be normalized at the boundary;
- one compressor unit and, after verified ingestion, 500 MiB of logical BSON data.

The source sequence is not treated as elapsed time. It remains perfectly regular while timestamps
contain operational gaps, so both values are persisted for independent lineage and temporal meaning.

## Component boundaries

The package uses small ports and adapters:

1. `MetroPTCsvSource` opens an extracted CSV or the sole CSV member of a ZIP. It uses positional
   `csv.reader` parsing because the raw shape is fixed and this avoids per-row dictionaries. It
   validates the header, exact row width, source sequence, timestamp syntax, finite analogue values,
   and binary digital values while using bounded memory.
2. `SensorReading` is the canonical immutable sample. It stores the source sequence, aware UTC
   timestamp, seven analogue fields, and correctly named digital fields.
3. `iter_buckets` is a pure transformation. It validates strictly increasing timestamps and emits
   five-minute `SensorBucket` domain values without knowing about files or MongoDB.
4. `IngestionService` holds at most one 1,000-document write batch plus the active bucket.
5. `MongoBucketRepository` refuses direct shard/standalone connections, owns majority-journaled
   writes, maintains the local time-query index, and sends deterministic replacement upserts through
   `mongos`.
6. The CLI creates adapters, requires an explicit source timezone, reports throughput, and can use a
   no-op repository for full validation/benchmark runs.
7. Idempotent initialization scripts form three replica sets, register two shards, enable sharding,
   create initial chunks, and install server-side validation and indexes.

## Time semantics

Source timestamps are naive. The data alone cannot establish whether they are UTC, local Portuguese
time, or a fixed recorder clock; timestamps continue through a European daylight-saving transition
without an obvious clock jump. Silently defaulting to UTC would convert an unknown into a false fact.

The caller must provide an IANA timezone. The input adapter assigns it and converts every reading to
UTC before bucketing or identity generation. The selected name is stored in every bucket. Changing
the timezone changes UTC bucket starts and therefore document identities, preventing accidental
replacement of data interpreted under a different time basis.

## Bucket resolution

The previous one-minute design was based on an incorrect 1 Hz assumption. Actual one-minute buckets
average only 6.00 readings and create 252,720 documents. Measured alternatives were:

| Window | Documents | Mean readings | Observed maximum |
|---|---:|---:|---:|
| 60 s | 252,720 | 6.00 | 7 |
| 300 s | 50,782 | 29.87 | 31 |
| 900 s | 17,090 | 88.76 | 91 |

Five minutes is the default compromise: ~30x fewer documents than readings, approximately 10.3 KB
per typical BSON document, and sufficiently fine retrieval for hour-scale predictive-maintenance
windows. The configured resolution is persisted and included in both deterministic `_id` generation
and the supporting compound query index, so alternate resolutions cannot collide.

Each bucket embeds raw readings, source lineage, actual observed span, min/max/average for all seven
analogue measurements, and the on-ratio for all eight digital signals. Uniform statistics replace
the former arbitrary subset and simplify generic querying.

## Persistence and topology

The Docker environment models the standard MongoDB component boundaries:

```text
                                    +-> shard1RS: 1 primary + 2 secondaries
client -> mongos query router ------|
                |                   +-> shard2RS: 1 primary + 2 secondaries
                +-> configRS: 1 primary + 2 secondaries (cluster metadata)
```

`mongos` is the only host-published process. Each shard is a three-member replica set so shard-local
data is replicated; the config server is also a three-member replica set so routing metadata can
survive one member failure. All members run on one Docker host, which makes elections and replicated
state observable but is not genuine fault-domain redundancy.

### Shard-key decision

The actual CSV has only one unit, strictly increasing timestamps, and 50,782 deterministic bucket
IDs. The collection is sharded on:

```javascript
{ _id: "hashed" }
```

The SHA-256 string IDs provide high cardinality. MongoDB hashes each ID again for chunk placement;
this is operationally redundant as a randomness function but valuable because a hashed shard key
creates an even, automatically routable key space. Two initial chunks allow both shards to accept
writes immediately. An exact `_id` predicate is the full shard key, so every replacement upsert is
targeted at one shard and remains idempotent.

Sharding on `unit_id` cannot partition this single-unit dataset. Range sharding on `bucket_start`
would make time reads targetable but concentrate the chronological insert stream in the highest
chunk. A compound `{unit_id: 1, bucket_start: 1}` retains both flaws for this data. The selected key
optimizes the write/distribution demonstration and exact-bucket access.

Time-window analytics are intentionally a broadcast operation because they omit `_id`. Each shard
uses this supporting index before `mongos` merges the two result streams:

```javascript
{ unit_id: 1, bucket_seconds: 1, bucket_start: 1 } // non-unique, local on each shard
```

It supports the dominant unit/resolution/time-range filter locally but cannot be unique: MongoDB
requires a unique secondary index on a sharded collection to start with the full shard key, and a
hashed index cannot be unique. Uniqueness is instead encoded in deterministic `_id`; because `_id`
is the shard key, equal identities always route to the same shard. `ReplaceOne({_id: ...}, ...,
upsert=true)` makes retries and complete reprocessing idempotent, and replacement removes stale
schema fields rather than merging incompatible versions.

The cluster initializer also creates a strict `$jsonSchema` validator before sharding the
collection. It requires the complete canonical analogue/digital sensor sets, binary integers,
aggregate fields, BSON types, and no unexpected structural fields, even if data bypasses the Python
adapter. Application validation remains richer for numeric finiteness, sequencing, timestamp
ordering, timezone conversion, and cross-field invariants.

## Performance and failure behavior

The complete real-data dry run processed 1,516,948 readings and 50,782 buckets in 16.1 seconds on the
development machine: approximately 94,300 readings/second and 108.8 MB peak RSS. A sampled 1,000-
document BSON batch was approximately 9.8 MiB, comfortably below MongoDB command/message limits.

Memory is bounded by the active five-minute bucket and one batch. There is no full-file sort: the
source's measured timestamp and source-sequence ordering are validated as the stream advances.

Structural or row errors stop ingestion with the source row number. Previous batches remain durable
and a retry safely replaces them. Writes use majority acknowledgement, journaling, a ten-second write
timeout, retryable writes, and unordered bulk operations. The source archive is never extracted, so
the workflow does not require a second 218 MB disk copy.

An unordered batch can contain writes routed to both shards. It is not a distributed transaction:
if a later operation fails, earlier operations may already be durable. This is intentional because
each replacement is independently idempotent and a complete retry converges to the same state
without the cost and coordination of a cross-shard transaction. Reads use the primary preference by
default, so the importer never treats potentially stale secondary state as confirmation of a write.

Row-limited processing is dry-run-only. A limit can terminate inside a five-minute window, and
allowing that partial bucket to reach MongoDB would replace the complete deterministic document.

MongoDB 8.0.29 on the local sharded topology inserted 50,782 documents/1,516,948 readings in 46.78
seconds (~32,427 readings/second). The unchanged second run took 29.85 seconds, matched every bucket,
and inserted/modified none. Hashed placement produced 25,292 documents (49.8%, 261.3 MB logical) on
shard 1 and 25,490 (50.2%, 263.2 MB) on shard 2. The two chunks are sufficient for this bounded
dataset: each covers half of the signed hashed-key space and lives on a different shard.

An exact `_id` lookup used a `SINGLE_SHARD` plan and examined one key/document. A representative
unit/day range used `SHARD_MERGE`; the supporting index limited the two local scans to 239 total keys
and documents for 239 results, with a measured 1 ms local execution time. This demonstrates the
chosen distribution/query-routing tradeoff directly.

In a controlled test, stopping shard 1's primary caused another member to become primary in under
three seconds. A majority replacement routed through `mongos` succeeded while the former primary
was down; after restart, that member rejoined as a secondary. This validates replica-set election
behavior within the Docker environment, while not claiming host-level availability.

## Production boundary

The local cluster is intentionally unauthenticated and must remain loopback-only. Internal Docker
network isolation is not an authentication boundary. Production requires keyfile or X.509 member
authentication, authenticated TLS clients, independent hosts/zones, multiple routers, secret
management, backups, restore drills, monitoring, resource limits, dependency scanning, and workload
testing against the chosen deployment. The local topology demonstrates replication and sharding
semantics; it does not turn one laptop into a highly available system.
