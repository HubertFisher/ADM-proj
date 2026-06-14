# MongoDB Blueprint: MetroPT-3 Predictive Maintenance Dataset

---

## 1. Data Import & Schema Design

### Schema Strategy: **Bucket Pattern (Time-Window Embedding)**

MetroPT-3 produces **15,169,480 records at 1 Hz** from a single compressor unit. Storing one document per reading is a critical anti-pattern at this scale — it creates 15M+ tiny documents, destroys index efficiency, and overwhelms the WiredTiger cache.

**Recommendation:** Apply the **Bucket Pattern** — group readings into fixed time-window documents (60-second buckets = ~250K documents total). Embed all 15 sensor channels inline. Maintain a separate `failure_events` reference collection for the 4 known fault windows.

**Rationale:**
- Reduces document count by **60×** (15M → ~252,824 docs)
- Analogue + digital readings always queried together → embedding is correct
- Time-range queries (e.g., "fetch 1 hour before failure #3") hit contiguous buckets
- Failure metadata is sparse and evolves separately → referencing is correct

---

### Minimal Document Example

```json
{
  "_id": ObjectId("..."),
  "unit_id":       "APU_METRO_01",
  "bucket_start":  ISODate("2020-04-18T00:00:00Z"),
  "bucket_end":    ISODate("2020-04-18T00:01:00Z"),
  "reading_count": 60,

  "readings": [
    {
      "seq": 1,
      "ts":            ISODate("2020-04-18T00:00:00Z"),
      "analogue": {
        "TP2":          8.41,
        "TP3":          8.39,
        "H1":           0.00,
        "DV_pressure":  0.00,
        "Reservoirs":   8.37,
        "Motor_Current": 7.12,
        "Oil_Temp":     68.3
      },
      "digital": {
        "COMP":           0,
        "DV_electric":    1,
        "TOWERS":         0,
        "MPG":            0,
        "LPS":            0,
        "Pressure_Switch":0,
        "Oil_Level":      0,
        "Caudal_Impulse": 1
      }
    }
  ],

  "bucket_stats": {
    "Motor_Current_avg": 7.08,
    "TP2_min":           8.10,
    "TP2_max":           8.51,
    "Oil_Temp_avg":      68.1,
    "anomaly_flag":      false
  },

  "failure_ref": null
}
```

**Separate `failure_events` collection (referenced):**

```json
{
  "_id":         "FAIL_003",
  "unit_id":     "APU_METRO_01",
  "start_time":  ISODate("2020-06-05T10:00:00Z"),
  "end_time":    ISODate("2020-06-07T14:30:00Z"),
  "type":        "Air_Leak",
  "severity":    "High_stress",
  "maintenance": ISODate("2020-06-08T16:00:00Z"),
  "report_nr":   3
}
```

---

### Import Pipeline

| Step | Tool | Purpose |
|---|---|---|
| 1. Parse raw CSV/TSV | `pandas` | Read 15M rows, parse timestamps |
| 2. Build 60-s buckets | `pymongo` ETL | Group by `unit_id + floor(ts, 60s)`, compute `bucket_stats` |
| 3. Bulk insert | `pymongo insert_many(batch_size=1000)` | Ordered=False for max throughput |
| 4. Load failure table | `mongoimport` | `--collection failure_events --file failures.json` |
| 5. Index creation | `createIndex` post-import | Always index **after** bulk load |

```python
# Core bucketing logic
df['bucket_start'] = df['ts'].dt.floor('60s')
buckets = df.groupby(['unit_id', 'bucket_start']).apply(build_bucket_doc)
collection.insert_many(buckets, ordered=False)
```

> **Never** run `mongoimport` on raw flat rows. Pre-aggregate to buckets in Python; import cost is paid once, query cost is paid forever.

---

## 2. Scalability & Distributed Queries

### Sharding Architecture

```js
sh.enableSharding("metropt")
sh.shardCollection(
  "metropt.sensor_buckets",
  { "unit_id": "hashed", "bucket_start": 1 }
)
```

### Shard Key Analysis

| Candidate Key | Cardinality | Write Distribution | Query Locality | Verdict |
|---|---|---|---|---|
| `bucket_start` alone | High, **monotonic** | All writes → last shard | Good range scans | ❌ Hotspot |
| `unit_id` alone | Extremely low (1 unit in v3) | All writes → 1 chunk | Perfect locality | ❌ No distribution |
| `Motor_Current` | Continuous float | Unpredictable | Poor | ❌ |
| **`{ unit_id: hashed, bucket_start: 1 }`** | High compound | Even via hash prefix | Time-range on same shard | ✅ |

**Why this key works for MetroPT-3:**
- `unit_id` has **low natural cardinality** (single APU) — hashing it creates artificial distribution across shards
- `bucket_start` as range component preserves **temporal locality** for sliding-window ML queries (e.g., "last 6 hours before failure")
- Pre-split chunks at ingestion time to avoid balancer overhead on a monotonic time series

```js
// Pre-split before bulk load
for (let d = new Date("2020-02-01"); d < new Date("2020-09-01"); ) {
  sh.splitAt("metropt.sensor_buckets", { unit_id: "APU_METRO_01", bucket_start: d });
  d.setDate(d.getDate() + 7); // weekly chunk boundaries
}
```

### Distributed Query Execution

```
mongos (query router)
     │
     ├─→ Shard A  ← hashed APU_METRO_01 + Feb–Mar buckets
     ├─→ Shard B  ← hashed APU_METRO_01 + Apr–May buckets
     └─→ Shard C  ← hashed APU_METRO_01 + Jun–Aug buckets
```

- **Targeted (shard key in filter):** `mongos` resolves to 1–2 shards — used by RUL/anomaly queries with known time range
- **Scatter-gather (aggregation pipeline, no shard key):** All shards queried in parallel, results merged at `mongos` — acceptable for offline ML training jobs
- **Compound index per shard:** `{ unit_id: 1, bucket_start: 1, "bucket_stats.anomaly_flag": 1 }` — covers failure-window lookups without collection scan

---

## 3. Availability: Replica Sets

### Topology for Industrial IoT Continuity

```
PRIMARY (write + strong reads)
    │── oplog replication ──→  SECONDARY 1 (analytics reads)
    └── oplog replication ──→  SECONDARY 2 (hot standby / arbiter fallback)
```

### Mechanism

| Phase | Behaviour |
|---|---|
| **Normal operation** | PRIMARY receives all 1Hz writes; oplog entries stream async to SECONDARIEs |
| **Heartbeat** | Members ping every 2 s; timeout threshold = `electionTimeoutMillis` (default 10 s) |
| **Primary failure** | No heartbeat detected → SECONDARY initiates election; majority vote required |
| **Election outcome** | Most up-to-date oplog + highest `priority` wins; new PRIMARY elected in ~10–30 s |
| **Data catch-up** | Rejoining node replays oplog delta; if too stale, performs full initial sync |
| **Oplog sizing** | Critical for MetroPT-3: set large oplog (`--oplogSize 10240` MB) to handle 1Hz write bursts without sync gaps |

```js
rs.initiate({
  _id: "rs_metropt",
  members: [
    { _id: 0, host: "node1:27017", priority: 2 },
    { _id: 1, host: "node2:27017", priority: 1 },
    { _id: 2, host: "node3:27017", priority: 1, tags: { use: "analytics" } }
  ]
})
```

> Tag `SECONDARY 2` as `analytics` — route all ML training reads there via `readPreference: { mode: "secondary", tags: [{ use: "analytics" }] }` to offload the PRIMARY entirely.

---

## 4. Consistency Model

### Tunable Consistency for MetroPT-3

MetroPT-3 has two distinct workload profiles requiring different consistency settings:

| Workload | Pattern | Consistency Need |
|---|---|---|
| **Live sensor ingestion** (1 Hz writes) | Append-only, high throughput | Durable but fast |
| **Failure window queries** | Rare, correctness-critical | Strongly consistent |
| **ML training aggregations** | Bulk offline reads | Throughput > consistency |
| **Anomaly alert reads** | Near-real-time | Majority-committed only |

---

### Write Concern Configuration

```js
// Live sensor bucket ingestion — durable, non-blocking
db.sensor_buckets.insertMany(bucketBatch, {
  writeConcern: { w: "majority", j: true, wtimeout: 3000 }
})

// Bulk historical backfill — throughput mode
db.sensor_buckets.insertMany(historicalBatch, {
  writeConcern: { w: 1, j: false }
})
```

### Read Concern Configuration

```js
// Failure window analysis — must not read uncommitted data
db.sensor_buckets.find(
  { bucket_start: { $gte: failStart, $lte: failEnd } },
  { readConcern: { level: "majority" } }
)

// ML training pipeline — stale reads acceptable, max throughput
db.sensor_buckets.aggregate(pipeline, {
  readConcern: { level: "local" },
  readPreference: { mode: "secondary", tags: [{ use: "analytics" }] }
})
```

### Summary Table

| Operation | `w` (Write Concern) | `j` | Read Concern | Justification |
|---|---|---|---|---|
| 1Hz bucket ingestion | `"majority"` | `true` | — | No sensor data loss on failover |
| Failure event insert | `"majority"` | `true` | — | Fault records must be durable |
| Bulk historical load | `1` | `false` | — | Speed; data is re-importable |
| Anomaly detection query | — | — | `"majority"` | Prevents false negatives from stale reads |
| RUL model training | — | — | `"local"` | Offline; stale by minutes is acceptable |
| Multi-doc transactions (failure + bucket update) | `"majority"` | `true` | `"snapshot"` | ACID atomicity across collections |

> **Key constraint:** At 1Hz continuous ingestion, `wtimeout: 3000ms` on `w: majority` is the safety threshold. If majority ACK exceeds 3 s, your replica set has a replication lag problem — monitor via `rs.printSecondaryReplicationInfo()`.