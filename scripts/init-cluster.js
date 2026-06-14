// =============================================================
// init-cluster.js
// MongoDB Sharded Cluster — Full Automated Initialization
//
// Execution context : mongosh --nodb
// =============================================================

"use strict";

// ─────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────

/**
 * Synchronous sleep using a busy-wait compatible with mongosh's
 * synchronous JS environment (no native setTimeout promise support).
 */
function sleep(ms) {
  const until = new Date(new Date().getTime() + ms);
  while (new Date() < until) { /* busy-wait */ }
}

/**
 * Retry a function up to `maxAttempts` times with `delayMs` between tries.
 * Throws the last error if all attempts fail.
 */
function retry(label, fn, maxAttempts, delayMs) {
  let lastErr;
  for (let i = 1; i <= maxAttempts; i++) {
    try {
      return fn();
    } catch (e) {
      lastErr = e;
      print(`    [RETRY ${i}/${maxAttempts}] ${label}: ${e.message}`);
      if (i < maxAttempts) sleep(delayMs);
    }
  }
  throw new Error(`FATAL — ${label} failed after ${maxAttempts} attempts: ${lastErr.message}`);
}

/**
 * Wait until the named replica set has elected a PRIMARY.
 */
function waitForPrimary(host, port, rsName, maxWaitMs) {
  const deadline = new Date().getTime() + maxWaitMs;
  print(`  Waiting for PRIMARY in ${rsName} ...`);
  while (new Date().getTime() < deadline) {
    try {
      const conn = new Mongo(`${host}:${port}`);
      const status = conn.getDB("admin").adminCommand({ replSetGetStatus: 1 });
      if (status.ok === 1) {
        const primary = status.members.find(m => m.stateStr === "PRIMARY");
        if (primary) {
          print(`  ✓ PRIMARY elected in ${rsName}: ${primary.name}`);
          return;
        }
      }
    } catch (_) { /* node not ready yet */ }
    sleep(2000);
  }
  throw new Error(`Timed out waiting for PRIMARY in ${rsName}`);
}

// ─────────────────────────────────────────────
// STEP 1 — Initiate Config Server Replica Set (configRS)
// ─────────────────────────────────────────────

print("\n══════════════════════════════════════════════");
print("STEP 1 — Initiating Config Server RS (configRS)");
print("══════════════════════════════════════════════");

retry("configRS initiation", () => {
  const configConn = new Mongo("config1:27017");
  const adminDB = configConn.getDB("admin");
  const result = adminDB.adminCommand({
    replSetInitiate: {
      _id: "configRS",
      configsvr: true,
      members: [
        { _id: 0, host: "config1:27017" },
        { _id: 1, host: "config2:27017" },
        { _id: 2, host: "config3:27017" },
      ],
    },
  });
  if (result.ok !== 1) throw new Error(JSON.stringify(result));
  print("  ✓ configRS rs.initiate() acknowledged");
}, 10, 3000);

// ─────────────────────────────────────────────
// STEP 2 — Initiate Shard 1 Replica Set (shard1RS)
// ─────────────────────────────────────────────

print("\n══════════════════════════════════════════════");
print("STEP 2 — Initiating Shard 1 RS (shard1RS)");
print("══════════════════════════════════════════════");

retry("shard1RS initiation", () => {
  const s1Conn = new Mongo("shard1a:27017");
  const result = s1Conn.getDB("admin").adminCommand({
    replSetInitiate: {
      _id: "shard1RS",
      members: [
        { _id: 0, host: "shard1a:27017" },
        { _id: 1, host: "shard1b:27017" },
        { _id: 2, host: "shard1c:27017" },
      ],
    },
  });
  if (result.ok !== 1) throw new Error(JSON.stringify(result));
  print("  ✓ shard1RS rs.initiate() acknowledged");
}, 10, 3000);

// ─────────────────────────────────────────────
// STEP 3 — Initiate Shard 2 Replica Set (shard2RS)
// ─────────────────────────────────────────────

print("\n══════════════════════════════════════════════");
print("STEP 3 — Initiating Shard 2 RS (shard2RS)");
print("══════════════════════════════════════════════");

retry("shard2RS initiation", () => {
  const s2Conn = new Mongo("shard2a:27017");
  const result = s2Conn.getDB("admin").adminCommand({
    replSetInitiate: {
      _id: "shard2RS",
      members: [
        { _id: 0, host: "shard2a:27017" },
        { _id: 1, host: "shard2b:27017" },
        { _id: 2, host: "shard2c:27017" },
      ],
    },
  });
  if (result.ok !== 1) throw new Error(JSON.stringify(result));
  print("  ✓ shard2RS rs.initiate() acknowledged");
}, 10, 3000);

// ─────────────────────────────────────────────
// STEP 4 — Wait for all THREE primaries to be elected
// ─────────────────────────────────────────────

print("\n══════════════════════════════════════════════");
print("STEP 4 — Waiting for Primary Elections");
print("══════════════════════════════════════════════");

waitForPrimary("config1", 27017, "configRS", 90000);
waitForPrimary("shard1a", 27017, "shard1RS", 90000);
waitForPrimary("shard2a", 27017, "shard2RS", 90000);

print("  Pausing 5 s for mongos to sync with configRS primary ...");
sleep(5000);

// ─────────────────────────────────────────────
// STEP 5 — Add Shards via the Mongos router
// ─────────────────────────────────────────────

print("\n══════════════════════════════════════════════");
print("STEP 5 — Adding Shards to the Cluster");
print("══════════════════════════════════════════════");

const mongosClient = new Mongo("mongos1:27017");
const mongosAdmin = mongosClient.getDB("admin");

retry("addShard shard1RS", () => {
  const result = mongosAdmin.adminCommand({
    addShard: "shard1RS/shard1a:27017,shard1b:27017,shard1c:27017",
    name: "shard1RS",
  });
  if (result.ok !== 1) throw new Error(JSON.stringify(result));
  print("  ✓ shard1RS added to cluster");
}, 15, 4000);

retry("addShard shard2RS", () => {
  const result = mongosAdmin.adminCommand({
    addShard: "shard2RS/shard2a:27017,shard2b:27017,shard2c:27017",
    name: "shard2RS",
  });
  if (result.ok !== 1) throw new Error(JSON.stringify(result));
  print("  ✓ shard2RS added to cluster");
}, 15, 4000);

// ─────────────────────────────────────────────
// STEP 6 — Enable sharding on database "metropt3"
// ─────────────────────────────────────────────

print("\n══════════════════════════════════════════════");
print("STEP 6 — Enabling Sharding on database 'metropt3'");
print("══════════════════════════════════════════════");

retry("enableSharding metropt3", () => {
  const result = mongosAdmin.adminCommand({ enableSharding: "metropt3" });
  if (result.ok !== 1 && result.code !== 23) throw new Error(JSON.stringify(result));
  print("  ✓ Sharding enabled on 'metropt3'");
}, 10, 3000);

// ─────────────────────────────────────────────
// STEP 7 — Shard collection "metropt3.sensor_buckets"
// ─────────────────────────────────────────────

print("\n══════════════════════════════════════════════");
print("STEP 7 — Sharding Collection 'metropt3.sensor_buckets'");
print("══════════════════════════════════════════════");

retry("create supporting index on sensor_buckets", () => {
  const metropt3 = mongosClient.getDB("metropt3");
  metropt3.sensor_buckets.createIndex({ unit_id: "hashed", bucket_start: 1 });
  print("  ✓ Compound index { unit_id: 'hashed', bucket_start: 1 } ensured");
}, 10, 3000);

retry("shardCollection metropt3.sensor_buckets", () => {
  const result = mongosAdmin.adminCommand({
    shardCollection: "metropt3.sensor_buckets",
    key: { unit_id: "hashed", bucket_start: 1 },
  });
  if (result.ok !== 1) throw new Error(JSON.stringify(result));
  print("  ✓ Collection 'metropt3.sensor_buckets' is now sharded");
}, 10, 3000);

// ─────────────────────────────────────────────
// Final Verification — Print cluster status summary
// ─────────────────────────────────────────────

print("\n══════════════════════════════════════════════");
print("CLUSTER INITIALIZATION COMPLETE");
print("══════════════════════════════════════════════");

const shardList = mongosAdmin.adminCommand({ listShards: 1 });
print("\nRegistered shards:");
shardList.shards.forEach(s => print(`  • ${s._id}  →  ${s.host}`));

const dbList = mongosAdmin.adminCommand({ listDatabases: 1 });
const metropt3Info = dbList.databases.find(d => d.name === "metropt3");
print(`\nDatabase 'metropt3': ${JSON.stringify(metropt3Info)}`);

const collInfo = mongosClient.getDB("config").collections.findOne({
  _id: "metropt3.sensor_buckets",
});
print(`\nSharded collection config:\n${JSON.stringify(collInfo, null, 2)}`);

print("\n✅  All steps completed successfully.");
print("    Connect to the cluster:  mongosh mongodb://localhost:27017/metropt3\n");