// Idempotent cluster bootstrap for MongoDB sharded topology.
// Usage: mongosh --host mongos:27020 /scripts/init_mongo.js

function waitForPrimary(host, port, timeoutMs) {
  const admin = new Mongo(`${host}:${port}`).getDB("admin");
  const started = Date.now();

  while (Date.now() - started < timeoutMs) {
    try {
      const res = admin.runCommand({ hello: 1 });
      if (res.ok === 1 && (res.isWritablePrimary === true || res.ismaster === true)) {
        print(`PRIMARY is ready on ${host}:${port}`);
        return;
      }
    } catch (e) {
      // keep waiting
    }
    sleep(2000);
  }

  throw new Error(`Timeout waiting PRIMARY on ${host}:${port}`);
}

function ensureReplicaSet(host, port, config, isConfigServer) {
  const admin = new Mongo(`${host}:${port}`).getDB("admin");
  const role = isConfigServer ? "ConfigServer" : config._id;
  print(`Ensuring replica set ${role}...`);

  try {
    const status = admin.runCommand({ replSetGetStatus: 1 });
    if (status.ok === 1) {
      print(`Replica set ${config._id} already initialized.`);
      return;
    }
  } catch (e) {
    // likely not initialized yet
  }

  const init = admin.runCommand({ replSetInitiate: config });
  if (init.ok !== 1 && init.codeName !== "AlreadyInitialized") {
    throw new Error(`Failed to init ${config._id}: ${tojson(init)}`);
  }

  waitForPrimary(host, port, 180000);
}

function ensureShard(adminDb, shardName, shardConn) {
  const shards = adminDb.runCommand({ listShards: 1 });
  if (shards.ok !== 1) {
    throw new Error(`Cannot list shards: ${tojson(shards)}`);
  }

  const exists = shards.shards.some((s) => s._id === shardName);
  if (exists) {
    print(`Shard ${shardName} already exists.`);
    return;
  }

  const added = adminDb.runCommand({ addShard: shardConn });
  if (added.ok !== 1) {
    throw new Error(`Cannot add shard ${shardName}: ${tojson(added)}`);
  }
  print(`Shard ${shardName} added.`);
}

function ensureDatabaseSharding(adminDb, dbName) {
  const res = adminDb.runCommand({ enableSharding: dbName });
  if (res.ok !== 1 && res.codeName !== "AlreadyInitialized") {
    const msg = (res.errmsg || "").toLowerCase();
    if (msg.indexOf("already enabled") === -1) {
      throw new Error(`enableSharding failed: ${tojson(res)}`);
    }
  }
}

function ensureTimeSeriesCollection(db, collName, metaField, timeField) {
  const existing = db.getCollectionInfos({ name: collName });
  if (existing.length > 0) {
    print(`Collection ${db.getName()}.${collName} already exists.`);
    return;
  }

  const created = db.createCollection(collName, {
    timeseries: {
      timeField: timeField,
      metaField: metaField,
      granularity: "seconds",
    },
  });

  if (created.ok !== 1) {
    throw new Error(`Cannot create timeseries collection: ${tojson(created)}`);
  }
}

function ensureCollectionSharding(adminDb, ns, key) {
  const res = adminDb.runCommand({ shardCollection: ns, key: key });
  if (res.ok !== 1) {
    const msg = (res.errmsg || "").toLowerCase();
    if (msg.indexOf("already") === -1) {
      throw new Error(`shardCollection failed: ${tojson(res)}`);
    }
  }
}

const DB_NAME = "metropt";
const COLLECTION_NAME = "sensor_data";
const META_FIELD = "COMP";
const TIME_FIELD = "timestamp";

print("=== Initializing replica sets ===");
ensureReplicaSet("configsvr", 27019, {
  _id: "csrs",
  configsvr: true,
  members: [{ _id: 0, host: "configsvr:27019" }],
}, true);

ensureReplicaSet("shard1_1", 27018, {
  _id: "rs1",
  members: [
    { _id: 0, host: "shard1_1:27018" },
    { _id: 1, host: "shard1_2:27028" },
    { _id: 2, host: "shard1_3:27038" },
  ],
}, false);

ensureReplicaSet("shard2_1", 27017, {
  _id: "rs2",
  members: [
    { _id: 0, host: "shard2_1:27017" },
    { _id: 1, host: "shard2_2:27027" },
    { _id: 2, host: "shard2_3:27037" },
  ],
}, false);

print("=== Registering shards on mongos ===");
const mongosAdmin = new Mongo("mongos:27020").getDB("admin");
ensureShard(mongosAdmin, "rs1", "rs1/shard1_1:27018,shard1_2:27028,shard1_3:27038");
ensureShard(mongosAdmin, "rs2", "rs2/shard2_1:27017,shard2_2:27027,shard2_3:27037");

print("=== Enabling sharding and creating collection ===");
ensureDatabaseSharding(mongosAdmin, DB_NAME);
const appDb = new Mongo("mongos:27020").getDB(DB_NAME);
ensureTimeSeriesCollection(appDb, COLLECTION_NAME, META_FIELD, TIME_FIELD);
ensureCollectionSharding(mongosAdmin, `${DB_NAME}.${COLLECTION_NAME}`, { COMP: 1, timestamp: 1 });

print("Cluster initialization complete.");
