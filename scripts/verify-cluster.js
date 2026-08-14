/* global Mongo, db, print */

const DATABASE = "metropt3";
const COLLECTION = "sensor_buckets";
const NAMESPACE = `${DATABASE}.${COLLECTION}`;
const EXPECTED_BUCKETS = 50782;
const EXPECTED_READINGS = 1516948;

function check(condition, message) {
  if (!condition) throw new Error(message);
}

function collectStages(value, result = new Set()) {
  if (!value || typeof value !== "object") return result;
  if (typeof value.stage === "string") result.add(value.stage);
  Object.values(value).forEach((child) => collectStages(child, result));
  return result;
}

function verifyReplicaSet(uri, expectedName) {
  const status = new Mongo(uri).getDB("admin").runCommand({ replSetGetStatus: 1 });
  check(status.ok === 1, `${expectedName} status command failed`);
  check(status.set === expectedName, `Expected ${expectedName}, got ${status.set}`);
  check(status.members.filter((member) => member.stateStr === "PRIMARY").length === 1,
    `${expectedName} does not have exactly one primary`);
  check(status.members.filter((member) => member.stateStr === "SECONDARY").length === 2,
    `${expectedName} does not have exactly two secondaries`);
}

const admin = db.getSiblingDB("admin");
const config = db.getSiblingDB("config");
const dataDb = db.getSiblingDB(DATABASE);
const collection = dataDb.getCollection(COLLECTION);

check(admin.runCommand({ hello: 1 }).msg === "isdbgrid", "Verification is not running via mongos");
const shards = admin.runCommand({ listShards: 1 }).shards;
check(shards.length === 2, `Expected 2 shards, got ${shards.length}`);
check(shards.every((shard) => shard.state === 1), "At least one shard is unavailable");

verifyReplicaSet("mongodb://config1:27019/?directConnection=true", "configRS");
verifyReplicaSet("mongodb://shard1a:27018/?directConnection=true", "shard1RS");
verifyReplicaSet("mongodb://shard2a:27018/?directConnection=true", "shard2RS");

const metadata = config.collections.findOne({ _id: NAMESPACE });
check(metadata && metadata.key && metadata.key._id === "hashed", "Unexpected or missing shard key");
const chunks = config.chunks.find({ uuid: metadata.uuid }).toArray();
check(chunks.length >= 2, `Expected at least 2 chunks, got ${chunks.length}`);
check(new Set(chunks.map((chunk) => chunk.shard)).size === 2, "Chunks do not span both shards");
check(admin.runCommand({ balancerStatus: 1 }).mode === "full", "Balancer is not enabled");

const indexNames = new Set(collection.getIndexes().map((index) => index.name));
for (const name of ["_id_", "_id_hashed", "unit_resolution_time"]) {
  check(indexNames.has(name), `Missing index ${name}`);
}
const options = dataDb.getCollectionInfos({ name: COLLECTION })[0].options;
check(options.validationLevel === "strict", "Collection validation is not strict");
check(options.validationAction === "error", "Collection validation does not reject errors");

let validationRejected = false;
try {
  collection.insertOne({ _id: "invalid-validation-probe" });
} catch (error) {
  validationRejected = error.code === 121;
}
check(validationRejected, "MongoDB JSON Schema accepted an invalid document");

const distribution = collection
  .aggregate([
    { $collStats: { storageStats: {} } },
    { $project: { _id: 0, shard: 1, documents: "$storageStats.count" } },
  ])
  .toArray();
check(distribution.length === 2, "Collection statistics did not return both shards");
const totalBuckets = distribution.reduce((total, shard) => total + shard.documents, 0);
check(totalBuckets === EXPECTED_BUCKETS, `Expected ${EXPECTED_BUCKETS} buckets, got ${totalBuckets}`);
check(distribution.every((shard) => shard.documents > 0), "A shard contains no bucket documents");
const imbalance = Math.abs(distribution[0].documents - distribution[1].documents) / totalBuckets;
check(imbalance < 0.05, `Document distribution differs by ${(imbalance * 100).toFixed(2)}%`);

const totals = collection
  .aggregate([{ $group: { _id: null, readings: { $sum: "$reading_count" } } }])
  .toArray()[0];
check(totals.readings === EXPECTED_READINGS,
  `Expected ${EXPECTED_READINGS} embedded readings, got ${totals.readings}`);

const sampleId = collection.findOne({}, { _id: 1 })._id;
const targetedExplain = collection.find({ _id: sampleId }).explain("queryPlanner");
const targetedStages = collectStages(targetedExplain.queryPlanner.winningPlan);
check(targetedStages.has("SINGLE_SHARD"), "Exact shard-key equality was not targeted");

const rangeExplain = collection
  .find({
    unit_id: "APU_METRO_01",
    bucket_seconds: 300,
    bucket_start: {
      $gte: new Date("2020-02-01T00:00:00Z"),
      $lt: new Date("2020-02-02T00:00:00Z"),
    },
  })
  .explain("queryPlanner");
const rangeStages = collectStages(rangeExplain.queryPlanner.winningPlan);
check(rangeStages.has("SHARD_MERGE"), "Time-range query did not demonstrate scatter/gather");
check(rangeStages.has("IXSCAN"), "Time-range query did not use the supporting index");

print(
  `PASS: ${totalBuckets} buckets/${totals.readings} readings; ` +
    `${distribution.map((item) => `${item.shard}=${item.documents}`).join(", ")}; ` +
    "replication, validation, indexes, chunks, and routing verified",
);
