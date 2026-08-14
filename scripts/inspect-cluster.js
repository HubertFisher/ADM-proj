/* global Mongo, db, print, printjson */

const DATABASE = "metropt3";
const COLLECTION = "sensor_buckets";
const NAMESPACE = `${DATABASE}.${COLLECTION}`;
const admin = db.getSiblingDB("admin");
const config = db.getSiblingDB("config");
const dataDb = db.getSiblingDB(DATABASE);
const collection = dataDb.getCollection(COLLECTION);

function replicaSetSummary(uri) {
  const status = new Mongo(uri).getDB("admin").runCommand({ replSetGetStatus: 1 });
  return {
    name: status.set,
    members: status.members.map((member) => ({ name: member.name, state: member.stateStr })),
  };
}

function planStages(value, result = new Set()) {
  if (!value || typeof value !== "object") return result;
  if (typeof value.stage === "string") result.add(value.stage);
  Object.values(value).forEach((child) => planStages(child, result));
  return result;
}

const metadata = config.collections.findOne({ _id: NAMESPACE });
const balancer = admin.runCommand({ balancerStatus: 1 });
const chunkPlacement = metadata
  ? config.chunks
      .aggregate([
        { $match: { uuid: metadata.uuid } },
        { $group: { _id: "$shard", chunks: { $sum: 1 } } },
        { $sort: { _id: 1 } },
      ])
      .toArray()
  : [];
const shardStats = collection
  .aggregate([
    { $collStats: { storageStats: {} } },
    {
      $project: {
        _id: 0,
        shard: 1,
        documents: "$storageStats.count",
        logicalBytes: "$storageStats.size",
        storageBytes: "$storageStats.storageSize",
      },
    },
    { $sort: { shard: 1 } },
  ])
  .toArray();

const sample = collection.findOne({}, { _id: 1 });
const exactId = sample ? sample._id : "0".repeat(64);
const targetedPlan = collection.find({ _id: exactId }).explain("executionStats");
const rangePlan = collection
  .find({
    unit_id: "APU_METRO_01",
    bucket_seconds: 300,
    bucket_start: {
      $gte: new Date("2020-02-01T00:00:00Z"),
      $lt: new Date("2020-02-02T00:00:00Z"),
    },
  })
  .explain("executionStats");

function querySummary(explain) {
  return {
    stages: Array.from(planStages(explain.queryPlanner.winningPlan)).sort(),
    returned: explain.executionStats.nReturned,
    keysExamined: explain.executionStats.totalKeysExamined,
    documentsExamined: explain.executionStats.totalDocsExamined,
    executionMillis: explain.executionStats.executionTimeMillis,
  };
}

print("\n=== Shards ===");
printjson(admin.runCommand({ listShards: 1 }).shards);
print("\n=== Replica sets ===");
printjson([
  replicaSetSummary("mongodb://config1:27019/?directConnection=true"),
  replicaSetSummary("mongodb://shard1a:27018/?directConnection=true"),
  replicaSetSummary("mongodb://shard2a:27018/?directConnection=true"),
]);
print("\n=== Sharded collection ===");
printjson({
  namespace: NAMESPACE,
  shardKey: metadata ? metadata.key : null,
  chunks: metadata ? config.chunks.countDocuments({ uuid: metadata.uuid }) : 0,
  chunkPlacement,
  balancer: { mode: balancer.mode, inRound: balancer.inBalancerRound },
  validation: dataDb.getCollectionInfos({ name: COLLECTION })[0].options.validationLevel,
  indexes: collection.getIndexes().map((index) => ({ name: index.name, key: index.key })),
});
print("\n=== Per-shard storage ===");
printjson(shardStats);
print("\n=== Query routing ===");
printjson({
  exactIdEquality: querySummary(targetedPlan),
  unitTimeRange: querySummary(rangePlan),
});
