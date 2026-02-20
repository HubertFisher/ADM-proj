// init_mongo.js
// Запускать через: mongo --host mongos:27020 /scripts/init_mongo.js

// --- Конфиг-серверный реплика-сет ---
print("Initializing Config Server ReplSet...");
var csConfig = {
    _id: "csrs",
    configsvr: true,
    members: [
        { _id: 0, host: "configsvr:27019" }
    ]
};
var csrs = new Mongo("configsvr:27019").getDB("admin");
csrs.adminCommand({ replSetInitiate: csConfig });
print("Waiting for Config Server to become PRIMARY...");
sleep(10000); // даём время на выбор PRIMARY

// --- Шард 1 (rs1) ---
print("Initializing Shard1 ReplSet...");
var rs1Config = {
    _id: "rs1",
    members: [
        { _id: 0, host: "shard1_1:27018" },
        { _id: 1, host: "shard1_2:27028" },
        { _id: 2, host: "shard1_3:27038" }
    ]
};
var rs1 = new Mongo("shard1_1:27018").getDB("admin");
rs1.adminCommand({ replSetInitiate: rs1Config });
print("Waiting for Shard1 to become PRIMARY...");
sleep(10000);

// --- Шард 2 (rs2) ---
print("Initializing Shard2 ReplSet...");
var rs2Config = {
    _id: "rs2",
    members: [
        { _id: 0, host: "shard2_1:27017" },
        { _id: 1, host: "shard2_2:27027" },
        { _id: 2, host: "shard2_3:27037" }
    ]
};
var rs2 = new Mongo("shard2_1:27017").getDB("admin");
rs2.adminCommand({ replSetInitiate: rs2Config });
print("Waiting for Shard2 to become PRIMARY...");
sleep(10000);

// --- Подключаем шарды к mongos ---
print("Adding shards to cluster...");
var mongos = new Mongo("mongos:27020").getDB("admin");

// rs1
mongos.runCommand({ addShard: "rs1/shard1_1:27018,shard1_2:27028,shard1_3:27038" });
// rs2
mongos.runCommand({ addShard: "rs2/shard2_1:27017,shard2_2:27027,shard2_3:27037" });

// --- Включаем шардирование для базы и коллекции ---
print("Enabling sharding on database 'metropt'...");
mongos.runCommand({ enableSharding: "metropt" });

print("Creating time-series collection 'sensor_data'...");
mongos.getDB("metropt").createCollection("sensor_data", {
    timeseries: {
        timeField: "timestamp",
        metaField: "COMP",
        granularity: "seconds"
    }
});

print("Sharding collection 'sensor_data' on {COMP:1, timestamp:1}...");
mongos.runCommand({
    shardCollection: "metropt.sensor_data",
    key: { COMP: 1, timestamp: 1 }
});

print("Initialization complete!");
