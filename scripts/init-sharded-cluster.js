/* global db, print */

const DATABASE = "metropt3";
const COLLECTION = "sensor_buckets";
const NAMESPACE = `${DATABASE}.${COLLECTION}`;
const admin = db.getSiblingDB("admin");

function commandWorked(result, context) {
  if (result.ok !== 1) throw new Error(`${context}: ${JSON.stringify(result)}`);
}

function addShardIfMissing(name, connectionString) {
  const exists = admin.runCommand({ listShards: 1 }).shards.some((shard) => shard._id === name);
  if (!exists) {
    print(`Adding ${name}`);
    commandWorked(admin.runCommand({ addShard: connectionString, name }), `Failed to add ${name}`);
  } else {
    print(`${name} is already registered`);
  }
}

addShardIfMissing("shard1", "shard1RS/shard1a:27018,shard1b:27018,shard1c:27018");
addShardIfMissing("shard2", "shard2RS/shard2a:27018,shard2b:27018,shard2c:27018");

commandWorked(admin.runCommand({ enableSharding: DATABASE }), "enableSharding failed");

const dataDb = db.getSiblingDB(DATABASE);
const analogueFields = [
  "TP2",
  "TP3",
  "H1",
  "DV_pressure",
  "Reservoirs",
  "Motor_current",
  "Oil_temperature",
];
const digitalFields = [
  "COMP",
  "DV_electric",
  "Towers",
  "MPG",
  "LPS",
  "Pressure_switch",
  "Oil_level",
  "Caudal_impulses",
];
const analogueValueProperties = Object.fromEntries(
  analogueFields.map((field) => [field, { bsonType: "double" }]),
);
const digitalValueProperties = Object.fromEntries(
  digitalFields.map((field) => [field, { bsonType: "int", enum: [0, 1] }]),
);
const measurementStatsProperties = Object.fromEntries(
  analogueFields.map((field) => [
    field,
    {
      bsonType: "object",
      required: ["min", "max", "avg"],
      additionalProperties: false,
      properties: {
        min: { bsonType: "double" },
        max: { bsonType: "double" },
        avg: { bsonType: "double" },
      },
    },
  ]),
);
const digitalRatioProperties = Object.fromEntries(
  digitalFields.map((field) => [field, { bsonType: "double", minimum: 0, maximum: 1 }]),
);
const bucketValidator = {
  $jsonSchema: {
    bsonType: "object",
    additionalProperties: false,
    required: [
      "_id",
      "unit_id",
      "bucket_seconds",
      "bucket_start",
      "bucket_end",
      "source_timezone",
      "reading_count",
      "observed_span_seconds",
      "source_sequence",
      "readings",
      "bucket_stats",
      "schema_version",
    ],
    properties: {
      _id: { bsonType: "string", minLength: 64, maxLength: 64 },
      unit_id: { bsonType: "string", minLength: 1 },
      bucket_seconds: { bsonType: "int", minimum: 1 },
      bucket_start: { bsonType: "date" },
      bucket_end: { bsonType: "date" },
      source_timezone: { bsonType: "string", minLength: 1 },
      reading_count: { bsonType: "int", minimum: 1 },
      observed_span_seconds: { bsonType: "int", minimum: 0 },
      source_sequence: {
        bsonType: "object",
        required: ["first", "last"],
        additionalProperties: false,
        properties: {
          first: { bsonType: "int", minimum: 0 },
          last: { bsonType: "int", minimum: 0 },
        },
      },
      readings: {
        bsonType: "array",
        minItems: 1,
        items: {
          bsonType: "object",
          required: ["seq", "source_seq", "ts", "analogue", "digital"],
          additionalProperties: false,
          properties: {
            seq: { bsonType: "int", minimum: 1 },
            source_seq: { bsonType: "int", minimum: 0 },
            ts: { bsonType: "date" },
            analogue: {
              bsonType: "object",
              required: analogueFields,
              additionalProperties: false,
              properties: analogueValueProperties,
            },
            digital: {
              bsonType: "object",
              required: digitalFields,
              additionalProperties: false,
              properties: digitalValueProperties,
            },
          },
        },
      },
      bucket_stats: {
        bsonType: "object",
        required: ["analogue", "digital_on_ratio"],
        additionalProperties: false,
        properties: {
          analogue: {
            bsonType: "object",
            required: analogueFields,
            additionalProperties: false,
            properties: measurementStatsProperties,
          },
          digital_on_ratio: {
            bsonType: "object",
            required: digitalFields,
            additionalProperties: false,
            properties: digitalRatioProperties,
          },
        },
      },
      schema_version: { bsonType: "int", enum: [2] },
    },
  },
};
const collectionExists = dataDb.getCollectionInfos({ name: COLLECTION }).length === 1;
if (!collectionExists) {
  commandWorked(
    dataDb.createCollection(COLLECTION, {
      validator: bucketValidator,
      validationLevel: "strict",
      validationAction: "error",
    }),
    "createCollection failed",
  );
} else {
  commandWorked(
    dataDb.runCommand({
      collMod: COLLECTION,
      validator: bucketValidator,
      validationLevel: "strict",
      validationAction: "error",
    }),
    "collMod failed",
  );
}

const metadata = db.getSiblingDB("config").collections.findOne({ _id: NAMESPACE });
if (!metadata || !metadata.key) {
  commandWorked(
    admin.runCommand({
      shardCollection: NAMESPACE,
      key: { _id: "hashed" },
      numInitialChunks: 2,
    }),
    "shardCollection failed",
  );
  print(`Sharded ${NAMESPACE} on {_id: "hashed"}`);
} else {
  if (metadata.key._id !== "hashed") {
    throw new Error(`${NAMESPACE} has an unexpected shard key: ${JSON.stringify(metadata.key)}`);
  }
  print(`${NAMESPACE} is already sharded on {_id: "hashed"}`);
}

commandWorked(
  dataDb.runCommand({
    createIndexes: COLLECTION,
    indexes: [
      {
        key: { unit_id: 1, bucket_seconds: 1, bucket_start: 1 },
        name: "unit_resolution_time",
      },
    ],
  }),
  "createIndexes failed",
);

commandWorked(admin.runCommand({ balancerStart: 1 }), "balancerStart failed");
print("Sharded cluster initialization complete");
