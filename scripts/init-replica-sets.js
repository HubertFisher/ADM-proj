/* global Mongo, print, sleep */

function commandWorked(result, context) {
  if (result.ok !== 1) throw new Error(`${context}: ${JSON.stringify(result)}`);
}

function waitFor(predicate, message) {
  const deadline = Date.now() + 120000;
  while (Date.now() < deadline) {
    if (predicate()) return;
    sleep(1000);
  }
  throw new Error(message);
}

function initiateReplicaSet(uri, configuration) {
  const connection = new Mongo(uri);
  const admin = connection.getDB("admin");

  let alreadyInitiated = false;
  try {
    alreadyInitiated = admin.runCommand({ replSetGetStatus: 1 }).ok === 1;
  } catch (error) {
    if (error.codeName !== "NotYetInitialized") throw error;
  }

  if (!alreadyInitiated) {
    print(`Initiating ${configuration._id}`);
    commandWorked(admin.runCommand({ replSetInitiate: configuration }), "replSetInitiate failed");
  } else {
    print(`${configuration._id} is already initiated`);
  }

  waitFor(
    () => {
      const status = admin.runCommand({ replSetGetStatus: 1 });
      return status.ok === 1 && status.members.some((member) => member.stateStr === "PRIMARY");
    },
    `${configuration._id} did not elect a primary`,
  );
}

initiateReplicaSet("mongodb://config1:27019/?directConnection=true", {
  _id: "configRS",
  configsvr: true,
  members: [
    { _id: 0, host: "config1:27019" },
    { _id: 1, host: "config2:27019" },
    { _id: 2, host: "config3:27019" },
  ],
});

initiateReplicaSet("mongodb://shard1a:27018/?directConnection=true", {
  _id: "shard1RS",
  members: [
    { _id: 0, host: "shard1a:27018" },
    { _id: 1, host: "shard1b:27018" },
    { _id: 2, host: "shard1c:27018" },
  ],
});

initiateReplicaSet("mongodb://shard2a:27018/?directConnection=true", {
  _id: "shard2RS",
  members: [
    { _id: 0, host: "shard2a:27018" },
    { _id: 1, host: "shard2b:27018" },
    { _id: 2, host: "shard2c:27018" },
  ],
});

print("All replica sets have a primary");
