// Ініціалізація шардованого кластера: config server, 2 шарди, mongos, користувачі
// Запускається сервісом mongo-init-sharding (profile sharding):
// mongosh --nodb --file /scripts/init-sharding.js
// Скрипт можна запускати повторно: усе, що вже зроблено, пропускається.

const TIMEOUT_SEC = 180;

function requireEnv(name) {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Змінна оточення ${name} не задана (перевірте .env)`);
  }
  return value;
}

const ADMIN_USER = requireEnv("MONGO_ADMIN_USER");
const ADMIN_PASSWORD = requireEnv("MONGO_ADMIN_PASSWORD");
const APP_USER = requireEnv("MONGO_APP_USER");
const APP_PASSWORD = requireEnv("MONGO_APP_PASSWORD");
const APP_DB = process.env.MONGO_DB || "eventhub";
const TEST_DB = process.env.MONGO_TEST_DB || "eventhub_test";

const REPLICA_SETS = [
  {
    _id: "configReplSet",
    configsvr: true,
    members: [{ _id: 0, host: "config-server:27020" }]
  },
  {
    _id: "shard1ReplSet",
    members: [
      { _id: 0, host: "shard1-node1:27021", priority: 2 },
      { _id: 1, host: "shard1-node2:27022", priority: 1 }
    ]
  },
  {
    _id: "shard2ReplSet",
    members: [
      { _id: 0, host: "shard2-node1:27023", priority: 2 },
      { _id: 1, host: "shard2-node2:27024", priority: 1 }
    ]
  }
];

function connectWithRetry(host) {
  const deadline = Date.now() + TIMEOUT_SEC * 1000;
  while (true) {
    try {
      const conn = new Mongo(`mongodb://${host}/?directConnection=true`);
      conn.getDB("admin").runCommand({ ping: 1 });
      print(`[sharding] ${host} доступний`);
      return conn;
    } catch (e) {
      if (Date.now() > deadline) {
        throw new Error(`[sharding] ${host} не відповідає за ${TIMEOUT_SEC} с: ${e.message}`);
      }
      print(`[sharding] чекаємо ${host}...`);
      sleep(1000);
    }
  }
}

function isInitialized(adminDb) {
  try {
    const res = adminDb.runCommand({ replSetGetStatus: 1 });
    if (res.ok === 1) return true;
    if (res.code === 94) return false;
    throw new Error(JSON.stringify(res));
  } catch (e) {
    if (e.code === 94 || /NotYetInitialized|no replset config/i.test(e.message)) {
      return false;
    }
    throw e;
  }
}

function waitForWritablePrimary(adminDb, name) {
  const deadline = Date.now() + TIMEOUT_SEC * 1000;
  while (!adminDb.runCommand({ hello: 1 }).isWritablePrimary) {
    if (Date.now() > deadline) {
      throw new Error(`[sharding] ${name}: PRIMARY не обрано за ${TIMEOUT_SEC} с`);
    }
    sleep(1000);
  }
  print(`[sharding] ${name}: PRIMARY готовий`);
}

for (const rs of REPLICA_SETS) {
  rs.members.forEach((m) => connectWithRetry(m.host));
  const first = connectWithRetry(rs.members[0].host).getDB("admin");
  if (isInitialized(first)) {
    print(`[sharding] ${rs._id} вже ініціалізований — пропускаємо`);
  } else {
    print(`[sharding] ініціалізуємо ${rs._id}...`);
    first.runCommand({ replSetInitiate: rs });
  }
  waitForWritablePrimary(first, rs._id);
}


const mongos = connectWithRetry("mongos-router:27025");
const mongosAdmin = mongos.getDB("admin");
{
  const deadline = Date.now() + TIMEOUT_SEC * 1000;
  let existing = null;
  while (existing === null) {
    try {
      existing = mongosAdmin.runCommand({ listShards: 1 }).shards.map((s) => s._id);
    } catch (e) {
      if (Date.now() > deadline) throw e;
      print("[sharding] чекаємо, доки mongos підключиться до config server...");
      sleep(2000);
    }
  }
  for (const rs of REPLICA_SETS.filter((r) => !r.configsvr)) {
    if (existing.includes(rs._id)) {
      print(`[sharding] шард ${rs._id} вже доданий — пропускаємо`);
      continue;
    }
    const seed = `${rs._id}/${rs.members.map((m) => m.host).join(",")}`;
    const res = mongosAdmin.runCommand({ addShard: seed, name: rs._id });
    if (res.ok !== 1) throw new Error(`[sharding] addShard ${seed}: ${JSON.stringify(res)}`);
    print(`[sharding] додано шард ${seed}`);
  }
}

function ensureUser(dbHandle, user, pwd, roles) {
  if (dbHandle.getUser(user)) {
    dbHandle.grantRolesToUser(user, roles);
    print(`[sharding] користувач ${user}@${dbHandle.getName()} вже існує — ролі оновлено`);
    return;
  }
  dbHandle.createUser({ user: user, pwd: pwd, roles: roles });
  print(`[sharding] створено користувача ${user}@${dbHandle.getName()}`);
}

ensureUser(mongosAdmin, ADMIN_USER, ADMIN_PASSWORD, [
  { role: "root", db: "admin" },
  { role: "clusterAdmin", db: "admin" }
]);
ensureUser(mongos.getDB(APP_DB), APP_USER, APP_PASSWORD, [
  { role: "readWrite", db: APP_DB },
  { role: "dbAdmin", db: APP_DB },
  { role: "readWrite", db: TEST_DB },
  { role: "dbAdmin", db: TEST_DB }
]);

printjson(mongosAdmin.runCommand({ listShards: 1 }).shards.map((s) => ({ id: s._id, host: s.host })));
print("[sharding] готово");
