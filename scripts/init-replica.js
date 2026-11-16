// Ініціалізація replica set rs0 і користувачів
// Запускається сервісом mongo-init: mongosh --nodb --file /scripts/init-replica.js
// Скрипт можна запускати повторно: усе, що вже зроблено, пропускається
// Паролі беруться зі змінних оточення (docker-compose бере їх із .env)

const TIMEOUT_SEC = 120;

const MEMBERS = [
  { _id: 0, host: "mongo-primary:27017", priority: 2 },
  { _id: 1, host: "mongo-secondary1:27018", priority: 1 },
  { _id: 2, host: "mongo-secondary2:27019", priority: 1 }
];

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

function connectWithRetry(host) {
  const deadline = Date.now() + TIMEOUT_SEC * 1000;
  while (true) {
    try {
      const conn = new Mongo(`mongodb://${host}/?directConnection=true`);
      conn.getDB("admin").runCommand({ ping: 1 });
      print(`[init] ${host} доступний`);
      return conn;
    } catch (e) {
      if (Date.now() > deadline) {
        throw new Error(`[init] ${host} не відповідає за ${TIMEOUT_SEC} с: ${e.message}`);
      }
      print(`[init] чекаємо ${host}...`);
      sleep(1000);
    }
  }
}


const connections = MEMBERS.map((m) => connectWithRetry(m.host));
const primaryConn = connections[0];
const adminDb = primaryConn.getDB("admin");

function isInitialized() {
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

if (isInitialized()) {
  print("[init] replica set rs0 вже ініціалізований — пропускаємо rs.initiate");
} else {
  print("[init] ініціалізуємо replica set rs0...");
  adminDb.runCommand({ replSetInitiate: { _id: "rs0", members: MEMBERS } });
}


{
  const deadline = Date.now() + TIMEOUT_SEC * 1000;
  while (!adminDb.runCommand({ hello: 1 }).isWritablePrimary) {
    if (Date.now() > deadline) {
      throw new Error(`[init] mongo-primary не став PRIMARY за ${TIMEOUT_SEC} с`);
    }
    print("[init] чекаємо вибору PRIMARY...");
    sleep(1000);
  }
  print("[init] mongo-primary:27017 - PRIMARY");
}


function ensureUser(dbHandle, user, pwd, roles) {
  if (dbHandle.getUser(user)) {
    dbHandle.grantRolesToUser(user, roles);
    print(`[init] користувач ${user}@${dbHandle.getName()} вже існує — ролі оновлено`);
    return;
  }
  dbHandle.createUser({ user: user, pwd: pwd, roles: roles });
  print(`[init] створено користувача ${user}@${dbHandle.getName()}`);
}

ensureUser(adminDb.getSiblingDB("admin"), ADMIN_USER, ADMIN_PASSWORD, [
  { role: "root", db: "admin" },
  { role: "clusterAdmin", db: "admin" }
]);

ensureUser(adminDb.getSiblingDB(APP_DB), APP_USER, APP_PASSWORD, [
  { role: "readWrite", db: APP_DB },
  { role: "dbAdmin", db: APP_DB },
  { role: "readWrite", db: TEST_DB },
  { role: "dbAdmin", db: TEST_DB }
]);

print("[init] готово");
