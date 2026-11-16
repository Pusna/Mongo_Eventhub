"""Реплікація та висока доступність."""
import logging
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

from pymongo import MongoClient
from pymongo.errors import AutoReconnect, ConnectionFailure, OperationFailure
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import ReadPreference
from pymongo.write_concern import WriteConcern

from app.config import settings

logger = logging.getLogger(__name__)


# ---------- Налаштування репліка-сету з Python ----------

class ReplicaSetManager:
    """Адмін-операції над rs0. Клієнт має бути з правами clusterAdmin (settings.mongo_admin_uri)"""

    def __init__(self, client: MongoClient) -> None:
        self.client = client
        self.db = client.admin

    def initiate_replica_set(self, members: List[Dict]) -> Dict:
        """Ініціалізація репліка-сету"""

        config = {
            "_id": "rs0",
            "members": members,

            "settings": {
                "heartbeatIntervalMillis": 2000,
                "electionTimeoutMillis": 10000,
                "catchUpTimeoutMillis": 20000,
            }
        }

        try:
            result = self.db.command({"replSetInitiate": config})
            logger.info(f"Replica set initiated: {result}")
            return {"initiated": True}
        except OperationFailure as e:
            if "already initialized" in str(e):
                logger.info("Replica set already initialized")
                return {"initiated": False, "reason": "already initialized"}
            raise

    def get_replica_set_status(self) -> Dict:
        """Отримати статус репліка-сету"""
        return self.db.command("replSetGetStatus")

    def check_secondary_lag(self) -> Dict:
        """Перевірка відставання secondary реплік"""

        status = self.get_replica_set_status()
        members = status["members"]

        primary_member = next((m for m in members if m["stateStr"] == "PRIMARY"), None)
        primary = primary_member["name"] if primary_member else None
        primary_optime = primary_member["optimeDate"] if primary_member else None

        secondary_lags = []
        for member in members:
            if member["stateStr"] == "SECONDARY":

                lag = ((primary_optime - member["optimeDate"]).total_seconds()
                       if primary_optime else None)
                secondary_lags.append({
                    "host": member["name"],
                    "lag_seconds": lag,
                    "state": member["stateStr"]
                })

        return {
            "primary": primary,
            "secondary_lags": secondary_lags,
            "total_members": len(members)
        }

    def get_primary(self) -> Optional[str]:
        """Ім'я поточного primary (host:port) або None."""
        try:
            status = self.get_replica_set_status()
        except (AutoReconnect, ConnectionFailure):
            return None
        return next((m["name"] for m in status["members"] if m["stateStr"] == "PRIMARY"), None)

    def wait_for_primary(self, expected: Optional[str] = None, timeout: int = 60) -> Optional[str]:
        """Чекати, доки з'явиться primary (або саме expected)."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            primary = self.get_primary()
            if primary and (expected is None or primary == expected):
                return primary
            time.sleep(1)
        raise TimeoutError(f"Primary {expected or ''} не обрано за {timeout} с")

    def force_election(self, new_primary: str, timeout: int = 90) -> str:
        """Примусові вибори нового primary"""

        config = self.db.command("replSetGetConfig")["config"]
        hosts = [m["host"] for m in config["members"]]
        if new_primary not in hosts:
            raise ValueError(f"{new_primary} не є членом репліка-сету: {hosts}")

        # Знижуємо пріоритет поточного primary і підвищуємо пріоритет нового
        for member in config["members"]:
            member["priority"] = 2 if member["host"] == new_primary else 1
        config["version"] += 1
        self.db.command({"replSetReconfig": config})

        # Ініціюємо вибори
        current = self.get_primary()
        if current and current != new_primary:
            try:
                self.db.command({"replSetStepDown": 15, "secondaryCatchUpPeriodSecs": 5,
                                 "force": True})
            except (AutoReconnect, ConnectionFailure):
                pass  # primary закриває з'єднання під час stepDown — це очікувано

        result = self.wait_for_primary(new_primary, timeout=timeout)
        logger.info(f"Forced election completed. New primary: {result}")
        return result


# ---------- Read Preferences та Consistency Levels ----------

class ReadConsistencyManager:
    """Читання з різними read preference і read concern (розділ 7.2)."""

    def __init__(self, client: MongoClient, db_name: Optional[str] = None) -> None:
        self.client = client
        self.db = client[db_name or settings.mongo_db]

    def read_with_consistency(self, collection: str, query: Dict,
                              consistency_level: str = "eventual") -> List[Dict]:
        """Читання з різними рівнями узгодженості"""

        read_concern = None
        if consistency_level == "eventual":
            read_pref = ReadPreference.SECONDARY
        elif consistency_level == "consistent":
            read_pref = ReadPreference.PRIMARY
            read_concern = ReadConcern("majority")
        elif consistency_level == "linearizable":
            read_pref = ReadPreference.PRIMARY
            read_concern = ReadConcern("linearizable")
        else:
            read_pref = ReadPreference.PRIMARY

        coll = self.db.get_collection(collection).with_options(
            read_preference=read_pref,
            read_concern=read_concern,
        )
        return list(coll.find(query).limit(100))

    def benchmark_consistency(self, query: Optional[Dict] = None) -> Dict:
        """Бенчмарк різних рівнів узгодженості"""

        results = {}
        query = query if query is not None else {"status": "active"}

        for level in ["eventual", "consistent", "linearizable"]:
            start = time.time()
            docs = self.read_with_consistency("events", query, level)
            elapsed = time.time() - start

            results[level] = {
                "time_ms": elapsed * 1000,
                "doc_count": len(docs)
            }

        return results


# ---------- Write Concerns для різних сценаріїв ----------

class WriteConcernManager:
    """Запис з різними write concern і порівняння швидкості"""

    CONCERNS = {
        "low": WriteConcern(w=1, j=False),
        "medium": WriteConcern(w=1, j=True),
        "majority": WriteConcern(w="majority", j=True, wtimeout=5000),
        "high": WriteConcern(w=3, j=True, wtimeout=10000),
        "durable": WriteConcern(w="majority", j=True, wtimeout=5000)
    }

    def __init__(self, client: MongoClient, db_name: Optional[str] = None) -> None:
        self.client = client
        self.db = client[db_name or settings.mongo_db]

    def write_with_concern(self, collection: str, document: Dict,
                           concern_level: str = "majority") -> Dict:
        """Запис з різними рівнями write concern"""

        concern = self.CONCERNS.get(concern_level, self.CONCERNS["majority"])

        try:
            result = self.db.get_collection(collection).with_options(
                write_concern=concern
            ).insert_one(document)
            return {"success": True, "id": result.inserted_id}
        except OperationFailure as e:
            return {"success": False, "error": str(e)}

    def compare_write_performance(self, iterations: int = 100,
                                  collection: str = "test_collection") -> Dict:
        """Порівняння продуктивності різних write concern"""

        results = {}

        for level in ["low", "medium", "majority", "high"]:
            times = []
            for _ in range(iterations):
                test_doc = {"test": "write_concern_benchmark",
                            "timestamp": datetime.now(timezone.utc)}
                start = time.perf_counter()
                self.write_with_concern(collection, test_doc, level)
                elapsed = time.perf_counter() - start
                times.append(elapsed * 1000)

            results[level] = {
                "avg_ms": sum(times) / len(times),
                "min_ms": min(times),
                "max_ms": max(times),
                "p95_ms": sorted(times)[int(len(times) * 0.95)]
            }

        return results
