"""Розділ 8. Шардінг та масштабування.

Команд shardStatus і listChunks з PDF у MongoDB не існує. Замість них:
listShards, колекції config.collections / config.chunks, $collStats і balancerStatus.
"""
import logging
from collections import Counter
from typing import Any, Dict, List, Optional

from bson.max_key import MaxKey
from bson.min_key import MinKey
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import BulkWriteError

from app.config import settings

logger = logging.getLogger(__name__)


class ShardingManager:
    """Працює через mongos-router (settings.mongos_uri)."""

    ZONES = {
        "EU": ("shard1ReplSet", ["Ukraine"]),
        "US": ("shard2ReplSet", ["USA"]),
    }

    def __init__(self, router_client: MongoClient) -> None:
        """router_client — підключення до mongos."""
        self.client = router_client
        self.admin = router_client.admin
        self.config = router_client.config

    def enable_sharding_for_database(self, db_name: str) -> None:
        """Включити шардінг для бази"""
        self.admin.command("enableSharding", db_name)
        logger.info(f"Sharding enabled for {db_name}")

    def shard_collection(self, db_name: str, collection: str,
                         shard_key: Dict, unique: bool = False) -> None:
        """Зашардувати колекцію з вказаним ключем"""
        self.admin.command(
            "shardCollection",
            f"{db_name}.{collection}",
            key=shard_key,
            unique=unique
        )
        logger.info(f"Collection {db_name}.{collection} sharded with key: {shard_key}")

    def manage_zones(self, db_name: Optional[str] = None) -> List[str]:
        """Налаштування зон для географічного розподілу."""
        ns = f"{db_name or settings.mongo_db}.users"
        configured = []
        for zone, (shard, countries) in self.ZONES.items():

            self.admin.command("addShardToZone", shard, zone=zone)
            for country in countries:
                self.admin.command(
                    "updateZoneKeyRange",
                    ns,
                    min={"address.country": country, "address.city": MinKey()},
                    max={"address.country": country, "address.city": MaxKey()},
                    zone=zone
                )
            configured.append(zone)
        return configured

    def _collection_uuid(self, ns: str) -> Any:
        """У MongoDB 5.0+ config.chunks посилається на колекцію за uuid, а не за ns."""
        coll = self.config.collections.find_one({"_id": ns})
        if not coll:
            raise ValueError(f"Колекція {ns} не шардована")
        return coll["uuid"]

    def get_chunks(self, ns: str) -> List[Dict]:
        """Чанки колекції (замість неіснуючої команди listChunks)."""
        return list(self.config.chunks.find({"uuid": self._collection_uuid(ns)}).sort("min", 1))

    def get_shard_statistics(self, db_name: Optional[str] = None,
                             collection: str = "events") -> Dict[str, Dict[str, Any]]:
        """Статистика по шардах"""
        db_name = db_name or settings.mongo_db
        ns = f"{db_name}.{collection}"

        shards = self.admin.command("listShards")["shards"]
        chunk_counts = Counter(c["shard"] for c in self.get_chunks(ns))

        storage = {
            s["shard"]: s["storageStats"]
            for s in self.client[db_name][collection].aggregate(
                [{"$collStats": {"storageStats": {}}}]
            )
        }

        shard_stats = {}
        for shard in shards:
            shard_name = shard["_id"]
            stats = storage.get(shard_name, {})
            shard_stats[shard_name] = {
                "total_chunks": chunk_counts.get(shard_name, 0),
                "doc_count": stats.get("count", 0),
                "size_mb": stats.get("size", 0) / 1024 / 1024,
                "zones": shard.get("tags", []),
            }
        return shard_stats

    def get_balancer_status(self) -> Dict:
        """Стан балансувальника (balancerStatus)."""
        status = self.admin.command("balancerStatus")
        return {"mode": status.get("mode"), "in_balancer_round": status.get("inBalancerRound")}

    def balance_chunks_manually(self, db_name: Optional[str] = None,
                                collection: str = "events") -> Dict:
        """Ручне балансування чанків"""
        ns = f"{db_name or settings.mongo_db}.{collection}"

        chunks = self.get_chunks(ns)
        shards = [s["_id"] for s in self.admin.command("listShards")["shards"]]
        counts = {s: 0 for s in shards}
        counts.update(Counter(c["shard"] for c in chunks))

        source = max(counts, key=counts.get)

        target = min((s for s in counts if s != source), key=counts.get, default=source)
        chunk = next(c for c in chunks if c["shard"] == source)

        self.admin.command("split", ns, bounds=[chunk["min"], chunk["max"]])

        new_chunk = self.get_chunks(ns)
        moved = next(c for c in new_chunk if c["shard"] == source and c["min"] == chunk["min"])
        if source != target:
            self.admin.command(
                "moveChunk", ns,
                bounds=[moved["min"], moved["max"]],
                to=target,
                _waitForDelete=True
            )
        logger.info(f"Split chunk {chunk['min']}..{chunk['max']} and moved half {source} -> {target}")
        return {
            "split": [chunk["min"], chunk["max"]],
            "moved_from": source,
            "moved_to": target,
            "chunks_before": dict(counts),
            "chunks_after": dict(Counter(c["shard"] for c in self.get_chunks(ns))),
        }


# ---------- Вибір ключа шардінгу ----------

class ShardKeyAnalyzer:
    """Оцінка кандидатів у ключ шардінгу за даними (розділ 8.2)."""

    def __init__(self, db: Database) -> None:
        """db — база з даними для аналізу."""
        self.db = db

    def analyze_shard_key_candidates(self) -> Dict:
        """Аналіз кандидатів на ключ шардінгу"""

        candidates = [
            "_id",
            "category",
            "location.city",
            "organizer.user_id",
            "schedule.start_date"
        ]

        results = {}

        for key in candidates:
            pipeline = [
                {"$group": {
                    "_id": f"${key}",
                    "count": {"$sum": 1}
                }},
                {"$group": {
                    "_id": None,
                    "unique_values": {"$sum": 1},
                    "max_count": {"$max": "$count"},
                    "avg_count": {"$avg": "$count"}
                }}
            ]

            stats = list(self.db.events.aggregate(pipeline))

            if stats:
                stats = stats[0]
                distribution = stats["max_count"] / stats["avg_count"] if stats["avg_count"] > 0 else 0

                results[key] = {
                    "unique_values": stats["unique_values"],
                    "max_count": stats["max_count"],
                    "avg_count": stats["avg_count"],
                    "distribution_ratio": distribution,
                    "is_good_candidate": distribution < 5
                }

        return results

    def recommend_shard_key(self) -> Dict:
        """Рекомендувати найкращий ключ шардінгу"""

        analysis = self.analyze_shard_key_candidates()
        if not analysis:
            return {"recommended_key": None, "reason": "Collection is empty", "details": {}}

        best_key = min(
            analysis.items(),
            key=lambda x: x[1]["distribution_ratio"]
        )

        return {
            "recommended_key": best_key[0],
            "reason": f"Distribution ratio: {best_key[1]['distribution_ratio']:.2f} (lowest)",
            "details": best_key[1]
        }


# ---------- Міграція даних між шардами ----------

class DataMigrationManager:
    """Перенесення даних з replica set у шардований кластер (розділ 8.3)."""

    def __init__(self, source_client: MongoClient, target_client: MongoClient) -> None:
        """source_client — звідки копіюємо; target_client — mongos."""
        self.source = source_client
        self.target = target_client

    def migrate_collection(self, db_name: str, collection: str,
                           batch_size: int = 1000,
                           target_db_name: Optional[str] = None) -> int:
        """Міграція даних пачками. Повертає кількість оброблених документів."""

        source_col = self.source[db_name][collection]
        target_col = self.target[target_db_name or db_name][collection]

        total_docs = source_col.count_documents({})
        logger.info(f"Migrating {total_docs} documents from {db_name}.{collection}")

        processed = 0
        last_id = None

        while processed < total_docs:
            query = {"_id": {"$gt": last_id}} if last_id else {}
            batch = list(source_col.find(query).sort("_id").limit(batch_size))

            if not batch:
                break

            try:
                target_col.insert_many(batch, ordered=False)
            except BulkWriteError:
                logger.info("Duplicates in batch, continuing...")

            processed += len(batch)
            last_id = batch[-1]["_id"]
            logger.info(f"Processed: {processed}/{total_docs} ({processed / total_docs * 100:.1f}%)")

        logger.info("Migration completed!")
        return processed

    def verify_migration(self, db_name: str, collection: str,
                         target_db_name: Optional[str] = None) -> Dict:
        """Верифікація міграції"""

        source_count = self.source[db_name][collection].count_documents({})
        target_count = self.target[target_db_name or db_name][collection].count_documents({})

        logger.info(f"Source: {source_count}, target: {target_count}, "
                    f"difference: {abs(source_count - target_count)}")

        return {
            "source_count": source_count,
            "target_count": target_count,
            "verified": source_count == target_count
        }
