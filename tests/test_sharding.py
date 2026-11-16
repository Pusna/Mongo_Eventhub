"""Шардінг. Потрібен профіль sharding, інакше тести пропускаються."""
import uuid

import pytest

from app.config import settings
from app.sharding import DataMigrationManager, ShardingManager, ShardKeyAnalyzer
from app.sharding_setup import setup_sharding

pytestmark = pytest.mark.sharding

TEST_DB = settings.mongo_test_db


@pytest.fixture(scope="module")
def sm(mongos_client):
    return ShardingManager(mongos_client)


@pytest.fixture(scope="module")
def sharded_events(sm, mongos_client):
    """events у тестовій базі шардованого кластера: hashed _id, 40 документів."""
    sm.enable_sharding_for_database(TEST_DB)
    sm.shard_collection(TEST_DB, "events", {"_id": "hashed"})
    coll = mongos_client[TEST_DB].events
    if coll.count_documents({}) < 40:
        coll.insert_many([{"title": f"sharded {i}", "category": "meetup"} for i in range(40)])
    return coll


def test_sharding_setup_is_rerunnable(mongos_client):
    setup_sharding()
    setup_sharding()
    ns = f"{settings.mongo_db}.users"
    assert mongos_client.config.collections.find_one({"_id": ns})["key"] == {
        "address.country": 1, "address.city": 1}


def test_shards_registered(mongos_client):
    shards = {s["_id"] for s in mongos_client.admin.command("listShards")["shards"]}
    assert shards == {"shard1ReplSet", "shard2ReplSet"}


def test_shard_statistics(sm, sharded_events):
    stats = sm.get_shard_statistics(TEST_DB, "events")
    assert set(stats) == {"shard1ReplSet", "shard2ReplSet"}
    assert sum(s["doc_count"] for s in stats.values()) >= 40
    assert all(s["total_chunks"] >= 1 for s in stats.values())
    # hashed-ключ розподіляє документи на обидва шарди
    assert all(s["doc_count"] > 0 for s in stats.values())


def test_get_chunks_for_unsharded_collection(sm):
    with pytest.raises(ValueError):
        sm.get_chunks(f"{TEST_DB}.not_sharded")


def test_balancer_status(sm):
    status = sm.get_balancer_status()
    assert status["mode"] in {"full", "off"}


def test_manage_zones(sm, mongos_client):
    sm.shard_collection(TEST_DB, "users", {"address.country": 1, "address.city": 1})
    assert sm.manage_zones(TEST_DB) == ["EU", "US"]
    zones = {s["_id"]: s.get("tags", []) for s in mongos_client.admin.command("listShards")["shards"]}
    assert "EU" in zones["shard1ReplSet"] and "US" in zones["shard2ReplSet"]
    ranges = list(mongos_client.config.tags.find({"ns": f"{TEST_DB}.users"}))
    assert {r["tag"] for r in ranges} == {"EU", "US"}


def test_shard_key_analyzer(db, seeded):
    analyzer = ShardKeyAnalyzer(db)
    analysis = analyzer.analyze_shard_key_candidates()
    assert set(analysis) == {"_id", "category", "location.city", "organizer.user_id",
                             "schedule.start_date"}
    assert analysis["_id"]["distribution_ratio"] == 1
    rec = analyzer.recommend_shard_key()
    assert rec["details"]["distribution_ratio"] == min(a["distribution_ratio"] for a in analysis.values())


def test_shard_key_analyzer_empty(db):
    empty = db.client[TEST_DB]["empty_for_analyzer"]
    analyzer = ShardKeyAnalyzer(type("FakeDb", (), {"events": empty})())
    assert analyzer.recommend_shard_key()["recommended_key"] is None


def test_data_migration(client, mongos_client):
    """8.3: перенесення колекції пачками з rs0 у шардований кластер."""
    name = f"migration_{uuid.uuid4().hex[:8]}"
    client[TEST_DB][name].insert_many([{"n": i} for i in range(25)])
    mig = DataMigrationManager(client, mongos_client)

    assert mig.migrate_collection(TEST_DB, name, batch_size=10) == 25
    assert mig.verify_migration(TEST_DB, name) == {"source_count": 25, "target_count": 25,
                                                   "verified": True}

    mig.migrate_collection(TEST_DB, name, batch_size=10)
    assert mig.verify_migration(TEST_DB, name)["verified"]
