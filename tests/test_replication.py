"""Реплікація, read/write concerns, вибори"""
import subprocess
import time

import pytest

from app.config import settings
from app.replication import ReadConsistencyManager, ReplicaSetManager, WriteConcernManager

PRIMARY = "mongo-primary:27017"


@pytest.fixture(scope="module")
def rs(admin_client):
    return ReplicaSetManager(admin_client)


def test_replica_set_status(rs):
    status = rs.get_replica_set_status()
    assert status["set"] == "rs0"
    assert len(status["members"]) == 3
    assert rs.get_primary() == PRIMARY


def test_initiate_is_safe_when_already_initialized(rs):
    assert rs.initiate_replica_set([{"_id": 0, "host": PRIMARY}]) == {
        "initiated": False, "reason": "already initialized"}


def test_secondary_lag(rs):
    lag = rs.check_secondary_lag()
    assert lag["primary"] == PRIMARY
    assert lag["total_members"] == 3
    assert len(lag["secondary_lags"]) == 2
    assert all(m["lag_seconds"] is not None and m["lag_seconds"] >= 0 for m in lag["secondary_lags"])


def test_force_election_requires_member(rs):
    with pytest.raises(ValueError):
        rs.force_election("unknown-host:1")


def test_read_with_consistency(client, seeded):
    rc = ReadConsistencyManager(client, settings.mongo_test_db)
    for level in ("eventual", "consistent", "linearizable", "other"):
        docs = rc.read_with_consistency("events", {"status": "published"}, level)
        assert isinstance(docs, list) and docs
    bench = rc.benchmark_consistency({"status": "published"})
    assert set(bench) == {"eventual", "consistent", "linearizable"}
    assert all(v["doc_count"] > 0 for v in bench.values())
    # запит за замовчуванням з PDF (status "active") — подій з таким статусом немає
    assert all(v["doc_count"] == 0 for v in rc.benchmark_consistency().values())


def test_write_with_concern(client):
    wc = WriteConcernManager(client, settings.mongo_test_db)
    for level in ("low", "medium", "majority", "high", "durable", "unknown"):
        result = wc.write_with_concern("write_concern_test", {"level": level}, level)
        assert result["success"], result


def test_write_with_impossible_concern(client):
    wc = WriteConcernManager(client, settings.mongo_test_db)
    from pymongo.write_concern import WriteConcern
    wc.CONCERNS = {**wc.CONCERNS, "impossible": WriteConcern(w=5, wtimeout=500)}
    result = wc.write_with_concern("write_concern_test", {"x": 1}, "impossible")
    assert result["success"] is False


def test_compare_write_performance(client):
    wc = WriteConcernManager(client, settings.mongo_test_db)
    perf = wc.compare_write_performance(iterations=5, collection="write_concern_test")
    assert set(perf) == {"low", "medium", "majority", "high"}
    assert all(v["min_ms"] <= v["avg_ms"] <= v["max_ms"] for v in perf.values())


@pytest.mark.slow
def test_force_election_roundtrip(rs):
    """Переносимо primary на secondary1 і повертаємо назад (пріоритети відновлюються)."""
    try:
        assert rs.force_election("mongo-secondary1:27018") == "mongo-secondary1:27018"
    finally:
        assert rs.force_election(PRIMARY) == PRIMARY
    priorities = {m["host"]: m["priority"]
                  for m in rs.db.command("replSetGetConfig")["config"]["members"]}
    assert priorities[PRIMARY] == 2


@pytest.mark.failover
def test_failover_container_stop(rs, client):
    """13.3 test_failover: справжня зупинка контейнера primary (у PDF лише sleep)."""
    assert rs.get_primary() == PRIMARY
    subprocess.run(["docker", "stop", "mongo-primary"], check=True, capture_output=True)
    try:
        new_primary = rs.wait_for_primary(timeout=60)
        assert new_primary != PRIMARY

        assert client[settings.mongo_test_db].failover_check.insert_one({"t": time.time()}).acknowledged
    finally:
        subprocess.run(["docker", "start", "mongo-primary"], check=True, capture_output=True)
    assert rs.wait_for_primary(PRIMARY, timeout=120) == PRIMARY
