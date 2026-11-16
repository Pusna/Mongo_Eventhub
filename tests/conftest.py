import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, Optional

import pytest
from bson import ObjectId
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from app.config import settings
from app.operations import EventHubOperations
from app.seed import seed

TEST_DB = settings.mongo_test_db


def pytest_collection_modifyitems(config, items):
    if os.environ.get("RUN_FAILOVER") == "1":
        return
    skip = pytest.mark.skip(reason="failover-тест зупиняє контейнер; запуск: RUN_FAILOVER=1 pytest -m failover")
    for item in items:
        if "failover" in item.keywords:
            item.add_marker(skip)


@pytest.fixture(scope="session")
def ops() -> EventHubOperations:
    """EventHubOperations для тестової бази (валідатори й індекси створюються тут)."""
    operations = EventHubOperations(db_name=TEST_DB)
    yield operations
    operations.client.close()


@pytest.fixture(scope="session")
def db(ops):
    return ops.db


@pytest.fixture(scope="session")
def client(ops) -> MongoClient:
    return ops.client


@pytest.fixture(scope="session")
def admin_client() -> MongoClient:
    c = MongoClient(settings.mongo_admin_uri, tz_aware=True, serverSelectionTimeoutMS=10000)
    yield c
    c.close()


@pytest.fixture(scope="session")
def mongos_client() -> MongoClient:
    c = MongoClient(settings.mongos_uri, tz_aware=True, serverSelectionTimeoutMS=3000)
    try:
        c.admin.command("ping")
    except PyMongoError:
        pytest.skip("mongos недоступний: docker compose --profile sharding up -d")
    yield c
    c.close()


@pytest.fixture(scope="session")
def seeded(ops) -> Dict[str, int]:
    """Тестові дані seed у тестовій базі (ідемпотентно)."""
    return seed(ops=ops)


@pytest.fixture
def uid() -> str:
    return uuid.uuid4().hex[:10]


@pytest.fixture
def make_user(ops, uid) -> Callable[..., ObjectId]:
    counter = {"n": 0}

    def _make(**extra) -> ObjectId:
        counter["n"] += 1
        data = {"email": f"pytest_{uid}_{counter['n']}@eventhub.test",
                "profile": {"name": f"Pytest {counter['n']}"},
                "interests": ["tech", "ai"]}
        data.update(extra)
        return ops.create_user(data)

    return _make


@pytest.fixture
def make_event(ops, uid, make_user) -> Callable[..., ObjectId]:
    def _make(capacity: int = 10, organizer: Optional[ObjectId] = None, **extra) -> ObjectId:
        data = {
            "title": f"Pytest Event {uid}",
            "description": "Event created by pytest",
            "category": "meetup",
            "tags": ["ai", "python"],
            "schedule": {"start_date": datetime.now(timezone.utc) + timedelta(days=7)},
            "location": {"type": "venue", "city": "Kyiv", "country": "Ukraine",
                         "coordinates": [30.5234, 50.4501]},
            "capacity": {"total": capacity},
            "organizer": {"user_id": organizer or make_user()},
        }
        data.update(extra)
        return ops.create_event(data)

    return _make
