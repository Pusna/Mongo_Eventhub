"""13.2 Performance Testing і 13.3 Integration Testing"""
import time
import uuid
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List

import pytest

from app.config import settings


# ---------- 13.2. Performance Testing ----------

class PerformanceTest:
    def __init__(self, repo):
        self.repo = repo

    def run_load_test(self, n_users: int = 1000, n_events: int = 100,
                      registrations_per_user: int = 5) -> Dict:
        """Навантажувальний тест.

        У PDF результати збирались через f.result(): перша ж помилка (дубль реєстрації —
        пари user/event повторюються) зупиняла тест. Тут кожен результат —
        успіх або помилка, і повертається статистика.
        """
        tag = uuid.uuid4().hex[:8]
        users = [self.repo.create_user({"email": f"load_{tag}_{i}@test.com",
                                        "profile": {"name": f"User {i}"}})
                 for i in range(n_users)]
        events = [self.repo.create_event({
            "title": f"Load Event {tag} {i}",
            "category": "webinar",
            "schedule": {"start_date": datetime.now(timezone.utc)},
            "capacity": {"total": 100},
            "organizer": {"user_id": users[i % len(users)]}
        }) for i in range(n_events)]

        def register(user_id, event_id):
            try:
                self.repo.register_user_for_event(user_id, event_id)
                return True
            except ValueError:
                return False

        start = time.time()
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(register, users[i % len(users)], events[i % len(events)])
                       for i in range(n_users * registrations_per_user)]
            results: List[bool] = [f.result() for f in futures]
        elapsed = time.time() - start

        return {"attempts": len(results), "successful": sum(results),
                "elapsed_s": elapsed, "throughput_ops": len(results) / elapsed,
                "events": events}


@pytest.mark.slow
def test_load(ops):
    result = PerformanceTest(ops).run_load_test(n_users=20, n_events=5, registrations_per_user=3)
    # 60 спроб, але унікальних пар (user, event) лише 20 → решта — відмова «already registered»
    assert result["attempts"] == 60
    assert result["successful"] == 20
    assert result["throughput_ops"] > 1
    for event_id in result["events"]:
        event = ops.events.find_one({"_id": event_id})
        assert len(event["participants"]) + event["capacity"]["remaining"] == 100


# ---------- 13.3. Integration Testing ----------

def test_transaction_consistency(client):
    """Дані в транзакції видно лише всередині сесії до коміту.

    У PDF перевірка робилась без session і очікувала, що незакомічені дані видно —
    це суперечить ізоляції транзакцій (snapshot), такий тест завжди падав би.
    """
    db = client[settings.mongo_test_db]
    marker = uuid.uuid4().hex
    with client.start_session() as session:
        session.start_transaction()
        db.integration_a.insert_one({"test": marker}, session=session)
        db.integration_b.insert_one({"test": marker}, session=session)

        assert db.integration_a.find_one({"test": marker}, session=session) is not None
        assert db.integration_a.find_one({"test": marker}) is None  # поза транзакцією не видно

        session.commit_transaction()

    assert db.integration_a.find_one({"test": marker}) is not None
    assert db.integration_b.find_one({"test": marker}) is not None


def test_rollback(client):
    db = client[settings.mongo_test_db]
    marker = uuid.uuid4().hex
    with client.start_session() as session:
        session.start_transaction()
        db.integration_a.insert_one({"test": marker}, session=session)
        try:
            raise ValueError("Test error")
        except ValueError:
            session.abort_transaction()

    assert db.integration_a.find_one({"test": marker}) is None
