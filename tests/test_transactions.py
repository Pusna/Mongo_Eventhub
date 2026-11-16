"""Розділ 5: транзакції та конкурентність."""
import pytest
from bson import ObjectId

from app.config import settings
from app.transactions import (OptimisticLockingMixin, TransactionManager,
                              VersionConflictError, concurrent_registration_test)


@pytest.fixture
def tm(client):
    return TransactionManager(client, settings.mongo_test_db)


def test_transfer_event_organizer_restores_status(ops, tm, make_user, make_event):
    old, new = make_user(), make_user()
    event_id = make_event(organizer=old)

    assert tm.transfer_event_organizer(event_id, old, new) is True

    event = ops.events.find_one({"_id": event_id})
    assert event["organizer"]["user_id"] == new
    assert event["status"] == "published"
    assert ops.users.find_one({"_id": old})["statistics"]["events_organized"] == 0
    assert ops.users.find_one({"_id": new})["statistics"]["events_organized"] == 1
    assert ops.audit_log.find_one({"action": "organizer_transferred", "resource_id": event_id})


def test_transfer_event_organizer_errors_roll_back(ops, tm, make_user, make_event):
    old, new, other = make_user(), make_user(), make_user()
    event_id = make_event(organizer=old)

    with pytest.raises(ValueError, match="User not found"):
        tm.transfer_event_organizer(event_id, old, ObjectId())
    with pytest.raises(ValueError, match="Event not found"):
        tm.transfer_event_organizer(ObjectId(), old, new)
    with pytest.raises(ValueError, match="does not match"):
        tm.transfer_event_organizer(event_id, other, new)

    event = ops.events.find_one({"_id": event_id})
    assert event["status"] == "published"
    assert event["organizer"]["user_id"] == old


def test_batch_cancel_registrations(ops, tm, make_user, make_event):
    event_id = make_event(capacity=5)
    users = [make_user() for _ in range(3)]
    for u in users:
        ops.register_user_for_event(u, event_id)

    assert tm.batch_cancel_registrations(event_id, users[:2]) == 2
    assert tm.batch_cancel_registrations(event_id, users[:2]) == 0

    event = ops.events.find_one({"_id": event_id})
    assert event["capacity"]["remaining"] == 4
    assert event["participants"] == [users[2]]
    assert ops.registrations.count_documents({"event_id": event_id, "status": "cancelled"}) == 2


def test_optimistic_locking(ops, make_user):
    mixin = OptimisticLockingMixin()
    user_id = make_user()
    assert mixin.update_with_version(ops.users, user_id, {"profile.bio": "v2"}, 1)
    with pytest.raises(VersionConflictError):
        mixin.update_with_version(ops.users, user_id, {"profile.bio": "stale"}, 1)


def test_retry_with_backoff(ops, make_user):
    mixin = OptimisticLockingMixin()
    user_id = make_user()
    attempts = []

    def update():
        attempts.append(1)
        version = ops.users.find_one({"_id": user_id})["version"]
        expected = version - 1 if len(attempts) == 1 else version
        return mixin.update_with_version(ops.users, user_id, {"profile.bio": "ok"}, expected)

    assert mixin.retry_with_backoff(update) is True
    assert len(attempts) == 2


def test_retry_with_backoff_gives_up():
    mixin = OptimisticLockingMixin()

    def always_conflict():
        raise VersionConflictError("conflict")

    with pytest.raises(VersionConflictError):
        mixin.retry_with_backoff(always_conflict, max_retries=2)


def test_concurrent_registrations_do_not_oversell(ops, make_user, make_event):
    """15 одночасних реєстрацій на 10 місць - рівно 10 успішних"""
    event_id = make_event(capacity=10)
    users = [make_user() for _ in range(15)]

    results = concurrent_registration_test(ops, event_id, users)

    assert sum(r["success"] for r in results) == 10
    event = ops.events.find_one({"_id": event_id})
    assert event["capacity"]["remaining"] == 0
    assert len(event["participants"]) == 10
    assert ops.registrations.count_documents({"event_id": event_id}) == 10
