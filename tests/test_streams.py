"""Розділ 9: Change Streams, Event Sourcing."""
import threading
import time
import uuid

import pytest
from pymongo.errors import PyMongoError

from app.config import settings
from app.streams import ChangeStreamManager, EventSourcingManager


def run_in_thread(target):
    result = {}
    thread = threading.Thread(target=lambda: result.setdefault("value", target()))
    thread.start()
    time.sleep(1.5)
    return thread, result


@pytest.fixture
def csm(client):
    return ChangeStreamManager(client, settings.mongo_test_db)


def test_watch_registrations_updates_cache(csm, ops, make_user, make_event):
    event_id = make_event(capacity=5)
    users = [make_user(), make_user()]

    thread, result = run_in_thread(
        lambda: csm.watch_registrations_and_update_cache(max_changes=2, timeout=20))
    for u in users:
        ops.register_user_for_event(u, event_id)
    thread.join()

    assert result["value"] == 2
    cache = ops.db.event_cache.find_one({"_id": event_id})
    assert cache["registrations_seen"] == 2
    assert set(cache["recent_participants"]) == set(users)
    assert ops.db.webhook_outbox.count_documents({"payload.event_id": event_id}) == 2
    assert ops.db.notifications.count_documents({"event_id": event_id}) == 2
    assert csm.load_resume_token("registrations") is not None


def test_update_event_is_seen_with_update_lookup(csm, ops, make_user, make_event):
    """update реєстрації (cancelled -> confirmed) теж приходить завдяки updateLookup."""
    event_id = make_event()
    user = make_user()
    reg_id = ops.register_user_for_event(user, event_id)
    ops.registrations.update_one({"_id": reg_id}, {"$set": {"status": "cancelled"}})

    thread, result = run_in_thread(
        lambda: csm.watch_registrations_and_update_cache(max_changes=1, timeout=20))
    ops.registrations.update_one({"_id": reg_id}, {"$set": {"status": "confirmed"}})
    thread.join()
    assert result["value"] == 1


def test_failed_change_is_saved(csm, ops, make_user, make_event, monkeypatch):
    event_id = make_event()

    def broken(*args):
        raise RuntimeError("cache is down")

    monkeypatch.setattr(csm, "update_redis_cache", broken)
    thread, result = run_in_thread(
        lambda: csm.watch_registrations_and_update_cache(max_changes=1, timeout=20))
    ops.register_user_for_event(make_user(), event_id)
    thread.join()
    assert result["value"] == 0
    failed = ops.db.failed_changes.find_one({"change.fullDocument.event_id": event_id})
    assert failed["error"] == "cache is down"


def test_resume_after_failure_continues_from_token(csm, ops, make_event):
    event_id = make_event()
    ops.db.stream_tokens.delete_one({"_id": "events"})

    thread, result = run_in_thread(lambda: csm.resume_after_failure(max_changes=1, timeout=20))
    ops.events.update_one({"_id": event_id}, {"$set": {"description": "first"}})
    thread.join()
    assert result["value"] == 1
    token = csm.load_resume_token("events")

    ops.events.update_one({"_id": event_id}, {"$set": {"description": "while down"}})
    seen = []
    assert csm.resume_after_failure(token, max_changes=1, timeout=20, handler=seen.append) == 1
    assert seen[0]["fullDocument"]["description"] == "while down"


def test_resume_after_failure_retries(csm, monkeypatch):
    calls = {"n": 0}

    def failing_watch(*args, **kwargs):
        calls["n"] += 1
        raise PyMongoError("stream broken")

    monkeypatch.setattr(type(csm.db.events), "watch", failing_watch)
    monkeypatch.setattr("app.streams.time.sleep", lambda s: None)
    with pytest.raises(PyMongoError):
        csm.resume_after_failure(max_retries=3)
    assert calls["n"] == 3


def test_watch_multiple_collections(csm, ops, make_user, make_event):
    user, event_id = make_user(), make_event()
    thread, result = run_in_thread(lambda: csm.watch_multiple_collections(max_changes=2, timeout=20))
    ops.users.update_one({"_id": user}, {"$set": {"profile.bio": "x"}})
    ops.events.update_one({"_id": event_id}, {"$set": {"description": "y"}})
    thread.join()
    assert result["value"]["users"] >= 1 and result["value"]["events"] >= 1


def test_watch_stops_by_event(csm):
    stop = threading.Event()
    stop.set()
    assert csm.watch_multiple_collections(stop=stop) == {"events": 0, "users": 0, "reviews": 0}


def test_event_sourcing_replay(db):
    es = EventSourcingManager(db)
    aggregate = f"event-{uuid.uuid4().hex[:8]}"
    es.append_event(aggregate, "event_created", {"title": "ES", "capacity_remaining": 10}, "u0")
    es.append_event(aggregate, "user_registered", {"user_id": "u1"}, "u1")
    es.append_event(aggregate, "review_added", {"rating": 4}, "u1")
    last = es.append_event(aggregate, "review_added", {"rating": 5}, "u2")
    es.append_event(aggregate, "unknown_type", {}, "u2")

    assert last["version"] == 4
    state = es.replay_events(aggregate)
    assert state["capacity_remaining"] == 9
    assert state["participants"] == ["u1"]
    assert state["average_rating"] == 4.5


def test_event_sourcing_concurrent_versions(db):
    es = EventSourcingManager(db)
    aggregate = f"event-{uuid.uuid4().hex[:8]}"
    threads = [threading.Thread(target=es.append_event,
                                args=(aggregate, "user_registered", {"user_id": f"u{i}"}, "x"))
               for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    versions = sorted(e["version"] for e in db.event_store.find({"aggregate_id": aggregate}))
    assert versions == [1, 2, 3, 4, 5]
