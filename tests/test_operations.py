"""Валідатори, CRUD, пошук, відгуки, індекси та оптимізатор."""
from datetime import datetime, timedelta, timezone

import pytest
from bson import ObjectId
from pymongo.errors import WriteError

from app.operations import ConcurrentModificationError
from app.optimizer import IndexOptimizer, _plan_has_stage
from app.schemas import VALIDATORS, apply_validators


# ---------- $jsonSchema-валідатори ----------

def test_validators_installed(db):
    for name in VALIDATORS:
        options = db[name].options()
        assert "$jsonSchema" in options["validator"], name
        assert options["validationLevel"] == "moderate"


def test_apply_validators_is_idempotent(db):
    apply_validators(db)
    apply_validators(db)
    assert "$jsonSchema" in db.users.options()["validator"]


def test_validator_rejects_invalid_user(db):
    with pytest.raises(WriteError):
        db.users.insert_one({"email": "not-an-email", "status": "active", "version": 1,
                             "created_at": datetime.now(timezone.utc)})


def test_validator_rejects_bad_review_rating(db, make_user, make_event):
    with pytest.raises(WriteError):
        db.reviews.insert_one({"event_id": make_event(), "user_id": make_user(), "rating": 7,
                               "status": "approved", "created_at": datetime.now(timezone.utc)})


def test_validator_rejects_unknown_category(ops, make_user):
    with pytest.raises(WriteError):
        ops.create_event({"title": "Bad", "category": "party",
                          "schedule": {"start_date": datetime.now(timezone.utc)},
                          "capacity": {"total": 1}, "organizer": {"user_id": make_user()}})


# ---------- користувачі ----------

def test_get_user_by_id_with_stats(ops, make_user, make_event):
    user_id = make_user()
    event_id = make_event()
    ops.register_user_for_event(user_id, event_id)
    ops.create_review(user_id, event_id, 5, "Great")

    user = ops.get_user_by_id(user_id)
    assert user["stats"] == {"registrations": 1, "avg_review_rating": 5, "total_reviews": 1}
    assert "registration_stats" not in user


def test_get_user_by_id_missing(ops):
    assert ops.get_user_by_id(ObjectId()) is None


def test_update_user_optimistic_locking(ops, make_user):
    user_id = make_user()
    assert ops.update_user(user_id, {"profile.bio": "updated"})
    user = ops.users.find_one({"_id": user_id})
    assert user["version"] == 2
    assert user["profile"]["bio"] == "updated"


def test_update_user_conflict(ops, make_user, monkeypatch):
    user_id = make_user()
    original_find_one = ops.users.find_one

    def stale_find_one(query, *args, **kwargs):
        doc = original_find_one(query, *args, **kwargs)

        ops.users.update_one({"_id": user_id}, {"$inc": {"version": 1}})
        return doc

    monkeypatch.setattr(ops.users, "find_one", stale_find_one)
    with pytest.raises(ConcurrentModificationError):
        ops.update_user(user_id, {"profile.bio": "lost update"})


def test_update_user_not_found(ops):
    with pytest.raises(ValueError):
        ops.update_user(ObjectId(), {"profile.bio": "x"})


# ---------- події та реєстрації ----------

def test_create_event_defaults_and_audit(ops, make_user, make_event):
    organizer = make_user()
    event_id = make_event(capacity=5, organizer=organizer)
    event = ops.events.find_one({"_id": event_id})
    assert event["status"] == "published"
    assert event["capacity"] == {"total": 5, "remaining": 5}
    assert event["participants"] == []
    assert ops.users.find_one({"_id": organizer})["statistics"]["events_organized"] == 1
    assert ops.audit_log.find_one({"action": "event_created", "resource_id": event_id})


def test_register_updates_everything(ops, make_user, make_event):
    user_id = make_user()
    event_id = make_event(capacity=3)
    reg_id = ops.register_user_for_event(user_id, event_id)

    reg = ops.registrations.find_one({"_id": reg_id})
    assert reg["status"] == "confirmed"
    event = ops.events.find_one({"_id": event_id})
    assert event["capacity"]["remaining"] == 2
    assert event["statistics"]["total_participants"] == 1
    assert ops.users.find_one({"_id": user_id})["statistics"]["events_attended"] == 1
    assert ops.audit_log.find_one({"action": "user_registered", "resource_id": reg_id})


def test_register_errors(ops, make_user, make_event):
    with pytest.raises(ValueError, match="Event not found"):
        ops.register_user_for_event(make_user(), ObjectId())

    cancelled = make_event(status="cancelled")
    with pytest.raises(ValueError, match="cancelled"):
        ops.register_user_for_event(make_user(), cancelled)


def test_register_is_rolled_back_on_error(ops, make_user, make_event):
    """Якщо подія повна, транзакція відкочується повністю."""
    event_id = make_event(capacity=1)
    ops.register_user_for_event(make_user(), event_id)
    late = make_user()
    with pytest.raises(ValueError, match="full"):
        ops.register_user_for_event(late, event_id)
    assert ops.registrations.count_documents({"user_id": late}) == 0
    assert ops.users.find_one({"_id": late})["statistics"]["events_attended"] == 0


# ---------- Відгуки ----------

def test_create_review_updates_rating(ops, make_user, make_event):
    event_id = make_event()
    u1, u2 = make_user(), make_user()
    for u in (u1, u2):
        ops.register_user_for_event(u, event_id)
    ops.create_review(u1, event_id, 5, "Excellent", pros=["Speakers"])
    ops.create_review(u2, event_id, 4)

    stats = ops.events.find_one({"_id": event_id})["statistics"]
    assert stats["average_rating"] == 4.5
    assert stats["total_reviews"] == 2
    user_stats = ops.users.find_one({"_id": u1})["statistics"]
    assert user_stats["total_ratings_given"] == 1
    assert user_stats["average_rating_given"] == 5
    reg = ops.registrations.find_one({"event_id": event_id, "user_id": u1})
    assert reg["attendance"]["feedback_submitted"] is True


def test_create_review_rules(ops, make_user, make_event):
    event_id = make_event()
    stranger = make_user()
    with pytest.raises(ValueError, match="confirmed participants"):
        ops.create_review(stranger, event_id, 4)

    user = make_user()
    ops.register_user_for_event(user, event_id)
    ops.create_review(user, event_id, 3)
    with pytest.raises(ValueError, match="already reviewed"):
        ops.create_review(user, event_id, 4)

    for bad in (0, 6, 4.5, True):
        with pytest.raises(ValueError, match="Rating"):
            ops.create_review(user, event_id, bad)


# ---------- Пошук ----------

def test_search_events_text(ops, seeded):
    results = ops.search_events("Summit")
    assert results
    assert all("Summit" in e["title"] for e in results[:3])


def test_search_events_filters(ops, seeded):
    now = datetime.now(timezone.utc)
    results = ops.search_events("", category="meetup", start_date=now,
                                end_date=now + timedelta(days=365), min_rating=None)
    assert all(e["category"] == "meetup" and e["schedule"]["start_date"] >= now for e in results)
    dates = [e["schedule"]["start_date"] for e in results]
    assert dates == sorted(dates)


def test_search_events_min_rating(ops, seeded):
    results = ops.search_events("", min_rating=4.0)
    assert results
    assert all(e["statistics"]["average_rating"] >= 4.0 for e in results)


def test_search_events_geo_near(ops, seeded):
    kyiv = [30.5234, 50.4501]
    results = ops.search_events("", coordinates=kyiv, max_distance=50)
    assert results
    assert all(e["location"]["city"] == "Kyiv" for e in results)


def test_search_events_text_and_geo(ops, seeded):
    """$text + гео разом: використовується $geoWithin (з $near MongoDB видає помилку)."""
    lviv = [24.0297, 49.8397]
    results = ops.search_events("Seed", coordinates=lviv, max_distance=20)
    assert all(e["location"]["city"] == "Lviv" for e in results)


# ---------- Оптимізатор ----------

@pytest.fixture(scope="module")
def optimizer(db):
    opt = IndexOptimizer(db)
    opt.create_compound_indexes()
    opt.create_special_indexes()
    return opt


def test_indexes_created(db, optimizer):
    names = {i["name"] for i in db.events.list_indexes()}
    assert {"category_rating_date_index", "active_events_date_index",
            "metadata_wildcard_index", "geo_category_date_index"} <= names
    assert "active_users_email_unique" in {i["name"] for i in db.users.list_indexes()}


def test_analyze_query_performance(optimizer, seeded):
    explain, hinted = optimizer.analyze_query_performance()
    assert "executionStats" in explain
    assert not _plan_has_stage(explain["queryPlanner"]["winningPlan"], "COLLSCAN")
    assert _plan_has_stage(hinted["queryPlanner"]["winningPlan"], "IXSCAN")


def test_index_coverage(optimizer, seeded):
    result = optimizer.test_index_coverage()
    # title немає в індексі разом з category — запит не покритий
    assert result["executionStats"]["totalDocsExamined"] > 0


def test_analyze_query_patterns(optimizer, db, monkeypatch):
    fake_profile = [{"command": {"filter": {"category": "meetup", "status": "x"}}}] * 11

    class FakeCursor(list):
        def sort(self, *a, **k):
            return self

        def limit(self, *a, **k):
            return self

    monkeypatch.setattr(type(db.system.profile), "find",
                        lambda self, *a, **k: FakeCursor(fake_profile))
    recs = optimizer.analyze_query_patterns()
    assert recs[0]["frequency"] == 11
    assert recs[0]["recommended_index"]["fields"] == [("category", 1), ("status", 1)]
    db.command("profile", 0)  # вимкнути профайлер після тесту


def test_suggest_index_handles_garbage(optimizer):
    assert optimizer._suggest_index("not a dict") == {"fields": []}
    assert optimizer._suggest_index("{'$or': 1, 'a': 1}") == {"fields": [("a", 1)]}
