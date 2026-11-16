"""Агрегації та матеріалізовані представлення"""
from datetime import datetime, timedelta, timezone

import pytest
from bson import ObjectId

from app.analytics import AnalyticsAggregator, MaterializedViewManager
from app.seed import seed


@pytest.fixture(scope="module")
def analytics(db, seeded):
    return AnalyticsAggregator(db)


def test_seed_is_idempotent(ops, seeded):
    again = seed(ops=ops)
    assert again["created_registrations"] == 0
    assert again["created_reviews"] == 0
    assert again["users"] == 50 and again["events"] == 20


def test_event_statistics(analytics, db):
    event = db.events.find_one({"metadata.seed_key": "seed-event-00"})
    stats = analytics.get_event_statistics(event["_id"])

    assert stats["participant_count"] == len(event["participants"])
    assert stats["registration_statuses"].get("confirmed") == len(event["participants"])
    assert stats["total_reviews"] == event["statistics"]["total_reviews"]
    assert stats["organizer"]["email"].startswith("seed_user_")
    assert analytics.get_event_statistics(ObjectId()) is None


def test_category_analytics(analytics):
    now = datetime.now(timezone.utc)
    result = analytics.get_category_analytics(now - timedelta(days=365), now)
    assert result, "у seed є завершені події"
    scores = [c["popularity_score"] for c in result]
    assert scores == sorted(scores, reverse=True)
    for c in result:
        assert 0 < c["avg_capacity_utilization"] <= 1
        assert c["avg_participants_per_event"] == c["total_participants"] / c["event_count"]


def test_user_engagement_report(analytics, db):
    user = db.users.find_one({"email": "seed_user_00@eventhub.test"})
    report = analytics.get_user_engagement_report(user["_id"])

    assert report["registration_stats"].get("confirmed", 0) >= 1
    assert len(report["organized_events"]) >= 1
    if report["review_stats"]:
        # у PDF $project губив ці поля
        assert "avg_rating" in report["review_stats"]
        assert "total_reviews" in report["review_stats"]
        assert len(report["review_stats"]["recent_comments"]) <= 5
    assert analytics.get_user_engagement_report(ObjectId()) is None


def test_recommendations(analytics, db):
    user = db.users.find_one({"email": "seed_user_00@eventhub.test"})
    recs = analytics.get_recommendations(user["_id"], limit=5)

    assert 0 < len(recs) <= 5
    scores = [r["recommendation_score"] for r in recs]
    assert scores == sorted(scores, reverse=True)
    for r in recs:
        event = db.events.find_one({"_id": r["_id"]})
        assert event["status"] == "published"
        assert user["_id"] not in event["participants"]
    assert analytics.get_recommendations(ObjectId()) == []


def test_materialized_views(db, seeded):
    views = MaterializedViewManager(db)
    assert views.refresh_daily_stats_view(days=1) == 0  # як у PDF: лише останній день
    assert views.refresh_daily_stats_view(days=120) > 0
    row = db.daily_stats_materialized.find_one()
    assert {"date", "category", "events_count", "total_participants", "total_revenue"} <= set(row)

    assert views.refresh_venue_performance_view() > 0
    venues = list(db.venue_performance_view.find())
    scores = [v["performance_score"] for v in venues]
    assert scores == sorted(scores, reverse=True)
