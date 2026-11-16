"""FastAPI - запити через TestClient до тестової бази."""
import io
import time
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from bson import ObjectId
from fastapi.testclient import TestClient
from PIL import Image

from app import main
from app.config import settings
from app.main import Services, get_services, to_json


@pytest.fixture(scope="module")
def services(seeded):
    return Services(settings.mongo_test_db)


@pytest.fixture(scope="module")
def api(services):
    main.settings.api_change_streams = True
    main.app.dependency_overrides[get_services] = lambda: services

    original = main.get_services
    main.get_services = lambda: services
    with TestClient(main.app) as client:
        yield client
    main.get_services = original
    main.app.dependency_overrides.clear()
    main.settings.api_change_streams = False


def new_user(api) -> str:
    r = api.post("/users", json={"email": f"api_{uuid.uuid4().hex[:8]}@eventhub-api.io",
                                 "profile": {"name": "API"}, "interests": ["ai"]})
    assert r.status_code == 201, r.text
    return r.json()["id"]


def new_event(api, organizer_id: str, capacity: int = 5) -> str:
    start = (datetime.now(timezone.utc) + timedelta(days=10)).isoformat()
    r = api.post("/events", json={
        "title": "API Conference", "description": "Created via API", "category": "conference",
        "schedule": {"start_date": start}, "location": {"city": "Kyiv", "coordinates": [30.52, 50.45]},
        "capacity": {"total": capacity}, "organizer": {"user_id": organizer_id}, "tags": ["ai"]})
    assert r.status_code == 201, r.text
    return r.json()["id"]


def test_root_and_health(api):
    assert api.get("/").json()["message"] == "EventHub Pro API"
    assert api.get("/health").json() == {"status": "ok"}


def test_user_lifecycle(api):
    user_id = new_user(api)
    r = api.get(f"/users/{user_id}")
    assert r.status_code == 200
    body = r.json()
    assert body["_id"] == user_id and body["stats"]["registrations"] == 0

    assert api.get(f"/users/{ObjectId()}").status_code == 404
    assert api.get("/users/not-an-id").status_code == 400


def test_duplicate_user_is_400(api):
    email = f"dup_{uuid.uuid4().hex[:8]}@eventhub-api.io"
    payload = {"email": email, "profile": {"name": "Dup"}}
    assert api.post("/users", json=payload).status_code == 201
    assert api.post("/users", json=payload).status_code == 400
    assert api.post("/users", json={"email": "bad", "profile": {}}).status_code == 422


def test_event_registration_review_flow(api, services):
    organizer, user = new_user(api), new_user(api)
    event_id = new_event(api, organizer, capacity=1)

    r = api.post("/registrations", json={"user_id": user, "event_id": event_id})
    assert r.status_code == 201, r.text

    assert services.repo.db.notifications.find_one(
        {"user_id": ObjectId(user), "template": "registration_confirmation"})


    other = new_user(api)
    r = api.post("/registrations", json={"user_id": other, "event_id": event_id})
    assert r.status_code == 400 and "full" in r.json()["detail"]

    r = api.post("/reviews", json={"user_id": user, "event_id": event_id, "rating": 5, "comment": "Top"})
    assert r.status_code == 201, r.text
    assert api.post("/reviews", json={"user_id": user, "event_id": event_id, "rating": 9,
                                      "comment": "x"}).status_code == 422

    stats = api.get(f"/events/{event_id}/statistics").json()
    assert stats["participant_count"] == 1
    assert stats["average_rating"] == 5
    assert api.get(f"/events/{ObjectId()}/statistics").status_code == 404


def test_invalid_event_is_400(api):
    r = api.post("/events", json={"title": "x", "description": "x", "category": "party",
                                  "schedule": {"start_date": "2030-01-01T00:00:00+00:00"},
                                  "location": {}, "capacity": {"total": 1},
                                  "organizer": {"user_id": str(ObjectId())}})
    assert r.status_code == 400


def test_search(api):
    r = api.get("/events/search", params={"q": "Summit"})
    assert r.status_code == 200 and r.json()["count"] > 0
    r = api.get("/events/search", params={"lng": 30.5234, "lat": 50.4501, "max_distance_km": 30})
    assert all(e["location"]["city"] == "Kyiv" for e in r.json()["results"])


def test_analytics_endpoints(api, services):
    r = api.get("/analytics/categories", params={"days": 365})
    assert r.status_code == 200 and r.json()["data"]
    user = services.repo.users.find_one({"email": "seed_user_00@eventhub.test"})
    r = api.get(f"/analytics/recommendations/{user['_id']}", params={"limit": 3})
    assert r.status_code == 200 and len(r.json()["recommendations"]) <= 3


def test_files_upload_and_stream(api):
    buf = io.BytesIO()
    Image.new("RGB", (640, 480), (uuid.uuid4().int % 255, 10, 10)).save(buf, "PNG")
    r = api.post("/files/images", files={"file": ("photo.png", buf.getvalue(), "image/png")})
    assert r.status_code == 201, r.text
    ids = r.json()

    thumb = api.get(f"/files/{ids['thumbnail_id']}")
    assert thumb.status_code == 200 and thumb.headers["content-type"] == "image/jpeg"
    assert Image.open(io.BytesIO(thumb.content)).size == (200, 150)
    assert api.get(f"/files/{ObjectId()}").status_code == 404

    bad = api.post("/files/images", files={"file": ("x.png", b"not an image", "image/png")})
    assert bad.status_code == 400


def test_background_change_stream_processes_registration(api, services):
    """Фоновий потік з lifespan обробляє нові реєстрації (event_cache)."""
    organizer, user = new_user(api), new_user(api)
    event_id = new_event(api, organizer)
    time.sleep(1)
    api.post("/registrations", json={"user_id": user, "event_id": event_id})
    for _ in range(40):
        if services.repo.db.event_cache.find_one({"_id": ObjectId(event_id)}):
            break
        time.sleep(0.25)
    assert services.repo.db.event_cache.find_one({"_id": ObjectId(event_id)})


def test_to_json_helper():
    oid = ObjectId()
    now = datetime.now(timezone.utc)
    assert to_json({"a": [oid, (now,)], "b": 1}) == {"a": [str(oid), [now.isoformat()]], "b": 1}
