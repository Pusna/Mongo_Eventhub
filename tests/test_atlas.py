"""Atlas. Без справжнього Atlas перевіряємо визначення, пайплайни й HTTP-виклики з підміною мережі."""
import json

import pytest

from app.atlas import SEARCH_INDEX_NAME, AtlasManager, AtlasTriggers, search_index_definition


class FakeEvents:
    def __init__(self):
        self.created, self.pipelines = [], []

    def create_search_index(self, model):
        self.created.append(model)

    def aggregate(self, pipeline):
        self.pipelines.append(pipeline)
        return iter([{"title": "found", "score": 1.0}])


class FakeClient:
    def __init__(self):
        self.events = FakeEvents()
        self.admin = self
        self.pinged = False

    def command(self, name):
        self.pinged = True
        return {"ok": 1}

    def __getitem__(self, name):
        db = type("Db", (), {})()
        db.name = name
        db.events = self.events
        return db


def test_requires_uri(monkeypatch):
    monkeypatch.setattr("app.atlas.settings.atlas_uri", None)
    with pytest.raises(ValueError):
        AtlasManager()


def test_search_index_definition_is_valid_shape():
    definition = search_index_definition()
    fields = definition["mappings"]["fields"]
    assert fields["location"]["type"] == "document"
    assert "fuzzy" not in json.dumps(definition)  # fuzzy — параметр запиту, не маппінгу
    assert definition["synonyms"][0]["source"] == {"collection": "synonyms"}


def test_create_search_index_and_search():
    client = FakeClient()
    atlas = AtlasManager(client=client, db_name="eventhub")
    assert atlas.ping_cloud() and client.pinged

    index = atlas.create_atlas_search_index(apply=True)
    assert index["name"] == SEARCH_INDEX_NAME
    assert client.events.created

    result = atlas.perform_atlas_search("mongo", {"category": "meetup", "city": "Kyiv",
                                                  "min_rating": 4})
    assert result == [{"title": "found", "score": 1.0}]
    search = client.events.pipelines[0][0]["$search"]
    assert search["compound"]["must"][0]["text"]["fuzzy"] == {"maxEdits": 2}
    assert len(search["compound"]["filter"]) == 3


def test_search_pipeline_without_filters():
    pipeline = AtlasManager.build_search_pipeline("ai")
    assert "filter" not in pipeline[0]["$search"]["compound"]
    assert pipeline[-1] == {"$limit": 50}


def test_ping_failure_returns_false():
    class Broken(FakeClient):
        def command(self, name):
            raise ConnectionError("no network")

    assert AtlasManager(client=Broken()).ping_cloud() is False


def test_data_lake(db):
    atlas = AtlasManager(client=FakeClient())
    assert atlas.use_atlas_data_lake() == []
    db.client[db.name].s3_events_data.insert_one({"year": 2031, "category": "x", "sales": 2, "price": 10})
    rows = atlas.use_atlas_data_lake(db.client, database=db.name, year=2031)
    assert rows[0]["total_sales"] >= 2


def test_trigger_config_and_dry_run():
    triggers = AtlasTriggers(app_id="app", api_key="priv", public_key="pub", group_id="grp")
    config = triggers.create_trigger()
    assert config["function_name"] == "updateEventStatistics"
    assert config["config"]["full_document"] is True
    assert "event_processors" not in config
    assert triggers.create_trigger({"disabled": True})["disabled"] is True


def test_trigger_api_calls():
    calls = []

    def fake_post(url, payload, headers):
        calls.append((url, payload, headers))
        return {"access_token": "token-123"} if url.endswith("/login") else {"_id": "trigger-1"}

    triggers = AtlasTriggers(app_id="app", api_key="priv", public_key="pub", group_id="grp",
                             http_post=fake_post)
    assert triggers.create_trigger(dry_run=False) == {"_id": "trigger-1"}
    assert calls[0][1] == {"username": "pub", "apiKey": "priv"}
    assert calls[1][0].endswith("/groups/grp/apps/app/triggers")
    assert calls[1][2]["Authorization"] == "Bearer token-123"


def test_trigger_requires_keys(monkeypatch):
    for key in ("atlas_app_id", "atlas_private_key", "atlas_public_key", "atlas_group_id"):
        monkeypatch.setattr(f"app.atlas.settings.{key}", None)
    with pytest.raises(ValueError):
        AtlasTriggers().create_trigger(dry_run=False)
