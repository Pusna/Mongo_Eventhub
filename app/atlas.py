""" MongoDB Atlas та хмарні сервіси"""
import json
import logging
import urllib.request
from typing import Any, Callable, Dict, List, Optional

import certifi
from pymongo import MongoClient
from pymongo.operations import SearchIndexModel

from app.config import settings

logger = logging.getLogger(__name__)

SEARCH_INDEX_NAME = "events_search_index"


def search_index_definition() -> Dict:
    """Визначення Atlas Search індексу для events"""
    return {
        "mappings": {
            "dynamic": False,
            "fields": {
                "title": {"type": "string", "analyzer": "lucene.standard"},
                "description": {"type": "string", "analyzer": "lucene.english"},
                "tags": {"type": "string", "analyzer": "lucene.standard"},
                "category": {"type": "string", "analyzer": "lucene.keyword"},
                "location": {"type": "document", "fields": {"city": {"type": "string"}}},
                "statistics": {"type": "document",
                               "fields": {"average_rating": {"type": "number"}}},
                "schedule": {"type": "document", "fields": {"start_date": {"type": "date"}}},
            }
        },
        "synonyms": [
            {"name": "tech_synonyms", "analyzer": "lucene.english",
             "source": {"collection": "synonyms"}}
        ]
    }


# ---------- Підключення до MongoDB Atlas ----------

class AtlasManager:
    """Підключення до Atlas, Atlas Search і Data Lake"""

    def __init__(self, connection_string: Optional[str] = None,
                 db_name: Optional[str] = None, client: Optional[MongoClient] = None) -> None:
        if client is None:
            uri = connection_string or settings.atlas_uri
            if not uri:
                raise ValueError("ATLAS_URI не задано в .env")
            client = MongoClient(uri, tlsCAFile=certifi.where(),
                                 serverSelectionTimeoutMS=10000)
        self.client = client
        self.db = self.client[db_name or settings.mongo_db]

    def ping_cloud(self) -> bool:
        """Перевірка підключення до кластера Atlas"""
        try:
            self.client.admin.command("ping")
            return True
        except Exception as e:
            logger.error(f"Atlas connection error: {e}")
            return False

    def create_atlas_search_index(self, apply: bool = False) -> Dict:
        """Створення пошукового індексу в Atlas"""
        search_index = {
            "name": SEARCH_INDEX_NAME,
            "collection": "events",
            "database": self.db.name,
            "definition": search_index_definition(),
        }
        if apply:
            self.db.events.create_search_index(
                SearchIndexModel(definition=search_index["definition"], name=SEARCH_INDEX_NAME)
            )
            logger.info(f"Atlas Search index {SEARCH_INDEX_NAME} requested")
        return search_index

    @staticmethod
    def build_search_pipeline(query: str, filters: Optional[Dict] = None) -> List[Dict]:
        """$search-пайплайн з PDF"""
        search_stage = {
            "index": SEARCH_INDEX_NAME,
            "compound": {
                "must": [
                    {
                        "text": {
                            "query": query,
                            "path": ["title", "description", "tags"],
                            "fuzzy": {"maxEdits": 2}
                        }
                    }
                ]
            }
        }

        if filters:
            filter_clauses = []

            if filters.get("category"):
                filter_clauses.append({
                    "text": {"query": filters["category"], "path": "category"}
                })

            if filters.get("city"):
                filter_clauses.append({
                    "text": {"query": filters["city"], "path": "location.city"}
                })

            if filters.get("min_rating"):
                filter_clauses.append({
                    "range": {
                        "gte": filters["min_rating"],
                        "path": "statistics.average_rating"
                    }
                })

            if filter_clauses:
                search_stage["compound"]["filter"] = filter_clauses

        return [
            {"$search": search_stage},
            {"$addFields": {"score": {"$meta": "searchScore"}}},
            {"$sort": {"score": -1}},
            {"$limit": 50}
        ]

    def perform_atlas_search(self, query: str, filters: Optional[Dict] = None) -> List[Dict]:
        """Розширений пошук через Atlas Search (працює лише на Atlas)"""
        return list(self.db.events.aggregate(self.build_search_pipeline(query, filters)))

    @staticmethod
    def data_lake_pipeline(year: int = 2025) -> List[Dict]:
        """Пайплайн аналізу архівних подій у Data Lake за рік."""
        return [
            {"$match": {"year": year}},
            {"$group": {
                "_id": "$category",
                "total_sales": {"$sum": "$sales"},
                "avg_price": {"$avg": "$price"}
            }}
        ]

    def use_atlas_data_lake(self, data_lake_client: Optional[MongoClient] = None,
                            database: str = "eventhub", collection: str = "s3_events_data",
                            year: int = 2025) -> List[Dict]:
        """Використання Atlas Data Lake (Data Federation) для аналітики"""
        if data_lake_client is None:
            logger.info("Data Federation не налаштовано — пайплайн не виконується")
            return []
        return list(data_lake_client[database][collection].aggregate(self.data_lake_pipeline(year)))


# ---------- Atlas Triggers та Functions ----------

class AtlasTriggers:
    """Тригери App Services через Admin API"""

    API = "https://services.cloud.mongodb.com/api/admin/v3.0"

    def __init__(self, app_id: Optional[str] = None, api_key: Optional[str] = None,
                 public_key: Optional[str] = None, group_id: Optional[str] = None,
                 http_post: Optional[Callable[[str, Dict, Dict], Dict]] = None) -> None:

        self.app_id = app_id or settings.atlas_app_id
        self.api_key = api_key or settings.atlas_private_key
        self.public_key = public_key or settings.atlas_public_key
        self.group_id = group_id or settings.atlas_group_id
        self.base_url = f"{self.API}/groups/{self.group_id}/apps/{self.app_id}"
        self._http_post = http_post or self._urllib_post

    @staticmethod
    def _urllib_post(url: str, payload: Dict, headers: Dict) -> Dict:
        """POST JSON через стандартну бібліотеку, відповідь — dict."""
        req = urllib.request.Request(url, data=json.dumps(payload).encode("utf-8"),
                                     headers={"Content-Type": "application/json", **headers},
                                     method="POST")
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = resp.read().decode("utf-8")
            return json.loads(body) if body else {}

    @staticmethod
    def build_trigger_config(overrides: Optional[Dict] = None) -> Dict:
        """Тригер для автоматичного оновлення статистики"""
        config = {
            "name": "update_event_stats",
            "type": "DATABASE",
            "config": {
                "operation_types": ["INSERT", "UPDATE"],
                "database": settings.mongo_db,
                "collection": "registrations",
                "service_name": "mongodb-atlas",
                "match": {
                    "fullDocument.status": "confirmed"
                },
                "project": {
                    "fullDocument.event_id": 1,
                    "fullDocument.user_id": 1,
                    "fullDocument.status": 1,
                    "operationType": 1
                },
                "full_document": True
            },
            "function_name": "updateEventStatistics",
            "disabled": False
        }
        if overrides:
            config.update(overrides)
        return config

    def _access_token(self) -> str:
        """Логін в Atlas Admin API за API-ключем, повертає access_token."""
        resp = self._http_post(f"{self.API}/auth/providers/mongodb-cloud/login",
                               {"username": self.public_key, "apiKey": self.api_key}, {})
        return resp["access_token"]

    def create_trigger(self, trigger_config: Optional[Dict] = None,
                       dry_run: bool = True) -> Dict:
        """Створення тригера в Atlas. dry_run=True — лише повернути конфігурацію."""
        config = self.build_trigger_config(trigger_config)
        if dry_run:
            return config
        if not all([self.app_id, self.api_key, self.public_key, self.group_id]):
            raise ValueError("ATLAS_APP_ID / ATLAS_GROUP_ID / ATLAS_PUBLIC_KEY / "
                             "ATLAS_PRIVATE_KEY не задані в .env")
        token = self._access_token()
        return self._http_post(f"{self.base_url}/triggers", config,
                               {"Authorization": f"Bearer {token}"})