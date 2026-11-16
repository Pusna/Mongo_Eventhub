"""Підключення до MongoDB, валідатори та індекси."""
from typing import List, Optional, Dict, Any
from datetime import datetime
from bson import ObjectId
from pymongo import MongoClient, ASCENDING, DESCENDING, TEXT
from pymongo.errors import DuplicateKeyError, OperationFailure
import logging

from app.config import settings
from app.schemas import apply_validators

logger = logging.getLogger(__name__)


class EventHubRepository:
    """Підключення до бази EventHub і доступ до її колекцій."""

    def __init__(self, connection_string: Optional[str] = None, db_name: Optional[str] = None) -> None:
        """Створює клієнта, застосовує валідатори та створює індекси."""
        self.client = MongoClient(
            connection_string or settings.mongo_uri,
            tz_aware=True,
            maxPoolSize=200,
            minPoolSize=20,
            maxIdleTimeMS=60000,
            waitQueueTimeoutMS=10000,
            socketTimeoutMS=15000,
            connectTimeoutMS=10000,
            retryWrites=True,
            retryReads=True
        )

        self.db = self.client[db_name or settings.mongo_db]
        self.users = self.db.users
        self.events = self.db.events
        self.registrations = self.db.registrations
        self.reviews = self.db.reviews
        self.audit_log = self.db.audit_log

        apply_validators(self.db)
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        """Створити індекси розділу 3.1 (повторний виклик безпечний)."""
        logger.info("Створюємо індекси в MongoDB...")

        self.users.create_index([("email", ASCENDING)], unique=True)
        self.users.create_index([("status", ASCENDING), ("created_at", DESCENDING)])
        self.users.create_index([("profile.name", TEXT)])
        self.users.create_index([("address.coordinates", "2dsphere")])

        self.events.create_index([("category", ASCENDING), ("schedule.start_date", ASCENDING)])
        self.events.create_index([("participants", ASCENDING)])
        self.events.create_index([("title", TEXT), ("description", TEXT), ("tags", TEXT)])
        self.events.create_index([("location.coordinates", "2dsphere")])
        self.events.create_index(
            [("statistics.average_rating", DESCENDING)],
            partialFilterExpression={"statistics.average_rating": {"$gte": 4.0}}
        )

        self.registrations.create_index([("event_id", ASCENDING), ("user_id", ASCENDING)], unique=True)
        self.registrations.create_index([("status", ASCENDING), ("registered_at", ASCENDING)])

        self.registrations.create_index([("registered_at", ASCENDING)], expireAfterSeconds=7776000)


        self.reviews.create_index([("event_id", ASCENDING), ("rating", DESCENDING)])
        self.reviews.create_index([("user_id", ASCENDING), ("created_at", DESCENDING)])


        self.audit_log.create_index([("timestamp", DESCENDING)])
        self.audit_log.create_index([("user_id", ASCENDING), ("action", ASCENDING)])

        self.audit_log.create_index([("timestamp", ASCENDING)], expireAfterSeconds=2592000)

        logger.info("Індекси успішно створено!")
