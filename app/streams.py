"""Change Streams та подієва архітектура"""
import logging
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterator, Optional

from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.change_stream import ChangeStream
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError, PyMongoError

from app.config import settings

logger = logging.getLogger(__name__)


def _now() -> datetime:
    """Поточний час у UTC."""
    return datetime.now(timezone.utc)


# ---------- Реальний час відстеження змін ----------

class ChangeStreamManager:
    """Change streams: реакція на зміни в реальному часі"""

    def __init__(self, client: MongoClient, db_name: Optional[str] = None) -> None:
        """db_name - база, за якою стежимо (за замовчуванням з .env)."""
        self.client = client
        self.db = client[db_name or settings.mongo_db]
        self.processed: Dict[str, int] = {"events": 0, "users": 0, "reviews": 0}


    @staticmethod
    def _iterate(stream: ChangeStream, max_changes: Optional[int], timeout: Optional[float],
                 stop: Optional[threading.Event] = None) -> Iterator[Dict]:
        """Ітерувати стрім, доки не оброблено max_changes змін, не минув timeout або stop."""
        count = 0
        deadline = time.time() + timeout if timeout else None
        while stream.alive:
            if stop is not None and stop.is_set():
                return
            if deadline and time.time() > deadline:
                return
            change = stream.try_next()
            if change is None:
                continue
            yield change
            count += 1
            if max_changes is not None and count >= max_changes:
                return

    def watch_registrations_and_update_cache(self, max_changes: Optional[int] = None,
                                             timeout: Optional[float] = None,
                                             resume_after: Optional[Dict] = None,
                                             stop: Optional[threading.Event] = None) -> int:
        """Слухання реєстрацій та оновлення кешу/статистики"""

        pipeline = [
            {
                "$match": {
                    "operationType": {"$in": ["insert", "update"]},
                    "fullDocument.status": "confirmed"
                }
            },
            {
                "$project": {
                    "fullDocument": 1,
                    "operationType": 1,
                    "ns": 1,
                    "documentKey": 1
                }
            }
        ]

        processed = 0
        with self.db.registrations.watch(pipeline, full_document="updateLookup",
                                         resume_after=resume_after,
                                         max_await_time_ms=500) as stream:
            for change in self._iterate(stream, max_changes, timeout, stop):
                try:
                    doc = change["fullDocument"]
                    event_id = doc["event_id"]
                    user_id = doc["user_id"]

                    self.update_redis_cache(event_id, user_id)

                    self.update_event_statistics(event_id)

                    self.send_webhook(event_id, user_id, "new_registration")

                    self.send_notification(event_id, user_id)

                    self.save_resume_token("registrations", change["_id"])
                    processed += 1
                    logger.info(f"Processed registration: {doc['_id']}")

                except Exception as e:
                    logger.error(f"Error processing change: {e}")
                    self.save_failed_change(change, str(e))
        return processed

    def watch_multiple_collections(self, max_changes: Optional[int] = None,
                                   timeout: Optional[float] = None,
                                   stop: Optional[threading.Event] = None) -> Dict[str, int]:
        """Слухання змін в декількох колекціях"""
        handlers = {
            "events": self.handle_event_change,
            "users": self.handle_user_change,
            "reviews": self.handle_review_change,
        }
        with self.db.watch(full_document="updateLookup", max_await_time_ms=500) as stream:
            for change in self._iterate(stream, max_changes, timeout, stop):
                collection = change.get("ns", {}).get("coll")
                handler = handlers.get(collection)
                if handler:
                    handler(change)
        return dict(self.processed)

    def resume_after_failure(self, resume_token: Optional[Dict] = None,
                             max_changes: Optional[int] = None,
                             timeout: Optional[float] = None,
                             max_retries: int = 3,
                             handler: Optional[Callable[[Dict], None]] = None) -> int:

        """Відновлення Change Stream після збою"""
        token = resume_token or self.load_resume_token("events")
        processed = 0
        retries = 0
        while True:
            try:
                with self.db.events.watch(resume_after=token, full_document="updateLookup",
                                          max_await_time_ms=500) as stream:
                    remaining = None if max_changes is None else max_changes - processed
                    for change in self._iterate(stream, remaining, timeout):
                        if handler:
                            handler(change)
                        logger.info(f"Resumed processing: {change['operationType']} "
                                    f"{change['documentKey']}")
                        token = change["_id"]
                        self.save_resume_token("events", token)
                        processed += 1
                return processed
            except PyMongoError as e:
                retries += 1
                logger.error(f"Stream error: {e}; retry {retries}/{max_retries}")
                if retries >= max_retries:
                    raise
                token = self.load_resume_token("events") or token
                time.sleep(min(5, retries))


    def update_redis_cache(self, event_id: Any, user_id: Any) -> None:
        """Кеш лічильників реєстрацій"""
        self.db.event_cache.update_one(
            {"_id": event_id},
            {"$addToSet": {"recent_participants": user_id},
             "$inc": {"registrations_seen": 1},
             "$set": {"updated_at": _now()}},
            upsert=True
        )

    def update_event_statistics(self, event_id: Any) -> int:
        """Перерахувати statistics.total_participants з підтверджених реєстрацій."""
        confirmed = self.db.registrations.count_documents(
            {"event_id": event_id, "status": "confirmed"}
        )
        self.db.events.update_one(
            {"_id": event_id},
            {"$set": {"statistics.total_participants": confirmed, "updated_at": _now()}}
        )
        return confirmed

    def send_webhook(self, event_id: Any, user_id: Any, event_type: str) -> None:
        """Поставити вебхук у чергу (webhook_outbox). Відправку робить окремий воркер."""
        self.db.webhook_outbox.insert_one({
            "event_type": event_type,
            "payload": {"event_id": event_id, "user_id": user_id},
            "status": "pending",
            "created_at": _now()
        })

    def send_notification(self, event_id: Any, user_id: Any) -> None:
        """Поставити email/push-сповіщення в чергу (notifications)."""
        self.db.notifications.insert_one({
            "user_id": user_id,
            "event_id": event_id,
            "channels": ["email", "push"],
            "template": "registration_confirmed",
            "status": "pending",
            "created_at": _now()
        })

    def save_failed_change(self, change: Dict, error: str = "") -> None:
        """Зберегти зміну, яку не вдалося обробити (failed_changes)."""
        self.db.failed_changes.insert_one({
            "change": change, "error": error, "created_at": _now(), "retries": 0
        })

    def save_resume_token(self, stream_name: str, token: Dict) -> None:
        """Запам'ятати resume token, щоб після перезапуску продовжити з того ж місця."""
        self.db.stream_tokens.update_one(
            {"_id": stream_name},
            {"$set": {"token": token, "updated_at": _now()}},
            upsert=True
        )

    def load_resume_token(self, stream_name: str) -> Optional[Dict]:
        """Збережений resume token або None."""
        doc = self.db.stream_tokens.find_one({"_id": stream_name})
        return doc["token"] if doc else None

    def handle_event_change(self, change: Dict) -> None:
        """Обробити зміну в events."""
        self.processed["events"] += 1
        logger.info(f"Event change: {change['operationType']} {change.get('documentKey')}")

    def handle_user_change(self, change: Dict) -> None:
        """Обробити зміну в users."""
        self.processed["users"] += 1
        logger.info(f"User change: {change['operationType']} {change.get('documentKey')}")

    def handle_review_change(self, change: Dict) -> None:
        """Обробити зміну в reviews."""
        self.processed["reviews"] += 1
        logger.info(f"Review change: {change['operationType']} {change.get('documentKey')}")


# ---------- Event Sourcing з MongoDB ----------

class EventSourcingManager:
    """Event sourcing: журнал подій і відновлення стану"""

    def __init__(self, db: Database) -> None:
        """db - база з колекцією event_store."""
        self.db = db
        self.event_store = db.event_store
        self.event_store.create_index([("aggregate_id", ASCENDING), ("version", ASCENDING)],
                                      unique=True)

    def append_event(self, aggregate_id: str, event_type: str,
                     data: Dict, user_id: str, max_retries: int = 5) -> Dict:
        """Збереження події в event store"""

        for _ in range(max_retries):
            event = {
                "aggregate_id": aggregate_id,
                "event_type": event_type,
                "data": data,
                "user_id": user_id,
                "timestamp": _now(),
                "version": 1,
                "correlation_id": str(uuid.uuid4()),
                "causation_id": str(uuid.uuid4())
            }

            last_event = self.event_store.find_one(
                {"aggregate_id": aggregate_id},
                sort=[("version", DESCENDING)]
            )

            if last_event:
                event["version"] = last_event["version"] + 1

            try:
                self.event_store.insert_one(event)
                return event
            except DuplicateKeyError:
                continue
        raise RuntimeError(f"Could not append event to {aggregate_id}: version conflicts")

    def replay_events(self, aggregate_id: str) -> Dict:
        """Відтворення історії подій для агрегату"""

        events = list(
            self.event_store.find(
                {"aggregate_id": aggregate_id}
            ).sort("version", 1)
        )

        state = {}
        for event in events:
            state = self.apply_event(state, event)

        return state

    def apply_event(self, state: Dict, event: Dict) -> Dict:
        """Застосування події до стану"""

        event_type = event["event_type"]
        data = event["data"]

        if event_type == "user_registered":
            state["participants"] = state.get("participants", []) + [data["user_id"]]
            state["capacity_remaining"] = state.get("capacity_remaining", 0) - 1

        elif event_type == "event_created":
            state = {**data}

        elif event_type == "review_added":
            reviews = state.get("reviews", [])
            reviews.append(data)
            state["reviews"] = reviews

            total_rating = sum(r["rating"] for r in reviews)
            state["average_rating"] = total_rating / len(reviews)

        return state
