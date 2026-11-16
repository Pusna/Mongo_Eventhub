"""Транзакції та конкурентність."""
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

from bson import ObjectId
from pymongo import MongoClient, ReturnDocument
from pymongo.collection import Collection
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import ReadPreference
from pymongo.write_concern import WriteConcern

from app.config import settings

if TYPE_CHECKING:
    from app.operations import EventHubOperations

logger = logging.getLogger(__name__)


class VersionConflictError(Exception):
    """Версія документа не збіглася (optimistic locking)."""


# ---------- Розширені транзакції з різними рівнями ізоляції ----------

class TransactionManager:
    """Багатодокументні транзакції EventHub (розділ 5.1)."""

    def __init__(self, client: MongoClient, db_name: Optional[str] = None) -> None:
        """db_name — база застосунку (за замовчуванням з .env)"""
        self.client = client
        self.db = client[db_name or settings.mongo_db]

    def transfer_event_organizer(self, event_id: ObjectId,
                                 old_organizer_id: ObjectId,
                                 new_organizer_id: ObjectId) -> bool:
        """Транзакція зміни організатора з аудитом"""

        with self.client.start_session() as session:
            session.start_transaction(
                read_concern=ReadConcern("majority"),
                write_concern=WriteConcern(w="majority", j=True),
                read_preference=ReadPreference.PRIMARY
            )

            try:
                # Перевірити існування обох користувачів
                old_user = self.db.users.find_one({"_id": old_organizer_id}, session=session)
                new_user = self.db.users.find_one({"_id": new_organizer_id}, session=session)

                if not old_user or not new_user:
                    raise ValueError("User not found")

                # Отримати подію з pessimistic lock.
                event = self.db.events.find_one_and_update(
                    {"_id": event_id},
                    {"$set": {"status": "updating"}},
                    return_document=ReturnDocument.BEFORE,
                    session=session
                )

                if not event:
                    raise ValueError("Event not found")

                if event.get("organizer", {}).get("user_id") != old_organizer_id:
                    raise ValueError("Old organizer does not match the event organizer")


                self.db.events.update_one(
                    {"_id": event_id},
                    {
                        "$set": {
                            "organizer.user_id": new_organizer_id,
                            "status": event.get("status", "published"),
                            "updated_at": datetime.now(timezone.utc)
                        }
                    },
                    session=session
                )

                self.db.users.update_one(
                    {"_id": old_organizer_id},
                    {"$inc": {"statistics.events_organized": -1}},
                    session=session
                )
                self.db.users.update_one(
                    {"_id": new_organizer_id},
                    {"$inc": {"statistics.events_organized": 1}},
                    session=session
                )

                self.db.audit_log.insert_one({
                    "action": "organizer_transferred",
                    "user_id": old_organizer_id,
                    "resource_type": "event",
                    "resource_id": event_id,
                    "changes": {
                        "old_organizer": old_organizer_id,
                        "new_organizer": new_organizer_id
                    },
                    "timestamp": datetime.now(timezone.utc)
                }, session=session)

                session.commit_transaction()
                return True

            except Exception as e:
                if session.in_transaction:
                    session.abort_transaction()
                logger.error(f"Transaction failed: {e}")
                raise

    def batch_cancel_registrations(self, event_id: ObjectId, user_ids: List[ObjectId]) -> int:
        """Масове скасування реєстрацій з транзакцією"""

        with self.client.start_session() as session:
            session.start_transaction()

            try:
                result = self.db.registrations.update_many(
                    {
                        "event_id": event_id,
                        "user_id": {"$in": user_ids},
                        "status": "confirmed"
                    },
                    {
                        "$set": {
                            "status": "cancelled",
                            "updated_at": datetime.now(timezone.utc)
                        }
                    },
                    session=session
                )

                if result.modified_count > 0:
                    self.db.events.update_one(
                        {"_id": event_id},
                        {
                            "$pull": {"participants": {"$in": user_ids}},
                            "$inc": {
                                "capacity.remaining": result.modified_count,
                                "statistics.total_participants": -result.modified_count
                            }
                        },
                        session=session
                    )

                session.commit_transaction()
                return result.modified_count

            except Exception:
                if session.in_transaction:
                    session.abort_transaction()
                raise


# ---------- Optimistic Locking Pattern ----------

class OptimisticLockingMixin:
    """Mixin для optimistic locking"""

    def update_with_version(self, collection: Collection, doc_id: ObjectId,
                            update_data: Dict, expected_version: int) -> bool:
        """Оновлення документа з перевіркою версії"""

        result = collection.update_one(
            {
                "_id": doc_id,
                "version": expected_version
            },
            {
                "$set": {
                    **update_data,
                    "updated_at": datetime.now(timezone.utc)
                },
                "$inc": {"version": 1}
            }
        )

        if result.matched_count == 0:
            raise VersionConflictError(
                f"Document {doc_id} was modified. Expected version {expected_version}"
            )

        return True

    def retry_with_backoff(self, func: Callable[[], Any], max_retries: int = 3) -> Any:
        """Повтор спроби з експоненційним відступом при VersionConflictError"""

        retries = 0
        while retries < max_retries:
            try:
                return func()
            except VersionConflictError:
                retries += 1
                if retries == max_retries:
                    raise
                time.sleep(0.1 * (2 ** retries))  # 0.2, 0.4, 0.8 сек


# ---------- Тестування конкурентності ----------

def concurrent_registration_test(repo: "EventHubOperations", event_id: ObjectId, user_ids: List[ObjectId]) -> List[Dict]:
    """Тест конкурентних реєстрацій (викликається з tests/)"""

    results = []

    def register_user(user_id: ObjectId) -> Dict[str, Any]:
        """Одна спроба реєстрації; помилка повертається як success=False."""
        try:
            registration_id = repo.register_user_for_event(user_id, event_id)
            return {"user_id": user_id, "success": True, "registration_id": registration_id}
        except Exception as e:
            return {"user_id": user_id, "success": False, "error": str(e)}

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(register_user, uid) for uid in user_ids]

        for future in as_completed(futures):
            results.append(future.result())

    success_count = sum(1 for r in results if r["success"])
    logger.info(f"Concurrent registration: total={len(results)}, "
                f"successful={success_count}, failed={len(results) - success_count}")

    event = repo.events.find_one({"_id": event_id})
    participant_count = len(event["participants"])
    remaining_capacity = event["capacity"]["remaining"]

    assert participant_count + remaining_capacity == event["capacity"]["total"]

    return results
