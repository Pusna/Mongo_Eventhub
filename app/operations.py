"""CRUD-операції, реєстрація на подію, відгуки, пошук."""
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
from bson import ObjectId
from pymongo import ASCENDING, DESCENDING
from pymongo.client_session import ClientSession
from pymongo.errors import DuplicateKeyError
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern
import logging

from app.config import settings
from app.repository import EventHubRepository

logger = logging.getLogger(__name__)

EARTH_RADIUS_KM = 6378.1


class ConcurrentModificationError(Exception):
    """Документ змінив інший процес (не збіглася version)."""


class EventHubOperations(EventHubRepository):
    """Бізнес-операції EventHub поверх репозиторію."""

    # ----------------- USER OPERATIONS -------------------

    def create_user(self, user_data: Dict[str, Any]) -> ObjectId:
        """Створити користувача з валідацією"""
        try:
            if self.users.find_one({"email": user_data["email"]}):
                raise ValueError(f"User with email {user_data['email']} already exists")

            user_data["created_at"] = datetime.now(timezone.utc)
            user_data["updated_at"] = datetime.now(timezone.utc)
            user_data["version"] = 1
            user_data["status"] = "active"
            user_data["statistics"] = {
                "events_organized": 0,
                "events_attended": 0,
                "total_ratings_given": 0,
                "average_rating_given": 0
            }
            result = self.users.insert_one(user_data)
            return result.inserted_id
        except Exception as e:
            logger.error(f"Error creating user: {e}")
            raise

    def get_user_by_id(self, user_id: ObjectId) -> Optional[Dict]:
        """Отримати користувача з агрегованою статистикою"""
        pipeline = [
            {"$match": {"_id": user_id}},
            {
                "$lookup": {
                    "from": "registrations",
                    "let": {"user_id": "$_id"},
                    "pipeline": [
                        {"$match": {
                            "$expr": {"$eq": ["$user_id", "$$user_id"]},
                            "status": "confirmed"
                        }},
                        {"$count": "registrations"}
                    ],
                    "as": "registration_stats"
                }
            },
            {
                "$lookup": {
                    "from": "reviews",
                    "let": {"user_id": "$_id"},
                    "pipeline": [
                        {"$match": {"$expr": {"$eq": ["$user_id", "$$user_id"]}}},
                        {"$group": {
                            "_id": None,
                            "avg_rating": {"$avg": "$rating"},
                            "total_reviews": {"$sum": 1}
                        }}
                    ],
                    "as": "review_stats"
                }
            },
            {
                "$addFields": {
                    "stats": {
                        "registrations": {"$ifNull": [{"$arrayElemAt": ["$registration_stats.registrations", 0]}, 0]},
                        "avg_review_rating": {"$ifNull": [{"$arrayElemAt": ["$review_stats.avg_rating", 0]}, 0]},
                        "total_reviews": {"$ifNull": [{"$arrayElemAt": ["$review_stats.total_reviews", 0]}, 0]}
                    }
                }
            },
            {"$project": {"registration_stats": 0, "review_stats": 0}}
        ]
        result = list(self.users.aggregate(pipeline))
        return result[0] if result else None

    def update_user(self, user_id: ObjectId, update_data: Dict[str, Any]) -> bool:
        """Оновити користувача з optimistic locking"""
        current = self.users.find_one({"_id": user_id})
        if not current:
            raise ValueError("User not found")

        result = self.users.update_one(
            {"_id": user_id, "version": current["version"]},
            {
                "$set": {**update_data, "updated_at": datetime.now(timezone.utc)},
                "$inc": {"version": 1}
            }
        )
        if result.matched_count == 0:
            raise ConcurrentModificationError("User was modified by another process")
        return True

    # ================= EVENT OPERATIONS =================

    def create_event(self, event_data: Dict[str, Any]) -> ObjectId:
        """Створити подію з валідацією capacity"""
        if "capacity" in event_data:
            event_data["capacity"]["remaining"] = event_data["capacity"]["total"]

        event_data.setdefault("status", "published")
        event_data["created_at"] = datetime.now(timezone.utc)
        event_data["updated_at"] = datetime.now(timezone.utc)
        event_data["version"] = 1
        event_data["participants"] = []
        event_data["statistics"] = {
            "total_participants": 0,
            "average_rating": 0.0,
            "total_reviews": 0,
            "waiting_list": 0
        }
        result = self.events.insert_one(event_data)

        organizer_id = event_data.get("organizer", {}).get("user_id")
        if organizer_id:
            self.users.update_one({"_id": organizer_id}, {"$inc": {"statistics.events_organized": 1}})

        self.audit_log.insert_one({
            "action": "event_created",
            "user_id": organizer_id,
            "resource_type": "event",
            "resource_id": result.inserted_id,
            "timestamp": datetime.now(timezone.utc)
        })
        return result.inserted_id

    def register_user_for_event(self, user_id: ObjectId, event_id: ObjectId) -> ObjectId:
        """Зареєструвати користувача на подію з транзакцією"""

        def _register(session: ClientSession) -> ObjectId:
            """Тіло транзакції реєстрації (with_transaction повторює його при збої)."""
            now = datetime.now(timezone.utc)

            event = self.events.find_one({"_id": event_id}, session=session)
            if not event:
                raise ValueError("Event not found")
            if event.get("status") == "cancelled":
                raise ValueError("Event is cancelled")

            if self.registrations.find_one({"event_id": event_id, "user_id": user_id}, session=session):
                raise ValueError("User already registered")

            seat = self.events.update_one(
                {"_id": event_id, "capacity.remaining": {"$gt": 0}},
                {
                    "$addToSet": {"participants": user_id},
                    "$inc": {"capacity.remaining": -1, "statistics.total_participants": 1},
                    "$set": {"updated_at": now}
                },
                session=session
            )
            if seat.modified_count == 0:
                raise ValueError("Event is full")

            result = self.registrations.insert_one({
                "user_id": user_id,
                "event_id": event_id,
                "status": "confirmed",
                "registered_at": now,
                "updated_at": now,
                "attendance": {"checked_in": False, "checked_in_at": None, "feedback_submitted": False}
            }, session=session)

            self.users.update_one(
                {"_id": user_id},
                {"$inc": {"statistics.events_attended": 1}, "$set": {"updated_at": now}},
                session=session
            )

            self.audit_log.insert_one({
                "action": "user_registered",
                "user_id": user_id,
                "resource_type": "registration",
                "resource_id": result.inserted_id,
                "details": {"event_id": event_id},
                "timestamp": now
            }, session=session)
            return result.inserted_id

        with self.client.start_session() as session:
            try:
                return session.with_transaction(
                    _register,
                    read_concern=ReadConcern("snapshot"),
                    write_concern=WriteConcern(w="majority", j=True)
                )
            except Exception as e:
                logger.error(f"Registration failed: {e}")
                raise

    # ================= REVIEW OPERATIONS =================

    def create_review(self, user_id: ObjectId, event_id: ObjectId, rating: int,
                      comment: str = "", pros: Optional[List[str]] = None,
                      cons: Optional[List[str]] = None) -> ObjectId:
        """Створити відгук і перерахувати statistics.average_rating події.

        Відгук може залишити лише користувач з підтвердженою реєстрацією, один раз на подію.
        """
        if not isinstance(rating, int) or isinstance(rating, bool) or not 1 <= rating <= 5:
            raise ValueError("Rating must be an integer from 1 to 5")

        def _review(session: ClientSession) -> ObjectId:
            """Тіло транзакції відгуку: відгук + перерахунок рейтингу."""
            now = datetime.now(timezone.utc)
            registration = self.registrations.find_one(
                {"event_id": event_id, "user_id": user_id, "status": "confirmed"}, session=session
            )
            if not registration:
                raise ValueError("Only confirmed participants can review the event")
            if self.reviews.find_one({"event_id": event_id, "user_id": user_id}, session=session):
                raise ValueError("User already reviewed this event")

            result = self.reviews.insert_one({
                "event_id": event_id,
                "user_id": user_id,
                "rating": rating,
                "comment": comment,
                "pros": pros or [],
                "cons": cons or [],
                "helpful_votes": 0,
                "reported": False,
                "status": "approved",
                "created_at": now,
                "updated_at": now
            }, session=session)

            stats = list(self.reviews.aggregate([
                {"$match": {"event_id": event_id, "status": "approved"}},
                {"$group": {"_id": None, "avg": {"$avg": "$rating"}, "count": {"$sum": 1}}}
            ], session=session))
            avg = round(stats[0]["avg"], 2) if stats else 0.0
            count = stats[0]["count"] if stats else 0
            self.events.update_one(
                {"_id": event_id},
                {"$set": {"statistics.average_rating": avg, "statistics.total_reviews": count,
                          "updated_at": now}},
                session=session
            )

            user_stats = list(self.reviews.aggregate([
                {"$match": {"user_id": user_id}},
                {"$group": {"_id": None, "avg": {"$avg": "$rating"}, "count": {"$sum": 1}}}
            ], session=session))
            self.users.update_one(
                {"_id": user_id},
                {"$set": {"statistics.total_ratings_given": user_stats[0]["count"],
                          "statistics.average_rating_given": round(user_stats[0]["avg"], 2),
                          "updated_at": now}},
                session=session
            )
            self.registrations.update_one(
                {"_id": registration["_id"]},
                {"$set": {"attendance.feedback_submitted": True, "updated_at": now}},
                session=session
            )
            self.audit_log.insert_one({
                "action": "review_created",
                "user_id": user_id,
                "resource_type": "review",
                "resource_id": result.inserted_id,
                "details": {"event_id": event_id, "rating": rating},
                "timestamp": now
            }, session=session)
            return result.inserted_id

        with self.client.start_session() as session:
            return session.with_transaction(
                _review,
                read_concern=ReadConcern("snapshot"),
                write_concern=WriteConcern(w="majority", j=True)
            )

    # ================= SEARCH =================

    def search_events(self, query: str, category: Optional[str] = None,
                      start_date: Optional[datetime] = None,
                      end_date: Optional[datetime] = None,
                      min_rating: Optional[float] = None,
                      max_distance: Optional[float] = None,
                      coordinates: Optional[List[float]] = None) -> List[Dict]:
        """Розширений пошук подій з текстовим індексом та фільтрами"""

        filter_query = {}

        if query:
            filter_query["$text"] = {"$search": query}
            filter_query["$text"]["$caseSensitive"] = False

        if category:
            filter_query["category"] = category

        if start_date or end_date:
            date_filter = {}
            if start_date:
                date_filter["$gte"] = start_date
            if end_date:
                date_filter["$lte"] = end_date
            filter_query["schedule.start_date"] = date_filter

        if min_rating:
            filter_query["statistics.average_rating"] = {"$gte": min_rating}


        use_near = False
        if coordinates and max_distance:
            if query:
                filter_query["location.coordinates"] = {
                    "$geoWithin": {
                        "$centerSphere": [coordinates, max_distance / EARTH_RADIUS_KM]
                    }
                }
            else:
                filter_query["location.coordinates"] = {
                    "$near": {
                        "$geometry": {
                            "type": "Point",
                            "coordinates": coordinates
                        },
                        "$maxDistance": max_distance * 1000
                    }
                }
                use_near = True

        query_result = self.events.find(filter_query)

        if query:
            query_result = query_result.sort([("score", {"$meta": "textScore"})])
        elif not use_near:
            query_result = query_result.sort([("schedule.start_date", ASCENDING)])

        return list(query_result.limit(50))
