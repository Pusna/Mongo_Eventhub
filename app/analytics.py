"""Розділ 6. Агрегації та аналітика."""
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from bson import ObjectId
from pymongo.database import Database

logger = logging.getLogger(__name__)


def _safe_ratio(numerator: str, denominator: str) -> Dict:
    """numerator / denominator, або 0, якщо знаменник 0 чи відсутній ($divide на 0 — помилка)."""
    return {
        "$cond": [
            {"$gt": [{"$ifNull": [denominator, 0]}, 0]},
            {"$divide": [numerator, denominator]},
            0
        ]
    }


# ---------- 6.1. Складні агрегаційні пайплайни ----------

class AnalyticsAggregator:
    """Аналітичні агрегаційні пайплайни (розділ 6.1)."""

    def __init__(self, db: Database) -> None:
        """db - база з колекціями events, users, registrations, reviews."""
        self.db = db

    def get_event_statistics(self, event_id: ObjectId) -> Optional[Dict]:
        """Детальна статистика по одній події"""

        pipeline = [
            {"$match": {"_id": event_id}},

            {
                "$lookup": {
                    "from": "users",
                    "localField": "organizer.user_id",
                    "foreignField": "_id",
                    "as": "organizer_info"
                }
            },

            {
                "$lookup": {
                    "from": "users",
                    "localField": "participants",
                    "foreignField": "_id",
                    "as": "participant_details"
                }
            },

            {
                "$lookup": {
                    "from": "reviews",
                    "let": {"event_id": "$_id"},
                    "pipeline": [
                        {"$match": {"$expr": {"$eq": ["$event_id", "$$event_id"]}}},
                        {"$group": {
                            "_id": None,
                            "average_rating": {"$avg": "$rating"},
                            "total_reviews": {"$sum": 1},
                            "rating_distribution": {
                                "$push": "$rating"
                            }
                        }}
                    ],
                    "as": "review_stats"
                }
            },


            {
                "$lookup": {
                    "from": "registrations",
                    "let": {"event_id": "$_id"},
                    "pipeline": [
                        {"$match": {"$expr": {"$eq": ["$event_id", "$$event_id"]}}},
                        {"$group": {
                            "_id": "$status",
                            "count": {"$sum": 1}
                        }},
                        {"$group": {
                            "_id": None,
                            "statuses": {
                                "$push": {"k": "$_id", "v": "$count"}
                            }
                        }},
                        {"$project": {
                            "statuses": {"$arrayToObject": "$statuses"}
                        }}
                    ],
                    "as": "registration_statuses"
                }
            },

            {
                "$addFields": {
                    "organizer": {"$arrayElemAt": ["$organizer_info", 0]},
                    "participant_count": {"$size": {"$ifNull": ["$participants", []]}},
                    "average_rating": {
                        "$ifNull": [
                            {"$arrayElemAt": ["$review_stats.average_rating", 0]},
                            0
                        ]
                    },
                    "total_reviews": {
                        "$ifNull": [
                            {"$arrayElemAt": ["$review_stats.total_reviews", 0]},
                            0
                        ]
                    },
                    "registration_statuses": {
                        "$ifNull": [
                            {"$arrayElemAt": ["$registration_statuses.statuses", 0]},
                            {}
                        ]
                    }
                }
            },

            {
                "$project": {
                    "_id": 1,
                    "title": 1,
                    "category": 1,
                    "schedule": 1,
                    "location": 1,
                    "capacity": 1,
                    "organizer.name": "$organizer.profile.name",
                    "organizer.email": "$organizer.email",
                    "participant_count": 1,
                    "average_rating": 1,
                    "total_reviews": 1,
                    "registration_statuses": 1,
                    "participants": 1,  # IDs
                    "participant_details.name": 1,
                    "participant_details.profile.name": 1,
                    "statistics": 1
                }
            }
        ]

        result = list(self.db.events.aggregate(pipeline))
        return result[0] if result else None

    def get_category_analytics(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Аналітика по категоріях за період"""

        pipeline = [
            {
                "$match": {
                    "schedule.start_date": {"$gte": start_date, "$lte": end_date},
                    "status": "completed"
                }
            },


            {
                "$group": {
                    "_id": "$category",
                    "event_count": {"$sum": 1},
                    "total_participants": {"$sum": "$statistics.total_participants"},
                    "average_rating": {"$avg": "$statistics.average_rating"},
                    "total_reviews": {"$sum": "$statistics.total_reviews"},
                    "avg_capacity_utilization": {
                        "$avg": _safe_ratio("$statistics.total_participants", "$capacity.total")
                    }
                }
            },


            {
                "$addFields": {
                    "avg_participants_per_event": {
                        "$divide": ["$total_participants", "$event_count"]
                    },
                    "popularity_score": {
                        "$multiply": [
                            "$total_participants",
                            {"$add": [1, {"$ifNull": ["$average_rating", 0]}]}
                        ]
                    }
                }
            },


            {"$sort": {"popularity_score": -1}}
        ]

        return list(self.db.events.aggregate(pipeline))

    def get_user_engagement_report(self, user_id: ObjectId) -> Optional[Dict]:
        """Детальний звіт по активності користувача"""

        pipeline = [
            {"$match": {"_id": user_id}},


            {
                "$lookup": {
                    "from": "registrations",
                    "let": {"user_id": "$_id"},
                    "pipeline": [
                        {"$match": {"$expr": {"$eq": ["$user_id", "$$user_id"]}}},
                        {"$group": {
                            "_id": "$status",
                            "count": {"$sum": 1}
                        }},
                        {"$group": {
                            "_id": None,
                            "statuses": {
                                "$push": {"k": "$_id", "v": "$count"}
                            }
                        }},
                        {"$project": {
                            "statuses": {"$arrayToObject": "$statuses"}
                        }}
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
                        {"$sort": {"created_at": -1}},
                        {"$group": {
                            "_id": None,
                            "avg_rating": {"$avg": "$rating"},
                            "total_reviews": {"$sum": 1},
                            "recent_comments": {
                                "$push": {
                                    "event_id": "$event_id",
                                    "rating": "$rating",
                                    "comment": "$comment",
                                    "date": "$created_at"
                                }
                            }
                        }},

                        {"$project": {
                            "avg_rating": 1,
                            "total_reviews": 1,
                            "recent_comments": {"$slice": ["$recent_comments", 5]}
                        }}
                    ],
                    "as": "review_stats"
                }
            },

            {
                "$lookup": {
                    "from": "events",
                    "let": {"user_id": "$_id"},
                    "pipeline": [
                        {"$match": {"$expr": {"$eq": ["$organizer.user_id", "$$user_id"]}}},
                        {"$project": {
                            "title": 1,
                            "category": 1,
                            "schedule.start_date": 1,
                            "statistics.total_participants": 1
                        }},
                        {"$sort": {"schedule.start_date": -1}},
                        {"$limit": 10}
                    ],
                    "as": "organized_events"
                }
            },

            {"$addFields": {
                "registration_stats": {
                    "$ifNull": [
                        {"$arrayElemAt": ["$registration_stats.statuses", 0]},
                        {}
                    ]
                },
                "review_stats": {
                    "$ifNull": [
                        {"$arrayElemAt": ["$review_stats", 0]},
                        {}
                    ]
                }
            }}
        ]

        result = list(self.db.users.aggregate(pipeline))
        return result[0] if result else None

    def get_recommendations(self, user_id: ObjectId, limit: int = 10) -> List[Dict]:
        """Рекомендаційна система на основі інтересів та схожих користувачів"""

        user = self.db.users.find_one({"_id": user_id})
        if not user:
            return []
        interests = user.get("interests", [])

        pipeline = [
            {
                "$match": {
                    "_id": {"$ne": user_id},
                    "interests": {"$in": interests}
                }
            },
            {"$project": {"_id": 1}},

            {
                "$lookup": {
                    "from": "registrations",
                    "localField": "_id",
                    "foreignField": "user_id",
                    "as": "registrations"
                }
            },
            {"$unwind": "$registrations"},
            {"$replaceRoot": {"newRoot": "$registrations"}},

            {
                "$lookup": {
                    "from": "events",
                    "localField": "event_id",
                    "foreignField": "_id",
                    "as": "event"
                }
            },
            {"$unwind": "$event"},

            {
                "$match": {
                    "event.participants": {"$nin": [user_id]},
                    "event.status": "published",
                    "event.schedule.start_date": {"$gte": datetime.now(timezone.utc)}
                }
            },

            {
                "$group": {
                    "_id": "$event_id",
                    "title": {"$first": "$event.title"},
                    "category": {"$first": "$event.category"},
                    "interests_match": {
                        "$sum": {
                            "$size": {
                                "$setIntersection": [
                                    {"$ifNull": ["$event.tags", []]},
                                    interests
                                ]
                            }
                        }
                    },
                    "popularity": {"$first": "$event.statistics.total_participants"},
                    "avg_rating": {"$first": "$event.statistics.average_rating"},
                    "similar_users": {"$addToSet": "$user_id"}
                }
            },

            {
                "$addFields": {
                    "recommendation_score": {
                        "$multiply": [
                            {"$add": [1, "$interests_match"]},
                            {"$add": [1, {"$divide": [{"$ifNull": ["$popularity", 0]}, 100]}]},
                            {"$add": [1, {"$ifNull": ["$avg_rating", 0]}]}
                        ]
                    }
                }
            },

            {"$sort": {"recommendation_score": -1}},
            {"$limit": limit}
        ]

        return list(self.db.users.aggregate(pipeline))


# ---------- 6.2. Матеріалізовані представлення (Materialized Views) ----------

class MaterializedViewManager:
    """Матеріалізовані представлення через $merge (розділ 6.2)."""

    def __init__(self, db: Database) -> None:
        """db — база, у якій зберігаються представлення."""
        self.db = db

    def refresh_daily_stats_view(self, days: int = 1) -> int:
        """Оновлення денної статистики."""

        pipeline = [
            {
                "$match": {
                    "status": "completed",
                    "schedule.end_date": {
                        "$gte": datetime.now(timezone.utc) - timedelta(days=days)
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "year": {"$year": "$schedule.start_date"},
                        "month": {"$month": "$schedule.start_date"},
                        "day": {"$dayOfMonth": "$schedule.start_date"},
                        "category": "$category"
                    },
                    "events_count": {"$sum": 1},
                    "total_participants": {"$sum": "$statistics.total_participants"},
                    "avg_rating": {"$avg": "$statistics.average_rating"},
                    "total_revenue": {
                        "$sum": {
                            "$multiply": [
                                "$statistics.total_participants",
                                {"$ifNull": ["$pricing.regular_price", 0]}
                            ]
                        }
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "date": {
                        "$dateFromParts": {
                            "year": "$_id.year",
                            "month": "$_id.month",
                            "day": "$_id.day"
                        }
                    },
                    "category": "$_id.category",
                    "events_count": 1,
                    "total_participants": 1,
                    "avg_rating": 1,
                    "total_revenue": 1
                }
            },
            {"$out": "daily_stats_materialized"}
        ]

        self.db.events.aggregate(pipeline)
        logger.info(f"Daily stats view updated at {datetime.now(timezone.utc)}")
        return self.db.daily_stats_materialized.count_documents({})

    def refresh_venue_performance_view(self) -> int:
        """Аналітика по місцям проведення."""

        pipeline = [
            {"$match": {"status": "completed"}},
            {
                "$group": {
                    "_id": {
                        "city": "$location.city",
                        "country": "$location.country",
                        "venue": "$location.venue"
                    },
                    "total_events": {"$sum": 1},
                    "total_participants": {"$sum": "$statistics.total_participants"},
                    "avg_capacity_utilization": {
                        "$avg": _safe_ratio("$statistics.total_participants", "$capacity.total")
                    },
                    "avg_rating": {"$avg": "$statistics.average_rating"}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "city": "$_id.city",
                    "country": "$_id.country",
                    "venue": "$_id.venue",
                    "total_events": 1,
                    "total_participants": 1,
                    "avg_capacity_utilization": 1,
                    "avg_rating": 1,
                    "performance_score": {
                        "$multiply": [
                            "$avg_capacity_utilization",
                            "$avg_rating"
                        ]
                    }
                }
            },
            {"$sort": {"performance_score": -1}},
            {"$out": "venue_performance_view"}
        ]

        self.db.events.aggregate(pipeline)
        return self.db.venue_performance_view.count_documents({})
