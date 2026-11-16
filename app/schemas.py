"""$jsonSchema-валідатори колекцій"""
from typing import Any, Dict

from pymongo.database import Database
from pymongo.errors import CollectionInvalid

USER_STATUSES = ["active", "suspended", "deleted"]
EVENT_CATEGORIES = ["conference", "workshop", "meetup", "webinar"]
EVENT_STATUSES = ["draft", "published", "cancelled", "completed", "updating"]
REGISTRATION_STATUSES = ["pending", "confirmed", "cancelled", "waitlisted"]
REVIEW_STATUSES = ["pending", "approved", "rejected"]

_COORDINATES = {
    "bsonType": "array",
    "minItems": 2,
    "maxItems": 2,
    "items": {"bsonType": "number"},
}

VALIDATORS: Dict[str, Dict[str, Any]] = {
    "users": {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["email", "status", "version", "created_at"],
            "properties": {
                "email": {"bsonType": "string", "pattern": "^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$"},
                "status": {"enum": USER_STATUSES},
                "version": {"bsonType": "int", "minimum": 1},
                "profile": {
                    "bsonType": "object",
                    "properties": {
                        "name": {"bsonType": "string"},
                        "age": {"bsonType": "int", "minimum": 0, "maximum": 150},
                    },
                },
                "address": {
                    "bsonType": "object",
                    "properties": {"coordinates": _COORDINATES},
                },
                "interests": {"bsonType": "array", "items": {"bsonType": "string"}},
                "created_at": {"bsonType": "date"},
                "updated_at": {"bsonType": "date"},
            },
        }
    },
    "events": {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["title", "category", "status", "schedule", "version", "created_at"],
            "properties": {
                "title": {"bsonType": "string", "minLength": 1},
                "category": {"enum": EVENT_CATEGORIES},
                "status": {"enum": EVENT_STATUSES},
                "tags": {"bsonType": "array", "items": {"bsonType": "string"}},
                "schedule": {
                    "bsonType": "object",
                    "required": ["start_date"],
                    "properties": {
                        "start_date": {"bsonType": "date"},
                        "end_date": {"bsonType": "date"},
                    },
                },
                "location": {
                    "bsonType": "object",
                    "properties": {"coordinates": _COORDINATES},
                },
                "capacity": {
                    "bsonType": "object",
                    "required": ["total", "remaining"],
                    "properties": {
                        "total": {"bsonType": "int", "minimum": 0},
                        "remaining": {"bsonType": "int", "minimum": 0},
                    },
                },
                "participants": {"bsonType": "array", "items": {"bsonType": "objectId"}},
                "version": {"bsonType": "int", "minimum": 1},
                "created_at": {"bsonType": "date"},
            },
        }
    },
    "registrations": {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["user_id", "event_id", "status", "registered_at"],
            "properties": {
                "user_id": {"bsonType": "objectId"},
                "event_id": {"bsonType": "objectId"},
                "status": {"enum": REGISTRATION_STATUSES},
                "registered_at": {"bsonType": "date"},
            },
        }
    },
    "reviews": {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["event_id", "user_id", "rating", "status", "created_at"],
            "properties": {
                "event_id": {"bsonType": "objectId"},
                "user_id": {"bsonType": "objectId"},
                "rating": {"bsonType": "int", "minimum": 1, "maximum": 5},
                "comment": {"bsonType": "string"},
                "status": {"enum": REVIEW_STATUSES},
                "created_at": {"bsonType": "date"},
            },
        }
    },
    "audit_log": {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["action", "resource_type", "timestamp"],
            "properties": {
                "action": {"bsonType": "string"},
                "resource_type": {"bsonType": "string"},
                "timestamp": {"bsonType": "date"},
            },
        }
    },
}


def apply_validators(db: Database) -> None:
    """Створює колекції з валідатором або оновлює валідатор наявних (collMod).

    Можна викликати повторно: collMod просто перезаписує валідатор.
    """
    existing = set(db.list_collection_names())
    for name, validator in VALIDATORS.items():
        if name in existing:
            db.command(
                "collMod", name,
                validator=validator,
                validationLevel="moderate",
                validationAction="error",
            )
        else:
            try:
                db.create_collection(
                    name,
                    validator=validator,
                    validationLevel="moderate",
                    validationAction="error",
                )
            except CollectionInvalid:
                # колекцію щойно створив інший процес — просто оновлюємо валідатор
                db.command("collMod", name, validator=validator,
                           validationLevel="moderate", validationAction="error")
